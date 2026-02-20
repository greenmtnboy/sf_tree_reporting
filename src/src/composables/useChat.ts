import { ref, computed } from 'vue'
import type { ChatMessage, ToolCallRecord } from '../types'
import { useTrilogyCore, buildCustomTrilogyPrompt } from '@trilogy-data/trilogy-studio-components'
import { useDuckDB } from './useDuckDB'
import { useFlyTo } from './useFlyTo'
import { useLandmarkData } from './useLandmarkData'
import { useMapData } from './useMapData'
import { getTreeCategory } from './useTreeCategories'
import { jitterCoord, randomRotation, randomSizeScale } from './useTreeData'
import TREES_MODEL from '../../../data/raw/tree_info.preql?raw'

const API_KEY_STORAGE = 'sf_trees_anthropic_key'
const MAX_LOOPS = 10
const TRILOGY_RESOLVER_URL = 'https://trilogy-service.fly.dev'
const LLM_CONNECTION = 'sf-trees'
const MODEL = 'claude-sonnet-4-6'

const TREES_MODEL_SOURCE = { alias: 'trees', contents: TREES_MODEL }

// Matches the library's LLMMessage shape for multi-turn history
interface HistoryMsg {
  role: 'user' | 'assistant'
  content: string
  toolCalls?: Array<{ id: string; name: string; input: Record<string, unknown> }>
  toolResults?: Array<{ toolCallId: string; toolName: string; result: string }>
}

const ALL_CATEGORIES = new Set(['palm', 'broadleaf', 'spreading', 'coniferous', 'columnar', 'ornamental', 'default'])
let speciesCategoryLookupPromise: Promise<Map<string, string>> | null = null

function normalizeSpecies(species: string): string {
  return species.trim().toLowerCase()
}

function loadSpeciesCategoryLookup(): Promise<Map<string, string>> {
  if (speciesCategoryLookupPromise) return speciesCategoryLookupPromise
  speciesCategoryLookupPromise = fetch(import.meta.env.BASE_URL + 'data/species_data.json')
    .then(async (res) => {
      if (!res.ok) return new Map<string, string>()
      const rows = await res.json() as Array<{ species?: string; tree_category?: string | null }>
      const map = new Map<string, string>()
      for (const row of rows) {
        const species = row.species?.trim()
        const category = row.tree_category?.trim()
        if (!species || !category || !ALL_CATEGORIES.has(category)) continue
        map.set(normalizeSpecies(species), category)
      }
      return map
    })
    .catch(() => new Map<string, string>())
  return speciesCategoryLookupPromise
}

const TOOLS = [
  {
    name: 'run_query',
    description:
      'Execute a Trilogy/PreQL query against the trees dataset. Write a SELECT statement using the available concepts — no FROM clause needed; Trilogy resolves the source automatically. Returns JSON rows. Use this to filter, aggregate, or explore the dataset. Limit results to 500 rows max unless the user specifically needs more.',
    input_schema: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'The Trilogy/PreQL SELECT statement to execute' },
      },
      required: ['query'],
    },
  },
  {
    name: 'publish_results',
    description:
      'Takes a Trilogy/PreQL SELECT query whose results contain at minimum latitude, longitude, and common_name concepts. Compiles and executes the query, then publishes the results as the new map data source, replacing what is currently displayed. Use this after the user asks to see specific trees on the map (e.g., "show me all palm trees"). Always include all relevant concepts so the map popups work correctly.',
    input_schema: {
      type: 'object',
      properties: {
        query: {
          type: 'string',
          description: 'Trilogy/PreQL SELECT returning rows with latitude, longitude, and tree concepts',
        },
      },
      required: ['query'],
    },
  },
  {
    name: 'navigate',
    description:
      'Fly the map camera to one or more locations. For a single location, provide latitude and longitude. For a tour of multiple locations, provide a "locations" array — the camera will visit each in sequence with a brief pause between stops.',
    input_schema: {
      type: 'object',
      properties: {
        latitude: { type: 'number', description: 'Latitude for a single location' },
        longitude: { type: 'number', description: 'Longitude for a single location' },
        zoom: { type: 'number', description: 'Zoom level (default 16)' },
        locations: {
          type: 'array',
          description: 'Array of locations to tour in sequence',
          items: {
            type: 'object',
            properties: {
              latitude: { type: 'number' },
              longitude: { type: 'number' },
              zoom: { type: 'number' },
            },
            required: ['latitude', 'longitude'],
          },
        },
      },
    },
  },
  {
    name: 'lookup_landmark',
    description:
      'Search the SF landmarks dataset by name (fuzzy match). Returns the landmark name, latitude, and longitude. Use this when the user mentions a place name in SF to find its coordinates.',
    input_schema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Landmark name to search for (partial match)' },
      },
      required: ['name'],
    },
  },
]

const SYSTEM_PROMPT = buildCustomTrilogyPrompt(
  ({ rulesInput, aggFunctions, functions, datatypes }) => `You are an assistant for the SF Trees map application. You help users explore San Francisco's urban forest dataset of approximately 10,000 street trees.

You have access to tools for querying the tree dataset, displaying query results on the map, navigating the map camera, and looking up SF landmarks.

When users ask about trees, write Trilogy/PreQL SELECT queries using the available concepts. When they want to visualize results on the map, use publish_results. When they mention locations, use lookup_landmark to find coordinates, then navigate there.

AVAILABLE CONCEPTS:
- tree_id (int) — unique identifier
- common_name (string) — e.g. "Swamp Myrtle"
- site_info (string) — planting site info (e.g. "Sidewalk: Curb side")
- plant_date (string) — date planted, format "MM/DD/YYYY HH:MM:SS AM"
- species (string) — full species string like "Tristaniopsis laurina :: Swamp Myrtle"
- latitude (float) — geographic latitude
- longitude (float) — geographic longitude
- diameter_at_breast_height (float) — trunk diameter in inches

TRILOGY SYNTAX RULES:
${rulesInput}

AGGREGATE FUNCTIONS: ${aggFunctions.join(', ')}

COMMON FUNCTIONS: ${functions.join(', ')}

VALID DATA TYPES: ${datatypes.join(', ')}

IMPORTANT GUIDELINES:
1. Always use a reasonable LIMIT (e.g., 100–500) unless the user asks for all data
2. When publishing to the map, always include tree_id, common_name, species, latitude, longitude, diameter_at_breast_height, plant_date, and site_info so popups have full data
3. If a query fails, explain the error and try a corrected version

Be concise and helpful. When showing query results, format them nicely.`,
)

// Module-level state (singleton)
const messages = ref<ChatMessage[]>([])
const isLoading = ref(false)
const apiKey = ref(localStorage.getItem(API_KEY_STORAGE) || '')

export function useChat() {
  const { query: duckQuery } = useDuckDB()
  const { flyTo } = useFlyTo()
  const { landmarks } = useLandmarkData()
  const { publishGeoJSON } = useMapData()
  const trilogy = useTrilogyCore()

  // Ensure the Trilogy resolver points at the production service
  if (!trilogy.userSettingsStore.settings.trilogyResolver ||
      trilogy.userSettingsStore.settings.trilogyResolver.includes('localhost')) {
    trilogy.userSettingsStore.updateSetting('trilogyResolver', TRILOGY_RESOLVER_URL)
  }

  // Register (or update the key on) the Anthropic LLM connection
  function ensureConnection() {
    const existing = trilogy.llmConnectionStore.connections[LLM_CONNECTION]
    if (!existing) {
      trilogy.llmConnectionStore.newConnection(LLM_CONNECTION, 'anthropic', {
        apiKey: apiKey.value,
        model: MODEL,
        saveCredential: false,
      })
    } else {
      existing.setApiKey(apiKey.value)
    }
  }

  async function compilePreQL(query: string): Promise<string> {
    const response = await trilogy.resolver.resolve_query(
      query,
      'duckdb',
      'preql',
      [TREES_MODEL_SOURCE],
      [{name: TREES_MODEL_SOURCE.alias, alias: ''}]
    )
    if (response.data.error) {
      throw new Error(`Trilogy compile error: ${response.data.error}`)
    }
    return response.data.generated_sql
  }

  function setApiKey(key: string) {
    apiKey.value = key
    localStorage.setItem(API_KEY_STORAGE, key)
    ensureConnection()
  }

  // Convert UI ChatMessage[] to the library's LLMMessage-compatible history format
  function buildHistory(): HistoryMsg[] {
    const result: HistoryMsg[] = []
    for (const msg of messages.value) {
      if (msg.isLoading) continue
      if (msg.role === 'user') {
        result.push({ role: 'user', content: msg.content })
      } else {
        result.push({
          role: 'assistant',
          content: msg.content,
          ...(msg.toolCalls?.length && {
            toolCalls: msg.toolCalls.map((tc) => ({ id: tc.id, name: tc.name, input: tc.input })),
          }),
        })
        if (msg.toolCalls?.length) {
          result.push({
            role: 'user',
            content: '',
            toolResults: msg.toolCalls.map((tc) => ({
              toolCallId: tc.id,
              toolName: tc.name,
              result: tc.result,
            })),
          })
        }
      }
    }
    return result
  }

  async function executeTool(
    name: string,
    input: Record<string, unknown>,
  ): Promise<{ result: string; isError: boolean }> {
    try {
      switch (name) {
        case 'run_query': {
          const { query } = input as { query: string }
          const sql = await compilePreQL(query)
          const { columns, rows } = await duckQuery(sql)
          const truncated = rows.slice(0, 100)
          return {
            result: JSON.stringify({ columns, rows: truncated, totalRows: rows.length }),
            isError: false,
          }
        }
        case 'publish_results': {
          const { query } = input as { query: string }
          const sql = await compilePreQL(query)
          const { rows } = await duckQuery(sql)
          const speciesCategoryLookup = await loadSpeciesCategoryLookup()
          const validRows = rows.filter((r: any) => r.latitude && r.longitude)
          // Detect co-located trees
          const locCounts = new Map<string, number>()
          for (const r of validRows) {
            const key = `${(r as any).latitude},${(r as any).longitude}`
            locCounts.set(key, (locCounts.get(key) || 0) + 1)
          }
          const features = validRows
            .map((r: any) => {
              const fallback = getTreeCategory(r.species || '')
              const category = speciesCategoryLookup.get(normalizeSpecies(r.species || '')) ?? fallback.category
              const color = fallback.color
              const key = `${r.latitude},${r.longitude}`
              const isCoLocated = (locCounts.get(key) || 0) > 1
              return {
                type: 'Feature' as const,
                geometry: {
                  type: 'Point' as const,
                  coordinates: isCoLocated
                    ? [jitterCoord(r.longitude), jitterCoord(r.latitude)]
                    : [r.longitude, r.latitude],
                },
                properties: {
                  id: r.tree_id,
                  commonName: (r.common_name || '').trim(),
                  species: r.species || '',
                  plantDate: r.plant_date || '',
                  dbh: r.diameter_at_breast_height ?? 3,
                  category,
                  color,
                  rotation: isCoLocated ? randomRotation() : 0,
                  sizeScale: randomSizeScale(),
                },
              }
            })
          publishGeoJSON({ type: 'FeatureCollection', features })
          return { result: `Published ${features.length} trees to the map.`, isError: false }
        }
        case 'navigate': {
          const { latitude, longitude, zoom, locations } = input as {
            latitude?: number
            longitude?: number
            zoom?: number
            locations?: Array<{ latitude: number; longitude: number; zoom?: number }>
          }
          if (locations && locations.length > 0) {
            const stops = locations.map((l) => ({
              lat: l.latitude,
              lng: l.longitude,
              zoom: l.zoom ?? zoom ?? 16,
            }))
            flyTo(stops[0])
            for (let i = 1; i < stops.length; i++) {
              const stop = stops[i]
              setTimeout(() => flyTo(stop), i * 6000)
            }
            return {
              result: `Touring ${stops.length} locations (3s between each stop).`,
              isError: false,
            }
          }
          if (latitude != null && longitude != null) {
            flyTo({ lat: latitude, lng: longitude, zoom: zoom ?? 16 })
            return { result: `Navigating to [${latitude.toFixed(4)}, ${longitude.toFixed(4)}]`, isError: false }
          }
          return { result: 'Must provide either latitude/longitude or a locations array.', isError: true }
        }
        case 'lookup_landmark': {
          const { name } = input as { name: string }
          const q = name.toLowerCase()
          const matches = landmarks.value.filter((l) => l.name.toLowerCase().includes(q))
          if (matches.length === 0) return { result: 'No landmarks found matching that name.', isError: false }
          return { result: JSON.stringify(matches.slice(0, 5)), isError: false }
        }
        default:
          return { result: `Unknown tool: ${name}`, isError: true }
      }
    } catch (e) {
      const err = e as Error
      console.error('[Tool Error]', {
        tool: name,
        input,
        message: err.message,
        stack: err.stack,
      })
      return { result: `Error: ${err.message}`, isError: true }
    }
  }

  async function sendMessage(userText: string) {
    messages.value.push({ role: 'user', content: userText })
    const assistantMsg: ChatMessage = { role: 'assistant', content: '', isLoading: true }
    messages.value.push(assistantMsg)
    isLoading.value = true

    try {
      ensureConnection()
      // buildHistory() includes all settled turns, ending with the current user message
      let loopHistory = buildHistory()
      let loopCount = 0

      while (loopCount < MAX_LOOPS) {
        loopCount++
        // prompt is empty — full conversation lives in loopHistory
        const response = await trilogy.llmConnectionStore.generateCompletion(
          LLM_CONNECTION,
          { prompt: '', systemPrompt: SYSTEM_PROMPT, tools: TOOLS, maxTokens: 4096 },
          loopHistory as any,
        )

        if (!response.toolCalls?.length) {
          assistantMsg.content = response.text
          assistantMsg.isLoading = false
          break
        }

        // Execute tools
        const toolCalls: ToolCallRecord[] = []
        for (const tc of response.toolCalls) {
          const { result, isError } = await executeTool(tc.name, tc.input)
          if (isError) {
            console.error('[Tool Error Result]', {
              tool: tc.name,
              input: tc.input,
              result,
            })
          }
          toolCalls.push({ id: tc.id, name: tc.name, input: tc.input, result, isError })
        }

        // Update UI with intermediate state
        assistantMsg.content = response.text
        assistantMsg.toolCalls = [...(assistantMsg.toolCalls || []), ...toolCalls]

        // Extend history so the LLM sees its tool calls and results
        loopHistory = [
          ...loopHistory,
          { role: 'assistant', content: response.text, toolCalls: response.toolCalls },
          {
            role: 'user',
            content: '',
            toolResults: toolCalls.map((tc) => ({
              toolCallId: tc.id,
              toolName: tc.name,
              result: tc.result,
            })),
          },
        ]

        if (loopCount >= MAX_LOOPS) {
          assistantMsg.isLoading = false
        }
      }
    } catch (e) {
      assistantMsg.content = `Error: ${(e as Error).message}`
      assistantMsg.isLoading = false
    } finally {
      isLoading.value = false
      assistantMsg.isLoading = false
    }
  }

  function clearMessages() {
    messages.value = []
  }

  return {
    messages,
    isLoading,
    apiKey: computed(() => apiKey.value),
    hasApiKey: computed(() => !!apiKey.value),
    setApiKey,
    sendMessage,
    clearMessages,
  }
}
