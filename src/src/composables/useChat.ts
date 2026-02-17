import { ref, computed } from 'vue'
import type {
  ChatMessage,
  ToolCallRecord,
  AnthropicMessage,
  AnthropicContentBlock,
  AnthropicTool,
  AnthropicResponse,
} from '../types'
import { useDuckDB } from './useDuckDB'
import { useFlyTo } from './useFlyTo'
import { useLandmarkData } from './useLandmarkData'
import { useMapData } from './useMapData'
import { getTreeCategory } from './useTreeCategories'

const API_KEY_STORAGE = 'sf_trees_anthropic_key'
const MAX_LOOPS = 10

const TOOLS: AnthropicTool[] = [
  {
    name: 'run_query',
    description:
      'Execute a SQL query against the trees table in DuckDB. The table "trees" has columns: tree_id (INTEGER), common_name (VARCHAR), q_site_info (VARCHAR), plant_date (VARCHAR), q_species (VARCHAR), latitude (DOUBLE), longitude (DOUBLE), diameter_at_breast_height (DOUBLE). Returns JSON rows. Use this to filter, aggregate, or explore the dataset. Limit results to 500 rows max unless the user specifically needs more.',
    input_schema: {
      type: 'object',
      properties: {
        sql: { type: 'string', description: 'The SQL query to execute' },
      },
      required: ['sql'],
    },
  },
  {
    name: 'publish_results',
    description:
      'Takes a SQL query whose results contain at minimum latitude, longitude, and common_name columns. Executes the query and publishes the results as the new map data source, replacing what is currently displayed. Use this after the user asks to see specific trees on the map (e.g., "show me all palm trees"). Always include all columns in the query so the map popups work correctly.',
    input_schema: {
      type: 'object',
      properties: {
        sql: {
          type: 'string',
          description: 'SQL query returning rows with latitude, longitude, and tree columns',
        },
      },
      required: ['sql'],
    },
  },
  {
    name: 'navigate',
    description: 'Fly the map camera to a specific latitude/longitude coordinate with optional zoom level.',
    input_schema: {
      type: 'object',
      properties: {
        latitude: { type: 'number' },
        longitude: { type: 'number' },
        zoom: { type: 'number', description: 'Zoom level (default 16)' },
      },
      required: ['latitude', 'longitude'],
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

const SYSTEM_PROMPT = `You are an assistant for the SF Trees map application. You help users explore San Francisco's urban forest dataset of approximately 10,000 street trees.

You have access to tools for querying the tree database, displaying query results on the map, navigating the map camera, and looking up SF landmarks.

When users ask about trees, write SQL queries against the "trees" table. When they want to visualize results on the map, use publish_results. When they mention locations, use lookup_landmark to find coordinates, then navigate there.

The trees table has these columns:
- tree_id (INTEGER) - unique identifier
- common_name (VARCHAR) - e.g. "Swamp Myrtle"
- q_site_info (VARCHAR) - planting site info
- plant_date (VARCHAR) - date planted, format "MM/DD/YYYY HH:MM:SS AM"
- q_species (VARCHAR) - full species string like "Tristaniopsis laurina :: Swamp Myrtle"
- latitude (DOUBLE), longitude (DOUBLE) - coordinates
- diameter_at_breast_height (DOUBLE) - trunk diameter in inches

Be concise and helpful. When showing query results, format them nicely.`

// Module-level state (singleton)
const messages = ref<ChatMessage[]>([])
const isLoading = ref(false)
const apiKey = ref(localStorage.getItem(API_KEY_STORAGE) || '')

export function useChat() {
  const { query: duckQuery } = useDuckDB()
  const { flyTo } = useFlyTo()
  const { landmarks } = useLandmarkData()
  const { publishGeoJSON } = useMapData()

  function setApiKey(key: string) {
    apiKey.value = key
    localStorage.setItem(API_KEY_STORAGE, key)
  }

  // Convert our ChatMessage[] into Anthropic API message format
  function buildApiMessages(): AnthropicMessage[] {
    const apiMsgs: AnthropicMessage[] = []

    for (const msg of messages.value) {
      if (msg.role === 'user') {
        apiMsgs.push({ role: 'user', content: msg.content })
      } else if (msg.role === 'assistant' && !msg.isLoading) {
        const blocks: AnthropicContentBlock[] = []
        if (msg.content) {
          blocks.push({ type: 'text', text: msg.content })
        }
        if (msg.toolCalls?.length) {
          for (const tc of msg.toolCalls) {
            blocks.push({ type: 'tool_use', id: tc.id, name: tc.name, input: tc.input })
          }
        }
        if (blocks.length > 0) {
          apiMsgs.push({ role: 'assistant', content: blocks })
        }
        // Add tool results as a user message
        if (msg.toolCalls?.length) {
          const results: AnthropicContentBlock[] = msg.toolCalls.map((tc) => ({
            type: 'tool_result' as const,
            tool_use_id: tc.id,
            content: tc.result,
            is_error: tc.isError,
          }))
          apiMsgs.push({ role: 'user', content: results })
        }
      }
    }

    return apiMsgs
  }

  async function executeTool(
    name: string,
    input: Record<string, unknown>,
  ): Promise<{ result: string; isError: boolean }> {
    try {
      switch (name) {
        case 'run_query': {
          const { sql } = input as { sql: string }
          const { columns, rows } = await duckQuery(sql)
          const truncated = rows.slice(0, 100)
          return {
            result: JSON.stringify({ columns, rows: truncated, totalRows: rows.length }),
            isError: false,
          }
        }
        case 'publish_results': {
          const { sql } = input as { sql: string }
          const { rows } = await duckQuery(sql)
          const features = rows
            .filter((r: any) => r.latitude && r.longitude)
            .map((r: any) => {
              const { category, color } = getTreeCategory(r.q_species || '')
              return {
                type: 'Feature' as const,
                geometry: {
                  type: 'Point' as const,
                  coordinates: [r.longitude, r.latitude],
                },
                properties: {
                  id: r.tree_id,
                  commonName: (r.common_name || '').trim(),
                  species: r.q_species || '',
                  plantDate: r.plant_date || '',
                  dbh: r.diameter_at_breast_height ?? 3,
                  category,
                  color,
                },
              }
            })
          publishGeoJSON({ type: 'FeatureCollection', features })
          return { result: `Published ${features.length} trees to the map.`, isError: false }
        }
        case 'navigate': {
          const { latitude, longitude, zoom } = input as {
            latitude: number
            longitude: number
            zoom?: number
          }
          flyTo({ lat: latitude, lng: longitude, zoom: zoom ?? 16 })
          return { result: `Navigating to [${latitude.toFixed(4)}, ${longitude.toFixed(4)}]`, isError: false }
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
      return { result: `Error: ${(e as Error).message}`, isError: true }
    }
  }

  async function callAnthropic(msgs: AnthropicMessage[]): Promise<AnthropicResponse> {
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey.value,
        'anthropic-version': '2023-06-01',
        'anthropic-dangerous-direct-browser-access': 'true',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-5-20250929',
        max_tokens: 4096,
        system: SYSTEM_PROMPT,
        tools: TOOLS,
        messages: msgs,
      }),
    })
    if (!res.ok) {
      const err = await res.text()
      throw new Error(`API error ${res.status}: ${err}`)
    }
    return res.json()
  }

  async function sendMessage(userText: string) {
    // Add user message
    messages.value.push({ role: 'user', content: userText })
    // Add placeholder assistant message
    const assistantMsg: ChatMessage = { role: 'assistant', content: '', isLoading: true }
    messages.value.push(assistantMsg)
    isLoading.value = true

    try {
      // Build API messages from history (excludes the current loading message)
      const apiMsgs = buildApiMessages()
      // The current user message is already in apiMsgs from buildApiMessages
      let loopCount = 0

      while (loopCount < MAX_LOOPS) {
        loopCount++
        const response = await callAnthropic(apiMsgs)

        const toolUseBlocks = response.content.filter((b) => b.type === 'tool_use') as Array<{
          type: 'tool_use'
          id: string
          name: string
          input: Record<string, unknown>
        }>
        const textBlocks = response.content.filter((b) => b.type === 'text') as Array<{
          type: 'text'
          text: string
        }>
        const textContent = textBlocks.map((b) => b.text).join('')

        if (toolUseBlocks.length === 0) {
          // Final text response
          assistantMsg.content = textContent
          assistantMsg.isLoading = false
          break
        }

        // Execute tools
        const toolCalls: ToolCallRecord[] = []
        const toolResults: AnthropicContentBlock[] = []
        for (const tb of toolUseBlocks) {
          const { result, isError } = await executeTool(tb.name, tb.input)
          toolCalls.push({ id: tb.id, name: tb.name, input: tb.input, result, isError })
          toolResults.push({
            type: 'tool_result',
            tool_use_id: tb.id,
            content: result,
            is_error: isError,
          })
        }

        // Update the UI assistant message with intermediate results
        assistantMsg.content = textContent
        assistantMsg.toolCalls = [...(assistantMsg.toolCalls || []), ...toolCalls]

        // Append to the API conversation so the LLM sees its tool calls + results
        apiMsgs.push({ role: 'assistant', content: response.content })
        apiMsgs.push({ role: 'user', content: toolResults })

        // If this was the last loop, mark as done
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
