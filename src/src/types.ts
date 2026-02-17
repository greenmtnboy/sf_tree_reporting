export interface RawTree {
  tree_id: number
  common_name: string
  q_site_info: string
  plant_date: string
  q_species: string
  latitude: number
  longitude: number
  diameter_at_breast_height: number | null
}

export type TreeCategory = 'palm' | 'broadleaf' | 'spreading' | 'coniferous' | 'columnar' | 'ornamental' | 'default'

export interface RawLandmark {
  name: string
  latitude: number
  longitude: number
}

export interface Landmark {
  name: string
  lng: number
  lat: number
}

// --- Chat types ---

export interface ChatMessage {
  role: 'user' | 'assistant'
  content: string
  toolCalls?: ToolCallRecord[]
  isLoading?: boolean
}

export interface ToolCallRecord {
  id: string
  name: string
  input: Record<string, unknown>
  result: string
  isError?: boolean
}

// Anthropic API types (subset)

export interface AnthropicMessage {
  role: 'user' | 'assistant'
  content: AnthropicContentBlock[] | string
}

export type AnthropicContentBlock =
  | { type: 'text'; text: string }
  | { type: 'tool_use'; id: string; name: string; input: Record<string, unknown> }
  | { type: 'tool_result'; tool_use_id: string; content: string; is_error?: boolean }

export interface AnthropicTool {
  name: string
  description: string
  input_schema: Record<string, unknown>
}

export interface AnthropicResponse {
  id: string
  content: AnthropicContentBlock[]
  stop_reason: 'end_turn' | 'tool_use' | 'max_tokens'
  usage: { input_tokens: number; output_tokens: number }
}
