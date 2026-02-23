<template>
  <!-- Chat panel -->
  <aside class="chat-panel">
    <div class="chat-header">
      <span>Tree Assistant</span>
      <div class="chat-header-actions">
        <button v-if="hasApiKey && messages.length" class="chat-clear-btn" @click="clearMessages" title="Clear chat">
          Clear
        </button>
      </div>
    </div>

    <!-- API Key setup -->
    <div v-if="!hasApiKey" class="chat-api-key">
      <p>Enter your Anthropic API key to start chatting:</p>
      <input v-model="keyInput" type="password" placeholder="sk-ant-..." @keydown.enter="saveKey" />
      <button @click="saveKey">Save</button>
    </div>

    <!-- Messages -->
    <div v-else class="chat-messages" ref="messagesContainer">
      <div v-if="messages.length === 0" class="chat-empty">
        Ask me about SF trees! Try:<br /><br />
        "How many palm trees are there?"<br />
        "Show me trees near Coit Tower"<br />
        "What are the oldest trees?"
      </div>
      <div v-for="(msg, i) in messages" :key="i" :class="['chat-msg', `chat-msg--${msg.role}`]">
        <div class="chat-msg-content">
          <div v-if="msg.isLoading" class="chat-loading">Thinking...</div>
          <template v-else>
            <MarkdownRenderer v-if="msg.content" :markdown="msg.content" />
            <div v-if="msg.toolCalls?.length" class="chat-tool-calls">
              <div v-for="tc in msg.toolCalls" :key="tc.id" class="chat-tool-call">
                <span class="tool-name">{{ tc.name }}</span>
                <span v-if="tc.input.sql" class="tool-sql">{{ tc.input.sql }}</span>
              </div>
            </div>
          </template>
        </div>
      </div>
    </div>

    <!-- Input -->
    <div v-if="hasApiKey" class="chat-input-area">
      <input
        v-model="userInput"
        type="text"
        placeholder="Ask about SF trees..."
        @keydown.enter="handleSend"
        :disabled="isLoading"
      />
      <button @click="handleSend" :disabled="isLoading || !userInput.trim()">Send</button>
    </div>
  </aside>
</template>

<script setup lang="ts">
import { ref, nextTick, watch } from 'vue'
import { MarkdownRenderer } from '@trilogy-data/trilogy-studio-components'
import { useChat } from '../composables/useChat'

const { messages, isLoading, hasApiKey, setApiKey, sendMessage, clearMessages } = useChat()

const userInput = ref('')
const keyInput = ref('')
const messagesContainer = ref<HTMLDivElement>()

function saveKey() {
  if (keyInput.value.trim()) {
    setApiKey(keyInput.value.trim())
    keyInput.value = ''
  }
}

async function handleSend() {
  const text = userInput.value.trim()
  if (!text || isLoading.value) return
  userInput.value = ''
  await sendMessage(text)
}

async function scrollToBottom() {
  await nextTick()
  if (messagesContainer.value) {
    messagesContainer.value.scrollTop = messagesContainer.value.scrollHeight
  }
}

watch(() => messages.value.length, scrollToBottom)
watch(
  () => messages.value[messages.value.length - 1]?.content,
  scrollToBottom,
)
</script>

<style scoped>
.chat-panel {
  width: 360px;
  min-width: 360px;
  height: 100%;
  background: #16213e;
  border-left: 1px solid #0f3460;
  display: flex;
  flex-direction: column;
  z-index: 15;
}

.chat-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px 16px;
  border-bottom: 1px solid #0f3460;
  font-weight: 600;
  color: #e0e0e0;
  font-size: 0.9rem;
}

.chat-header-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.chat-clear-btn {
  background: none;
  border: none;
  color: #7a7a9e;
  font-size: 0.75rem;
  cursor: pointer;
  padding: 2px 6px;
}
.chat-clear-btn:hover {
  color: #e0e0e0;
}

.chat-messages {
  flex: 1;
  overflow-y: auto;
  padding: 12px;
}

.chat-empty {
  color: #7a7a9e;
  font-size: 0.8rem;
  padding: 20px 0;
  line-height: 1.6;
}

.chat-msg {
  margin-bottom: 12px;
}

.chat-msg--user .chat-msg-content {
  background: #0f3460;
  color: #e0e0e0;
  border-radius: 12px 12px 4px 12px;
  padding: 8px 12px;
  margin-left: 40px;
  font-size: 0.85rem;
  line-height: 1.5;
}

.chat-msg--assistant .chat-msg-content {
  background: #1a1a2e;
  color: #e0e0e0;
  border-radius: 12px 12px 12px 4px;
  padding: 8px 12px;
  margin-right: 20px;
  font-size: 0.85rem;
  line-height: 1.5;
}

.chat-msg-content :deep(pre) {
  background: rgba(0, 0, 0, 0.3);
  border-radius: 4px;
  padding: 6px 8px;
  margin: 4px 0;
  overflow-x: auto;
  font-size: 0.75rem;
}

.chat-msg-content :deep(code) {
  background: rgba(0, 0, 0, 0.2);
  padding: 1px 4px;
  border-radius: 3px;
  font-size: 0.8rem;
}

.chat-msg-content :deep(pre code) {
  background: none;
  padding: 0;
}

.chat-tool-calls {
  margin-top: 6px;
  padding-top: 6px;
  border-top: 1px solid rgba(79, 195, 247, 0.15);
}

.chat-tool-call {
  font-size: 0.7rem;
  color: #7a7a9e;
  margin-bottom: 4px;
}

.tool-name {
  color: #4fc3f7;
  font-weight: 500;
}

.tool-sql {
  display: block;
  font-family: monospace;
  font-size: 0.65rem;
  color: #666;
  margin-top: 2px;
  white-space: pre-wrap;
  word-break: break-all;
}

.chat-input-area {
  display: flex;
  gap: 8px;
  padding: 12px;
  border-top: 1px solid #0f3460;
}

.chat-input-area input {
  flex: 1;
  padding: 8px 12px;
  border: 1px solid #0f3460;
  border-radius: 6px;
  background: #1a1a2e;
  color: #e0e0e0;
  font-size: 0.85rem;
  outline: none;
}
.chat-input-area input:focus {
  border-color: #4fc3f7;
}
.chat-input-area input:disabled {
  opacity: 0.5;
}

.chat-input-area button {
  padding: 8px 16px;
  background: #0f3460;
  color: #4fc3f7;
  border: 1px solid #4fc3f7;
  border-radius: 6px;
  cursor: pointer;
  font-size: 0.85rem;
  transition: background 0.15s;
}
.chat-input-area button:hover:not(:disabled) {
  background: #16213e;
}
.chat-input-area button:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.chat-loading {
  color: #4fc3f7;
  font-style: italic;
  font-size: 0.85rem;
}

.chat-api-key {
  padding: 20px 16px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}
.chat-api-key p {
  font-size: 0.8rem;
  color: #a0a0c0;
  line-height: 1.5;
}
.chat-api-key input {
  padding: 8px 12px;
  border: 1px solid #0f3460;
  border-radius: 6px;
  background: #1a1a2e;
  color: #e0e0e0;
  font-size: 0.85rem;
  outline: none;
}
.chat-api-key input:focus {
  border-color: #4fc3f7;
}
.chat-api-key button {
  padding: 8px 12px;
  background: #0f3460;
  color: #4fc3f7;
  border: 1px solid #4fc3f7;
  border-radius: 6px;
  cursor: pointer;
  font-size: 0.85rem;
}
</style>
