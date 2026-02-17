<template>
  <AppSidebar />
  <main class="main-content">
    <router-view />
  </main>
  <ChatPanel />
</template>

<script setup lang="ts">
import AppSidebar from './components/AppSidebar.vue'
import ChatPanel from './components/ChatPanel.vue'
</script>
