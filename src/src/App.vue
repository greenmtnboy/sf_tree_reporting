<template>
  <AppSidebar />
  <main class="main-content">
    <router-view />
  </main>
</template>

<script setup lang="ts">
import AppSidebar from './components/AppSidebar.vue'
</script>
