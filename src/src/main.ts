import { createApp } from 'vue'
import { createPinia } from 'pinia'
import App from './App.vue'
import { router } from './router'
import 'maplibre-gl/dist/maplibre-gl.css'
import './assets/main.css'

createApp(App).use(createPinia()).use(router).mount('#app')
