import { createRouter, createWebHashHistory } from 'vue-router'
import MapView from './views/MapView.vue'

export const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: '/', name: 'map', component: MapView },
  ],
})
