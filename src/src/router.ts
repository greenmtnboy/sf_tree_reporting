import { createRouter, createWebHashHistory } from 'vue-router'
import MapView from './views/MapView.vue'
import InfoView from './views/InfoView.vue'

export const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: '/', name: 'map', component: MapView },
    { path: '/info', name: 'info', component: InfoView },
  ],
})
