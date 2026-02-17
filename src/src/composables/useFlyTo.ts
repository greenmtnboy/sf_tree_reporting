import { ref } from 'vue'

export interface FlyToTarget {
  lng: number
  lat: number
  zoom?: number
  label?: string
}

const target = ref<FlyToTarget | null>(null)
let counter = 0

export function useFlyTo() {
  function flyTo(t: FlyToTarget) {
    counter++
    target.value = { ...t }
  }

  return { target, flyTo, counter: () => counter }
}
