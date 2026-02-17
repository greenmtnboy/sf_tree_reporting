import { ref } from 'vue'
import type { RawLandmark, Landmark } from '../types'

export function useLandmarkData() {
  const landmarks = ref<Landmark[]>([])
  const loading = ref(true)
  const error = ref<string | null>(null)

  async function load() {
    try {
      const res = await fetch(import.meta.env.BASE_URL + 'data/landmark_data.json')
      if (!res.ok) throw new Error(`Failed to fetch landmark data: ${res.status}`)
      const raw: RawLandmark[] = await res.json()

      landmarks.value = raw
        .filter((l) => l.name && l.latitude != null && l.longitude != null)
        .map((l) => ({
          name: l.name.trim(),
          lng: l.longitude,
          lat: l.latitude,
        }))
        .sort((a, b) => a.name.localeCompare(b.name))
    } catch (e) {
      error.value = (e as Error).message
    } finally {
      loading.value = false
    }
  }

  load()
  return { landmarks, loading, error }
}
