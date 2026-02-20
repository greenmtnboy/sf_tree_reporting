import { ref } from 'vue'

const currentMapQuery = ref<string | null>(null)
const mapQueryRevision = ref(0)

export function useMapData() {
  function publishMapQuery(query: string) {
    currentMapQuery.value = query.trim()
    mapQueryRevision.value += 1
  }

  return { currentMapQuery, mapQueryRevision, publishMapQuery }
}
