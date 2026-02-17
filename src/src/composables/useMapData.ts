import { ref } from 'vue'
import type { TreeGeoJSON } from './useTreeData'

const currentGeoJSON = ref<TreeGeoJSON | null>(null)

export function useMapData() {
  function publishGeoJSON(data: TreeGeoJSON) {
    currentGeoJSON.value = data
  }

  return { currentGeoJSON, publishGeoJSON }
}
