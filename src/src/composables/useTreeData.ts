import { ref } from 'vue'
import type { RawTree } from '../types'
import { getTreeCategory } from './useTreeCategories'

export interface TreeGeoJSON {
  type: 'FeatureCollection'
  features: GeoJSON.Feature<GeoJSON.Point>[]
}

export function useTreeData() {
  const geojson = ref<TreeGeoJSON | null>(null)
  const loading = ref(true)
  const error = ref<string | null>(null)

  async function load() {
    try {
      const res = await fetch(import.meta.env.BASE_URL + 'data/raw_data.json')
      if (!res.ok) throw new Error(`Failed to fetch tree data: ${res.status}`)
      const raw: RawTree[] = await res.json()

      const features: GeoJSON.Feature<GeoJSON.Point>[] = raw
        .filter((t) => t.latitude && t.longitude)
        .map((tree) => {
          const { category, color } = getTreeCategory(tree.q_species)
          return {
            type: 'Feature' as const,
            geometry: {
              type: 'Point' as const,
              coordinates: [tree.longitude, tree.latitude],
            },
            properties: {
              id: tree.tree_id,
              commonName: tree.common_name.trim(),
              species: tree.q_species,
              plantDate: tree.plant_date,
              dbh: tree.diameter_at_breast_height ?? 3,
              category,
              color,
            },
          }
        })

      geojson.value = { type: 'FeatureCollection', features }
    } catch (e) {
      error.value = (e as Error).message
    } finally {
      loading.value = false
    }
  }

  load()
  return { geojson, loading, error }
}
