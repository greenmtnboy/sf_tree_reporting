import { ref } from 'vue'

const currentMapQuery = ref<string | null>(null)
const publishedTreeIdFilterSql = ref<string | null>(null)
const mapQueryRevision = ref(0)

export function useMapData() {
  function publishMapQuery(query: string) {
    currentMapQuery.value = query.trim()
    mapQueryRevision.value += 1
  }

  function publishMapTreeIdFilterSql(sql: string) {
    publishedTreeIdFilterSql.value = sql.trim()
    mapQueryRevision.value += 1
  }

  function clearMapTreeIdFilter() {
    publishedTreeIdFilterSql.value = null
    mapQueryRevision.value += 1
  }

  return {
    currentMapQuery,
    publishedTreeIdFilterSql,
    mapQueryRevision,
    publishMapQuery,
    publishMapTreeIdFilterSql,
    clearMapTreeIdFilter,
  }
}
