<template>
  <div ref="mapContainer" class="tree-map"></div>
  <div class="zoom-indicator">Zoom: {{ zoomLevel.toFixed(2) }}</div>
  <div v-if="isInitialLoading" class="map-loading">Loading tree data...</div>
  <div v-if="displayError" class="map-error">{{ displayError }}</div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch, computed } from 'vue'
import maplibregl from 'maplibre-gl'
import { useTreeData } from '../composables/useTreeData'
import { registerTreeIcons, CATEGORY_COLORS } from '../composables/useTreeCategories'
import { useFlyTo } from '../composables/useFlyTo'
import { useMapData } from '../composables/useMapData'
import { useDuckDB } from '../composables/useDuckDB'
import type { TreeCategory } from '../types'

const mapContainer = ref<HTMLDivElement>()
const zoomLevel = ref(13)
const mapError = ref<string | null>(null)
const defaultQueryLoading = ref(true)
let map: maplibregl.Map | null = null
let mapInitStartedAt = 0
let mapDataPublishedAt = 0
let firstTreesSourceLoadedLogged = false
let firstMapIdleAfterPublishLogged = false

const { query: duckQuery } = useDuckDB()
const { categoryIcons, loading, error, buildTreeGeoJSON, getSpeciesEnrichment } = useTreeData()
const { target: flyToTarget } = useFlyTo()
const { currentGeoJSON, publishGeoJSON } = useMapData()
const displayError = computed(() => error.value ?? mapError.value)
const isInitialLoading = computed(() => loading.value || defaultQueryLoading.value)

const DEFAULT_MAP_QUERY = `
SELECT
  tree_id,
  species,
  latitude,
  longitude,
  diameter_at_breast_height
FROM trees
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
`

function nowMs(): number {
  return typeof performance !== 'undefined' ? performance.now() : Date.now()
}

function buildColorExpression(): maplibregl.ExpressionSpecification {
  const entries = Object.entries(CATEGORY_COLORS)
  const expr: any[] = ['match', ['get', 'category']]
  for (const [cat, color] of entries) {
    expr.push(cat, color)
  }
  expr.push('#66BB6A') // fallback
  return expr as maplibregl.ExpressionSpecification
}

function buildIconExpression(): maplibregl.ExpressionSpecification {
  const categories: TreeCategory[] = ['palm', 'broadleaf', 'spreading', 'coniferous', 'columnar', 'ornamental', 'default']
  const expr: any[] = ['match', ['get', 'category']]
  for (const cat of categories) {
    expr.push(cat, `tree-${cat}`)
  }
  expr.push('tree-default') // fallback
  return expr as maplibregl.ExpressionSpecification
}

function formatPopupHtml(row: any, enrichment?: ReturnType<typeof getSpeciesEnrichment>): string {
  const planted = row.plant_date?.split(' ')[0] ?? null
  const evergreen = enrichment?.is_evergreen == null ? null : (enrichment.is_evergreen ? 'Yes' : 'No')
  const detailLines = [
    ['ID', row.tree_id],
    ['Planted', planted],
    ['DBH', row.diameter_at_breast_height != null ? `${row.diameter_at_breast_height}\"` : null],
    ['Site', row.site_info],
    ['Native', enrichment?.native_status],
    ['Evergreen', evergreen],
    ['Mature height', enrichment?.mature_height_ft != null ? `${enrichment.mature_height_ft} ft` : null],
    ['Bloom', enrichment?.bloom_season],
    ['Wildlife value', enrichment?.wildlife_value],
    ['Fire risk', enrichment?.fire_risk],
  ]
    .filter(([, value]) => value != null && value !== '' && value !== 'Unknown')
    .map(([label, value]) => `${label}: ${value}<br/>`)
    .join('')

  return `
    <strong>${row.common_name || 'Unknown tree'}</strong><br/>
    <em>${row.species || ''}</em><br/>
    ${detailLines}
  `
}

async function loadDefaultMapData() {
  const t0 = nowMs()
  try {
    console.info('[Perf] map:default-query:start')
    const { rows } = await duckQuery(DEFAULT_MAP_QUERY)
    const t1 = nowMs()
    console.info('[Perf] map:default-query:done', { ms: Math.round(t1 - t0), rows: rows.length })
    const geojson = buildTreeGeoJSON(rows as any)
    const t2 = nowMs()
    console.info('[Perf] map:geojson:built', { ms: Math.round(t2 - t1), features: geojson.features.length })
    mapDataPublishedAt = nowMs()
    firstTreesSourceLoadedLogged = false
    firstMapIdleAfterPublishLogged = false
    publishGeoJSON(geojson)
    console.info('[Perf] map:publish:done', { ms: Math.round(nowMs() - t2) })
  } catch (e) {
    const err = e as Error
    mapError.value = err.message
    console.error('[Map Init Error]', err)
  } finally {
    defaultQueryLoading.value = false
  }
}

async function showTreePopup(feature: GeoJSON.Feature, offset: number) {
  if (!map) return
  const coords = (feature.geometry as GeoJSON.Point).coordinates.slice() as [number, number]
  const id = Number(feature.properties?.id)
  if (!Number.isFinite(id)) return

  try {
    const { rows } = await duckQuery(`
      SELECT tree_id, common_name, species, plant_date, site_info, diameter_at_breast_height
      FROM trees
      WHERE tree_id = ${id}
      LIMIT 1
    `)
    const row = rows[0] as any
    if (!row) return
    const enrichment = getSpeciesEnrichment(row.species)

    new maplibregl.Popup({ offset, className: 'tree-popup' })
      .setLngLat(coords)
      .setHTML(formatPopupHtml(row, enrichment))
      .addTo(map)
  } catch (e) {
    console.error('[Popup Query Error]', e)
  }
}

function addTreeLayers() {
  if (!map || !currentGeoJSON.value) return
  const t0 = nowMs()
  console.info('[Perf] map:layers:add:start')

  map.addSource('trees', {
    type: 'geojson',
    data: currentGeoJSON.value,
  })

  // Layer 1: Heatmap at far zoom
  map.addLayer({
    id: 'trees-heat',
    type: 'heatmap',
    source: 'trees',
    maxzoom: 15,
    paint: {
      'heatmap-weight': ['interpolate', ['linear'], ['get', 'dbh'], 0, 0.1, 30, 1],
      'heatmap-intensity': ['interpolate', ['linear'], ['zoom'], 10, 0.5, 15, 1.5],
      'heatmap-radius': ['interpolate', ['linear'], ['zoom'], 10, 8, 15, 20],
      'heatmap-color': [
        'interpolate',
        ['linear'],
        ['heatmap-density'],
        0, 'rgba(0,0,0,0)',
        0.2, '#1a237e',
        0.4, '#0f3460',
        0.6, '#2E7D32',
        0.8, '#4CAF50',
        1, '#8BC34A',
      ],
      'heatmap-opacity': ['interpolate', ['linear'], ['zoom'], 13, 0.9, 15, 0],
    },
  })

  // Layer 2: Colored circles at medium zoom
  map.addLayer({
    id: 'trees-circle',
    type: 'circle',
    source: 'trees',
    minzoom: 13.1,
    maxzoom: 16,
    paint: {
      'circle-radius': [
        'interpolate', ['linear'], ['zoom'],
        13.1, 2,
        16, ['interpolate', ['linear'], ['get', 'dbh'], 0, 3, 30, 8],
      ],
      'circle-color': buildColorExpression(),
      'circle-opacity': [
        'interpolate', ['linear'], ['zoom'],
        13.1, 0,
        14, 0.8,
        15.5, 0.8,
        16, 0,
      ],
      'circle-stroke-width': 0.5,
      'circle-stroke-color': 'rgba(255,255,255,0.3)',
    },
  })

  // Layer 3: Tree icons at close zoom
  map.addLayer({
    id: 'trees-icon',
    type: 'symbol',
    source: 'trees',
    minzoom: 15,
    layout: {
      'icon-image': buildIconExpression(),
      'icon-size': [
        'interpolate', ['linear'], ['zoom'],
        15, ['interpolate', ['linear'], ['get', 'dbh'], 0, 0.25, 30, 0.5],
        18, ['interpolate', ['linear'], ['get', 'dbh'], 0, 0.4, 30, 0.9],
        20, ['interpolate', ['linear'], ['get', 'dbh'], 0, 0.5, 30, 1.2],
      ],
      'icon-rotate': ['get', 'rotation'],
      'icon-rotation-alignment': 'viewport',
      'icon-allow-overlap': true,
      'icon-ignore-placement': true,
    },
    paint: {
      'icon-opacity': ['interpolate', ['linear'], ['zoom'], 15, 0, 15.5, 1],
    },
  })

  // Click popup
  map.on('click', 'trees-icon', (e) => {
    if (!e.features?.length) return
    void showTreePopup(e.features[0] as unknown as GeoJSON.Feature, 15)
  })

  map.on('click', 'trees-circle', (e) => {
    if (!e.features?.length) return
    void showTreePopup(e.features[0] as unknown as GeoJSON.Feature, 8)
  })

  // Cursor style
  for (const layer of ['trees-icon', 'trees-circle']) {
    map.on('mouseenter', layer, () => { map!.getCanvas().style.cursor = 'pointer' })
    map.on('mouseleave', layer, () => { map!.getCanvas().style.cursor = '' })
  }
  console.info('[Perf] map:layers:add:done', { ms: Math.round(nowMs() - t0) })
}

function updateZoomLevel() {
  if (!map) return
  zoomLevel.value = map.getZoom()
}

onMounted(() => {
  mapInitStartedAt = nowMs()
  console.info('[Perf] map:init:start')
  map = new maplibregl.Map({
    container: mapContainer.value!,
    style: {
      version: 8,
      glyphs: 'https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf',
      sources: {
        'carto-dark': {
          type: 'raster',
          tiles: [
            'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
            'https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
            'https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
          ],
          tileSize: 256,
          attribution: '&copy; <a href="https://carto.com/">CARTO</a> &copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>',
        },
      },
      layers: [
        {
          id: 'carto-dark-layer',
          type: 'raster',
          source: 'carto-dark',
          minzoom: 0,
          maxzoom: 20,
        },
      ],
    },
    center: [-122.44, 37.76],
    zoom: 13,
    pitch: 60,
    bearing: -20,
    maxPitch: 70,
    keyboard: true,
  })

  map.addControl(new maplibregl.NavigationControl(), 'top-right')
  map.on('zoom', updateZoomLevel)
  map.on('move', updateZoomLevel)

  map.on('load', () => {
    console.info('[Perf] map:style:load', { ms: Math.round(nowMs() - mapInitStartedAt) })
    updateZoomLevel()
    registerTreeIcons(map!, categoryIcons.value)
    console.info('[Perf] map:icons:registered', { ms: Math.round(nowMs() - mapInitStartedAt) })

    map!.on('sourcedata', (e) => {
      if (!mapDataPublishedAt) return
      if (e.sourceId === 'trees' && e.isSourceLoaded && !firstTreesSourceLoadedLogged) {
        firstTreesSourceLoadedLogged = true
        console.info('[Perf] map:trees-source:loaded', {
          msSincePublish: Math.round(nowMs() - mapDataPublishedAt),
        })
      }
      if (e.sourceId === 'carto-dark' && e.isSourceLoaded) {
        console.info('[Perf] map:basemap-source:loaded', {
          msSinceMapInit: Math.round(nowMs() - mapInitStartedAt),
        })
      }
    })

    map!.on('idle', () => {
      if (!mapDataPublishedAt || firstMapIdleAfterPublishLogged) return
      firstMapIdleAfterPublishLogged = true
      console.info('[Perf] map:first-idle-after-publish', {
        msSincePublish: Math.round(nowMs() - mapDataPublishedAt),
      })
    })

    void loadDefaultMapData()
  })
})

watch(categoryIcons, (icons) => {
  if (!map?.loaded()) return
  registerTreeIcons(map, icons)
})

// If data loads after map is ready
watch(currentGeoJSON, (newData) => {
  if (!map?.loaded() || !newData) return
  const source = map.getSource('trees') as maplibregl.GeoJSONSource | undefined
  if (source) {
    // Update existing source data (chat publish_results)
    const t0 = nowMs()
    source.setData(newData)
    console.info('[Perf] map:source:setData', {
      ms: Math.round(nowMs() - t0),
      features: newData.features.length,
    })
  } else {
    // First time — add layers
    addTreeLayers()
  }
})

// Compute bearing from current map center to a target point
function bearingTo(from: [number, number], to: [number, number]): number {
  const toRad = (d: number) => (d * Math.PI) / 180
  const toDeg = (r: number) => (r * 180) / Math.PI
  const dLon = toRad(to[0] - from[0])
  const lat1 = toRad(from[1])
  const lat2 = toRad(to[1])
  const y = Math.sin(dLon) * Math.cos(lat2)
  const x = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLon)
  return (toDeg(Math.atan2(y, x)) + 360) % 360
}

// Swoop camera to landmark — pivot to face the target first, then fly
watch(flyToTarget, (t) => {
  if (!map || !t) return
  const center = map.getCenter()
  const targetBearing = bearingTo([center.lng, center.lat], [t.lng, t.lat])

  // Rotate to face the destination, then blend into the swoop
  map.easeTo({
    bearing: targetBearing,
    duration: 3000,
    easing: (x) => x * (2 - x), // ease-out quad
  })
  // Kick off the fly before the rotation fully settles so they overlap
  setTimeout(() => {
    map!.flyTo({
      center: [t.lng, t.lat],
      zoom: t.zoom ?? 16,
      pitch: 60,
      bearing: targetBearing,
      duration: 2500,
      essential: true,
    })
  }, 2200)
})

onUnmounted(() => {
  map?.remove()
  map = null
})
</script>

<style scoped>
.tree-map {
  width: 100%;
  height: 100%;
}

.zoom-indicator {
  position: absolute;
  top: 58px;
  right: 12px;
  z-index: 5;
  background: rgba(22, 33, 62, 0.9);
  color: #4fc3f7;
  border: 1px solid #0f3460;
  border-radius: 6px;
  padding: 6px 10px;
  font-size: 0.75rem;
  font-weight: 600;
  letter-spacing: 0.02em;
}

.map-loading,
.map-error {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  padding: 12px 24px;
  border-radius: 8px;
  font-size: 0.875rem;
  z-index: 5;
}

.map-loading {
  background: rgba(22, 33, 62, 0.9);
  color: #4fc3f7;
}

.map-error {
  background: rgba(183, 28, 28, 0.9);
  color: #ffcdd2;
}
</style>

<style>
.tree-popup .maplibregl-popup-content {
  background: #16213e;
  color: #e0e0e0;
  border: 1px solid #0f3460;
  border-radius: 8px;
  padding: 10px 14px;
  font-size: 0.8rem;
  line-height: 1.5;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
}

.tree-popup .maplibregl-popup-close-button {
  color: #7a7a9e;
  font-size: 1rem;
  padding: 2px 6px;
}

.tree-popup .maplibregl-popup-tip {
  border-top-color: #16213e;
}
</style>
