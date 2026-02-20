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
const introActive = ref(true)
let map: maplibregl.Map | null = null
let mapInitStartedAt = 0
let mapQueryChangedAt = 0
let firstTreesSourceLoadedLogged = false
let firstMapIdleAfterPublishLogged = false
let treeInteractionsBound = false
let lastIconDebugAt = 0
let prewarmStartedForRevision = -1
let introStarted = false
let introRafId: number | null = null
let introCancelled = false
let lastVisibleRangeSig = ''
const introLockedRangeByZoom = new Map<number, { minX: number; maxX: number; minY: number; maxY: number }>()

const {
  query: duckQuery,
  ensureTileProtocolRegistered,
  setTileQuery,
  setViewportZoom,
  setViewportCenter,
  setVisibleTileRange,
  prefetchVisibleDetailTilesAtZoom,
  prewarmLodCaches,
} = useDuckDB()
const { categoryIcons, loading, error, getSpeciesEnrichment } = useTreeData()
const { target: flyToTarget } = useFlyTo()
const { currentMapQuery, mapQueryRevision, publishMapQuery } = useMapData()
const displayError = computed(() => error.value ?? mapError.value)
const isInitialLoading = computed(() => loading.value || defaultQueryLoading.value || introActive.value)

const INTRO_CENTER: [number, number] = [-122.4194, 37.7749]
const INTRO_START_ZOOM = 18.5
const INTRO_END_ZOOM = 13.5
const INTRO_DURATION_MS = 10_000
const INTRO_ROTATION_DEG = 240
const INITIAL_TILE_PREFETCH_SCALE = 3.5

function setMapInteractions(enabled: boolean) {
  if (!map) return
  const action = enabled ? 'enable' : 'disable'
  map.boxZoom[action]()
  map.doubleClickZoom[action]()
  map.dragPan[action]()
  map.dragRotate[action]()
  map.keyboard[action]()
  map.scrollZoom[action]()
  map.touchZoomRotate[action]()
  map.touchPitch[action]()
}

async function waitForTreesMilestone(timeoutMs = 2500): Promise<void> {
  if (!map) return
  if (map.isSourceLoaded('trees')) return

  await new Promise<void>((resolve) => {
    if (!map) return resolve()
    let done = false
    const finish = () => {
      if (done) return
      done = true
      map?.off('sourcedata', onSourceData)
      clearTimeout(timer)
      resolve()
    }
    const onSourceData = (e: any) => {
      if (e?.sourceId === 'trees' && e?.isSourceLoaded) finish()
    }
    const timer = setTimeout(finish, timeoutMs)
    map.on('sourcedata', onSourceData)
  })
}

function runIntroZoomSegment(
  fromZoom: number,
  toZoom: number,
  fromT: number,
  toT: number,
  durationMs: number,
  startBearing: number,
  pitch: number,
): Promise<void> {
  return new Promise((resolve) => {
    if (!map) return resolve()
    const baseLng = INTRO_CENTER[0]
    const baseLat = INTRO_CENTER[1]
    const segmentStart = nowMs()

    const step = () => {
      if (!map || introCancelled) {
        introRafId = null
        return resolve()
      }

      const local = Math.max(0, Math.min(1, (nowMs() - segmentStart) / durationMs))
      const easedLocal = local * local * (3 - 2 * local)
      const globalT = fromT + (toT - fromT) * easedLocal

      const zoom = fromZoom + (toZoom - fromZoom) * easedLocal
      const bearing = startBearing + INTRO_ROTATION_DEG * globalT
      const angle = globalT * Math.PI * 2
      const radiusDeg = 0.0012 * (1 - globalT)
      const lng = baseLng + (Math.cos(angle) * radiusDeg) / Math.max(0.2, Math.cos((baseLat * Math.PI) / 180))
      const lat = baseLat + Math.sin(angle) * radiusDeg

      map.jumpTo({
        center: [lng, lat],
        zoom,
        bearing,
        pitch,
      })

      if (local < 1) {
        introRafId = requestAnimationFrame(step)
        return
      }
      introRafId = null
      resolve()
    }

    introRafId = requestAnimationFrame(step)
  })
}

async function runIntroZoomOut() {
  if (!map || introStarted) return
  introStarted = true
  introCancelled = false
  introActive.value = true
  setMapInteractions(false)

  const startBearing = map.getBearing()
  const startPitch = map.getPitch()
  const checkpoints = [INTRO_START_ZOOM, 17.6, 16.8, 16.0, 15.2, 14.3, INTRO_END_ZOOM]
  const segments = checkpoints.length - 1
  const segmentDuration = Math.round(INTRO_DURATION_MS / segments)

  try {
    for (let i = 0; i < segments; i += 1) {
      if (!map || introCancelled) break
      const fromT = i / segments
      const toT = (i + 1) / segments
      await runIntroZoomSegment(
        checkpoints[i],
        checkpoints[i + 1],
        fromT,
        toT,
        segmentDuration,
        startBearing,
        startPitch,
      )

      const stageZoom = Math.round(checkpoints[i + 1])
      if (stageZoom >= 15) {
        await prefetchVisibleDetailTilesAtZoom(stageZoom)
      }

      await waitForTreesMilestone(2500)
    }
  } finally {
    if (map) {
      map.jumpTo({
        center: INTRO_CENTER,
        zoom: INTRO_END_ZOOM,
        bearing: startBearing + INTRO_ROTATION_DEG,
        pitch: startPitch,
      })
    }
    introActive.value = false
    introLockedRangeByZoom.clear()
    setMapInteractions(true)
  }
}

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

function isIconLayerDebugEnabled(): boolean {
  if (typeof window === 'undefined') return false
  return window.location.search.includes('debugIcons=1')
}

function logIconLayerDebug(message: string, payload: Record<string, unknown>) {
  if (!isIconLayerDebugEnabled()) return
  console.info(`[IconLayer] ${message}`, payload)
}

function logIconLayerSnapshot(reason: string) {
  if (!map || !isIconLayerDebugEnabled()) return
  const now = Date.now()
  if (now - lastIconDebugAt < 400) return
  lastIconDebugAt = now

  const zoom = map.getZoom()
  const iconLayerExists = !!map.getLayer('trees-icon')
  const circleLayerExists = !!map.getLayer('trees-circle')
  const heatLayerExists = !!map.getLayer('trees-heat')
  const iconFeatures = iconLayerExists ? map.queryRenderedFeatures(undefined, { layers: ['trees-icon'] }).length : 0
  const circleFeatures = circleLayerExists ? map.queryRenderedFeatures(undefined, { layers: ['trees-circle'] }).length : 0
  const heatFeatures = heatLayerExists ? map.queryRenderedFeatures(undefined, { layers: ['trees-heat'] }).length : 0
  const iconOpacity = iconLayerExists ? map.getPaintProperty('trees-icon', 'icon-opacity') : null
  const circleOpacity = circleLayerExists ? map.getPaintProperty('trees-circle', 'circle-opacity') : null

  logIconLayerDebug('snapshot', {
    reason,
    zoom: Number(zoom.toFixed(2)),
    iconLayerExists,
    circleLayerExists,
    heatLayerExists,
    iconFeatures,
    circleFeatures,
    heatFeatures,
    iconOpacity,
    circleOpacity,
  })
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
  if (!currentMapQuery.value) {
    publishMapQuery(DEFAULT_MAP_QUERY)
    return
  }

  mapQueryChangedAt = nowMs()
  firstTreesSourceLoadedLogged = false
  firstMapIdleAfterPublishLogged = false
  defaultQueryLoading.value = true
  setTileQuery(currentMapQuery.value)
  addTreeLayers()
}

async function showTreePopup(feature: GeoJSON.Feature, offset: number) {
  if (!map) return
  const coords = (feature.geometry as GeoJSON.Point).coordinates.slice() as [number, number]
  const id = Number(feature.properties?.id)
  if (!Number.isFinite(id) || id <= 0) return

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
  if (!map) return
  const t0 = nowMs()
  console.info('[Perf] map:layers:add:start')

  if (map.getLayer('trees-icon')) map.removeLayer('trees-icon')
  if (map.getLayer('trees-circle')) map.removeLayer('trees-circle')
  if (map.getLayer('trees-heat')) map.removeLayer('trees-heat')
  if (map.getSource('trees')) map.removeSource('trees')

  map.addSource('trees', {
    type: 'vector',
    tiles: [`duckdb://trees/{z}/{x}/{y}.pbf?r=${mapQueryRevision.value}`],
    minzoom: 0,
    maxzoom: 22,
  })

  // Layer 1: Heatmap at far zoom
  map.addLayer({
    id: 'trees-heat',
    type: 'heatmap',
    source: 'trees',
    'source-layer': 'trees',
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
    'source-layer': 'trees',
    minzoom: 13.1,
    maxzoom: 18,
    paint: {
      'circle-radius': [
        'interpolate', ['linear'], ['zoom'],
        13.1, 2,
        16, ['interpolate', ['linear'], ['get', 'dbh'], 0, 3, 30, 8],
        18, ['interpolate', ['linear'], ['get', 'dbh'], 0, 4, 30, 10],
      ],
      'circle-color': buildColorExpression(),
      'circle-opacity': [
        'interpolate', ['linear'], ['zoom'],
        13.1, 0,
        14, 0.8,
        16.5, 0.65,
        18, 0,
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
    'source-layer': 'trees',
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

  if (!treeInteractionsBound) {
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
    treeInteractionsBound = true
  }
  console.info('[Perf] map:layers:add:done', { ms: Math.round(nowMs() - t0) })
}

function updateZoomLevel() {
  if (!map) return
  zoomLevel.value = map.getZoom()
  setViewportZoom(zoomLevel.value)
  const c = map.getCenter()
  setViewportCenter(c.lng, c.lat)

  const z = Math.round(map.getZoom())
  if (z >= 13 && z <= 20) {
    const n = Math.pow(2, z)
    const bounds = map.getBounds()
    const lonToTileX = (lon: number) => Math.floor(((lon + 180) / 360) * n)
    const latToTileY = (lat: number) => {
      const latRad = (Math.max(-85.05112878, Math.min(85.05112878, lat)) * Math.PI) / 180
      return Math.floor(((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2) * n)
    }

    let minX = Math.max(0, Math.min(n - 1, lonToTileX(bounds.getWest())))
    let maxX = Math.max(0, Math.min(n - 1, lonToTileX(bounds.getEast())))
    let minY = Math.max(0, Math.min(n - 1, latToTileY(bounds.getNorth())))
    let maxY = Math.max(0, Math.min(n - 1, latToTileY(bounds.getSouth())))

    if (introActive.value && z >= 15) {
      const locked = introLockedRangeByZoom.get(z)
      if (locked) {
        minX = locked.minX
        maxX = locked.maxX
        minY = locked.minY
        maxY = locked.maxY
      } else {
        introLockedRangeByZoom.set(z, { minX, maxX, minY, maxY })
      }
    }

    // Expanding range every animation frame during intro can create excessive
    // tile churn and stall startup. Only apply wide prefetch once intro ends.
    if (isInitialLoading.value && !introActive.value) {
      const width = Math.max(1, maxX - minX + 1)
      const height = Math.max(1, maxY - minY + 1)
      const extraX = Math.ceil((width * (INITIAL_TILE_PREFETCH_SCALE - 1)) / 2)
      const extraY = Math.ceil((height * (INITIAL_TILE_PREFETCH_SCALE - 1)) / 2)
      minX = Math.max(0, minX - extraX)
      maxX = Math.min(n - 1, maxX + extraX)
      minY = Math.max(0, minY - extraY)
      maxY = Math.min(n - 1, maxY + extraY)
    }

    const rangeSig = `${z}:${minX}-${maxX}:${minY}-${maxY}`
    if (rangeSig !== lastVisibleRangeSig) {
      lastVisibleRangeSig = rangeSig
      setVisibleTileRange(z, minX, maxX, minY, maxY)
    }

    if (isInitialLoading.value) {
      const tilesVisible = Math.max(1, (maxX - minX + 1) * (maxY - minY + 1))
      logIconLayerDebug('initial-visible-tiles', { z, minX, maxX, minY, maxY, tilesVisible })
    }
  }

  logIconLayerSnapshot('zoom/move')
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
          // Use larger raster tiles to reduce base-layer tile churn/pop-in.
          tileSize: 512,
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
          paint: {
            'raster-resampling': 'linear',
            'raster-fade-duration': 150,
          },
        },
      ],
    },
    zoom: INTRO_START_ZOOM,
    center: INTRO_CENTER,
    pitch: 60,
    bearing: -20,
    maxPitch: 70,
    maxZoom: INTRO_START_ZOOM,
    keyboard: true,
  })

  setMapInteractions(false)

  map.addControl(new maplibregl.NavigationControl(), 'top-right')
  map.on('zoom', updateZoomLevel)
  map.on('move', updateZoomLevel)

  map.on('load', () => {
    console.info('[Perf] map:style:load', { ms: Math.round(nowMs() - mapInitStartedAt) })
    updateZoomLevel()
    registerTreeIcons(map!, categoryIcons.value)
    console.info('[Perf] map:icons:registered', { ms: Math.round(nowMs() - mapInitStartedAt) })

    map!.on('sourcedata', (e) => {
      if (!mapQueryChangedAt) return
      if (e.sourceId === 'trees') {
        logIconLayerDebug('trees-sourcedata', {
          isSourceLoaded: e.isSourceLoaded,
          sourceDataType: (e as any).sourceDataType,
          coord: (e as any).coord,
          tile: (e as any).tile,
        })
      }
      if (e.sourceId === 'trees' && !firstTreesSourceLoadedLogged) {
        firstTreesSourceLoadedLogged = true
        defaultQueryLoading.value = false
        console.info('[Perf] map:trees-source:loaded', {
          msSincePublish: Math.round(nowMs() - mapQueryChangedAt),
          isSourceLoaded: e.isSourceLoaded,
        })

        if (prewarmStartedForRevision !== mapQueryRevision.value) {
          prewarmStartedForRevision = mapQueryRevision.value
          void prewarmLodCaches().catch((err) => {
            console.warn('[Perf] map:prewarm:failed', err)
          })
        }

        runIntroZoomOut()
      }
      if (e.sourceId === 'carto-dark' && e.isSourceLoaded) {
        console.info('[Perf] map:basemap-source:loaded', {
          msSinceMapInit: Math.round(nowMs() - mapInitStartedAt),
        })
      }
    })

    map!.on('styleimagemissing', (e) => {
      logIconLayerDebug('style-image-missing', { id: e.id })
    })

    map!.on('moveend', () => {
      logIconLayerSnapshot('moveend')
    })

    map!.on('idle', () => {
      if (!mapQueryChangedAt || firstMapIdleAfterPublishLogged) return
      firstMapIdleAfterPublishLogged = true
      console.info('[Perf] map:first-idle-after-publish', {
        msSincePublish: Math.round(nowMs() - mapQueryChangedAt),
      })
      logIconLayerSnapshot('first-idle-after-publish')
    })

    void ensureTileProtocolRegistered()
      .then(() => {
        void loadDefaultMapData()
      })
      .catch((e) => {
        mapError.value = (e as Error).message
      })
  })
})

watch(categoryIcons, (icons) => {
  if (!map?.loaded()) return
  registerTreeIcons(map, icons)
})

// If data loads after map is ready
watch([currentMapQuery, mapQueryRevision], ([query]) => {
  if (!map?.loaded()) return
  mapQueryChangedAt = nowMs()
  firstTreesSourceLoadedLogged = false
  firstMapIdleAfterPublishLogged = false
  defaultQueryLoading.value = true
  setTileQuery(query)
  addTreeLayers()
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
  introCancelled = true
  if (introRafId != null) cancelAnimationFrame(introRafId)
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
