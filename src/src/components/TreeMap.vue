<template>
  <div ref="mapContainer" class="tree-map"></div>
  <div v-if="loading" class="map-loading">Loading tree data...</div>
  <div v-if="error" class="map-error">{{ error }}</div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch } from 'vue'
import maplibregl from 'maplibre-gl'
import { useTreeData } from '../composables/useTreeData'
import { registerTreeIcons, CATEGORY_COLORS } from '../composables/useTreeCategories'
import { useFlyTo } from '../composables/useFlyTo'
import { useMapData } from '../composables/useMapData'
import type { TreeCategory } from '../types'

const mapContainer = ref<HTMLDivElement>()
let map: maplibregl.Map | null = null

const { geojson, loading, error } = useTreeData()
const { target: flyToTarget } = useFlyTo()
const { currentGeoJSON, publishGeoJSON } = useMapData()

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

function addTreeLayers() {
  if (!map || !currentGeoJSON.value) return

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
    minzoom: 13,
    maxzoom: 16,
    paint: {
      'circle-radius': [
        'interpolate', ['linear'], ['zoom'],
        13, 2,
        16, ['interpolate', ['linear'], ['get', 'dbh'], 0, 3, 30, 8],
      ],
      'circle-color': buildColorExpression(),
      'circle-opacity': [
        'interpolate', ['linear'], ['zoom'],
        13, 0,
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
    const f = e.features[0]
    const coords = (f.geometry as GeoJSON.Point).coordinates.slice() as [number, number]
    const p = f.properties!

    new maplibregl.Popup({ offset: 15, className: 'tree-popup' })
      .setLngLat(coords)
      .setHTML(`
        <strong>${p.commonName}</strong><br/>
        <em>${p.species}</em><br/>
        ID: ${p.id}<br/>
        Planted: ${p.plantDate?.split(' ')[0] ?? 'Unknown'}<br/>
        DBH: ${p.dbh}"
      `)
      .addTo(map!)
  })

  map.on('click', 'trees-circle', (e) => {
    if (!e.features?.length) return
    const f = e.features[0]
    const coords = (f.geometry as GeoJSON.Point).coordinates.slice() as [number, number]
    const p = f.properties!

    new maplibregl.Popup({ offset: 8, className: 'tree-popup' })
      .setLngLat(coords)
      .setHTML(`
        <strong>${p.commonName}</strong><br/>
        <em>${p.species}</em><br/>
        ID: ${p.id}<br/>
        Planted: ${p.plantDate?.split(' ')[0] ?? 'Unknown'}<br/>
        DBH: ${p.dbh}"
      `)
      .addTo(map!)
  })

  // Cursor style
  for (const layer of ['trees-icon', 'trees-circle']) {
    map.on('mouseenter', layer, () => { map!.getCanvas().style.cursor = 'pointer' })
    map.on('mouseleave', layer, () => { map!.getCanvas().style.cursor = '' })
  }
}

onMounted(() => {
  map = new maplibregl.Map({
    container: mapContainer.value!,
    style: {
      version: 8,
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

  map.on('load', () => {
    registerTreeIcons(map!)
    if (currentGeoJSON.value) {
      addTreeLayers()
    }
  })
})

// Seed shared map data when tree data first loads
watch(geojson, (val) => {
  if (val && !currentGeoJSON.value) {
    publishGeoJSON(val)
  }
})

// If data loads after map is ready
watch(currentGeoJSON, (newData) => {
  if (!map?.loaded() || !newData) return
  const source = map.getSource('trees') as maplibregl.GeoJSONSource | undefined
  if (source) {
    // Update existing source data (chat publish_results)
    source.setData(newData)
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
