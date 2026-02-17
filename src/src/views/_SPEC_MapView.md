# MapView Spec

## Overview
An angled flyover-style map of San Francisco's urban forest, rendering tree data from `raw_data.json` with species-specific visual representations.

## Camera
- **Initial center:** [-122.44, 37.76] (SF center)
- **Zoom:** 13
- **Pitch:** 60° (angled "raven height" flyover)
- **Bearing:** -20° (slight rotation for depth)
- **Navigation:** Arrow keys to pan, mouse drag to rotate/tilt, scroll to zoom

## LOD Tiers
| Zoom | Rendering | Purpose |
|------|-----------|---------|
| ≤13 | Heatmap (density) | Overview of tree distribution across SF |
| 14-16 | Colored circles (3-8px, DBH-scaled) | Identify clusters, category colors visible |
| 16+ | Tree silhouette icons (category-specific shape, DBH-scaled) | Individual tree identification |

## Tree Categories
Trees are categorized by genus (extracted from `q_species` field, first word before `::` separator):

| Category | Genera | Shape | Color |
|----------|--------|-------|-------|
| Palm | Washingtonia | Fan fronds on tall trunk | #e6a835 |
| Broadleaf | Lophostemon, Pittosporum, Ulmus, Magnolia, Ligustrum, Olea, Ginkgo, Acer, Myoporum | Round canopy | #4CAF50 |
| Spreading | Platanus, Acacia | Wide flat canopy | #8BC34A |
| Conical | Callistemon, Tristaniopsis, Melaleuca, Metrosideros, Geijera | Narrow triangle | #2E7D32 |
| Ornamental | Prunus, Pyrus, Ceanothus, Dodonaea, Hymenosporum | Small round with bloom dots | #E91E63 |
| Default | (any unmatched) | Generic round | #66BB6A |

## Interactions
- **Click tree (circle or icon layer):** Popup showing common name, species, plant date, DBH
- **Hover:** Pointer cursor on interactive layers
- **Keyboard:** Arrow keys for panning (built-in MapLibre)

## Technology
- MapLibre GL JS for WebGL-accelerated map rendering with pitch/bearing
- CartoDB Dark Matter raster tiles as base map
- GeoJSON source with data-driven styling expressions
- Canvas-generated tree silhouette icons registered via `map.addImage()`
