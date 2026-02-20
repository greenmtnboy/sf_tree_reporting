import { ref } from 'vue'
import * as duckdb from '@duckdb/duckdb-wasm'
import maplibregl from 'maplibre-gl'
import duckdb_wasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url'
import duckdb_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url'
import duckdb_wasm_eh from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url'
import duckdb_worker_eh from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url'

let db: duckdb.AsyncDuckDB | null = null
let conn: duckdb.AsyncDuckDBConnection | null = null
const ready = ref(false)
const initError = ref<string | null>(null)

const TABLE_DDL = `
CREATE TABLE IF NOT EXISTS trees (
  tree_id INTEGER,
  common_name VARCHAR,
  site_info VARCHAR,
  plant_date VARCHAR,
  species VARCHAR,
  latitude DOUBLE,
  longitude DOUBLE,
  diameter_at_breast_height DOUBLE
);
`

const SPECIES_DDL = `
CREATE TABLE IF NOT EXISTS species_enrichment (
  species VARCHAR,
  tree_category VARCHAR,
  native_status VARCHAR,
  is_evergreen BOOLEAN,
  mature_height_ft DOUBLE,
  bloom_season VARCHAR,
  wildlife_value VARCHAR,
  fire_risk VARCHAR
);
`

let initPromise: Promise<void> | null = null
let protocolRegistered = false
const tileQuerySql = ref<string | null>(null)
let tileQueryRevision = 0
const activeViewportZoom = ref<number>(13)
const activeViewportCenter = ref<{ lng: number; lat: number } | null>(null)
const tileCache = new Map<string, Uint8Array>()
const inflightTileRequests = new Map<string, Promise<Uint8Array>>()
const zoomBatchReady = new Set<string>()
const inflightZoomBatch = new Map<string, Promise<void>>()
const inflightNeighborhoodBatch = new Map<string, Promise<void>>()
const MAX_TILE_CACHE_ENTRIES = 512
let activeTileRequests = 0
let activeDuckdbCalls = 0
const MAX_PARALLEL_TILE_WORK = 2
let activeQueuedWorkers = 0
let queueLastLoggedAt = 0
let timingLastLoggedAt = 0
const WEB_MERCATOR_MAX = 20037508.342789244
const WEB_MERCATOR_WORLD = WEB_MERCATOR_MAX * 2

type TimingBucket = {
  count: number
  totalMs: number
  maxMs: number
}

let tileTimingBucket: TimingBucket = { count: 0, totalMs: 0, maxMs: 0 }
let zoomBatchTimingBucket: TimingBucket = { count: 0, totalMs: 0, maxMs: 0 }

type QueuedTileRequest = {
  z: number
  x: number
  y: number
  enqueuedAt: number
  resolve: (tile: Uint8Array) => void
  reject: (error: unknown) => void
}

const pendingTileQueue: QueuedTileRequest[] = []

function isIconTileDebugEnabled(): boolean {
  if (typeof window === 'undefined') return false
  return window.location.search.includes('debugIcons=1')
}

function logIconTileDebug(message: string, payload: Record<string, unknown>) {
  if (!isIconTileDebugEnabled()) return
  console.info(`[IconTiles] ${message}`, payload)
}

function logQueueStats(reason: string): void {
  if (!isIconTileDebugEnabled()) return
  const now = Date.now()
  if (now - queueLastLoggedAt < 500) return
  queueLastLoggedAt = now
  console.info('[IconTiles] queue-stats', {
    reason,
    pendingQueue: pendingTileQueue.length,
    activeQueuedWorkers,
    activeTileRequests,
    activeDuckdbCalls,
    inflightTiles: inflightTileRequests.size,
    inflightZoomBatches: inflightZoomBatch.size,
    inflightNeighborhoodBatches: inflightNeighborhoodBatch.size,
  })
}

function resetTimingBuckets(): void {
  tileTimingBucket = { count: 0, totalMs: 0, maxMs: 0 }
  zoomBatchTimingBucket = { count: 0, totalMs: 0, maxMs: 0 }
}

function recordDuckdbTiming(kind: 'tile' | 'zoom-batch', ms: number, payload: Record<string, unknown>): void {
  const bucket = kind === 'tile' ? tileTimingBucket : zoomBatchTimingBucket
  bucket.count += 1
  bucket.totalMs += ms
  bucket.maxMs = Math.max(bucket.maxMs, ms)

  if (ms >= 1200) {
    logIconTileDebug('duckdb-slow-query', { kind, ms: Math.round(ms), ...payload })
  }

  if (!isIconTileDebugEnabled()) return
  const now = Date.now()
  if (now - timingLastLoggedAt < 2000) return
  timingLastLoggedAt = now

  const tileAvg = tileTimingBucket.count ? Math.round(tileTimingBucket.totalMs / tileTimingBucket.count) : 0
  const batchAvg = zoomBatchTimingBucket.count
    ? Math.round(zoomBatchTimingBucket.totalMs / zoomBatchTimingBucket.count)
    : 0

  console.info('[IconTiles] duckdb-timing', {
    tileQueries: tileTimingBucket.count,
    tileAvgMs: tileAvg,
    tileMaxMs: Math.round(tileTimingBucket.maxMs),
    zoomBatchQueries: zoomBatchTimingBucket.count,
    zoomBatchAvgMs: batchAvg,
    zoomBatchMaxMs: Math.round(zoomBatchTimingBucket.maxMs),
    pendingQueue: pendingTileQueue.length,
    activeDuckdbCalls,
  })

  resetTimingBuckets()
}

function nowMs(): number {
  return typeof performance !== 'undefined' ? performance.now() : Date.now()
}

function tileBounds3857(z: number, x: number, y: number): { minX: number; minY: number; maxX: number; maxY: number } {
  const n = Math.pow(2, z)
  const span = WEB_MERCATOR_WORLD / n
  const minX = -WEB_MERCATOR_MAX + x * span
  const maxX = minX + span
  const maxY = WEB_MERCATOR_MAX - y * span
  const minY = maxY - span
  return { minX, minY, maxX, maxY }
}

function tileXExpr(alias: string, z: number): string {
  if (z >= 13 && z <= 20) return `${alias}.xtile_z${z}`
  return `CAST(floor(((${alias}.x_3857) + ${WEB_MERCATOR_MAX}) / (${WEB_MERCATOR_WORLD} / pow(2, ${z}))) AS INTEGER)`
}

function tileYExpr(alias: string, z: number): string {
  if (z >= 13 && z <= 20) return `${alias}.ytile_z${z}`
  return `CAST(floor((${WEB_MERCATOR_MAX} - (${alias}.y_3857)) / (${WEB_MERCATOR_WORLD} / pow(2, ${z}))) AS INTEGER)`
}

async function doInit() {
  if (db) return
  const t0 = nowMs()
  console.info('[Perf] duckdb:init:start')

  const bundles: duckdb.DuckDBBundles = {
    mvp: {
      mainModule: duckdb_wasm,
      mainWorker: duckdb_worker,
    },
    eh: {
      mainModule: duckdb_wasm_eh,
      mainWorker: duckdb_worker_eh,
    },
  }
  const bundle = await duckdb.selectBundle(bundles)

  const logger = new duckdb.ConsoleLogger()
  const worker = new Worker(bundle.mainWorker!)
  db = new duckdb.AsyncDuckDB(logger, worker)
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker)
  console.info('[Perf] duckdb:instantiate:done', { ms: Math.round(nowMs() - t0) })

  conn = await db.connect()
  await conn.query(TABLE_DDL)
  await conn.query(SPECIES_DDL)
  const tAfterSchema = nowMs()
  console.info('[Perf] duckdb:schema:done', { ms: Math.round(tAfterSchema - t0) })

  // Fetch tree JSON and register in DuckDB virtual filesystem
  const res = await fetch(import.meta.env.BASE_URL + 'data/raw_data.json')
  const jsonText = await res.text()
  console.info('[Perf] duckdb:json:fetched', {
    ms: Math.round(nowMs() - tAfterSchema),
    bytes: jsonText.length,
  })
  await db.registerFileText('trees.json', jsonText)

  await conn.query(`INSERT INTO trees SELECT * FROM read_json_auto('trees.json')`)

  // Species enrichment lookup table for category + popup metadata
  try {
    const speciesRes = await fetch(import.meta.env.BASE_URL + 'data/species_data.json')
    if (speciesRes.ok) {
      const speciesJson = await speciesRes.text()
      await db.registerFileText('species.json', speciesJson)
      await conn.query(`
        INSERT INTO species_enrichment
        SELECT
          species,
          tree_category,
          native_status,
          is_evergreen,
          mature_height_ft,
          bloom_season,
          wildlife_value,
          fire_risk
        FROM read_json_auto('species.json')
      `)
    }
  } catch (e) {
    console.warn('[Perf] duckdb:species-load:failed', e)
  }

  // Precompute map-focused fields once so tile queries stay fast.
  const tFastStart = nowMs()
  await conn.query(`
    CREATE OR REPLACE TABLE trees_fast AS
    WITH joined AS (
      SELECT
        t.tree_id,
        t.common_name,
        t.site_info,
        t.plant_date,
        t.species,
        t.latitude,
        t.longitude,
        COALESCE(t.diameter_at_breast_height, 3) AS dbh,
        CASE lower(trim(COALESCE(se.tree_category, 'default')))
          WHEN 'palm' THEN 'palm'
          WHEN 'broadleaf' THEN 'broadleaf'
          WHEN 'spreading' THEN 'spreading'
          WHEN 'coniferous' THEN 'coniferous'
          WHEN 'columnar' THEN 'columnar'
          WHEN 'ornamental' THEN 'ornamental'
          ELSE 'default'
        END AS tree_category,
        se.native_status,
        se.is_evergreen,
        se.mature_height_ft,
        se.bloom_season,
        se.wildlife_value,
        se.fire_risk,
        LEAST(GREATEST(t.latitude, -85.05112878), 85.05112878) AS latitude_clamped,
        ((t.longitude + 180.0) / 360.0) AS x_norm,
        ((1.0 - ln(tan(radians(LEAST(GREATEST(t.latitude, -85.05112878), 85.05112878))) + (1.0 / cos(radians(LEAST(GREATEST(t.latitude, -85.05112878), 85.05112878))))) / pi()) / 2.0) AS y_norm
      FROM trees t
      LEFT JOIN species_enrichment se
        ON t.species = se.species
      WHERE t.latitude IS NOT NULL
        AND t.longitude IS NOT NULL
    )
    SELECT
      tree_id,
      common_name,
      site_info,
      plant_date,
      species,
      latitude,
      longitude,
      TRY_CAST(dbh AS DOUBLE) AS dbh,
      tree_category,
      native_status,
      is_evergreen,
      mature_height_ft,
      bloom_season,
      wildlife_value,
      fire_risk,
      ((longitude * ${WEB_MERCATOR_MAX}) / 180.0) AS x_3857,
      (6378137.0 * ln(tan(pi() / 4.0 + radians(latitude_clamped) / 2.0))) AS y_3857,
      CAST(floor(x_norm * pow(2, 13)) AS INTEGER) AS xtile_z13,
      CAST(floor(y_norm * pow(2, 13)) AS INTEGER) AS ytile_z13,
      CAST(floor(x_norm * pow(2, 14)) AS INTEGER) AS xtile_z14,
      CAST(floor(y_norm * pow(2, 14)) AS INTEGER) AS ytile_z14,
      CAST(floor(x_norm * pow(2, 15)) AS INTEGER) AS xtile_z15,
      CAST(floor(y_norm * pow(2, 15)) AS INTEGER) AS ytile_z15,
      CAST(floor(x_norm * pow(2, 16)) AS INTEGER) AS xtile_z16,
      CAST(floor(y_norm * pow(2, 16)) AS INTEGER) AS ytile_z16,
      CAST(floor(x_norm * pow(2, 17)) AS INTEGER) AS xtile_z17,
      CAST(floor(y_norm * pow(2, 17)) AS INTEGER) AS ytile_z17,
      CAST(floor(x_norm * pow(2, 18)) AS INTEGER) AS xtile_z18,
      CAST(floor(y_norm * pow(2, 18)) AS INTEGER) AS ytile_z18,
      CAST(floor(x_norm * pow(2, 19)) AS INTEGER) AS xtile_z19,
      CAST(floor(y_norm * pow(2, 19)) AS INTEGER) AS ytile_z19,
      CAST(floor(x_norm * pow(2, 20)) AS INTEGER) AS xtile_z20,
      CAST(floor(y_norm * pow(2, 20)) AS INTEGER) AS ytile_z20
    FROM joined
  `)
  console.info('[Perf] duckdb:trees_fast:done', { ms: Math.round(nowMs() - tFastStart) })

  // Spatial extension for on-demand MVT generation
  try {
    await conn.query('LOAD spatial;')
  } catch {
    try {
      await conn.query('INSTALL spatial;')
      await conn.query('LOAD spatial;')
    } catch (e) {
      console.error('DuckDB spatial extension unavailable:', e)
    }
  }
  console.info('[Perf] duckdb:insert:done', { ms: Math.round(nowMs() - tAfterSchema) })

  ready.value = true
  console.info('[Perf] duckdb:init:done', { ms: Math.round(nowMs() - t0) })
}

function parseDuckdbTileUrl(url: string): { z: number; x: number; y: number } | null {
  const m = url.match(/^duckdb:\/\/trees\/(\d+)\/(\d+)\/(\d+)\.pbf(?:\?.*)?$/)
  if (!m) return null
  return { z: Number(m[1]), x: Number(m[2]), y: Number(m[3]) }
}

function sanitizeBaseQuery(sql: string | null): string {
  if (!sql?.trim()) {
    return `
      SELECT tree_id, species, latitude, longitude, diameter_at_breast_height
      FROM trees
      WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    `
  }
  return sql.trim().replace(/;+\s*$/, '')
}

function tileCacheKey(z: number, x: number, y: number): string {
  return `${tileQueryRevision}:${z}/${x}/${y}`
}

function tileCacheKeyForRevision(rev: number, z: number, x: number, y: number): string {
  return `${rev}:${z}/${x}/${y}`
}

function zoomBatchKey(rev: number, z: number): string {
  return `${rev}:${z}`
}

function shouldUseZoomBatch(z: number): boolean {
  // Batch by zoom for the costly ranges where many nearby tiles are requested together.
  return z >= 13 && z <= 16
}

function neighborhoodBlockSizeForZoom(z: number): number {
  if (z >= 19) return 4
  if (z >= 17) return 6
  return 1
}

function neighborhoodBatchKey(rev: number, z: number, minX: number, maxX: number, minY: number, maxY: number): string {
  return `${rev}:${z}:${minX}-${maxX}:${minY}-${maxY}`
}

async function ensureNeighborhoodBatchTiles(z: number, x: number, y: number, baseQuery: string): Promise<void> {
  if (!conn) return
  const blockSize = neighborhoodBlockSizeForZoom(z)
  if (blockSize <= 1) return

  const rev = tileQueryRevision
  const tileCount = Math.pow(2, z)
  const blockX = Math.floor(x / blockSize)
  const blockY = Math.floor(y / blockSize)
  const minX = Math.max(0, blockX * blockSize)
  const maxX = Math.min(tileCount - 1, minX + blockSize - 1)
  const minY = Math.max(0, blockY * blockSize)
  const maxY = Math.min(tileCount - 1, minY + blockSize - 1)
  const bKey = neighborhoodBatchKey(rev, z, minX, maxX, minY, maxY)

  const inflight = inflightNeighborhoodBatch.get(bKey)
  if (inflight) {
    await inflight
    return
  }

  const startedAt = nowMs()
  const request = (async () => {
    const sql = `
WITH base AS (
  ${baseQuery}
), pts AS (
  SELECT
    tf.x_3857,
    tf.y_3857,
    COALESCE(base.tree_id, tf.tree_id, 0) AS id,
    COALESCE(base.diameter_at_breast_height, tf.dbh, 3) AS dbh,
    COALESCE(tf.tree_category, 'default') AS category,
    0 AS rotation,
    ${tileXExpr('tf', z)} AS xtile,
    ${tileYExpr('tf', z)} AS ytile
  FROM base
  INNER JOIN trees_fast tf
    ON base.tree_id = tf.tree_id
  WHERE ${tileXExpr('tf', z)} BETWEEN ${minX} AND ${maxX}
    AND ${tileYExpr('tf', z)} BETWEEN ${minY} AND ${maxY}
), tiled AS (
  SELECT
    x_3857,
    y_3857,
    id,
    dbh,
    category,
    rotation,
    xtile,
    ytile
  FROM pts
), rows AS (
  SELECT
    xtile,
    ytile,
    {
      'geom': ST_AsMVTGeom(
        ST_Point(x_3857, y_3857),
        ST_Extent(ST_TileEnvelope(${z}, xtile, ytile)),
        4096,
        64,
        true
      ),
      'id': id,
      'dbh': TRY_CAST(dbh AS DOUBLE),
      'category': category,
      'rotation': rotation
    } AS feature
  FROM tiled
)
SELECT
  xtile,
  ytile,
  ST_AsMVT(feature, 'trees', 4096, 'geom') AS mvt
FROM rows
WHERE feature.geom IS NOT NULL AND NOT ST_IsEmpty(feature.geom)
GROUP BY xtile, ytile
`

    activeDuckdbCalls += 1
    try {
      logIconTileDebug('neighborhood-batch:start', { z, rev, minX, maxX, minY, maxY, activeDuckdbCalls })
      const queryStartedAt = nowMs()
      const result = await conn.query(sql)
      const queryMs = nowMs() - queryStartedAt
      recordDuckdbTiming('zoom-batch', queryMs, { z, rev, lodMode: 'detailed-neighborhood' })
      const rows = result.toArray()

      if (rev === tileQueryRevision) {
        for (let tx = minX; tx <= maxX; tx += 1) {
          for (let ty = minY; ty <= maxY; ty += 1) {
            setCachedTile(tileCacheKeyForRevision(rev, z, tx, ty), new Uint8Array())
          }
        }
        for (const row of rows) {
          const xtile = Number(row.xtile)
          const ytile = Number(row.ytile)
          const raw = row.mvt as Uint8Array | undefined
          const tile = !raw || raw.length === 0
            ? new Uint8Array()
            : new Uint8Array(raw.buffer.slice(raw.byteOffset, raw.byteOffset + raw.byteLength))
          setCachedTile(tileCacheKeyForRevision(rev, z, xtile, ytile), tile)
        }
      }

      logIconTileDebug('neighborhood-batch:done', {
        z,
        rev,
        minX,
        maxX,
        minY,
        maxY,
        tilesInBlock: (maxX - minX + 1) * (maxY - minY + 1),
        rows: rows.length,
        ms: Math.round(nowMs() - startedAt),
      })
    } finally {
      activeDuckdbCalls -= 1
    }
  })()

  inflightNeighborhoodBatch.set(bKey, request)
  try {
    await request
  } finally {
    inflightNeighborhoodBatch.delete(bKey)
  }
}

function lngLatToTile(lng: number, lat: number, z: number): { x: number; y: number } {
  const n = Math.pow(2, z)
  const x = Math.floor(((lng + 180) / 360) * n)
  const latRad = (lat * Math.PI) / 180
  const y = Math.floor(
    ((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2) * n,
  )
  return {
    x: Math.max(0, Math.min(n - 1, x)),
    y: Math.max(0, Math.min(n - 1, y)),
  }
}

function pickNextQueuedTileIndex(): number {
  if (pendingTileQueue.length <= 1) return 0
  const viewport = activeViewportCenter.value
  const viewportZoom = Math.round(activeViewportZoom.value)
  const now = Date.now()
  let bestIdx = 0
  let bestScore = Number.POSITIVE_INFINITY

  for (let i = 0; i < pendingTileQueue.length; i += 1) {
    const q = pendingTileQueue[i]
    const zoomPenalty = Math.abs(q.z - viewportZoom) * 1000
    let distancePenalty = 0
    if (viewport) {
      const centerTile = lngLatToTile(viewport.lng, viewport.lat, q.z)
      distancePenalty = (Math.abs(q.x - centerTile.x) + Math.abs(q.y - centerTile.y)) * 10
    }
    const ageBonus = (now - q.enqueuedAt) / 100
    const score = zoomPenalty + distancePenalty - ageBonus
    if (score < bestScore) {
      bestScore = score
      bestIdx = i
    }
  }

  return bestIdx
}

function processTileQueue(): void {
  while (activeQueuedWorkers < MAX_PARALLEL_TILE_WORK && pendingTileQueue.length > 0) {
    const idx = pickNextQueuedTileIndex()
    const item = pendingTileQueue.splice(idx, 1)[0]
    activeQueuedWorkers += 1
    logQueueStats('dequeue')

    void generatePointTileMvt(item.z, item.x, item.y)
      .then((tile) => item.resolve(tile))
      .catch((e) => item.reject(e))
      .finally(() => {
        activeQueuedWorkers -= 1
        logQueueStats('complete')
        processTileQueue()
      })
  }
}

function queueTileRequest(z: number, x: number, y: number): Promise<Uint8Array> {
  return new Promise<Uint8Array>((resolve, reject) => {
    pendingTileQueue.push({ z, x, y, enqueuedAt: Date.now(), resolve, reject })
    logQueueStats('enqueue')
    processTileQueue()
  })
}

function getCachedTile(key: string): Uint8Array | null {
  const tile = tileCache.get(key)
  if (!tile) return null
  // LRU bump
  tileCache.delete(key)
  tileCache.set(key, tile)
  return new Uint8Array(tile)
}

function setCachedTile(key: string, tile: Uint8Array): void {
  if (tileCache.has(key)) tileCache.delete(key)
  tileCache.set(key, tile)
  if (tileCache.size > MAX_TILE_CACHE_ENTRIES) {
    const firstKey = tileCache.keys().next().value as string | undefined
    if (firstKey) tileCache.delete(firstKey)
  }
}

async function ensureZoomBatchTiles(z: number, baseQuery: string, simplifyGridMeters: number): Promise<void> {
  if (!conn) return
  const rev = tileQueryRevision
  const bKey = zoomBatchKey(rev, z)
  if (zoomBatchReady.has(bKey)) return

  const inflight = inflightZoomBatch.get(bKey)
  if (inflight) {
    await inflight
    return
  }

  const lodMode = simplifyGridMeters > 0 ? 'aggregated' : 'detailed'
  const startedAt = nowMs()
  const request = (async () => {
    const sql = simplifyGridMeters > 0 ? `
WITH base AS (
  ${baseQuery}
), pts AS (
  SELECT
    tf.x_3857,
    tf.y_3857,
    COALESCE(base.diameter_at_breast_height, tf.dbh, 3) AS dbh,
    ${tileXExpr('tf', z)} AS xtile,
    ${tileYExpr('tf', z)} AS ytile
  FROM base
  INNER JOIN trees_fast tf
    ON base.tree_id = tf.tree_id
), bounded AS (
  SELECT *
  FROM pts
  WHERE xtile >= 0
    AND ytile >= 0
    AND xtile < CAST(pow(2, ${z}) AS INTEGER)
    AND ytile < CAST(pow(2, ${z}) AS INTEGER)
), agg AS (
  SELECT
    xtile,
    ytile,
    floor(x_3857 / ${simplifyGridMeters}) * ${simplifyGridMeters} + ${simplifyGridMeters} / 2.0 AS gx,
    floor(y_3857 / ${simplifyGridMeters}) * ${simplifyGridMeters} + ${simplifyGridMeters} / 2.0 AS gy,
    AVG(dbh) AS dbh,
    COUNT(*) AS point_count
  FROM bounded
  GROUP BY 1, 2, 3, 4
), rows AS (
  SELECT
    xtile,
    ytile,
    {
      'geometry': ST_AsMVTGeom(
        ST_Point(gx, gy),
        ST_Extent(ST_TileEnvelope(${z}, xtile, ytile)),
        4096,
        64,
        true
      ),
      'id': -1,
      'dbh': TRY_CAST(dbh AS DOUBLE),
      'category': 'default',
      'rotation': 0,
      'point_count': TRY_CAST(point_count AS INTEGER)
    } AS feature
  FROM agg
)
SELECT
  xtile,
  ytile,
  ST_AsMVT(feature, 'trees', 4096, 'geometry') AS mvt
FROM rows
WHERE feature.geometry IS NOT NULL AND NOT ST_IsEmpty(feature.geometry)
GROUP BY xtile, ytile
` : `
WITH base AS (
  ${baseQuery}
), points AS (
  SELECT
    tf.x_3857,
    tf.y_3857,
    COALESCE(base.tree_id, tf.tree_id, 0) AS id,
    COALESCE(base.diameter_at_breast_height, tf.dbh, 3) AS dbh,
    COALESCE(tf.tree_category, 'default') AS category,
    0 AS rotation,
    ${tileXExpr('tf', z)} AS xtile,
    ${tileYExpr('tf', z)} AS ytile
  FROM base
  INNER JOIN trees_fast tf
    ON base.tree_id = tf.tree_id
), bounded AS (
  SELECT *
  FROM points
  WHERE xtile >= 0
    AND ytile >= 0
    AND xtile < CAST(pow(2, ${z}) AS INTEGER)
    AND ytile < CAST(pow(2, ${z}) AS INTEGER)
), rows AS (
  SELECT
    xtile,
    ytile,
    {
      'geom': ST_AsMVTGeom(
        ST_Point(x_3857, y_3857),
        ST_Extent(ST_TileEnvelope(${z}, xtile, ytile)),
        4096,
        64,
        true
      ),
      'id': id,
      'dbh': TRY_CAST(dbh AS DOUBLE),
      'category': category,
      'rotation': rotation
    } AS feature
  FROM bounded
)
SELECT
  xtile,
  ytile,
  ST_AsMVT(feature, 'trees', 4096, 'geom') AS mvt
FROM rows
WHERE feature.geom IS NOT NULL AND NOT ST_IsEmpty(feature.geom)
GROUP BY xtile, ytile
`

    activeDuckdbCalls += 1
    try {
      logIconTileDebug('zoom-batch:start', { z, rev, lodMode, simplifyGridMeters, activeDuckdbCalls })
      const queryStartedAt = nowMs()
      const result = await conn.query(sql)
      const queryMs = nowMs() - queryStartedAt
      recordDuckdbTiming('zoom-batch', queryMs, { z, rev, lodMode, simplifyGridMeters })
      const rows = result.toArray()
      let stored = 0
      if (rev === tileQueryRevision) {
        for (const row of rows) {
          const xtile = Number(row.xtile)
          const ytile = Number(row.ytile)
          const raw = row.mvt as Uint8Array | undefined
          const tile = !raw || raw.length === 0
            ? new Uint8Array()
            : new Uint8Array(raw.buffer.slice(raw.byteOffset, raw.byteOffset + raw.byteLength))
          setCachedTile(tileCacheKeyForRevision(rev, z, xtile, ytile), tile)
          stored += 1
        }
        zoomBatchReady.add(bKey)
      }
      logIconTileDebug('zoom-batch:done', {
        z,
        rev,
        lodMode,
        simplifyGridMeters,
        stored,
        ms: Math.round(nowMs() - startedAt),
      })
    } finally {
      activeDuckdbCalls -= 1
    }
  })()

  inflightZoomBatch.set(bKey, request)
  try {
    await request
  } finally {
    inflightZoomBatch.delete(bKey)
  }
}

async function generatePointTileMvt(z: number, x: number, y: number): Promise<Uint8Array> {
  if (!conn) return new Uint8Array()
  activeTileRequests += 1
  try {
    const sqlBuiltAt = nowMs()
    const tileStartedAt = sqlBuiltAt
    const key = tileCacheKey(z, x, y)
    const isIconRange = z >= 15

    // When the user is zoomed into icon/detail ranges, aggressively drop low-zoom
    // tile work that MapLibre may still request during rapid zoom transitions.
    // This prevents long DuckDB queue backlogs that delay visible high-zoom tiles.
    if (activeViewportZoom.value >= 15 && z <= 14) {
      logIconTileDebug('dropped-stale-lowzoom', {
        z,
        x,
        y,
        rev: tileQueryRevision,
        viewportZoom: Number(activeViewportZoom.value.toFixed(2)),
      })
      return new Uint8Array()
    }

    const cached = getCachedTile(key)
    if (cached) {
      return cached
    }

    const inflight = inflightTileRequests.get(key)
    if (inflight) {
      logIconTileDebug('inflight-hit', { z, x, y, rev: tileQueryRevision, iconRange: isIconRange })
      return inflight
    }

    const baseQuery = sanitizeBaseQuery(tileQuerySql.value)
    const simplifyGridMeters = z <= 10 ? 256 : z <= 12 ? 128 : z <= 13 ? 64 : z <= 14 ? 32 : 0
    const lodMode = simplifyGridMeters > 0 ? 'aggregated' : 'detailed'
    const bounds = tileBounds3857(z, x, y)
    const batchKey = zoomBatchKey(tileQueryRevision, z)

    if (shouldUseZoomBatch(z)) {
      await ensureZoomBatchTiles(z, baseQuery, simplifyGridMeters)
      const batched = getCachedTile(key)
      if (batched) {
        return batched
      }

      // If zoom batch finished and this tile was not emitted, treat it as empty.
      // Avoid falling back to one-per-tile queries for empty tiles.
      if (zoomBatchReady.has(batchKey)) {
        const emptyTile = new Uint8Array()
        setCachedTile(key, emptyTile)
        return new Uint8Array(emptyTile)
      }
    }

    if (simplifyGridMeters === 0 && z >= 17) {
      await ensureNeighborhoodBatchTiles(z, x, y, baseQuery)
      const neighborBatched = getCachedTile(key)
      if (neighborBatched) return neighborBatched
    }

    logIconTileDebug('tile-start', {
      z,
      x,
      y,
      rev: tileQueryRevision,
      lodMode,
      simplifyGridMeters,
      iconRange: isIconRange,
      activeTileRequests,
      activeDuckdbCalls,
    })
    const request = (async () => {
    const sql = simplifyGridMeters > 0 ? `
WITH base AS (
  ${baseQuery}
), pts AS (
  SELECT
    tf.x_3857,
    tf.y_3857,
    COALESCE(base.diameter_at_breast_height, tf.dbh, 3) AS dbh
  FROM base
  INNER JOIN trees_fast tf
    ON base.tree_id = tf.tree_id
  WHERE tf.x_3857 BETWEEN ${bounds.minX} AND ${bounds.maxX}
    AND tf.y_3857 BETWEEN ${bounds.minY} AND ${bounds.maxY}
), agg AS (
  SELECT
    floor(x_3857 / ${simplifyGridMeters}) * ${simplifyGridMeters} + ${simplifyGridMeters} / 2.0 AS gx,
    floor(y_3857 / ${simplifyGridMeters}) * ${simplifyGridMeters} + ${simplifyGridMeters} / 2.0 AS gy,
    AVG(dbh) AS dbh,
    COUNT(*) AS point_count
  FROM pts
  GROUP BY 1, 2
), tiles AS (
  SELECT {
    'geometry': ST_AsMVTGeom(
      ST_Point(gx, gy),
      ST_Extent(ST_TileEnvelope(${z}, ${x}, ${y})),
      4096,
      64,
      true
    ),
    'id': -1,
    'dbh': TRY_CAST(dbh AS DOUBLE),
    'category': 'default',
    'rotation': 0,
    'point_count': TRY_CAST(point_count AS INTEGER)
  } AS feature
  FROM agg
)
SELECT ST_AsMVT(feature, 'trees', 4096, 'geometry') AS mvt
FROM tiles
WHERE feature.geometry IS NOT NULL AND NOT ST_IsEmpty(feature.geometry)
` : `
WITH base AS (
  ${baseQuery}
), points AS (
  SELECT
    COALESCE(base.tree_id, tf.tree_id, 0) AS id,
    COALESCE(base.diameter_at_breast_height, tf.dbh, 3) AS dbh,
    COALESCE(tf.tree_category, 'default') AS category,
    0 AS rotation,
    ST_AsMVTGeom(
      ST_Point(tf.x_3857, tf.y_3857),
      ST_Extent(ST_TileEnvelope(${z}, ${x}, ${y})),
      4096,
      64,
      true
    ) AS geom
  FROM base
  INNER JOIN trees_fast tf
    ON base.tree_id = tf.tree_id
  WHERE tf.x_3857 BETWEEN ${bounds.minX} AND ${bounds.maxX}
    AND tf.y_3857 BETWEEN ${bounds.minY} AND ${bounds.maxY}
  LIMIT 50000
)
SELECT ST_AsMVT(points, 'trees', 4096, 'geom') AS mvt
FROM points
WHERE geom IS NOT NULL
`

    const submittedAt = nowMs()
    activeDuckdbCalls += 1
    const duckdbQueueDepthAtSubmit = activeDuckdbCalls
    let duckdbDoneAt = submittedAt
    const result = await (async () => {
      try {
        const r = await conn.query(sql)
        duckdbDoneAt = nowMs()
        return r
      } finally {
        activeDuckdbCalls -= 1
      }
    })()
    recordDuckdbTiming('tile', duckdbDoneAt - submittedAt, {
      z,
      x,
      y,
      rev: tileQueryRevision,
      lodMode,
    })
    const raw = result.get(0)?.mvt as Uint8Array | undefined
    const tile = !raw || raw.length === 0
      ? new Uint8Array()
      : new Uint8Array(raw.buffer.slice(raw.byteOffset, raw.byteOffset + raw.byteLength))
    logIconTileDebug('tile-done', {
      z,
      x,
      y,
      rev: tileQueryRevision,
      lodMode,
      simplifyGridMeters,
      bytes: tile.byteLength,
      ms: Math.round(nowMs() - tileStartedAt),
      sqlBuildMs: Math.round(submittedAt - sqlBuiltAt),
      beforeDuckdbMs: Math.round(submittedAt - tileStartedAt),
      duckdbMs: Math.round(duckdbDoneAt - submittedAt),
      afterDuckdbMs: Math.round(nowMs() - duckdbDoneAt),
      duckdbQueueDepthAtSubmit,
      activeTileRequests,
      activeDuckdbCalls,
      iconRange: isIconRange,
    })
    setCachedTile(key, tile)
    return new Uint8Array(tile)
    })()

    inflightTileRequests.set(key, request)
    try {
      return await request
    } finally {
      inflightTileRequests.delete(key)
    }
  } finally {
    activeTileRequests -= 1
  }
}

export function useDuckDB() {
  // Initialize eagerly (single shared promise)
  if (!initPromise) {
    initPromise = doInit().catch((e) => {
      initError.value = (e as Error).message
      console.error('DuckDB init failed:', e)
    })
  }

  async function ensureInit() {
    await initPromise
  }

  function setTileQuery(sql: string | null) {
    tileQuerySql.value = sql
    tileQueryRevision += 1
    tileCache.clear()
    inflightTileRequests.clear()
    zoomBatchReady.clear()
    inflightZoomBatch.clear()
    inflightNeighborhoodBatch.clear()
    resetTimingBuckets()
  }

  function setViewportZoom(zoom: number) {
    if (!Number.isFinite(zoom)) return
    activeViewportZoom.value = zoom
  }

  function setViewportCenter(lng: number, lat: number) {
    if (!Number.isFinite(lng) || !Number.isFinite(lat)) return
    activeViewportCenter.value = { lng, lat }
  }

  async function ensureTileProtocolRegistered() {
    if (protocolRegistered) return
    await ensureInit()
    maplibregl.addProtocol('duckdb', async (params) => {
      const parsed = parseDuckdbTileUrl(params.url)
      if (!parsed) return { data: new Uint8Array() }
      const tile = await queueTileRequest(parsed.z, parsed.x, parsed.y)
      return { data: tile }
    })
    protocolRegistered = true
  }

  async function query(sql: string): Promise<{ columns: string[]; rows: Record<string, unknown>[] }> {
    await ensureInit()
    if (!conn) throw new Error('DuckDB not initialized')

    const result = await conn.query(sql)
    const columns = result.schema.fields.map((f) => f.name)
    const rows = result.toArray().map((row) => {
      const obj: Record<string, unknown> = {}
      for (const col of columns) {
        obj[col] = row[col]
      }
      return obj
    })
    return { columns, rows }
  }

  return {
    ready,
    initError,
    query,
    ensureInit,
    ensureTileProtocolRegistered,
    setTileQuery,
    setViewportZoom,
    setViewportCenter,
  }
}
