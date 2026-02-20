import { ref } from 'vue'
import * as duckdb from '@duckdb/duckdb-wasm'
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

let initPromise: Promise<void> | null = null

function nowMs(): number {
  return typeof performance !== 'undefined' ? performance.now() : Date.now()
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
  console.info('[Perf] duckdb:insert:done', { ms: Math.round(nowMs() - tAfterSchema) })

  ready.value = true
  console.info('[Perf] duckdb:init:done', { ms: Math.round(nowMs() - t0) })
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

  return { ready, initError, query, ensureInit }
}
