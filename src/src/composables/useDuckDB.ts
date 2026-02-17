import { ref } from 'vue'
import * as duckdb from '@duckdb/duckdb-wasm'
import duckdb_wasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url'
import duckdb_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url'

let db: duckdb.AsyncDuckDB | null = null
let conn: duckdb.AsyncDuckDBConnection | null = null
const ready = ref(false)
const initError = ref<string | null>(null)

const TABLE_DDL = `
CREATE TABLE IF NOT EXISTS trees (
  tree_id INTEGER,
  common_name VARCHAR,
  q_site_info VARCHAR,
  plant_date VARCHAR,
  q_species VARCHAR,
  latitude DOUBLE,
  longitude DOUBLE,
  diameter_at_breast_height DOUBLE
);
`

let initPromise: Promise<void> | null = null

async function doInit() {
  if (db) return

  const logger = new duckdb.ConsoleLogger()
  const worker = new Worker(duckdb_worker)
  db = new duckdb.AsyncDuckDB(logger, worker)
  await db.instantiate(duckdb_wasm)

  conn = await db.connect()
  await conn.query(TABLE_DDL)

  // Fetch tree JSON and register in DuckDB virtual filesystem
  const res = await fetch(import.meta.env.BASE_URL + 'data/raw_data.json')
  const jsonText = await res.text()
  await db.registerFileText('trees.json', jsonText)

  await conn.query(`INSERT INTO trees SELECT * FROM read_json_auto('trees.json')`)

  ready.value = true
}

export function useDuckDB() {
  // Ensure init runs only once
  if (!initPromise) {
    initPromise = doInit().catch((e) => {
      initError.value = (e as Error).message
      console.error('DuckDB init failed:', e)
    })
  }

  async function query(sql: string): Promise<{ columns: string[]; rows: Record<string, unknown>[] }> {
    await initPromise
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

  return { ready, initError, query }
}
