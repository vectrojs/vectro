// Tables from files in a data folder, for the servers (the HTTP SQL server
// in http.mjs, the MCP server in mcp/): CSV / TSV, JSON lines (with a vector
// index when the lines carry embeddings), Arrow / Feather files and .vectro
// datasets (opened on disk, not loaded). Files outside the folder are refused.
import { readFileSync, readdirSync, realpathSync, statSync } from 'node:fs';
import { resolve, sep, extname, basename, join } from 'node:path';
import { VectorIndex } from '../vectors.mjs';
import { parseNDJSON } from '../ndjson.mjs';

export const MAX_FILE_BYTES = 2 ** 31;
export const LOADABLE = new Set(['.csv', '.tsv', '.jsonl', '.ndjson', '.arrow', '.feather', '.ipc', '.vectro']);

// (a mistake by whoever asked: shown to them as it is)
export class LoadError extends Error {}

export const kindOf = (c) => (c.strings ? 'text' : c.dict ? 'category' : c.date ? 'date' : 'number');
export const columnsOf = (t) => Object.fromEntries([...t.cols].map(([n, c]) => [n, kindOf(c)]));
export const tableName = (file) => basename(file, extname(file)).replace(/[^A-Za-z0-9_]/g, '_').replace(/^(\d)/, '_$1');

// A path inside root (symbolic links resolved), or a LoadError.
export function safePath(root, path) {
  if (typeof path !== 'string' || !path) throw new LoadError('path is required');
  let real;
  try { real = realpathSync(resolve(root, path)); } catch { throw new LoadError(`no such file: ${path}`); }
  if (real !== root && !real.startsWith(root + sep)) throw new LoadError('that file is outside the data folder this server may read');
  const st = statSync(real);
  if (!st.isFile()) throw new LoadError(`not a file: ${path}`);
  if (st.size > MAX_FILE_BYTES && extname(real).toLowerCase() !== '.vectro') throw new LoadError('file is larger than 2 GB (convert it to a .vectro dataset)');
  return real;
}

// One file as a table: { table, index?, embedding?, skipped?, onDisk? }.
export async function loadFile(engine, real, { types } = {}) {
  const ext = extname(real).toLowerCase();
  if (ext === '.vectro') return { table: engine.openDataset(real), onDisk: true };
  const bytes = new Uint8Array(readFileSync(real));
  if (ext === '.arrow' || ext === '.feather' || ext === '.ipc') return { table: engine.fromArrow(bytes) };
  if (ext === '.jsonl' || ext === '.ndjson') return loadJSONLines(engine, bytes, types);
  if (ext === '.csv' || ext === '.tsv' || ext === '.txt') return { table: engine.parseCSV(bytes, { types: types ?? {}, delimiter: ext === '.tsv' ? '\t' : ',' }) };
  throw new LoadError(`can't load ${ext || 'files without an extension'} (CSV, TSV, JSON lines, Arrow / Feather or .vectro)`);
}

// Every loadable file directly in root: [{ name, path }].
export function filesIn(root) {
  return readdirSync(root, { withFileTypes: true })
    .filter((d) => d.isFile() && LOADABLE.has(extname(d.name).toLowerCase()))
    .map((d) => ({ name: tableName(d.name), path: join(root, d.name) }));
}

// A table's memory back (datasets closed).
export function release(table) { if (table?.isDataset) table.close(); else table?.dispose(); }

async function loadWithEmbeddings(engine, bytes, field, dim, kinds) {
  const names = [...kinds].filter(([k, v]) => v && k !== field).map(([k]) => k);
  const cols = Object.fromEntries(names.map((n) => [n, []]));
  const vectors = [];
  let skipped = 0;
  for (const line of new TextDecoder().decode(bytes).split('\n')) {
    if (!line.trim()) continue;
    let o;
    try { o = JSON.parse(line); } catch { skipped++; continue; }
    const v = o?.[field];
    if (!o || typeof o !== 'object' || !Array.isArray(v) || v.length !== dim) { skipped++; continue; }
    vectors.push(v);
    for (const n of names) {
      const x = o[n];
      cols[n].push(kinds.get(n) === 'number' ? (typeof x === 'number' ? x : typeof x === 'boolean' ? +x : typeof x === 'string' && x.trim() ? Number(x) : NaN) : x == null || typeof x === 'object' ? '' : String(x));
    }
  }
  const table = engine.table(cols);
  const index = await VectorIndex.create(engine, { vectors, dim, metric: 'cosine', columns: cols });
  return { table, index, skipped, embedding: { field, dim } };
}

function loadJSONLines(engine, bytes, types) {
  // column names and kinds from the first 1,000 lines
  const kinds = new Map(), arrays = new Map();
  const text = new TextDecoder().decode(bytes.subarray(0, Math.min(bytes.length, 1 << 20)));
  for (const line of text.split('\n').slice(0, 1000)) {
    let o;
    try { o = JSON.parse(line); } catch { continue; }
    if (!o || typeof o !== 'object' || Array.isArray(o)) continue;
    for (const [k, v] of Object.entries(o)) {
      if (Array.isArray(v) && v.length && v.every((x) => typeof x === 'number')) { if (!arrays.has(k)) arrays.set(k, v.length); }
      if (v === null || typeof v === 'object') { if (!kinds.has(k)) kinds.set(k, null); continue; }
      const kind = typeof v === 'number' || typeof v === 'boolean' ? 'number' : 'category';
      if (kinds.get(k) !== 'category') kinds.set(k, kind);
    }
  }
  for (const [k, v] of Object.entries(types ?? {})) kinds.set(k, v === 'number' ? 'number' : 'category');
  if (arrays.size) { const [field, dim] = arrays.entries().next().value; return loadWithEmbeddings(engine, bytes, field, dim, kinds); }
  const names = [...kinds].filter(([, v]) => v).map(([k]) => k);
  if (!names.length) throw new LoadError('no JSON objects with plain values found in the first lines');
  const r = parseNDJSON(bytes, 0, bytes.length, names, names.map((n) => kinds.get(n)));
  const cols = Object.fromEntries(names.map((n, k) => [n, r.dicts[k] ? Array.from(r.cols[k], (code) => r.dicts[k][code]) : r.cols[k]]));
  return { table: engine.table(cols), skipped: r.bad };
}

export { realpathSync };
