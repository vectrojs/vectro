// Tables from files in a data folder, for the servers (the HTTP SQL server
// in http.mjs, the MCP server in mcp/): CSV / TSV, JSON lines (with a vector
// index when the lines carry embeddings), Arrow / Feather files and .vectro
// datasets (opened on disk, not loaded). Files outside the folder are refused.
import { readFileSync, readdirSync, realpathSync, statSync } from 'node:fs';
import { resolve, sep, extname, basename, join } from 'node:path';
import { VectorIndex } from '../vectors.mjs';
import { Engine } from '../node.mjs';
import { parseNDJSON, textKind } from '../ndjson.mjs';
import { cleanNames } from '../csv.mjs';

export const MAX_FILE_BYTES = 2 ** 31;
export const FREE_TEXT_ROWS = 5_000_000;
export const LOADABLE = new Set(['.csv', '.tsv', '.jsonl', '.ndjson', '.arrow', '.feather', '.ipc', '.vectro']);

// (a mistake by whoever asked: shown to them as it is)
export class LoadError extends Error {}

export const kindOf = (c) => (c.strings ? 'text' : c.dict ? 'category' : c.date ? 'date' : 'number');
export const columnsOf = (t) => Object.fromEntries([...t.cols].filter(([, c]) => !c.hidden).map(([n, c]) => [n, kindOf(c)]));
export const tableName = (file) => basename(file, extname(file)).replace(/[^\p{L}\p{M}\p{N}_]/gu, '_').replace(/^(\d)/, '_$1');

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
  if (ext === '.csv' || ext === '.tsv' || ext === '.txt') {
    const delimiter = ext === '.tsv' ? '\t' : ',';
    // (async: queries other clients are running on the engine go on meanwhile)
    const table = await engine.parseCSVAsync(bytes, { types: types ?? {}, delimiter });
    // text that rarely repeats (names, titles) loads as free text, which can be
    // shown but not grouped, filtered or joined on: tools need that, so up to
    // FREE_TEXT_ROWS rows it is loaded again as categories
    const free = [...table.cols].filter(([n, c]) => c.strings && !(types && n in types)).map(([n]) => n);
    if (!free.length || table.length > FREE_TEXT_ROWS) return { table };
    table.dispose();
    return { table: await engine.parseCSVAsync(bytes, { types: { ...types, ...Object.fromEntries(free.map((n) => [n, 'category'])) }, delimiter }) };
  }
  throw new LoadError(`can't load ${ext || 'files without an extension'} (CSV, TSV, JSON lines, Arrow / Feather or .vectro)`);
}

// Every loadable file directly in root: [{ name, path }].
export function filesIn(root) {
  return readdirSync(root, { withFileTypes: true })
    .filter((d) => d.isFile() && LOADABLE.has(extname(d.name).toLowerCase()))
    .map((d) => d.name).sort()
    .map((f) => ({ name: tableName(f), path: join(root, f) }));
}

// A table's memory back (datasets closed).
export function release(table) { if (table?.isDataset) table.close(); else table?.dispose(); }

// Every object is a row of the table; the ones with a vector (`field`, `dim` numbers) also go
// into the vector index, with their columns.
async function loadWithEmbeddings(engine, bytes, field, dim, kinds, types) {
  const names = [...kinds].filter(([k, v]) => v && k !== field).map(([k]) => k);
  const clean = cleanNames(names);
  const cols = Object.fromEntries(clean.map((n) => [n, []])), withVector = Object.fromEntries(clean.map((n) => [n, []]));
  const vectors = [];
  let skipped = 0, late = false;
  for (const line of new TextDecoder().decode(bytes).split('\n')) {
    if (!line.trim()) continue;
    let o;
    try { o = JSON.parse(line.replace(/^\uFEFF/, '')); } catch { skipped++; continue; }
    if (!o || typeof o !== 'object' || Array.isArray(o)) { skipped++; continue; }
    const v = o[field], hasVector = Array.isArray(v) && v.length === dim && v.every((x) => typeof x === 'number');
    if (hasVector) vectors.push(v);
    for (const [n, x] of Object.entries(o)) { // after the first lines: text in a number column, new keys (loaded again with them)
      if (n === field || (types && n in types) || x === null || typeof x === 'object') continue;
      const kind = typeof x === 'string' ? textKind(x) : 'number';
      if (!kinds.get(n) || (kinds.get(n) === 'number' && kind === 'category')) { kinds.set(n, kinds.get(n) || kind === 'blank' ? 'category' : kind); late = true; }
    }
    if (late) continue;
    names.forEach((n, k) => {
      const x = o[n];
      const value = kinds.get(n) === 'number' ? (typeof x === 'number' ? x : typeof x === 'boolean' ? +x : typeof x === 'string' && x.trim() ? Number(x) : NaN) : x == null ? '' : typeof x === 'object' ? JSON.stringify(x) : String(x); // (nested: its JSON text)
      cols[clean[k]].push(value);
      if (hasVector) withVector[clean[k]].push(value);
    });
  }
  if (late) return loadWithEmbeddings(engine, bytes, field, dim, kinds, types);
  const table = engine.table(cols);
  const index = await VectorIndex.create(engine, { vectors, dim, metric: 'cosine', columns: withVector });
  return { table, index, skipped, embedding: { field, dim } };
}

function loadJSONLines(engine, bytes, types) {
  // column names and kinds from the first 1,000 lines
  const kinds = new Map(), arrays = new Map(), notVector = new Set();
  const text = new TextDecoder().decode(bytes.subarray(0, Math.min(bytes.length, 1 << 20)));
  for (const line of text.split('\n').slice(0, 1000)) {
    let o;
    try { o = JSON.parse(line.replace(/^\uFEFF/, '')); } catch { continue; }
    if (!o || typeof o !== 'object' || Array.isArray(o)) continue;
    for (const [k, v] of Object.entries(o)) {
      // (a vector field: a list of numbers of one length in every sampled row that has the key; a key
      // also holding other values is a plain column, nested values in it missing)
      if (v !== null) {
        const vec = Array.isArray(v) && v.length && v.every((x) => typeof x === 'number');
        if (vec && !notVector.has(k) && (!arrays.has(k) || arrays.get(k) === v.length)) arrays.set(k, v.length);
        else { notVector.add(k); arrays.delete(k); }
      }
      if (v === null || typeof v === 'object') { if (!kinds.has(k)) kinds.set(k, null); continue; }
      const kind = typeof v === 'string' ? textKind(v) : 'number'; // (numeric strings stay numbers; blank text changes nothing)
      if (kind === 'blank') { if (!kinds.get(k)) kinds.set(k, 'blank'); }
      else if (kinds.get(k) !== 'category') kinds.set(k, kind);
    }
  }
  for (const [k, v] of kinds) if (v === 'blank' || (v === null && !arrays.has(k))) kinds.set(k, 'category'); // (only nested or null values: a text column, nested values as their JSON text; every key is a column)
  for (const [k, v] of Object.entries(types ?? {})) kinds.set(k, v === 'number' ? 'number' : 'category');
  if (arrays.size) { const [field, dim] = arrays.entries().next().value; return loadWithEmbeddings(engine, bytes, field, dim, kinds, types); }
  let names = [...kinds].filter(([, v]) => v).map(([k]) => k);
  if (!names.length) throw new LoadError('no JSON objects with plain values found in the first lines');
  const seen = {};
  let r = parseNDJSON(bytes, 0, bytes.length, names, names.map((n) => kinds.get(n)), seen, { nestedText: true });
  // after the first lines: text in a number column (loaded as text, as CSV does) and new keys
  const given = (n) => types && n in types;
  const late = names.filter((n, k) => seen.text[k] && !given(n));
  for (const n of late) kinds.set(n, 'category');
  for (const [n, kind] of seen.extra) if (!kinds.get(n)) { late.push(n); kinds.set(n, kind === 'blank' ? 'category' : kind); }
  if (late.length) {
    names = [...kinds].filter(([, v]) => v).map(([k]) => k);
    r = parseNDJSON(bytes, 0, bytes.length, names, names.map((n) => kinds.get(n)), null, { nestedText: true });
  }
  const clean = cleanNames(names); // ("@timestamp", "user-agent": names that work in expressions, as CSV headers get)
  const cols = Object.fromEntries(names.map((n, k) => [clean[k], r.dicts[k] ? Array.from(r.cols[k], (code) => r.dicts[k][code]) : r.cols[k]]));
  return { table: engine.table(cols), skipped: r.bad };
}

// The tables a server serves: given in code, or the files in a data folder
// (reload() reads it again). Shared by the HTTP and PostgreSQL servers.
//   { engine, entries: Map name -> { table, source, owned }, named(), reload(), hold(), whenFree(fn), version, close() }
// A server calls hold() before it reads the tables and the function it returns
// once it's done with them: reload() frees the tables it replaces only when
// every hold taken before it has ended, so queries running meanwhile finish
// on the tables they started with.
export async function openTables({ tables = {}, root = null, engine = null, log = () => {} } = {}) {
  const own = !engine && !Object.values(tables).some((t) => t?.engine);
  engine ??= Object.values(tables).find((t) => t?.engine)?.engine ?? new Engine();
  const entries = new Map();
  for (const [name, table] of Object.entries(tables)) entries.set(name, { table, source: 'given', owned: false });
  const rootPath = root ? realpathSync(resolve(String(root).replace(/^~(?=\/|$)/, process.env.HOME ?? '~'))) : null;
  // holds since the last reload; old: the tables a reload replaced, freed when its holds end
  let gen = { holds: 0, old: [], after: [] }, reloading = Promise.resolve();
  const waiting = new Set(); // earlier ones with holds still running
  const retire = (g) => { for (const t of g.old) release(t); g.old = []; waiting.delete(g); };
  const self = {
    engine, entries, root: rootPath, version: 0,
    named: () => Object.fromEntries([...entries].map(([n, e]) => [n, e.table])),
    hold() {
      const g = gen;
      g.holds++;
      let done = false;
      return () => {
        if (done || (done = true, --g.holds > 0)) return;
        for (const fn of g.after.splice(0)) fn();
        if (g !== gen) retire(g);
      };
    },
    // fn() once every hold taken so far has ended (now, if none is running): for things
    // queries may still read, such as a server's own tables made from the old ones
    whenFree(fn) {
      const busy = [...waiting, gen].filter((g) => g.holds > 0);
      if (!busy.length) return fn();
      let left = busy.length;
      for (const g of busy) g.after.push(() => { if (--left === 0) fn(); });
    },
    reload() { // (one at a time)
      const r = reloading.then(() => readFolder());
      reloading = r.catch(() => {});
      return r;
    },
    async close() {
      for (const e of entries.values()) if (e.owned) release(e.table);
      entries.clear();
      for (const g of [...waiting, gen]) for (const fn of g.after.splice(0)) fn();
      for (const g of waiting) retire(g);
      if (own) await engine.close();
    },
  };
  // the files loaded first, then swapped in for the old tables
  async function readFolder() {
    if (!rootPath) throw new LoadError('this server has no data folder to reload');
    const fresh = new Map(), loaded = [], failed = [];
    for (const { name, path } of filesIn(rootPath)) {
      if (entries.has(name) && !entries.get(name).owned) continue; // (tables given in code win)
      if (fresh.has(name)) { // (dup.csv and dup.jsonl, "a b.csv" and "a_b.csv": the first file in name order wins)
        const error = `same table name "${name}" as ${basename(fresh.get(name).source)}: rename one of the files`;
        failed.push({ file: path, error }); log(`skipped ${path}: ${error}`);
        continue;
      }
      try {
        const { table } = await loadFile(engine, path);
        fresh.set(name, { table, source: path, owned: true });
        loaded.push(name);
      } catch (err) { failed.push({ file: path, error: err.message }); log(`skipped ${path}: ${err.message}`); }
    }
    const was = gen;
    for (const [name, e] of entries) if (e.owned) { was.old.push(e.table); entries.delete(name); }
    for (const [name, e] of fresh) entries.set(name, e);
    gen = { holds: 0, old: [], after: [] };
    if (was.holds === 0) retire(was); else waiting.add(was);
    self.version++;
    return { loaded, failed };
  }
  if (rootPath) await self.reload();
  return self;
}

export { realpathSync };
