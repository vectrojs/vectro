#!/usr/bin/env node
// Vectro MCP server: lets an AI assistant (Claude, ChatGPT, any MCP client)
// load data files and ask analytics questions, answered by Vectro on all
// cores. Speaks the Model Context Protocol over stdio (JSON-RPC 2.0, one
// message per line), with no dependencies.
//
//   node mcp/server.mjs --root ~/data     (files outside --root are refused)
//
// Safety: the assistant only reads files under --root, and its expressions
// are Vectro's small math language (no code runs, no eval).
import { readFileSync, realpathSync, statSync } from 'node:fs';
import { resolve, sep, extname, basename } from 'node:path';
import { createInterface } from 'node:readline';
import { Engine, VectorIndex, sql } from '../src/node.mjs';
import { parseNDJSON } from '../src/ndjson.mjs';

const VERSION = '0.1.0';
const PROTOCOLS = ['2025-06-18', '2025-03-26', '2024-11-05'];
const MAX_ROWS = 500, DEFAULT_ROWS = 50, MAX_FILE_BYTES = 2 ** 31;

const args = process.argv.slice(2);
const rootArg = args.includes('--root') ? args[args.indexOf('--root') + 1] : process.cwd();
const ROOT = realpathSync(resolve(rootArg));
const engine = new Engine();
const tables = new Map(); // name -> { table, source, chain: tables to free when dropped (add_column builds on the previous one) }
const log = (...a) => process.stderr.write(`[vectro-mcp] ${a.join(' ')}\n`);

// ---- tools --------------------------------------------------------------------
const TOOLS = [
  {
    name: 'load_file',
    description: 'Load a CSV, TSV or JSON-lines file (one flat JSON object per line) from the data folder into a named table. Returns its columns, their kinds and a few sample rows. Text columns become categories (filterable, groupable).',
    inputSchema: {
      type: 'object',
      properties: {
        path: { type: 'string', description: 'File path, relative to the data folder' },
        name: { type: 'string', description: 'Table name to use in later calls (default: the file name without extension)' },
        types: { type: 'object', description: 'Optional column kinds: { column: "number" | "category" | "string" } (string = free text, returned but not filtered)', additionalProperties: { type: 'string' } },
      },
      required: ['path'],
    },
  },
  {
    name: 'list_tables',
    description: 'List the loaded tables with their row counts and columns.',
    inputSchema: { type: 'object', properties: {} },
  },
  {
    name: 'describe_table',
    description: 'Column statistics: for numbers min, max, mean and missing count; for categories the number of distinct labels and the most common ones.',
    inputSchema: { type: 'object', properties: { table: { type: 'string' } }, required: ['table'] },
  },
  {
    name: 'query',
    description: [
      'Answer a question about a table. All parts are optional except table.',
      'Expressions use column names, numbers, + - * /, comparisons (== != < <= > >=), && || !, cond ? a : b, and abs sqrt floor ceil min max. Text is compared with quotes: region == \'EU\'. Category text functions: startsWith(col, \'x\'), endsWith(col, \'x\'), contains(col, \'x\'), in(col, \'a\', \'b\'), like(col, \'EU-%\') (% any run, _ one character). Date columns compare with ISO text (ts >= \'2024-03-01\') and have year(ts), month(ts), day(ts), hour(ts), weekday(ts) (1 = Monday), dayStart(ts), weekStart(ts), monthStart(ts). Missing values: isMissing(x), coalesce(x, 0); sum, mean, min, max and count(x) skip them.',
      'filter: which rows, e.g. "region == \'EU\' && price > 100".',
      'aggregates: { name: "sum(expr)" | "mean(expr)" | "min(expr)" | "max(expr)" | "count()" | "count(condition)" | "countDistinct(expr)" }.',
      'group_by: a column (any text or number column) or a list of columns, to aggregate per group.',
      'sort: a column, aggregate name or expression ("-" first for largest first), or a list of them.',
      'limit: rows to return (default 50, at most 500).',
      'Without aggregates it returns matching rows; with aggregates and no group_by, one row of totals.',
    ].join(' '),
    inputSchema: {
      type: 'object',
      properties: {
        table: { type: 'string' },
        filter: { type: 'string' },
        group_by: { oneOf: [{ type: 'string' }, { type: 'array', items: { type: 'string' } }] },
        aggregates: { type: 'object', additionalProperties: { type: 'string' } },
        sort: { oneOf: [{ type: 'string' }, { type: 'array', items: { type: 'string' } }] },
        limit: { type: 'integer', minimum: 1, maximum: MAX_ROWS },
      },
      required: ['table'],
    },
  },
  {
    name: 'sql',
    description: [
      'Run one SQL SELECT over the loaded tables (by their names).',
      'SELECT [DISTINCT] ... FROM t [JOIN u ON t.x = u.y | USING (x)] [LEFT JOIN ...] WHERE ... GROUP BY ... HAVING ... ORDER BY ... [ASC|DESC] LIMIT n [OFFSET m].',
      'Aggregates: COUNT(*), COUNT(x), COUNT(DISTINCT x), SUM, AVG, MIN, MAX, APPROX_COUNT_DISTINCT. Also BETWEEN, IN, LIKE, IS NULL, CASE WHEN, COALESCE, ROUND, LEAST, GREATEST, CAST, DATE \'2024-01-01\', INTERVAL \'7 days\', EXTRACT(YEAR FROM ts), DATE_TRUNC(\'month\', ts), YEAR(ts), MONTH(ts).',
      'Text columns can be compared, matched (LIKE, IN) and grouped, not computed (no ||, UPPER...).',
      'At most 500 rows come back (add LIMIT); the answer says how many rows the query has.',
    ].join(' '),
    inputSchema: { type: 'object', properties: { query: { type: 'string' } }, required: ['query'] },
  },
  {
    name: 'search_similar',
    description: 'For tables loaded from JSON lines that have an embedding field (an array of numbers): the k rows whose embeddings are most similar (cosine) to a given row\'s, or to a given vector. Optional filter on the other columns.',
    inputSchema: {
      type: 'object',
      properties: {
        table: { type: 'string' },
        like: { type: 'object', description: 'The row to compare with, found by a column value, e.g. { "column": "id", "value": 42 }', properties: { column: { type: 'string' }, value: {} }, required: ['column', 'value'] },
        vector: { type: 'array', items: { type: 'number' }, description: 'Or an embedding to compare with (same length as the stored ones)' },
        k: { type: 'integer', minimum: 1, maximum: MAX_ROWS },
        filter: { type: 'string', description: 'Which rows may be returned, e.g. "year >= 2020"' },
      },
      required: ['table'],
    },
  },
  {
    name: 'add_column',
    description: 'Add a computed column to a table, e.g. name "revenue", expression "price * qty * (1 - discount)".',
    inputSchema: { type: 'object', properties: { table: { type: 'string' }, name: { type: 'string' }, expression: { type: 'string' } }, required: ['table', 'name', 'expression'] },
  },
  {
    name: 'drop_table',
    description: 'Unload a table and free its memory.',
    inputSchema: { type: 'object', properties: { table: { type: 'string' } }, required: ['table'] },
  },
];

class ToolError extends Error {}

function release(name) {
  const entry = tables.get(name);
  if (!entry) return;
  for (const t of entry.chain.reverse()) t.dispose();
  entry.index?.dispose();
  tables.delete(name);
}

function safePath(path) {
  if (typeof path !== 'string' || !path) throw new ToolError('path is required');
  let real;
  try { real = realpathSync(resolve(ROOT, path)); } catch { throw new ToolError(`no such file: ${path}`); }
  if (real !== ROOT && !real.startsWith(ROOT + sep)) throw new ToolError('that file is outside the data folder this server may read');
  const st = statSync(real);
  if (!st.isFile()) throw new ToolError(`not a file: ${path}`);
  if (st.size > MAX_FILE_BYTES) throw new ToolError('file is larger than 2 GB');
  return real;
}

function getTable(name) {
  const entry = tables.get(name);
  if (!entry) throw new ToolError(`no table "${name}" (loaded: ${[...tables.keys()].join(', ') || 'none'}; use load_file)`);
  return entry;
}

const kindOf = (c) => (c.strings ? 'text' : c.dict ? 'category' : c.date ? 'date' : 'number');
const clean = (rows) => rows.map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, v instanceof Date ? v.toISOString() : typeof v === 'number' && !Number.isFinite(v) ? null : v])));
const columnsOf = (t) => Object.fromEntries([...t.cols].map(([n, c]) => [n, kindOf(c)]));

// JSON lines with an array-of-numbers field: parse every line with JSON.parse
// (slower, but the embedding is needed), build the table and a vector index.
async function loadWithEmbeddings(bytes, field, dim, kinds) {
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

function loadJSONLines(bytes, types) {
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
  if (arrays.size) { const [field, dim] = arrays.entries().next().value; return loadWithEmbeddings(bytes, field, dim, kinds); }
  const names = [...kinds].filter(([, v]) => v).map(([k]) => k);
  if (!names.length) throw new ToolError('no JSON objects with plain values found in the first lines');
  const r = parseNDJSON(bytes, 0, bytes.length, names, names.map((n) => kinds.get(n)));
  const cols = Object.fromEntries(names.map((n, k) => [n, r.dicts[k] ? Array.from(r.cols[k], (code) => r.dicts[k][code]) : r.cols[k]]));
  return { table: engine.table(cols), skipped: r.bad };
}

const handlers = {
  sql({ query }) {
    const all = Object.fromEntries([...tables].map(([n, e]) => [n, e.table]));
    const t = sql(query, all).collect();
    try {
      const rows = clean(t.toRows(MAX_ROWS));
      return { rows_in_answer: t.length, returned: rows.length, rows };
    } finally { t.dispose(); }
  },
  async load_file({ path, name, types }) {
    const real = safePath(path);
    const ext = extname(real).toLowerCase();
    const tname = name || basename(real, extname(real)).replace(/[^A-Za-z0-9_]/g, '_');
    const bytes = new Uint8Array(readFileSync(real));
    let table, skipped = 0, index = null, embedding = null;
    if (ext === '.jsonl' || ext === '.ndjson') ({ table, skipped, index = null, embedding = null } = await loadJSONLines(bytes, types));
    else table = engine.parseCSV(bytes, { types: types ?? {}, delimiter: ext === '.tsv' ? '\t' : ',' });
    release(tname);
    tables.set(tname, { table, source: path, chain: [table], index });
    return { table: tname, rows: table.length, columns: columnsOf(table), sample: clean(table.toRows(5)), ...(embedding ? { embedding } : {}), ...(skipped ? { skipped_lines: skipped } : {}) };
  },
  list_tables() {
    return [...tables].map(([name, { table, source }]) => ({ table: name, source, rows: table.length, columns: columnsOf(table) }));
  },
  describe_table({ table: name }) {
    const { table } = getTable(name);
    const out = {};
    for (const [col, c] of table.cols) {
      if (c.strings) { out[col] = { kind: 'text' }; continue; }
      if (c.dict) {
        const g = table.groupBy(col).agg({ rows: 'count()' });
        const top = clean(g.top(10, ['-rows', col]).toRows());
        out[col] = { kind: 'category', distinct: g.length, most_common: top };
        g.dispose();
        continue;
      }
      const a = table.filter(`${col} == ${col}`).agg({ n: 'count()', min: `min(${col})`, max: `max(${col})`, mean: `mean(${col})` });
      if (c.date) { // ISO text; filter with ts >= '2024-01-01', year(ts), month(ts)...
        const iso = (v) => (Number.isFinite(v) ? new Date(v).toISOString() : null);
        out[col] = { kind: 'date', min: iso(a.min), max: iso(a.max), missing: table.length - a.n };
        continue;
      }
      out[col] = { kind: 'number', ...clean([a])[0], missing: table.length - a.n };
    }
    return { table: name, rows: table.length, columns: out };
  },
  query({ table: name, filter, group_by: groupBy, aggregates, sort, limit = DEFAULT_ROWS }) {
    const entry = getTable(name);
    const lim = Math.max(1, Math.min(MAX_ROWS, Math.floor(limit)));
    const view = filter ? entry.table.filter(filter) : entry.table;
    const hasAgg = aggregates && Object.keys(aggregates).length;
    if (groupBy) {
      if (!hasAgg) throw new ToolError('group_by needs aggregates, e.g. { "orders": "count()" }');
      const g = view.groupBy(groupBy).agg(aggregates);
      try {
        const groups = g.length;
        const t = sort ? g.top(lim, sort) : null;
        const rows = clean((t ?? g).toRows(lim));
        t?.dispose();
        return { groups, returned: rows.length, rows };
      } finally { g.dispose(); }
    }
    if (hasAgg) return { rows: clean([view.agg(aggregates)]) };
    const matching = filter ? view.count() : view.length;
    const t = sort ? view.top(lim, sort) : null;
    const rows = clean((t ?? view).toRows(lim));
    t?.dispose();
    return { matching_rows: matching, returned: rows.length, rows };
  },
  async search_similar({ table: name, like, vector, k = 10, filter }) {
    const entry = getTable(name);
    if (!entry.index) throw new ToolError(`table "${name}" has no embeddings (load a JSON-lines file with an array-of-numbers field)`);
    let q = vector;
    if (!q) {
      if (!like?.column) throw new ToolError('give like: { column, value } or vector');
      const value = typeof like.value === 'string' ? `'${like.value.replaceAll("'", '')}'` : Number(like.value);
      const dims = Array.from({ length: entry.index.dim }, (_, j) => `v${j}`);
      const [row] = entry.index.table.filter(`${like.column} == ${value}`).select(...dims).toRows(1);
      if (!row) throw new ToolError(`no row where ${like.column} is ${JSON.stringify(like.value)}`);
      q = dims.map((d) => row[d]);
    }
    const hits = await entry.index.searchAsync(q, { k: Math.max(1, Math.min(MAX_ROWS, Math.floor(k))), filter });
    return { returned: hits.length, rows: clean(hits) };
  },
  add_column({ table: name, name: col, expression }) {
    const entry = getTable(name);
    const derived = entry.table.derive(col, expression); // shares the old columns
    entry.table = derived;
    entry.chain.push(derived);
    return { table: name, columns: columnsOf(derived), sample: clean(derived.toRows(3)) };
  },
  drop_table({ table: name }) {
    getTable(name);
    release(name);
    return { dropped: name };
  },
};

// ---- JSON-RPC over stdio ----------------------------------------------------------
const send = (msg) => process.stdout.write(`${JSON.stringify({ jsonrpc: '2.0', ...msg })}\n`);

async function handle(msg) {
  const { id, method, params } = msg;
  const reply = (result) => id !== undefined && send({ id, result });
  const error = (code, message) => id !== undefined && send({ id, error: { code, message } });
  switch (method) {
    case 'initialize': {
      const asked = params?.protocolVersion;
      return reply({
        protocolVersion: PROTOCOLS.includes(asked) ? asked : PROTOCOLS[0],
        capabilities: { tools: { listChanged: false } },
        serverInfo: { name: 'vectro', title: 'Vectro analytics', version: VERSION },
        instructions: `Load CSV or JSON-lines files from ${ROOT} with load_file, then answer questions with query (filter, group_by, aggregates, sort, limit). Use describe_table to see columns and common values first.`,
      });
    }
    case 'notifications/initialized':
    case 'notifications/cancelled':
      return undefined;
    case 'ping':
      return reply({});
    case 'tools/list':
      return reply({ tools: TOOLS });
    case 'tools/call': {
      const fn = handlers[params?.name];
      if (!fn) return error(-32602, `unknown tool: ${params?.name}`);
      try {
        const t0 = performance.now();
        const result = await fn(params.arguments ?? {});
        const ms = Math.round((performance.now() - t0) * 100) / 100;
        const body = Array.isArray(result) ? { tables: result, ms } : { ...result, ms };
        return reply({ content: [{ type: 'text', text: JSON.stringify(body) }], structuredContent: body, isError: false });
      } catch (err) {
        // tool errors go back to the model, so it can fix its call
        return reply({ content: [{ type: 'text', text: err instanceof ToolError || /^vectro:/.test(err.message) ? err.message : `error: ${err.message}` }], isError: true });
      }
    }
    default:
      if (method?.startsWith('notifications/')) return undefined;
      return error(-32601, `method not found: ${method}`);
  }
}

let queue = Promise.resolve();
const rl = createInterface({ input: process.stdin });
rl.on('line', (line) => {
  if (!line.trim()) return;
  let msg;
  try { msg = JSON.parse(line); } catch { return send({ id: null, error: { code: -32700, message: 'parse error' } }); }
  for (const m of Array.isArray(msg) ? msg : [msg]) queue = queue.then(() => handle(m)); // one call at a time, in order
});
rl.on('close', () => queue.then(() => engine.close()).then(() => process.exit(0)));
log(`ready; data folder ${ROOT}`);
