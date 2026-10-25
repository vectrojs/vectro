# Vectro

Multi-core analytics inside any JavaScript app. Vectro keeps data in columns
and compiles each query, filter included, into one SIMD WebAssembly loop, then
runs it on every CPU core. It runs in Node, in browsers (any page) and on
Cloudflare Workers, with no native addon, compiler or server.

> Status: prototype (0.1.0). Not published yet. Formerly "nodeplus". MIT license.

```js
import { Engine } from 'vectrojs';

const engine = new Engine();                         // all cores
const orders = engine.readCSV('orders.csv');         // parsed in parallel

orders.filter("region == 'EU' && price > 100").agg({
  orders: 'count()',
  revenue: 'sum(price * qty)',
  avgPrice: 'mean(price)',
});                                                  // ~3 ms on 10M rows

orders.groupBy('region').agg({ revenue: 'sum(price * qty)' }).toRows();
```

## What it does

- **Tables:** `engine.table({ price: Float64Array, region: string[] })`,
  `engine.readCSV(path)`, `engine.parseCSV(text | bytes)`.
- **Queries:** `filter(expr)` (lazy), `agg({ name: 'sum(expr)' | 'mean' | 'min' | 'max' | 'count()' })`,
  `sum/mean/min/max/count(expr)`, `groupBy(key).agg(...)`, `derive(name, expr)`,
  `collect()`, `toRows(limit)`.
- **Sort and top-k:** `sort(by)` and `top(k, by)`, e.g. `top(10, '-revenue')`
  for the 10 largest. Keys are columns or expressions (`'-(price * qty)'`),
  several keys sort in order (`['region', '-price']`), text sorts by label,
  ties keep table order and missing values come last. `top` doesn't sort
  everything: it finds a cutoff from a sample, then collects only the rows
  past it, on all cores. Split tables and streams have `topAsync`.
- **Async API:** every query has an `…Async` version that runs on the worker
  threads while your server or page keeps working.
- **Expressions** are a small, safe language: numbers, columns, `+ - * /`,
  comparisons, `&& || !`, `? :`, `abs sqrt floor ceil min max`. There is no
  `eval`, so formulas typed by users or AI models are safe to run.

## For AI assistants (MCP server)

`mcp/server.mjs` lets Claude, ChatGPT or any MCP client analyze your data
files with Vectro: it loads CSV, TSV or JSON-lines files and answers questions
on all cores, locally. It needs no dependencies.

```bash
# Claude Code
claude mcp add vectro -- node /path/to/vectro/mcp/server.mjs --root ~/data
```

```json
// Claude Desktop: claude_desktop_config.json
{ "mcpServers": { "vectro": { "command": "node", "args": ["/path/to/vectro/mcp/server.mjs", "--root", "/Users/you/data"] } } }
```

Tools: `load_file`, `list_tables`, `describe_table` (column stats, common
labels), `query` (filter, group_by, aggregates, sort, limit),
`search_similar`, `add_column`, `drop_table`. A JSON-lines file with an
embedding field (an array of numbers) also gets a vector index, so the
assistant can ask for the rows most similar to a row (or to a vector), with a
filter. The assistant can only read files under `--root`, and its
expressions are Vectro's small math language (no code runs). Mistakes come
back as tool errors the assistant can read and fix. Answers include `ms`, the
time Vectro took.

## Vector search

Exact nearest-neighbour search over embeddings, on every core, in any
runtime (Node, browsers, edge), with filters on any other column:

```js
import { Engine, VectorIndex } from 'vectrojs';

const index = await VectorIndex.create(new Engine(), {
  vectors: embeddings,          // number[][] or one flat Float32Array
  dim: 384,
  metric: 'cosine',             // or 'dot', 'l2'
  columns: { id: ids, title: titles, year: years },
});
await index.searchAsync(queryEmbedding, { k: 10, filter: 'year >= 2020' });
// -> [{ id, title, year, score }, ...] best first
```

Embeddings are stored as float32 columns, the score is a fused expression
computed 24 dimensions per pass with the query as scalars, and `top(k)` picks
the best, so it reuses the compiled kernels, filters and split tables (pass a
`SplitEngine` to use Web Workers on any page). It is exact, not approximate:
no index to build or tune, results identical to brute force.

`npm run bench:vectors` (M1 Pro, 384 dimensions, top 10, cosine):

| Embeddings | Vectro | Plain JS loop (Float32Array) |
|---|---|---|
| 100,000 (154 MB) | 11.7 ms | 35.9 ms (3.1×) |
| 200,000 (307 MB) | 13.8 ms | 71.5 ms (5.2×) |
| 500,000 (768 MB) | 22.8 ms | 179.3 ms (7.9×) |

With a `SplitEngine`, every worker searches its own rows in one round trip
and the page keeps the best k. In Chrome (`examples/check/vectors.html?n=200000`),
8 Web Workers took 12.4 ms against 21 ms on the page's own thread; at 50,000
vectors the two were about equal (10.1 vs 8.8 ms). Keep the tab in front when
measuring: browsers slow down hidden tabs.

`table.select(...columns)` (a narrow view, no copy) and
`splitTable.topAsync(k, by, { columns })` keep top-k cheap on wide tables.

## Streaming (prototype)

A stream is a live table spread over split-table workers. Rows arrive as CSV
lines or JSON lines and are parsed in 256 KB batches into growing columns.
Queries are the same as for any split table.

```js
import { SplitEngine } from 'vectrojs';             // browsers: from 'vectrojs/browser'

const engine = new SplitEngine({ workers: 4 });     // default: one worker per core
const orders = await engine.streamAsync({
  format: 'ndjson',                                  // or 'csv' (no header)
  columns: { ts: 'number', price: 'number', qty: 'number', region: 'category' },
  keepRows: 2_000_000,                               // or keepMs: 60_000, timeColumn: 'ts'
});

// each worker reads its own feed: nothing passes through this thread
await Promise.all(engine.workers.map((_, w) =>
  orders.connect(w, { url: `wss://feed.example.com/orders?part=${w}` })));  // or { host, port } for TCP (Node)
// ...or push chunks you receive yourself: socket.on('data', (c) => orders.push(c))

// running totals, updated as rows arrive
const live = await orders.windowAsync({
  by: 'region', every: 1000, last: 300_000, timeColumn: 'ts',
  agg: { orders: 'count()', revenue: 'sum(price * qty)', avg: 'mean(price)' },
});
await live.readAsync();                  // per region, last 5 minutes (~1 ms)
await live.readAsync({ series: true });  // per second and region, for charts

await orders.filter("region == 'EU'").aggAsync({ big: 'count(price > 900)' });  // any query, over the rows kept
const fixed = await orders.snapshotAsync();                                      // a fixed copy, for derive and toRows
```

- **JSON lines** are parsed byte by byte straight into columns (no object per
  row): keys in any order, extra and nested keys skipped, `null` and missing
  values empty, bad lines counted (`badLines`) and skipped.
- **Windows** keep one small total per (slice of `every` ms, group) in each
  worker, so reading costs about a millisecond, and the totals outlive rows
  dropped by `keepRows`. The window ends at the newest time seen.
- **Backpressure (TCP and push):** when a worker has more than 4 MB waiting to
  be parsed, it pauses its socket (or delays its reply to `push`) until it's
  down to 1 MB. TCP then slows the sender. WebSockets can't be paused, so a
  WebSocket feed faster than the workers grows their backlog.
- **Queries go first:** parsing runs one batch per event-loop turn, so a query
  waits for at most one batch.

**In a browser** (`examples/stream/`, Chrome, Web Workers each reading its own
WebSocket of JSON lines): with 4 workers it kept up with the feed's
**2 million orders per second**; with 8 workers it took in **3.5–3.6 million per
second** (the test feed, one Node thread writing JSON, is the likely limit).
The page never waited more than 4 ms, and the 30-second window read in
0.1–11 ms. Run it: `npm run serve:split`, then open
`/examples/stream/?workers=8&rate=500000`.

**In Node** (`npm run bench:stream`, M1 Pro, 7 workers, 10M rows kept,
localhost TCP, a live query every 250 ms over every kept row):

| Receiver | Rows/s taken in | Query median / max | Main thread stalled up to |
|---|---|---|---|
| Plain Node, 1 connection (split/Number parsing, a loop per query) | 3.2–3.3M | 19–20 / 23 ms | 40–41 ms |
| Vectro, 1 connection, chunks pushed to workers | 17.0–17.5M | 13–14 / 22–32 ms | 6–9 ms |
| Vectro, 1 connection per worker | 16.7–18.5M | 12–14 / 29–62 ms | 12 ms (19–47 ms on an overloaded machine) |

The producer runs on the same machine, so all 8 cores are busy. JSON lines
(`FORMAT=ndjson`): Vectro took in 9.3–10.3M rows/s against 2.45M for plain Node
using `JSON.parse` per line, with live queries at 12–14 ms (plain: 18.5 ms).

## Runtimes

| Import | Where | How it runs |
|---|---|---|
| `vectrojs` | Node | Worker threads on all cores, shared memory |
| `vectrojs/browser` → `Engine` | Pages with COOP/COEP headers | Web Workers, shared memory (async API) |
| `vectrojs/browser` → `SplitEngine` | Any page | Each Web Worker owns a share of the rows; async API |
| `vectrojs/portable` | Cloudflare Workers and other edge runtimes | One thread; a precompiled 11 KB module plus a vectorized interpreter |

Tested in Node 25, Chrome, Safari 26.5, Firefox 158, Safari on iOS (real
iPhone) and Cloudflare Workers.

## Performance (10M rows, Apple M1 Pro, Node)

| Query | Vectro | DuckDB 2.0 | SQLite | Arquero |
|---|---|---|---|---|
| Filtered revenue | 3.2 ms | 10.0 ms | 757 ms | 63 ms |
| Dashboard, 4 aggregates | 3.1 ms | 21.3 ms | 345 ms | 170 ms |
| Group by region (8 groups) | 7.9 ms | 33.7 ms | 2.75 s | 998 ms |
| Group by customer (100K groups) | 11.2 ms | 11.5 ms | 4.32 s | 738 ms |
| New column | 3.2 ms | 26.1 ms | 1.75 s | 128 ms |

Run them yourself: `cd compare && npm install && npm run bench`. Timings vary
with the machine's state (battery, background load), so compare systems in the
same run. DuckDB is a full SQL database and wins elsewhere: joins, files larger
than memory, quoted-CSV loading.

## Layout

```
src/         the engine (Node, browser and portable entry points)
dist/        vectro.wasm, the precompiled kernels for edge runtimes (npm run build)
test/        correctness tests; VECTRO_MODE=single|interp|edge|browser-sim selects a mode
bench/       Vectro-only benchmarks (threads, async, skewed filters, CSV)
compare/     Vectro vs DuckDB, SQLite and Arquero (own package.json)
examples/    browser pages (threads, split tables, correctness check, phone test)
             and a Cloudflare Worker
scripts/     build-wasm.mjs, make-check-expected.mjs
```

## Development

```bash
npm run test:all        # every test in all five modes (Node, single thread, interpreter, edge, browser-sim)
npm run bench           # Node benchmarks
npm run serve:split     # browser pages without isolation headers, port 8767
```

Browser checks: run a server, then open `/examples/check/` in a browser to run
the same queries on every engine it supports and compare them with Node's
answers.

## Not done yet

Joins, integer/date types, missing values and persistence. Full `sort` runs
on the calling thread (top-k is parallel), and split tables have `topAsync`
but no full sort.
Streams take flat records only (no nested JSON fields), and windows group by
at most one category column. Memory is capped at 4 GB per engine and freed with `dispose()`.
