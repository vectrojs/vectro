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
- **Async API:** every query has an `…Async` version that runs on the worker
  threads while your server or page keeps working.
- **Expressions** are a small, safe language: numbers, columns, `+ - * /`,
  comparisons, `&& || !`, `? :`, `abs sqrt floor ceil min max`. There is no
  `eval`, so formulas typed by users or AI models are safe to run.

## Streaming (prototype)

A stream is a live table spread over split-table workers. Rows arrive as CSV
lines and are parsed in 256 KB batches into growing columns. Queries are the
same as for any split table.

```js
import { SplitEngine } from 'vectrojs';

const engine = new SplitEngine({ workers: 7 });     // default: one worker per core
const orders = await engine.streamAsync({
  columns: { ts: 'number', price: 'number', qty: 'number', region: 'category' },
  keepRows: 10_000_000,                              // or keepMs: 60_000, timeColumn: 'ts'
});

await Promise.all(engine.workers.map((_, w) =>      // each worker reads its own TCP stream...
  orders.connect(w, { host: 'feed.internal', port: 9000 + w })));
socket.on('data', (chunk) => orders.push(chunk));    // ...or push chunks from one connection

await orders.groupBy('region').aggAsync({ revenue: 'sum(price * qty)' });
const fixed = await orders.snapshotAsync();          // a fixed copy, for derive and toRows
```

- **Backpressure:** when a worker has more than 4 MB waiting to be parsed, it
  pauses its socket (or delays its reply to `push`) until it's down to 1 MB.
  TCP then slows the sender, so memory stays bounded.
- **Queries go first:** parsing runs one batch per event-loop turn, so a query
  waits for at most one batch.
- **Kept rows:** `keepRows` (newest rows, split evenly over workers) or
  `keepMs` with `timeColumn` (rows newer than the newest time minus `keepMs`).

`npm run bench:stream` (M1 Pro, 7 workers, 10M rows kept, localhost TCP, a live
query every 250 ms over every kept row):

| Receiver | Rows/s taken in | Query median / max | Main thread stalled up to |
|---|---|---|---|
| Plain Node, 1 connection (split/Number parsing, a loop per query) | 3.2–3.3M | 19–20 / 23 ms | 40–41 ms |
| Vectro, 1 connection, chunks pushed to workers | 17.0–17.5M | 13–14 / 22–32 ms | 6–9 ms |
| Vectro, 1 connection per worker | 16.7–18.5M | 12–13 / 31–62 ms | 19–47 ms |

The producer runs on the same machine, so all 8 cores are busy. With one
connection per worker the main thread does almost nothing, yet the OS
sometimes pauses it (the 19–47 ms stalls).

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

Sort, top-k, joins, integer/date types, missing values and persistence.
Streams take CSV lines only (no JSON lines yet), connect over TCP in Node
only (no WebSocket in browsers yet), and have no running totals or sliding
windows beyond `keepRows`/`keepMs`. Memory is capped at 4 GB per engine and freed with `dispose()`.
