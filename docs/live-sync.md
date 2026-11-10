# Live sync: design

Status: phase 1 built on the `live-sync` branch (after 0.2.1), not merged.
Updates Part B ("Vectro Live") of the earlier plan. Code: `src/live/`
(`table.mjs`, `hub.mjs`, `diff.mjs`, `ws.mjs`, `client.mjs`); tests:
`test/test-live.mjs`; demo: `examples/live/`; latency: `bench/bench-live.mjs`.

## The idea in one paragraph

A page says "watch this SQL". The server runs it once and sends the answer
(a **snapshot**). When rows in a table change, the server re-runs only the
queries that read that table, compares the new answer with the old one **by
key**, and sends only the difference (a **patch**): which rows were removed,
which were added or changed, and which moved to a new place in the sort
order. The page applies the patch to the rows it already has, like React
reconciliation: only the changed rows are touched.

## What exists today (checked against 0.2.1)

- `serve()` (`src/serve/http.mjs`) answers `/sql`, `/tables`, `/odata` over
  HTTP. There is **no WebSocket server in `src/`**; only
  `examples/stream/ws-server.mjs` has a small one (send only).
- Tables (`Table`) and datasets are **fixed** once built; datasets grow only
  by appending files. Streams (`StreamTable`, `src/split.mjs`) are
  **append-only**: rows enter by `push()`, leave from the head (`keepRows`,
  `keepMs`). No keys, updates, deletes or versions anywhere.
- `sql()` runs on `Table` and `Dataset`, not on split or stream tables (those
  can be copied into a `Table` with `snapshotAsync()` + `toColumnsAsync()`).
- `Emitter` is TCP and load-balances; it does not broadcast.

So live sync needs: a table that can change (keys, insert/update/delete), a
WebSocket endpoint, a per-query diff, and a small client.

### A change from the earlier plan

Part B said "sync whole tables to the browser and query there". This branch
does what was asked now: **the server computes the answer and sends the
answer's difference**. The page needs no engine, no wasm and no copy of the
table, only the rows it shows. (Syncing tables to the browser can come later
on the same wire.)

## API

Server (Node):

```js
import { serve, liveTable } from 'vectrojs/serve';

const orders = liveTable({ key: 'id', columns: { id: 'number', region: 'text', product: 'text', price: 'number' } });
orders.insert([{ id: 1, region: 'EU', product: 'pen', price: 3 }]);

const s = await serve({ live: { orders }, port: 8787 });   // WebSocket at ws://127.0.0.1:8787/live

orders.upsert([{ id: 1, price: 4 }]);   // change some columns of row 1 (insert if new)
orders.delete([1]);                     // by key
s.live.changed('sales');                // a normal table or dataset changed: re-run its watchers
s.live.replace('sales', newTable);      // or serve another table under that name
```

`liveTable` changes are merged by key until the next recompute (two updates
of one row become one; insert then delete becomes nothing).

Browser (or Node 22+), no dependencies, no wasm:

```js
import { connectLive } from 'vectrojs/live';

const live = connectLive('ws://127.0.0.1:8787/live', { token });
const w = live.watch(
  'SELECT region, SUM(price) AS revenue, COUNT(*) AS n FROM orders GROUP BY region ORDER BY revenue DESC',
  { key: 'region' },
  (rows, patch) => render(rows, patch),  // rows: the whole answer, in order; patch: what changed
);
w.close();
```

- `key`: the columns that name a row of the **answer** (a GROUP BY's
  columns, or the table's key). If not given: the table key if the answer has
  that column, otherwise the whole row (then a changed row shows as removed +
  added, which is still correct).
- the second argument = `{ snapshot, added: [rows], updated: [rows], removed: [keys], moved: [keys], ms, latency }`,
  so a UI can flash changed cells or animate moved rows. `rows` is kept up to
  date by the client, so simple pages just redraw from it. Rows that did not
  change stay the same objects, so a framework can skip them.
- `w.first` resolves with the first answer (or rejects with the query's error).

## Wire format (WebSocket, JSON text frames, `v: 1`)

Client to server:

| message | meaning |
|---|---|
| `{ op: 'watch', id, sql, key? }` | start watching (id chosen by the client) |
| `{ op: 'unwatch', id }` | stop |
| `{ op: 'resync', id }` | send me a fresh snapshot (client saw a gap) |
| `{ op: 'ping', c }` | clock check; answered by `{ t: 'pong', c, s }` (server time) |

Server to client:

| message | meaning |
|---|---|
| `{ t: 'hello', v: 1, tables, s }` | after the handshake |
| `{ t: 'snap', id, ver, columns, key, rows }` | the whole answer; `rows` are arrays in `columns` order |
| `{ t: 'patch', id, ver, from, del, set, put, ms, at }` | the difference from version `from` to `ver` |
| `{ t: 'error', id, message }` | a bad query; the watch ends |

In a patch:

- keys are text: the key column's value as JSON (`"EU"`, `42`), several key
  columns as a JSON array; a key that repeats gets `#2`, `#3`;
- `del`: keys of rows that left the answer;
- `set`: `[key, row]` for rows that changed but keep their place;
- `put`: `[key, index, row]` for rows added (or changed and moved), and
  `[key, index]` (no row) for rows that only moved; `index` is the row's
  place in the new answer;
- `ms`: server time spent (recompute + diff); `at`: server clock time of the
  oldest change in this patch (for measuring latency).

The client applies a patch in one pass over its rows: replace the `set`
rows, drop `del` and `put` rows, then merge the `put` rows in at their
`index` (ascending). Rows that did not change keep their objects and are
never re-rendered by a keyed UI. If `from` is not the version it
holds, it sends `resync`. JSON keeps phase 1 simple and readable in the
browser's network tab; a binary (Arrow) snapshot for big answers can come
later behind the same messages.

## How answers are diffed cheaply

1. **Only affected queries run.** Each watch knows the tables its SQL may
   read (every table whose name appears in it as a word: at worst an extra
   run, never a missed change). A change to `orders` re-runs only watches that read
   `orders`. Two pages watching the same SQL share one run and one diff.
2. **Re-run, then diff the answer** (phase 1). The query runs on the engine
   as any `/sql` query does (SIMD, all of Vectro's fast paths). Then the new
   answer is compared with the previous one: a `Map` from key to row, one
   pass, O(answer rows), not O(table rows).
3. **Moves are minimal.** Rows present before and after keep their place
   unless they must move: the server finds the longest run of rows that stayed
   in order (longest increasing subsequence, O(n log n)) and sends a move only
   for the others. One price change in a sorted top-20 sends one row, not 20.
4. **Rebuilding the table copy.** A `liveTable` keeps its rows in plain
   column arrays (dense, delete = swap with the last row), and makes a fresh
   engine `Table` from them only when it changed and a query needs it. That is
   one copy per batch, not per change. (Phase 2: append-only changes extend
   the table instead of copying it.)
5. **Later (phase 3): maintain, don't re-run.** COUNT / SUM / AVG with
   filters and GROUP BY can be updated from the changed rows alone; median,
   distinct, top-k and windows keep re-running. Chosen per query at plan time.

Limits: an answer is capped (default 50,000 rows; above that the watch fails
with "add a LIMIT"), each client can watch up to 64 queries, SQL is read only
(the same rules as `/sql`), and the token is checked on the WebSocket
handshake.

## Batching and back-pressure

- **Coalescing:** changes to a `liveTable` merge per key until the next
  recompute.
- **Pacing:** the first change after a quiet time is handled at once (next
  tick: lowest latency). While a recompute runs, new changes wait and are all
  handled in the next one. At most one recompute per query is running, and
  at most one per `minInterval` (default 16 ms, about one screen frame), so
  1,000 changes a second cost about 60 recomputes, not 1,000.
- **Slow clients:** if a client's socket buffer passes 1 MB, the server stops
  sending it patches for that watch and marks it stale. When the buffer
  drains it gets one fresh snapshot instead of the patches it missed. A slow
  page never makes the server queue without end, and never sees a gap.
- **Reconnect:** the client reconnects (with backoff) and watches again; it
  gets a fresh snapshot. (Replay from a version log is phase 2.)
- **Idle costs nothing:** no timers run when no data changes.

## How "minimal time" is measured

Latency = from the moment the server code calls `insert / upsert / delete`
to the moment the page has applied the patch (the rows are updated).

- `bench/bench-live.mjs`: server and a WebSocket client in one Node process
  (one clock). For tables of 10k, 100k and 1M rows and changes of 1 and 100
  rows, it reports p50 and p99 of change to patch applied, split into
  server recompute + diff and the network / apply part, and the bytes sent.
  Numbers go into `compare/results.json` (`live` section) only from a quiet
  machine (load under 4), as the rules say.
- The demo page shows its own latency live: each patch carries `at` (server
  clock); the page learns the clock offset from a ping at connect, and shows
  change to screen in ms.

## Phases

1. **This branch:** `liveTable` (insert / upsert / delete by key), `serve({
   live })` with `/live` WebSocket, recompute + keyed diff with minimal
   moves, coalescing, pacing, slow-client resnapshot, `connectLive` client
   with reconnect, `changed(name)` for tables and datasets, tests, demo
   (`examples/live/`), latency bench.
2. Versions log and replay on reconnect; streams as sources (re-run on
   pushed rows); binary snapshots; append-only fast path.
3. Incremental aggregates (no re-run for COUNT / SUM / AVG GROUP BY);
   `useLiveQuery` React hook.
4. Postgres logical replication as a change source; per-user row filters.
