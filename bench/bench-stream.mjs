// Streaming: how many rows per second can an app take in from TCP while
// answering a live dashboard query, and how long do those queries take?
//
// A producer process streams orders (ts,price,qty,region lines) over TCP as
// fast as the receiver accepts them. Each receiver runs in its own process
// for DURATION_MS, keeps the newest KEEP_ROWS rows, and every 250 ms asks
// "orders and revenue per region" plus a filtered aggregate.
//   plain    one connection; the usual toString/split/Number parsing, a loop per query
//   push     Vectro, one connection: the main thread deals whole lines to the workers
//   connect  Vectro, one connection per worker: data never touches the main thread
// FORMAT=ndjson sends JSON lines instead ({"ts":…,"price":…,"qty":…,"region":"EU"});
// plain Node then uses JSON.parse per line, the way most apps read them.
// Usage: node bench/bench-stream.mjs   (WORKERS=n DURATION_MS=n KEEP_ROWS=n FORMAT=csv|ndjson MODES=plain,push,connect)
import { fork } from 'node:child_process';
import { createServer, connect } from 'node:net';
import { availableParallelism } from 'node:os';
import { fileURLToPath } from 'node:url';

const WORKERS = Number(process.env.WORKERS ?? Math.max(1, availableParallelism() - 1));
const DURATION_MS = Number(process.env.DURATION_MS ?? 4000);
const KEEP_ROWS = Number(process.env.KEEP_ROWS ?? 10_000_000);
const FORMAT = process.env.FORMAT ?? 'csv';
const REG = ['EU', 'US', 'APAC', 'LATAM', 'MEA', 'UK', 'CA', 'ANZ'];
const role = process.argv[2];

// ---- producer: one prebuilt buffer of lines, written again and again --------
// (FORMAT=vectro: an Emitter writing one prebuilt 65,536-row batch again and again)
const BINARY = FORMAT === 'vectro' || FORMAT === 'arrow';
if (role === 'producer' && BINARY) {
  const { Emitter } = await import('../src/node.mjs');
  let seed = 5;
  const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
  const B = 65_536;
  const batch = { ts: new Float64Array(B), price: new Float64Array(B), qty: new Float64Array(B), region: { codes: new Uint32Array(B), labels: REG } };
  for (let i = 0; i < B; i++) { batch.ts[i] = 1_700_000_000_000 + i; batch.price[i] = Math.round(rnd() * 1e5) / 100; batch.qty[i] = 1 + Math.floor(rnd() * 9); batch.region.codes[i] = Math.floor(rnd() * 8); }
  const feed = await Emitter.listen({ format: FORMAT, threads: Number(process.env.EMIT_THREADS ?? 1), columns: { ts: 'number', price: 'number', qty: 'number', region: 'category' } });
  process.send({ port: feed.port, rowsPerBuffer: B, bytesPerBuffer: B * 28 });
  for (;;) await feed.write(batch); // waits while no collector is connected or all are full
}
if (role === 'producer' && !BINARY) {
  let seed = 5;
  const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
  const lines = [];
  for (let i = 0; i < 1_000_000; i++) {
    const ts = 1_700_000_000_000 + i, price = (rnd() * 1000).toFixed(2), qty = 1 + Math.floor(rnd() * 9), region = REG[Math.floor(rnd() * 8)];
    lines.push(FORMAT === 'ndjson' ? `{"ts":${ts},"price":${price},"qty":${qty},"region":"${region}"}` : `${ts},${price},${qty},${region}`);
  }
  const buf = Buffer.from(lines.join('\n') + '\n');
  const server = createServer((socket) => {
    socket.on('error', () => {});
    let open = true;
    socket.on('close', () => { open = false; });
    (function more() {
      while (open && socket.write(buf));
      if (open) socket.once('drain', more);
    })();
  });
  server.listen(0, '127.0.0.1', () => process.send({ port: server.address().port, rowsPerBuffer: lines.length, bytesPerBuffer: buf.length }));
}

// ---- receivers -------------------------------------------------------------------
async function measure(label, { rowsIn, query }) {
  // the main thread's longest stall, sampled every 5 ms
  let last = performance.now(), worstLag = 0;
  const lagTimer = setInterval(() => { const now = performance.now(); worstLag = Math.max(worstLag, now - last - 5); last = now; }, 5);
  const queries = [];
  let stop = false;
  const queryLoop = (async () => {
    while (!stop) {
      await new Promise((r) => setTimeout(r, 250));
      if (stop) break;
      const s = performance.now();
      await query();
      queries.push(performance.now() - s);
    }
  })();
  await new Promise((r) => setTimeout(r, 1000)); // warm up
  const r0 = await rowsIn(), t0 = performance.now();
  await new Promise((r) => setTimeout(r, DURATION_MS));
  const r1 = await rowsIn(), t1 = performance.now();
  stop = true;
  await queryLoop;
  clearInterval(lagTimer);
  queries.sort((a, b) => a - b);
  return { label, rowsPerSec: (r1 - r0) / ((t1 - t0) / 1000), queryMedian: queries[queries.length >> 1], queryMax: queries[queries.length - 1], queries: queries.length, worstLag };
}

if (role === 'plain') {
  const port = Number(process.argv[3]);
  const ts = new Float64Array(KEEP_ROWS), price = new Float64Array(KEEP_ROWS), qty = new Float64Array(KEEP_ROWS), region = new Uint8Array(KEEP_ROWS);
  const codes = new Map();
  let n = 0, total = 0, partial = '';
  const socket = connect(port, '127.0.0.1');
  socket.on('data', (chunk) => {
    const lines = (partial + chunk.toString()).split('\n');
    partial = lines.pop();
    for (const line of lines) {
      let t, p, q, r;
      if (FORMAT === 'ndjson') { const o = JSON.parse(line); t = o.ts; p = o.price; q = o.qty; r = o.region; }
      else { const f = line.split(','); t = Number(f[0]); p = Number(f[1]); q = Number(f[2]); r = f[3]; }
      let c = codes.get(r);
      if (c === undefined) { c = codes.size; codes.set(r, c); }
      const i = n % KEEP_ROWS; // ring buffer of the newest rows
      ts[i] = t; price[i] = p; qty[i] = q; region[i] = c;
      n++; total++;
    }
  });
  const result = await measure('plain Node, 1 connection', {
    rowsIn: () => total,
    query: () => {
      const m = Math.min(n, KEEP_ROWS), cnt = new Float64Array(16), rev = new Float64Array(16);
      let fn = 0, fs = 0;
      for (let i = 0; i < m; i++) { cnt[region[i]]++; rev[region[i]] += price[i] * qty[i]; if (price[i] > 900) { fn++; fs += qty[i]; } }
      return [cnt, rev, fn, fs];
    },
  });
  socket.destroy();
  process.send(result);
  process.exit(0);
}

if (role === 'push' || role === 'connect') {
  const port = Number(process.argv[3]);
  const { SplitEngine } = await import('../src/node.mjs');
  const engine = new SplitEngine({ workers: WORKERS });
  const stream = await engine.streamAsync({ format: FORMAT, columns: { ts: 'number', price: 'number', qty: 'number', region: 'category' }, keepRows: KEEP_ROWS });
  let socket;
  if (role === 'connect') {
    await Promise.all(engine.workers.map((_, w) => stream.connect(w, { port })));
  } else {
    // one connection here; hand whole lines to the workers, pausing if they fall behind
    socket = connect(port, '127.0.0.1');
    let inflight = 0;
    socket.on('data', (chunk) => {
      inflight++;
      stream.push(chunk).then(() => { if (--inflight < 16) socket.resume(); });
      if (inflight >= 64) socket.pause();
    });
  }
  const result = await measure(role === 'connect' ? `Vectro, ${WORKERS} connections (1 per worker)` : `Vectro, 1 connection -> ${WORKERS} workers`, {
    rowsIn: async () => (await stream.statsAsync()).rowsIn,
    query: () => Promise.all([
      stream.groupBy('region').aggAsync({ n: 'count()', rev: 'sum(price * qty)' }),
      stream.filter('price > 900').aggAsync({ n: 'count()', s: 'sum(qty)' }),
    ]),
  });
  const end = await stream.statsAsync();
  result.keptRows = end.rows;
  if (process.env.DEBUG) result.debug = { bytesIn: end.bytesIn, unparsedBytes: end.unparsedBytes, batches: end.batches };
  socket?.destroy();
  await stream.dispose();
  process.send(result);
  process.exit(0);
}

// ---- orchestration -----------------------------------------------------------------
if (!role) {
  const self = fileURLToPath(import.meta.url);
  const producer = fork(self, ['producer']);
  const info = await new Promise((r) => producer.once('message', r));
  const avgRowBytes = info.bytesPerBuffer / info.rowsPerBuffer;
  console.log(`Streaming orders as ${FORMAT === 'vectro' ? 'binary Vectro frames' : FORMAT === 'arrow' ? 'Arrow IPC' : FORMAT === 'ndjson' ? 'JSON lines' : 'CSV lines'} over TCP (localhost), ${avgRowBytes.toFixed(1)} bytes per row; ${WORKERS} workers; keep the newest ${(KEEP_ROWS / 1e6).toFixed(0)}M rows; ${DURATION_MS / 1000} s per run`);
  console.log('Live query every 250 ms: orders + revenue per region, and a filtered aggregate, over every row kept.\n');
  const rows = [];
  for (const mode of (process.env.MODES ?? (BINARY ? 'connect' : 'plain,push,connect')).split(',')) {
    const child = fork(self, [mode, String(info.port)], { execArgv: ['--max-old-space-size=8192'] });
    const r = await new Promise((res, rej) => { child.once('message', res); child.once('exit', (code) => code && rej(new Error(`${mode} exited with ${code}`))); });
    rows.push(r);
    if (r.debug) console.log('   ', JSON.stringify(r.debug));
    console.log(`${r.label.padEnd(40)} ${(r.rowsPerSec / 1e6).toFixed(2).padStart(6)}M rows/s  ${((r.rowsPerSec * avgRowBytes) / 1e6).toFixed(0).padStart(5)} MB/s   query median ${r.queryMedian.toFixed(1).padStart(6)} ms, max ${r.queryMax.toFixed(1).padStart(6)} ms   main thread stalled up to ${r.worstLag.toFixed(1)} ms`);
  }
  producer.kill();
}
