// CSV loading on split tables: one piece per worker (step 12) vs many small
// pieces dealt round-robin (this step). Same file, best of 5.
// Usage: node bench-split-csv.mjs [../12-cross-browser/src/split.mjs]
import { Worker } from 'node:worker_threads';

const WORKERS = Number(process.env.WORKERS ?? 8);
let seed = 9;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const REG = ['EU', 'US', 'APAC', 'LATAM', 'MEA', 'UK', 'CA', 'ANZ'];
const plain = ['day,price,qty,region'], quoted = ['day,price,region,note'];
for (let i = 0; i < 2_000_000; i++) {
  const d = Math.floor((i * 365) / 2e6), p = (rnd() * 1000).toFixed(2), r = REG[Math.floor(rnd() * 8)];
  plain.push(`${d},${p},${1 + Math.floor(rnd() * 9)},${r}`);
  quoted.push(`${d},${p},${r},${i % 3 ? `note ${i}` : `"say ""hi"", ${i}"`}`);
}
const files = { plain: new TextEncoder().encode(plain.join('\n')), 'quoted free text': new TextEncoder().encode(quoted.join('\n')) };
const layouts = [['blocks (this step)', '../src/split.mjs', '../src/split-worker-node.mjs']];
if (process.argv[2]) layouts.unshift(['one piece per worker', process.argv[2], new URL('split-worker-node.mjs', new URL(process.argv[2], import.meta.url)).href]);
for (const [layout, mod, workerUrl] of layouts) {
  const { SplitEngine } = await import(new URL(mod, import.meta.url).href);
  const spawn = () => { const w = new Worker(new URL(workerUrl, import.meta.url)); w.unref(); return { post: (m, t) => w.postMessage(m, t), onMessage: (fn) => w.on('message', fn), terminate: () => w.terminate() }; };
  const split = new SplitEngine({ workers: WORKERS, spawn });
  for (const [name, bytes] of Object.entries(files)) {
    let best = Infinity, t;
    for (let i = 0; i < 6; i++) { await t?.dispose(); const s = performance.now(); t = await split.parseCSVAsync(bytes, { types: { note: 'string' } }); best = Math.min(best, performance.now() - s); }
    const lastWeek = await t.filter('day >= 358').collectAsync();
    console.log(`${layout.padEnd(22)} ${name.padEnd(17)} ${(bytes.length / 1e6).toFixed(0)} MB  ${best.toFixed(0).padStart(4)} ms   last week collected, thousand rows per worker: ${lastWeek.counts.map((c) => Math.round(c / 1000)).join(' ')}`);
    await lastWeek.dispose(); await t.dispose();
  }
  await split.close();
}
