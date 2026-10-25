// Vector search against brute force. Vectors are stored as float32, so near
// ties may swap places: each position's score must match the brute-force
// score at that position (within float32 rounding), and the hit's own score
// must be its true score.
import { Engine, SplitEngine, VectorIndex } from '../src/node.mjs';
import { Engine as PortableEngine } from '../src/portable.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };

let seed = 17;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff) - 0.5;
const f32 = Math.fround;

function score(metric, v, q) {
  let dot = 0, vv = 0, qq = 0;
  for (let j = 0; j < v.length; j++) { const a = f32(v[j]); dot += a * q[j]; vv += a * a; qq += q[j] * q[j]; }
  if (metric === 'cosine') return vv && qq ? dot / Math.sqrt(vv * qq) : 0;
  if (metric === 'dot') return dot;
  return Math.max(0, vv - 2 * dot + qq); // squared distance
}
const better = (metric) => (metric === 'l2' ? (a, b) => a - b : (a, b) => b - a);

async function check(L, idx, vecs, q, metric, { k, filter, keep }) {
  const ids = vecs.map((_, i) => i).filter((i) => !keep || keep(i));
  const want = ids.map((i) => score(metric, vecs[i], q)).sort(better(metric)).slice(0, k);
  const hits = await idx.searchAsync(q, { k, filter });
  checks++;
  if (hits.length !== want.length) return fail(`${L}: ${hits.length} hits, want ${want.length}`);
  const tol = 1e-4 * Math.max(1, Math.abs(want[0] ?? 1));
  for (let j = 0; j < hits.length; j++) {
    const truth = score(metric, vecs[hits[j].id], q);
    if (keep && !keep(hits[j].id)) return fail(`${L}: hit ${hits[j].id} doesn't match the filter`);
    if (Math.abs(truth - want[j]) > tol || Math.abs(hits[j].score - truth) > tol) {
      return fail(`${L} pos ${j}: id ${hits[j].id} score ${hits[j].score} (true ${truth}), want ${want[j]}`);
    }
  }
}

const engines = [['threads', () => new Engine()], ['split, 3 workers', () => new SplitEngine({ workers: 3 })], ['interpreter', () => new PortableEngine({ jit: false })]];
for (const [ename, make] of engines) {
  const engine = make();
  for (const [n, dim] of [[1, 1], [40, 7], [3_000, 50], [60_000, 100]]) {
    if (ename === 'interpreter' && n > 3_000) continue; // slow path, smaller sizes are enough
    const vecs = Array.from({ length: n }, () => Array.from({ length: dim }, rnd));
    for (const metric of ['cosine', 'dot', 'l2']) {
      const L = `${ename} n=${n} dim=${dim} ${metric}`;
      const idx = await VectorIndex.create(engine, { vectors: vecs, dim, metric, columns: { id: vecs.map((_, i) => i), group: vecs.map((_, i) => ['a', 'b', 'c'][i % 3]) } });
      const q = Array.from({ length: dim }, rnd);
      for (const k of [0, 1, 10, n + 3]) await check(`${L} k=${k}`, idx, vecs, q, metric, { k });
      await check(`${L} filtered`, idx, vecs, q, metric, { k: 7, filter: "group == 'b' && id >= 5", keep: (i) => i % 3 === 1 && i >= 5 });
      await idx.dispose();
    }
  }
  // flat Float32Array input, and a zero query (cosine: every score 0)
  {
    const dim = 16, n = 500;
    const flat = Float32Array.from({ length: n * dim }, rnd);
    const vecs = Array.from({ length: n }, (_, i) => Array.from(flat.subarray(i * dim, (i + 1) * dim)));
    const idx = await VectorIndex.create(engine, { vectors: flat, dim, columns: { id: vecs.map((_, i) => i) } });
    await check(`${ename} flat input`, idx, vecs, Array.from({ length: dim }, rnd), 'cosine', { k: 5 });
    const zero = await idx.searchAsync(new Array(dim).fill(0), { k: 3 });
    checks++; if (zero.length !== 3 || zero.some((h) => h.score !== 0)) fail(`${ename} zero query: ${JSON.stringify(zero)}`);
    await idx.dispose();
  }
  // bad input is refused
  for (const [label, run, text] of [
    ['metric', () => VectorIndex.create(engine, { vectors: [[1, 2]], dim: 2, metric: 'manhattan' }), "'cosine', 'dot' or 'l2'"],
    ['vector length', () => VectorIndex.create(engine, { vectors: [[1, 2], [3]], dim: 2 }), 'vector 1 has 1 values'],
    ['column length', () => VectorIndex.create(engine, { vectors: [[1, 2]], dim: 2, columns: { id: [1, 2] } }), 'expected 1'],
    ['reserved name', () => VectorIndex.create(engine, { vectors: [[1, 2]], dim: 2, columns: { v3: [1] } }), 'reserved'],
    ['query length', async () => { const i = await VectorIndex.create(engine, { vectors: [[1, 2]], dim: 2 }); try { await i.searchAsync([1, 2, 3]); } finally { await i.dispose(); } }, 'has dim 2'],
  ]) {
    let msg = '';
    try { await run(); } catch (err) { msg = err.message; }
    checks++; if (!msg.includes(text)) fail(`${ename} bad ${label} accepted: ${msg}`);
  }
  await engine.close();
}
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} vector-search checks passed`);
process.exit(failures ? 1 : 0);
