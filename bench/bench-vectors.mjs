// Exact nearest-neighbour search (cosine, top 10) over N embeddings of DIM
// dimensions: Vectro on all cores vs a tight single-thread JS loop over a
// flat Float32Array (dot products of pre-normalized vectors + a top-10 list).
import { Engine, VectorIndex } from '../src/node.mjs';

const DIM = Number(process.env.DIM ?? 384);
const SIZES = (process.env.SIZES ?? '100000,200000').split(',').map(Number);
let seed = 9;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff) - 0.5;
async function best(f, reps = 7) { await f(); let m = Infinity; for (let i = 0; i < reps; i++) { const s = performance.now(); await f(); m = Math.min(m, performance.now() - s); } return m; }

console.log(`exact cosine search, top 10, ${DIM} dimensions`);
for (const N of SIZES) {
  const flat = new Float32Array(N * DIM);
  for (let i = 0; i < flat.length; i++) flat[i] = rnd();
  for (let i = 0; i < N; i++) { // normalize, as an embedding model would
    let s = 0; for (let j = 0; j < DIM; j++) s += flat[i * DIM + j] ** 2;
    const k = 1 / Math.sqrt(s); for (let j = 0; j < DIM; j++) flat[i * DIM + j] *= k;
  }
  const q = Float32Array.from({ length: DIM }, rnd);
  const engine = new Engine();
  let t0 = performance.now();
  const index = await VectorIndex.create(engine, { vectors: flat, dim: DIM, columns: { id: Float64Array.from({ length: N }, (_, i) => i) } });
  const build = performance.now() - t0;
  const vectro = await best(() => index.searchAsync(q, { k: 10 }));
  const jsTop = () => {
    const top = new Float64Array(10).fill(-Infinity), at = new Int32Array(10);
    for (let i = 0; i < N; i++) {
      let d = 0; const o = i * DIM;
      for (let j = 0; j < DIM; j++) d += flat[o + j] * q[j];
      if (d <= top[9]) continue;
      let p = 9; while (p > 0 && top[p - 1] < d) { top[p] = top[p - 1]; at[p] = at[p - 1]; p--; }
      top[p] = d; at[p] = i;
    }
    return at;
  };
  const js = await best(jsTop, 3);
  const same = (await index.searchAsync(q, { k: 10 })).map((h) => h.id).join(',') === Array.from(jsTop()).join(',');
  console.log(`${N.toLocaleString().padStart(9)} vectors (${((N * DIM * 4) / 1e6).toFixed(0)} MB): Vectro ${vectro.toFixed(1)} ms, plain JS ${js.toFixed(1)} ms (${(js / vectro).toFixed(1)}x); index built in ${build.toFixed(0)} ms; same top 10 as JS: ${same ? 'yes' : 'NO'}`);
  await index.dispose();
  await engine.close();
}
