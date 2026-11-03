// Vector search: nearest embeddings to a query, exact (no approximate index),
// on every core.
//
//   const index = await VectorIndex.create(engine, {
//     vectors,                      // n embeddings: number[][] or one flat Float32Array (n * dim)
//     dim: 384,
//     metric: 'cosine',             // 'cosine' (default) | 'dot' | 'l2'
//     columns: { id, title, year }, // anything to return or filter on
//   });
//   await index.searchAsync(query, { k: 10, filter: 'year >= 2020' });
//   // -> [{ id, title, year, score }, ...] best first
//
// Embeddings are stored as columns of a float32 table (v0 … v{dim-1}), so the
// score is an ordinary fused expression, q0*v0 + q1*v1 + …, computed 24
// dimensions per pass with the query as scalars, then top(k). That makes
// filters, split tables (Web Workers on any page) and edge runtimes work with
// no extra code. cosine: vectors and query are normalized, score = dot
// (higher is closer). l2: score = squared distance (lower is closer).
import { trackAsyncMethods } from './engine.mjs';

const DIMS_PER_PASS = 24; // a pass reads 24 vector columns + the running sum
const METRICS = new Set(['cosine', 'dot', 'l2']);


export class VectorIndex {
  constructor(table, { dim, metric, names }) {
    this.table = table;
    this.dim = dim;
    this.metric = metric;
    this.names = names; // the non-vector columns returned with each hit
  }

  // engine: a normal Engine (Node, browser, portable) or a SplitEngine.
  static async create(engine, { vectors, dim, metric = 'cosine', columns = {} }) {
    if (!METRICS.has(metric)) throw new Error(`vectro: metric must be 'cosine', 'dot' or 'l2', got "${metric}"`);
    if (!(dim >= 1)) throw new Error('vectro: VectorIndex needs dim, the length of each embedding');
    const flat = flatten(vectors, dim);
    const n = flat.length / dim;
    for (const [name, values] of Object.entries(columns)) {
      if (/^v\d+$/.test(name) || name.startsWith('__np_')) throw new Error(`vectro: column name "${name}" is reserved for vectors`);
      if (values.length !== n) throw new RangeError(`vectro: column "${name}" has ${values.length} values, expected ${n} (one per vector)`);
    }
    const cols = { ...columns };
    const vcols = Array.from({ length: dim }, () => new Float32Array(n));
    const norm2 = new Float32Array(n);
    for (let i = 0; i < n; i++) {
      let s = 0;
      for (let j = 0; j < dim; j++) s += flat[i * dim + j] ** 2;
      const scale = metric === 'cosine' && s > 0 ? 1 / Math.sqrt(s) : 1;
      for (let j = 0; j < dim; j++) vcols[j][i] = flat[i * dim + j] * scale;
      norm2[i] = metric === 'cosine' ? 1 : s;
    }
    vcols.forEach((c, j) => { cols[`v${j}`] = c; });
    if (metric === 'l2') cols.__np_norm2 = norm2;
    const table = typeof engine.tableAsync === 'function'
      ? await engine.tableAsync(cols, { type: Float32Array })
      : engine.table(cols, { type: Float32Array });
    return new VectorIndex(table, { dim, metric, names: Object.keys(columns) });
  }

  get length() { return this.table.length; }

  // The k closest vectors to `query` (among rows matching `filter`), best first.
  async searchAsync(query, { k = 10, filter = null, params = {} } = {}) {
    const q = Array.from(query);
    if (q.length !== this.dim) throw new RangeError(`vectro: query has ${q.length} values, the index has dim ${this.dim}`);
    const view = filter ? this.table.filter(filter, params) : this.table;
    const opts = { k, metric: this.metric, dim: this.dim, names: this.names };
    // split tables: every worker searches its own rows in one round trip
    if (typeof view.vectorSearchAsync === 'function') return view.vectorSearchAsync(q, opts);
    return searchTable(view, q, opts);
  }

  async dispose() { await this.table.dispose(); }
}

function flatten(vectors, dim) {
  if (ArrayBuffer.isView(vectors)) {
    if (vectors.length % dim) throw new RangeError(`vectro: ${vectors.length} values is not a whole number of ${dim}-dimension vectors`);
    return vectors;
  }
  const flat = new Float32Array(vectors.length * dim);
  vectors.forEach((v, i) => {
    if (v.length !== dim) throw new RangeError(`vectro: vector ${i} has ${v.length} values, expected ${dim}`);
    flat.set(v, i * dim);
  });
  return flat;
}

// Search one table (or filtered view): running dot product, DIMS_PER_PASS
// dimensions per pass, then top k over a narrow view (not the vectors).
export async function searchTable(view, query, { k, metric, dim, names }) {
  const q = Float64Array.from(query);
  if (metric === 'cosine') {
    const len = Math.hypot(...q);
    if (len > 0) for (let j = 0; j < q.length; j++) q[j] /= len;
  }
  const made = [];
  try {
    let prev = null;
    for (let j0 = 0, pass = 0; j0 < dim; j0 += DIMS_PER_PASS, pass++) {
      const terms = [], scalars = {};
      for (let j = j0; j < Math.min(dim, j0 + DIMS_PER_PASS); j++) {
        terms.push(`__np_q${j - j0} * v${j}`);
        scalars[`__np_q${j - j0}`] = q[j];
      }
      const name = `__np_dot${pass}`;
      view = await view.deriveAsync(name, prev ? `${prev} + ${terms.join(' + ')}` : terms.join(' + '), scalars);
      made.push(view);
      prev = name;
    }
    let key, scoreCol = prev;
    if (metric === 'l2') {
      view = await view.deriveAsync('__np_score', `__np_norm2 - 2 * ${prev} + __np_qq`, { __np_qq: q.reduce((a, x) => a + x * x, 0) });
      made.push(view);
      key = scoreCol = '__np_score';
    } else {
      key = `-${prev}`;
    }
    const best = await view.select(...names, scoreCol).topAsync(k, key);
    try {
      return best.toRows().map((r) => {
        const hit = {};
        for (const name of names) hit[name] = r[name];
        hit.score = metric === 'l2' ? Math.max(0, r[scoreCol]) : r[scoreCol];
        return hit;
      });
    } finally {
      best.dispose();
    }
  } finally {
    for (const t of made.reverse()) await t.dispose();
  }
}

// Merge per-worker hits (each best first) into the overall best k.
export function mergeHits(lists, k, metric) {
  const all = lists.flat();
  const better = metric === 'l2' ? (a, b) => a.score - b.score : (a, b) => b.score - a.score;
  return all.sort(better).slice(0, k);
}

// (close() while a search runs: see Engine _track)
trackAsyncMethods(VectorIndex, (v) => v.table?.engine, ['dispose']);
{
  const create = VectorIndex.create;
  VectorIndex.create = function (engine, opts) { const out = create.call(this, engine, opts); return engine?._track ? engine._track(out) : out; };
}
