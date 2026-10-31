// Group-by with many groups on every thread, with no group numbers, global
// sort or gathers (the kernels are in partition-kernels.mjs):
//
// 1. Each key becomes a bit field of r, a key that sorts like the keys do.
// 2. A sample of rows gives P partitions of about the same number of rows,
//    in key order (a lookup on the top bits of r's first word), and an
//    estimate of the number of groups.
// 3. Count the rows of each partition in each unit, turn the counts into
//    each unit's first place in each partition, and copy every row's record
//    (r and the values to add up) there: each partition holds its rows in
//    row order.
// 4. One task per partition: a hash table in that thread's own scratch
//    memory adds up the rows (in row order, so the sums are the same on any
//    number of threads), then its groups are sorted by r and staged over
//    the partition's first records. A partition with more groups than the
//    scratch holds is split again by key range (recursively; by the next
//    word of r once a word is the same in all its rows).
// 5. Each partition's groups go into the result columns at its offset,
//    which leaves them sorted by key.
//
// partitionedGroupsG returns null when the keys or the memory needed don't
// suit it (or memory runs out); the caller then numbers the groups with a
// hash table instead.

import { OP, unitCount } from './kernels.mjs';
import { analyze, toSource, walk } from './fuse.mjs';
import { decodeOne } from './pack.mjs';
import { NB, FIELD, rowsLayout, rowsModule, aggregateModule, scratchBytes, writeLayout, writeModule, splitModule } from './partition-kernels.mjs';

export const PARTITION_ROWS = 65_536; // smaller tables: the hash path (less to set up)
export const PARTITION_GROUPS = 16_384; // fewer groups expected: the hash path (per-thread totals are cheaper)
export const DIRECT_SLOTS = 2 ** 22; // threads x totals x groups past which numbering groups directly is slower
const MAX_WORDS = 4, MAX_ACCS = 32, SAMPLE = 16_384, MAX_DEPTH = 12;
const RECORD_BYTES = 2 ** 30; // every row's record at once; more: the hash path
const SCRATCH_BYTES = 2 ** 28; // every thread's scratch together (fewer groups per partition fit: more splits)
const OVERFLOW = 0xffffffff;  // a partition with more groups than the scratch holds
const NO_PLAN = Symbol('no plan'); // splitting didn't converge: the hash path instead

function kernel(engine, name, make) {
  engine._partitionKernels ??= new Map();
  let v = engine._partitionKernels.get(name);
  if (!v) { v = engine._register(make()); engine._partitionKernels.set(name, v); }
  return v;
}

const bitsFor = (maxCode) => { let b = 1; while (2 ** b <= maxCode) b++; return b; };
export const partitionsFor = (rows) => Math.max(16, Math.min(1024, 2 ** Math.round(Math.log2(rows / 8192))));
const view = (engine, a) => { const b = engine.live(a); return new DataView(b.buffer, b.byteOffset, b.byteLength); };

// keys (and t, sources: 'count(1)', 'sum(x)'... with missing-value rules
// applied, the first counting rows; est: groups expected, see
// estimateGroups) -> { G, keyCols, accCols, map } or null: G groups, sorted
// by key; keyCols: each key's values (codes for text), accCols: each
// accumulator's totals (engine Float64Arrays, the caller's to free); source
// i's totals are accCols[map[i]] (identical sources share one).
export function* partitionedGroupsG(t, keys, sources, params, est) {
  const { engine, n } = t;
  if (!n) return null;
  const T = engine.T, shared = engine.isShared, tf = t.type === Float32Array ? 'f32' : 'f64';
  const kf = yield* keyFieldsG(t, keys);
  if (!kf) return null;
  const { fields, W } = kf;
  const { accs, map } = accumulators(sources);
  const J = accs.length, RB = 8 * W + 8 * J;
  if (J > MAX_ACCS || n * RB > (engine._partitionBytes ?? RECORD_BYTES)) return null;
  // the values to add up, and the columns and scalars they read
  const vals = accs.flatMap((a, slot) => (a.ast ? [{ ast: a.ast, slot }] : []));
  const arrays = [...keys], scalarNames = [];
  for (const { ast } of vals) {
    walk(ast, (m) => {
      if (m.t !== 'var') return;
      const list = t.cols.has(m.name) ? arrays : scalarNames;
      if (!list.includes(m.name)) list.push(m.name);
    });
  }
  const encs = arrays.map((nm) => t.cols.get(nm).enc ?? null);
  const accShape = accs.map((a) => a.key).join(';');
  const shape = JSON.stringify([tf, arrays, encs, fields.map((k) => [k.kind, k.word]), accShape, scalarNames]);
  const spec = { t: tf, arrays, encs, fields, W, vals, scalarNames, RB };
  const count = kernel(engine, `count|${shape}`, () => rowsModule(spec, 'count', shared));
  const scatter = kernel(engine, `scatter|${shape}`, () => rowsModule(spec, 'scatter', shared));
  const write = kernel(engine, `write|${JSON.stringify([W, RB, fields.map((k) => [k.kind, k.word]), J])}`, () => writeModule({ W, RB, fields, J }, shared));
  const P = engine._partitionP ?? partitionsFor(n);
  const plan = planPartitions(sampleWord0(t, fields, Math.min(n, SAMPLE)), P);
  const branchFree = est > 0.2 * n; // many rows start a group: no branch on which
  const agg = kernel(engine, `aggregate|${branchFree}|${W}|${RB}|${accShape}`, () => aggregateModule({ W, RB, accs: accs.map((a) => ({ op: a.op, stored: !!a.ast })) }, branchFree, shared));
  const splitCount = kernel(engine, `split|count|${RB}`, () => splitModule(RB, 'count', shared));
  const splitCopy = kernel(engine, `split|copy|${RB}`, () => splitModule(RB, 'copy', shared));
  const at = (x) => engine.live(x).byteOffset;
  const ptr = (b) => (((at(b.arr) - b.base * RB) % 2 ** 32) + 2 ** 32) % 2 ** 32; // where record 0 of b would be (b.arr holds records b.base on)
  const U = unitCount(OP.UNITS, T, n, null);
  const made = [], out = [];
  const A = (Type, len, list = made) => { const x = engine.alloc(Type, len); list.push(x); return x; };
  try {
    const lay = rowsLayout(arrays.length, fields.length, scalarNames.length);
    const lut = A(Uint16Array, NB), cnt = A(Uint32Array, U * P), prm = A(Uint8Array, lay.bytes), starts = A(Uint32Array, P + 1);
    const recs = A(Uint8Array, n * RB);
    engine.live(lut).set(plan.lut);
    engine.live(cnt).fill(0);
    {
      const dv = view(engine, prm);
      lookupParams(dv, at(lut), at(cnt), at(recs), P, plan);
      arrays.forEach((nm, i) => dv.setUint32(32 + 4 * i, at(t.cols.get(nm).data), true));
      fieldParams(dv, lay.FIELDS, fields);
      scalarNames.forEach((nm, i) => dv.setFloat64(lay.SC + 8 * i, Number(params[nm]), true));
    }
    yield* engine._unitsG(count, n, [at(prm)]);
    const maxRows = offsets(engine.live(cnt), U, P, engine.live(starts));
    yield* engine._unitsG(scatter, n, [at(prm)]);

    // per-thread scratch: room for gMax groups in a partition (more: split again)
    const gMax = Math.max(1, Math.min(maxRows, engine._partitionScratch ?? Math.max(65_536, 4 * Math.ceil(n / P)), Math.floor(SCRATCH_BYTES / (T * (RB + 32)))));
    let capMax = 16;
    while (capMax < 2 * gMax) capMax *= 2;
    const stride = scratchBytes(capMax, gMax, RB);
    const scratch = A(Uint8Array, T * stride + 64), aprm = A(Uint8Array, 32);
    const groups = A(Uint32Array, P), order = A(Uint32Array, P);
    const aggregate = function* (src, partStarts, partGroups, partOrder, parts) {
      bySize(engine.live(partStarts), engine.live(partOrder), parts);
      const dv = view(engine, aprm);
      [ptr(src), at(partStarts), at(partGroups), Math.ceil(at(scratch) / 64) * 64, stride, capMax, gMax, at(partOrder)].forEach((x, i) => dv.setUint32(4 * i, x, true));
      yield* engine._tasksG(agg, parts, [at(aprm)]);
    };
    const all = { arr: recs, base: 0 };
    yield* aggregate(all, starts, groups, order, P);

    // A partition with too many groups: split again by key range (by r's
    // word w, once the words before it are the same in all its records):
    // records s..e of src go to the same places in dst, part by part, each
    // part is aggregated there (or split again, back into src's free
    // places), and the parts' groups, in order, come back over src's first
    // records. src and dst take turns: one extra copy however deep it goes.
    const split = function* (src, dst, s, e, depth, w) {
      if (depth > MAX_DEPTH) throw NO_PLAN;
      const rows = e - s, P2 = partitionsFor(rows), U2 = unitCount(OP.UNITS, T, rows, null);
      for (;;) {
        if (w >= W) throw NO_PLAN; // (can't happen: one key is one group)
        const local = [], B = (Type, len) => A(Type, len, local);
        try {
          const lut2 = B(Uint16Array, NB), cnt2 = B(Uint32Array, U2 * P2), sprm = B(Uint8Array, 48), mm = B(BigUint64Array, 2 * U2);
          const countRecords = function* (pl) {
            engine.live(lut2).set(pl.lut);
            engine.live(cnt2).fill(0);
            const dv = view(engine, sprm);
            lookupParams(dv, at(lut2), at(cnt2), ptr(src), P2, pl);
            [ptr(dst), s, 8 * w, at(mm)].forEach((x, i) => dv.setUint32(32 + 4 * i, x, true));
            yield* engine._unitsG(splitCount, rows, [at(sprm)]);
          };
          yield* countRecords(planPartitions(sampleRecords(engine.live(src.arr), s - src.base, e - src.base, RB, Math.min(rows, SAMPLE), w), P2));
          const x = engine.live(mm);
          let lo = x[0], hi = x[1];
          for (let u = 1; u < U2; u++) { if (x[2 * u] < lo) lo = x[2 * u]; if (x[2 * u + 1] > hi) hi = x[2 * u + 1]; }
          if (lo === hi) { w++; continue; } // this word is the same in every record here
          // the sample's plan put every record in one part: parts evenly over the word's range instead
          if (largest(engine.live(cnt2), U2, P2) === rows) yield* countRecords(planUniform(lo, hi, P2));
          const starts2 = B(Uint32Array, P2 + 1), groups2 = B(Uint32Array, P2), order2 = B(Uint32Array, P2);
          offsets(engine.live(cnt2), U2, P2, engine.live(starts2), s);
          yield* engine._unitsG(splitCopy, rows, [at(sprm)]);
          yield* aggregate(dst, starts2, groups2, order2, P2);
          for (let q = 0; q < P2; q++) {
            if (engine.live(groups2)[q] !== OVERFLOW) continue;
            const st = engine.live(starts2), g = yield* split(dst, src, st[q], st[q + 1], depth + 1, w);
            engine.live(groups2)[q] = g;
          }
          const from = engine.live(dst.arr), to = engine.live(src.arr), st = engine.live(starts2), g = engine.live(groups2);
          let o = s;
          for (let q = 0; q < P2; q++) { to.set(from.subarray((st[q] - dst.base) * RB, (st[q] + g[q] - dst.base) * RB), (o - src.base) * RB); o += g[q]; }
          return o - s;
        } finally {
          for (const y of local) engine.free(y);
        }
      }
    };
    {
      const over = [];
      for (let p = 0; p < P; p++) if (engine.live(groups)[p] === OVERFLOW) over.push(p);
      if (over.length) {
        const st0 = engine.live(starts), alt = A(Uint8Array, Math.max(...over.map((p) => st0[p + 1] - st0[p])) * RB);
        for (const p of over) {
          const st = engine.live(starts), g = yield* split(all, { arr: alt, base: st[p] }, st[p], st[p + 1], 1, 0);
          engine.live(groups)[p] = g;
        }
      }
    }

    // every partition's groups into the result columns at its offset
    const outOff = A(Uint32Array, P);
    let G = 0;
    { const g = engine.live(groups), o = engine.live(outOff); for (let p = 0; p < P; p++) { o[p] = G; G += g[p]; } }
    const keyCols = fields.map(() => A(Float64Array, G, out)), accCols = accs.map(() => A(Float64Array, G, out));
    const wlay = writeLayout(fields.length, J), wprm = A(Uint8Array, wlay.bytes);
    {
      const dv = view(engine, wprm);
      [at(recs), at(starts), at(groups), at(outOff)].forEach((x, i) => dv.setUint32(4 * i, x, true));
      keyCols.forEach((c, i) => dv.setUint32(wlay.KO + 4 * i, at(c), true));
      accCols.forEach((c, j) => dv.setUint32(wlay.AO + 4 * j, at(c), true));
      fieldParams(dv, wlay.FIELDS, fields);
    }
    if (G) yield* engine._tasksG(write, P, [at(wprm)]);
    out.length = 0; // (the caller's now)
    return { G, keyCols, accCols, map };
  } catch (err) {
    if (err === NO_PLAN || err instanceof RangeError) return null; // (RangeError: out of memory; the hash path needs less)
    throw err;
  } finally {
    for (const x of [...made, ...out]) engine.free(x);
  }
}

// Each key's bit field: text codes and whole numbers (within 2^53 of their
// minimum) as their offset from it, NaN as the code after the largest;
// anything else as float bits (64). Fields go into 64-bit words in key
// order without straddling one; more than MAX_WORDS words: null.
function* keyFieldsG(t, keys) {
  const { engine } = t, fields = [];
  for (const arr of keys) {
    const c = t.cols.get(arr);
    if (c.dict) { // (codes can be NaN, e.g. a join's rows without a match)
      const span = Math.max(0, c.dict.length - 1);
      fields.push({ arr, kind: 'int', lo: 0, nanCode: span + 1, maxCode: span + 1 });
      continue;
    }
    const k = c.enc ? { data: c.data, enc: c.enc, n: t.n, type: t.type === Float32Array ? 'f32' : 'f64' } : c.data;
    const [lo, hi, frac, nan] = yield* engine.evalG(['min(k == k ? k : Infinity)', 'max(k == k ? k : -Infinity)', 'count(k == k && k != floor(k))', 'count(k != k)'], { k });
    if (!(hi >= lo)) fields.push({ arr, kind: 'int', lo: 0, nanCode: 1, maxCode: 1 }); // only NaN
    else if (frac === 0 && Number.isFinite(lo) && Number.isFinite(hi) && hi - lo < 2 ** 53) fields.push({ arr, kind: 'int', lo, nanCode: hi - lo + 1, maxCode: hi - lo + (nan ? 1 : 0) });
    else fields.push({ arr, kind: 'float' });
  }
  let word = 0, used = 0;
  for (const k of fields) {
    k.bits = k.kind === 'float' ? 64 : bitsFor(k.maxCode);
    if (used + k.bits > 64) { word++; used = 0; }
    k.word = word;
    used += k.bits;
  }
  if (word >= MAX_WORDS) return null;
  fields.forEach((k, i) => { k.shift = fields.slice(i + 1).filter((x) => x.word === k.word).reduce((s, x) => s + x.bits, 0); });
  return { fields, W: word + 1 };
}

// sources -> accumulators: identical ones shared; count(1) adds a constant
// 1 (nothing stored per row). Each is { key, op, ast (null: constant) }.
function accumulators(sources) {
  const accs = [], map = [];
  for (const src of sources) {
    const it = analyze(src).items[0]; // (one at a time: no limit on reductions here)
    const key = `${it.reduce}|${toSource(it.ast)}`;
    let j = accs.findIndex((a) => a.key === key);
    if (j < 0) {
      const constant = it.reduce === 'count' && toSource(it.ast) === '(1 != 0)';
      j = accs.push({ key, op: it.reduce === 'min' || it.reduce === 'max' ? it.reduce : 'add', ast: constant ? null : it.ast }) - 1;
    }
    map.push(j);
  }
  return { accs, map };
}

function fieldParams(dv, o, fields) {
  fields.forEach((k, i) => {
    const b = o + FIELD * i, int = k.kind === 'int';
    dv.setFloat64(b, int ? k.lo : 0, true);
    dv.setBigUint64(b + 8, BigInt(int ? k.nanCode : 0), true);
    dv.setBigUint64(b + 16, BigInt(k.bits), true);
    dv.setBigUint64(b + 24, BigInt(k.shift), true);
    dv.setBigUint64(b + 32, (1n << BigInt(k.bits)) - 1n, true);
  });
}

function lookupParams(dv, lut, cnt, recs, P, plan) {
  [lut, cnt, recs, P].forEach((x, i) => dv.setUint32(4 * i, x, true));
  dv.setBigUint64(16, plan.base, true);
  dv.setUint32(24, plan.sh, true);
}

// The number of groups, estimated from how often each key (tuple) shows up
// among rows spread over the table (Chao1: seen + once^2 / (2 twice); with
// no key seen twice, the keys seen once scaled up to the whole table); keys
// as 53-bit hashes of their bits, -0 as 0, every NaN alike.
export function estimateGroups(t, keys) {
  const { engine, n } = t, m = Math.min(n, SAMPLE), f32 = t.type === Float32Array, step = n / m;
  const cols = keys.map((k) => { const c = t.cols.get(k); return { d: engine.live(c.data), enc: c.enc ?? null }; });
  const hashes = new Float64Array(m), f = new Float64Array(1), u = new Uint32Array(f.buffer);
  for (let j = 0; j < m; j++) {
    const i = Math.floor(j * step);
    let h1 = 0x811c9dc5, h2 = 0x2545f491;
    for (const { d, enc } of cols) {
      const v = enc ? decodeOne(enc, d[i], f32) : d[i];
      f[0] = v === v ? v + 0 : NaN;
      h1 = Math.imul(Math.imul(h1 ^ u[0], 0x01000193) ^ u[1], 0x01000193);
      h2 = Math.imul(Math.imul(h2 ^ u[1], 0x5bd1e995) ^ u[0], 0x5bd1e995);
      h2 ^= h2 >>> 15;
    }
    hashes[j] = (h1 >>> 0) * 2 ** 21 + (h2 >>> 11);
  }
  hashes.sort();
  let seen = 0, once = 0, twice = 0;
  for (let j = 0; j < m; ) {
    let q = j + 1;
    while (q < m && hashes[q] === hashes[j]) q++;
    seen++;
    if (q - j === 1) once++; else if (q - j === 2) twice++;
    j = q;
  }
  return Math.min(n, seen + (twice ? (once * once) / (2 * twice) : once * (n / m)));
}

// word 0 of r for m rows spread over the table, as doubles (only the
// partitions' balance depends on them), sorted
function sampleWord0(t, fields, m) {
  const { engine, n } = t, f32 = t.type === Float32Array, step = n / m;
  const cols = fields.filter((k) => k.word === 0).map((k) => { const c = t.cols.get(k.arr); return { k, d: engine.live(c.data), enc: c.enc ?? null }; });
  const out = new Float64Array(m), f = new Float64Array(1), u = new Uint32Array(f.buffer);
  for (let j = 0; j < m; j++) {
    const i = Math.floor(j * step);
    let r = 0;
    for (const { k, d, enc } of cols) {
      const v = enc ? decodeOne(enc, d[i], f32) : d[i];
      if (k.kind === 'int') r = r * 2 ** k.bits + (v === v ? v - k.lo : k.nanCode);
      else if (v !== v) r = 2 ** 64;
      else { f[0] = v + 0; r = u[1] >= 0x80000000 ? (~u[1] >>> 0) * 2 ** 32 + (~u[0] >>> 0) : (u[1] + 0x80000000) * 2 ** 32 + u[0]; }
    }
    out[j] = r;
  }
  return out.sort();
}

// word w of m records spread over records s..e (as doubles), sorted
function sampleRecords(b, s, e, RB, m, w) {
  const u = new Uint32Array(b.buffer, b.byteOffset, Math.floor(b.length / 4)), out = new Float64Array(m), step = (e - s) / m;
  for (let j = 0; j < m; j++) { const a = ((s + Math.floor(j * step)) * RB + 8 * w) / 4; out[j] = u[a + 1] * 2 ** 32 + u[a]; }
  return out.sort();
}

// The lookup from sorted samples of word 0: NB buckets of (r0 - base) >>
// sh over the bulk of the samples (the m/2P smallest and largest may fall
// outside, in the end buckets), merged in order into P partitions of about
// the same number of samples. Any lookup in order keeps the partitions in
// key order; the samples only make them even.
function planPartitions(sorted, P) {
  const m = sorted.length, cut = Math.floor(m / (2 * P));
  let lo = sorted[cut], hi = sorted[m - 1 - cut];
  if (!(hi > lo)) { lo = sorted[0]; hi = sorted[m - 1]; } // (few distinct values: keep them all apart)
  lo = Math.min(lo, 2 ** 64 - 2 ** 12); // (below 2^64 as a uint64)
  let sh = 0;
  while ((hi - lo) / 2 ** sh >= NB) sh++;
  const counts = new Float64Array(NB), scale = 2 ** -sh;
  for (let j = 0; j < m; j++) { const b = sorted[j] < lo ? 0 : Math.floor((sorted[j] - lo) * scale); counts[b < NB ? b : NB - 1]++; }
  const lut = new Uint16Array(NB);
  for (let b = 0, cum = 0; b < NB; b++) { lut[b] = Math.min(P - 1, Math.floor((cum * P) / m)); cum += counts[b]; }
  return { lut, base: BigInt(lo), sh };
}

// buckets evenly over an exact range [lo, hi] (BigInts), P partitions
// evenly over the buckets: lo and hi always end up apart
function planUniform(lo, hi, P) {
  let sh = 0;
  while ((hi - lo) >> BigInt(sh) >= BigInt(NB)) sh++;
  const span = Number((hi - lo) >> BigInt(sh)) + 1, lut = new Uint16Array(NB);
  for (let b = 0; b < NB; b++) lut[b] = Math.min(P - 1, Math.floor((Math.min(b, span - 1) * P) / span));
  return { lut, base: lo, sh };
}

// counters [units][P] -> partition starts (P + 1, from `base`) and, in
// place, each unit's first place in each partition; returns the most rows
// in one partition
function offsets(c, U, P, starts, base = 0) {
  const tot = new Float64Array(P);
  for (let u = 0; u < U; u++) for (let p = 0, b = u * P; p < P; p++) tot[p] += c[b + p];
  let at = base, most = 0;
  for (let p = 0; p < P; p++) { starts[p] = at; at += tot[p]; if (tot[p] > most) most = tot[p]; }
  starts[P] = at;
  const next = Float64Array.from(starts.subarray(0, P));
  for (let u = 0; u < U; u++) for (let p = 0, b = u * P; p < P; p++) { const x = c[b + p]; c[b + p] = next[p]; next[p] += x; }
  return most;
}

function largest(c, U, P) {
  const tot = new Float64Array(P);
  for (let u = 0; u < U; u++) for (let p = 0, b = u * P; p < P; p++) tot[p] += c[b + p];
  return Math.max(...tot);
}

// partitions, biggest first (the last task to start shouldn't be the longest)
function bySize(starts, order, P) {
  const idx = Array.from({ length: P }, (_, p) => p);
  idx.sort((a, b) => starts[b + 1] - starts[b] - (starts[a + 1] - starts[a]) || a - b);
  order.set(idx);
}
