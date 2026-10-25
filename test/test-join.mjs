// Hash joins against plain JavaScript: number and text keys, several keys,
// duplicates on both sides, NaN keys (never match), -0 = 0, labels only one
// side has, filters on either side, inner and left joins, big tables (every
// thread) and every engine.
import { Engine } from '../src/node.mjs';
import { Engine as PortableEngine } from '../src/portable.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
let seed = 41;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const pick = (xs) => xs[Math.floor(rnd() * xs.length)];
const norm = (v) => (typeof v === 'number' && v !== v ? 'NaN' : Object.is(v, -0) ? 0 : v);

function tables(nL, nR, keySpace) {
  const regionsL = ['EU', 'US', 'APAC', 'LATAM'], regionsR = ['US', 'EU', 'MEA', 'APAC']; // MEA / LATAM: one side only
  const left = { cust: [], region: [], amount: [] }, right = { cust: [], region: [], name: [], score: [] };
  for (let i = 0; i < nL; i++) {
    left.cust.push(rnd() < 0.02 ? NaN : rnd() < 0.05 ? -0 : Math.floor(rnd() * keySpace) * 1.5);
    left.region.push(pick(regionsL));
    left.amount.push(Math.round(rnd() * 1000));
  }
  for (let i = 0; i < nR; i++) {
    right.cust.push(rnd() < 0.02 ? NaN : Math.floor(rnd() * keySpace * 1.2) * 1.5);
    right.region.push(pick(regionsR));
    right.name.push(`n${Math.floor(rnd() * 50)}`);
    right.score.push(i);
  }
  return { left, right };
}

// reference: nested loop by key strings (NaN never matches)
function reference(left, right, lk, rk, how, filterL = () => true, filterR = () => true) {
  const nL = left[Object.keys(left)[0]].length, nR = right[Object.keys(right)[0]].length;
  const keyOf = (t, keys, i) => { const parts = keys.map((k) => t[k][i]); return parts.some((v) => v !== v) ? null : JSON.stringify(parts.map(norm)); };
  const index = new Map();
  for (let j = 0; j < nR; j++) {
    if (!filterR(j)) continue;
    const key = keyOf(right, rk, j);
    if (key === null) continue;
    (index.get(key) ?? index.set(key, []).get(key)).push(j);
  }
  const out = [];
  for (let i = 0; i < nL; i++) {
    if (!filterL(i)) continue;
    const key = keyOf(left, lk, i);
    const m = key === null ? undefined : index.get(key);
    if (m) for (const j of m) out.push([i, j]);
    else if (how === 'left') out.push([i, -1]);
  }
  return out;
}

const CASES = [
  // [on, lk, rk, how, left filter (text, js), right filter]
  ['cust', ['cust'], ['cust'], 'inner'],
  ['cust', ['cust'], ['cust'], 'left'],
  [['cust', 'region'], ['cust', 'region'], ['cust', 'region'], 'inner'],
  [['region', 'cust'], ['region', 'cust'], ['region', 'cust'], 'left'],
  [{ left: 'region', right: 'region' }, ['region'], ['region'], 'inner', ['amount < 20', (t, i) => t.amount[i] < 20], ['score < 30', (t, j) => t.score[j] < 30]],
  ['cust', ['cust'], ['cust'], 'left', ['amount > 500', (t, i) => t.amount[i] > 500], ["region == 'EU'", (t, j) => t.region[j] === 'EU']],
];

function check(label, got, left, right, pairs, rCols) {
  checks++;
  if (got.length !== pairs.length) { fail(`${label}: ${got.length} rows, want ${pairs.length}`); return; }
  for (let r = 0; r < pairs.length; r++) {
    const [i, j] = pairs[r], row = got[r];
    const want = { cust: left.cust[i], region: left.region[i], amount: left.amount[i] };
    for (const [name, outName] of rCols) want[outName] = j < 0 ? (typeof right[name][0] === 'string' ? '' : NaN) : right[name][j];
    for (const k of Object.keys(want)) {
      if (norm(row[k]) !== norm(want[k])) { fail(`${label} row ${r} ${k}: got ${row[k]}, want ${want[k]}`); return; }
    }
  }
}

const engines = [
  ['threads', () => new Engine(), {}],
  ['2 threads', () => new Engine({ threads: 2 }), {}],
  ['single thread', () => new Engine({ threads: 1 }), {}],
  ['interpreter', () => new PortableEngine({ jit: false }), {}],
  ['float32', () => new Engine(), { type: Float32Array }],
];
for (const [label, make, opts] of engines) {
  const e = make();
  for (const [nL, nR, space] of [[0, 5, 10], [40, 0, 10], [3000, 800, 400], [250_000, 60_000, 50_000]]) {
    if (label === 'interpreter' && nL > 10_000) continue;
    const { left, right } = tables(nL, nR, space);
    const L = e.table(left, opts), R = e.table(right, opts);
    for (const [on, lk, rk, how, fl, fr] of CASES) {
      if ((fl || fr) && (!nL || !nR)) continue; // (an empty table's text column can't be filtered as text)
      const lt = fl ? L.filter(fl[0]) : L, rt = fr ? R.filter(fr[0]) : R;
      const pairs = reference(left, right, lk, rk, how, fl ? (i) => fl[1](left, i) : undefined, fr ? (j) => fr[1](right, j) : undefined);
      const sameNames = new Set(rk.filter((n, k) => lk[k] === n));
      const rCols = Object.keys(right).filter((n) => !sameNames.has(n)).map((n) => [n, n in left ? `${n}_right` : n]);
      let got;
      try { got = lt.join(rt, { on, how }).toRows(); } catch (err) { fail(`${label} ${nL}x${nR} ${JSON.stringify(on)} ${how}: ${err.message}`); continue; }
      check(`${label} ${nL}x${nR} ${JSON.stringify(on)} ${how}${fl ? ' filtered' : ''}`, got, left, right, pairs, rCols);
    }
    L.dispose(); R.dispose();
  }
  await e.close();
}

// mistakes are refused clearly
{
  const e = new Engine();
  const a = e.table({ id: [1, 2], name: ['x', 'y'] }), b = e.table({ id: [1], tag: ['t'] });
  for (const [label, f, text] of [
    ['unknown column', () => a.join(b, { on: 'nope' }), 'no column "nope"'],
    ['text vs number', () => a.join(b, { on: { left: 'name', right: 'id' } }), 'both be text or both numbers'],
    ['bad how', () => a.join(b, { on: 'id', how: 'outer' }), "'inner' or 'left'"],
    ['no on', () => a.join(b, {}), 'join needs on'],
    ['key counts', () => a.join(b, { on: { left: ['id', 'name'], right: ['id'] } }), 'as many left keys'],
  ]) {
    let msg = '';
    try { f(); } catch (err) { msg = err.message; }
    checks++; if (!msg.includes(text)) fail(`${label}: ${msg}`);
  }
  await e.close();
}
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} join checks passed`);
process.exit(failures ? 1 : 0);
