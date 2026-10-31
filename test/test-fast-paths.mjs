// Shortcuts for common queries give the same answers as the general path:
// each query runs as written and in a form the shortcut doesn't apply to
// (e.g. `rn + 0 <= 3`), on tables big enough for the shortcuts (ties,
// missing values, descending and text keys, several partition kinds).
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { Engine, sql } from '../src/node.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
const norm = (rows) => JSON.stringify(rows.map((r) => Object.values(r).map((v) => (typeof v === 'number' && v !== v ? null : v))).sort((a, b) => (JSON.stringify(a) < JSON.stringify(b) ? -1 : 1)));

let seed = 7;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const N = 30_000, REG = ['EU', 'US', 'APAC', 'LATAM', 'MEA'];
const cols = {
  cust: Float64Array.from({ length: N }, () => Math.floor(rnd() * 500)),
  ts: Float64Array.from({ length: N }, () => (rnd() < 0.02 ? NaN : Math.floor(rnd() * 200))), // many ties, some missing
  price: Float64Array.from({ length: N }, () => (rnd() < 0.02 ? NaN : Math.round(rnd() * 100000) / 100)),
  qty: Float64Array.from({ length: N }, () => 1 + Math.floor(rnd() * 9)),
  region: Array.from({ length: N }, () => REG[Math.floor(rnd() * REG.length)]),
  day: Array.from({ length: N }, () => new Date(Date.UTC(2024, 0, 1) + Math.floor(rnd() * 400) * 86_400_000 + Math.floor(rnd() * 86_400) * 1000)),
};

for (const opts of [{}, { threads: 1 }, { jit: false }]) {
  const engine = new Engine(opts);
  const t = { t: engine.table(cols) };
  const L = JSON.stringify(opts);
  const same = (fast, plain, label, mark) => {
    checks++;
    const a = norm(sql(fast, t).toRows()), b = norm(sql(plain, t).toRows());
    if (a !== b) fail(`${L} ${label}: answers differ\n  ${a.slice(0, 200)}\n  ${b.slice(0, 200)}`);
    if (mark) { checks++; if (!sql(fast, t).explain().includes(mark)) fail(`${L} ${label}: shortcut not used`); }
  };

  // first rows per partition: ROW_NUMBER / RANK with rn <= k
  const FIRST = 'rows that can rank';
  const win = (fn, over, cond) => `SELECT * FROM (SELECT cust, ts, price, region, qty, ${fn}() OVER (${over}) AS r FROM t) x WHERE ${cond}`;
  for (const [fn, over] of [
    ['ROW_NUMBER', 'PARTITION BY cust ORDER BY ts'],
    ['RANK', 'PARTITION BY cust ORDER BY ts'],
    ['RANK', 'PARTITION BY region ORDER BY price DESC'],
    ['ROW_NUMBER', 'ORDER BY ts DESC'],
    ['RANK', 'PARTITION BY cust ORDER BY region'],
    ['RANK', 'PARTITION BY cust ORDER BY region DESC, ts'],
    ['ROW_NUMBER', 'PARTITION BY price ORDER BY ts'], // (partition numbers from the hash table)
    ['ROW_NUMBER', 'PARTITION BY region, qty ORDER BY ts, price DESC'],
  ]) {
    for (const [cond, plain] of [['r <= 3', 'r + 0 <= 3'], ['r < 2', 'r + 0 < 2'], ['4 >= r', '4 >= r + 0'], ['r = 2', 'r + 0 = 2']]) {
      same(win(fn, over, cond), win(fn, over, plain), `${fn} OVER (${over}) WHERE ${cond}`, FIRST);
    }
  }
  // other items in the same window, and a ranking condition plus another filter
  const several = (cond) => `SELECT * FROM (SELECT cust, ts, ROW_NUMBER() OVER (PARTITION BY cust ORDER BY ts) AS r, RANK() OVER (PARTITION BY cust ORDER BY ts) AS k, LAG(price) OVER (PARTITION BY cust ORDER BY ts) AS prev FROM t) x WHERE ${cond}`;
  same(several('r <= 2 AND k <= 2'), several('r + 0 <= 2 AND k + 0 <= 2'), 'several items', FIRST);
  // COUNT(DISTINCT) of whole numbers in a small range (a byte map) vs the
  // hash table (the same values shifted by 0.5)
  for (const [x, by] of [['cust', 'region'], ['cust', 'qty'], ['ts', 'region'], ['qty - 5', 'cust'], ['cust', null], ['ts * -1', 'region, qty']]) {
    const q = (v) => `SELECT ${by ? `${by}, ` : ''}COUNT(DISTINCT ${v}) AS d, COUNT(*) AS n FROM t ${by ? `GROUP BY ${by}` : ''}`;
    same(q(x), q(`${x} + 0.5`), `COUNT(DISTINCT ${x})${by ? ` BY ${by}` : ''}`);
  }
  // percentiles of few partitions on big tables (bucket counts on every
  // thread): against sorting each partition's values here
  {
    const M = 100_000, groups = ['a', 'b', 'c', 'd', 'e', 'f', 'g'];
    const pc = { grp: Array.from({ length: M }, () => groups[Math.floor(rnd() * groups.length)]), k: Float64Array.from({ length: M }, () => Math.floor(rnd() * 5)) };
    for (const [label, gen] of [
      ['spread', () => (rnd() < 0.03 ? NaN : Math.round((rnd() - 0.3) * 1e6) / 100)],
      ['skewed', () => (rnd() < 0.9 ? 42 : rnd() < 0.5 ? -0 : rnd() * 1e9)], // most values equal
      ['tiny range', () => 1 + Math.floor(rnd() * 3) * 1e-9],
      ['infinities', () => (rnd() < 0.01 ? -Infinity : rnd() * 100)],
    ]) {
      const v = Float64Array.from({ length: M }, gen);
      const tt = { p: engine.table({ ...pc, v }) };
      const ref = (keyOf) => {
        const by = new Map();
        for (let i = 0; i < M; i++) { if (v[i] !== v[i]) continue; const key = keyOf(i); if (!by.has(key)) by.set(key, []); by.get(key).push(v[i]); }
        const out = new Map();
        for (const [key, xs] of by) {
          xs.sort((a, b) => a - b);
          const m = xs.length, cont = (p) => { const r = p * (m - 1), lo = Math.floor(r); return lo + 1 < m && r > lo ? xs[lo] + (r - lo) * (xs[lo + 1] - xs[lo]) : xs[lo]; };
          out.set(key, [cont(0.5), cont(0.9), xs[Math.max(0, Math.ceil(0.25 * m) - 1)]]);
        }
        return out;
      };
      const SEL = 'MEDIAN(v) AS med, QUANTILE_CONT(v, 0.9) AS p90, PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY v) AS q1';
      for (const [by, keyOf] of [['grp', (i) => pc.grp[i]], ['grp, k', (i) => `${pc.grp[i]}|${pc.k[i]}`], [null, () => '']]) {
        const want = ref(keyOf), rows = sql(`SELECT ${by ? `${by}, ` : ''}${SEL} FROM p ${by ? `GROUP BY ${by}` : ''}`, tt).toRows();
        checks++;
        const bad = rows.find((r) => { const w = want.get(by === 'grp' ? r.grp : by ? `${r.grp}|${r.k}` : ''); return !w || [r.med, r.p90, r.q1].some((x, j) => !(x === w[j] || Math.abs(x - w[j]) <= 1e-9 * Math.max(1, Math.abs(w[j])))); });
        if (bad || rows.length !== want.size) fail(`${L} percentiles (${label}) by ${by}: ${JSON.stringify(bad)} want ${JSON.stringify(bad && want.get(by === 'grp' ? bad.grp : by ? `${bad.grp}|${bad.k}` : ''))}`);
      }
    }
  }
  // CASE with quoted labels only (codes on every thread) vs a label made by an
  // expression ('hi' || 'gh': the per-value path)
  for (const [fast, plain] of [
    ["CASE WHEN price >= 500 THEN 'high' WHEN price >= 100 THEN 'mid' ELSE 'low' END", "CASE WHEN price >= 500 THEN 'hi' || 'gh' WHEN price >= 100 THEN 'mid' ELSE 'low' END"],
    ["CASE WHEN qty > 6 THEN 'x' WHEN qty > 3 THEN NULL END", "CASE WHEN qty > 6 THEN 'x' || '' WHEN qty > 3 THEN NULL END"],
    ["CASE region WHEN 'EU' THEN 'europe' WHEN 'US' THEN 'america' WHEN 'LATAM' THEN 'america' ELSE region END", null], // (ELSE region: not all quoted)
    ["CASE region WHEN 'EU' THEN 'europe' WHEN 'US' THEN 'america' WHEN 'LATAM' THEN 'america' ELSE 'other' END", "CASE region WHEN 'EU' THEN 'eur' || 'ope' WHEN 'US' THEN 'america' WHEN 'LATAM' THEN 'america' ELSE 'other' END"],
    ["CASE WHEN ts IS NULL THEN 'none' ELSE 'some' END", "CASE WHEN ts IS NULL THEN 'no' || 'ne' ELSE 'some' END"],
  ]) {
    if (!plain) continue;
    same(`SELECT ${fast} AS b, COUNT(*) AS n, SUM(qty) AS q FROM t GROUP BY 1`, `SELECT ${plain} AS b, COUNT(*) AS n, SUM(qty) AS q FROM t GROUP BY 1`, `GROUP BY ${fast}`);
    same(`SELECT cust, ${fast} AS b FROM t WHERE ${fast} = 'mid' OR ${fast} = 'x' OR ${fast} = 'america'`, `SELECT cust, ${plain} AS b FROM t WHERE ${plain} = 'mid' OR ${plain} = 'x' OR ${plain} = 'america'`, `WHERE ${fast}`);
    checks++;
    const a = sql(`SELECT ${fast} AS b FROM t ORDER BY b LIMIT 5000`, t).toRows(), b = sql(`SELECT ${plain} AS b FROM t ORDER BY b LIMIT 5000`, t).toRows();
    if (JSON.stringify(a) !== JSON.stringify(b)) fail(`${L} ORDER BY ${fast}`);
  }
  // group keys a whole number of days / hours / seconds apart over a wide
  // range (numbered by that step) vs the hash table (shifted by 0.5)
  for (const [step, by] of [[86_400_000, ''], [3_600_000, ', region'], [1_000, ', qty'], [86_400_000 * 7, ', region, qty']]) {
    const d = `(cust * ${step} + 1700000000000)`;
    same(`SELECT ${d} AS d${by}, COUNT(*) AS n, SUM(price) AS s FROM t GROUP BY 1${by}`,
      `SELECT d0 - 0.5 AS d${by}, n, s FROM (SELECT ${d} + 0.5 AS d0${by}, COUNT(*) AS n, SUM(price) AS s FROM t GROUP BY 1${by}) x`, `GROUP BY ${step} ms steps${by}`);
  }
  {
    const days = new Map();
    for (let i = 0; i < N; i++) { const k = Math.floor(cols.day[i].getTime() / 86_400_000) * 86_400_000; days.set(k, (days.get(k) ?? 0) + 1); }
    const rows = sql("SELECT DATE_TRUNC('day', day) AS d, COUNT(*) AS n FROM t GROUP BY 1", t).toRows();
    checks++;
    if (rows.length !== days.size || rows.some((r) => days.get(r.d.getTime()) !== r.n)) fail(`${L} DATE_TRUNC day: ${JSON.stringify(rows.slice(0, 3))}`);
  }
  // the same on a dataset (run chunk by chunk) as on the table
  if (engine.jit) {
    const path = join(mkdtempSync(join(tmpdir(), 'vectro-fast-')), 'd.vectro');
    engine.writeDataset(path, t.t, { rowsPerChunk: 7_000 });
    const ds = { t: engine.openDataset(path) };
    for (const q of [
      "SELECT CASE WHEN price >= 500 THEN 'high' WHEN price >= 100 THEN 'mid' ELSE 'low' END AS b, COUNT(*) AS n FROM t GROUP BY 1",
      'SELECT region, COUNT(DISTINCT cust) AS d, MEDIAN(price) AS m FROM t GROUP BY region',
      'SELECT * FROM (SELECT cust, ts, ROW_NUMBER() OVER (PARTITION BY cust ORDER BY ts) AS r FROM t) x WHERE r <= 2',
    ]) {
      checks++;
      const a = norm(sql(q, ds).toRows()), b = norm(sql(q, t).toRows());
      if (a !== b) fail(`${L} dataset: ${q}\n  ${a.slice(0, 200)}\n  ${b.slice(0, 200)}`);
    }
    await ds.t.close?.();
    rmSync(dirname(path), { recursive: true, force: true });
  }
  // a window the shortcut can't serve (LEAD reads later rows): same answers, no shortcut
  checks++;
  const lead = 'SELECT * FROM (SELECT cust, ts, ROW_NUMBER() OVER (PARTITION BY cust ORDER BY ts) AS r, LEAD(price) OVER (PARTITION BY cust ORDER BY ts) AS nxt FROM t) x WHERE r <= 3';
  if (sql(lead, t).explain().includes(FIRST)) fail(`${L} LEAD: shortcut used`);
  await engine.close();
}

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} fast-path checks passed`);
process.exit(failures ? 1 : 0);
