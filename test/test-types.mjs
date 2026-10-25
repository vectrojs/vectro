// Missing values (SQL rules) and dates, against plain JavaScript, on every engine.
import { Engine, SplitEngine, readArrow } from '../src/node.mjs';
import { Engine as PortableEngine } from '../src/portable.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
let seed = 23;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const pick = (xs) => xs[Math.floor(rnd() * xs.length)];
const close = (a, b) => (Number.isNaN(a) && Number.isNaN(b)) || a === b || Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(b));
const present = (v) => v === v;

// ---- missing values ------------------------------------------------------------------
function missingData(n) {
  const cols = { g: [], x: [], y: [] };
  for (let i = 0; i < n; i++) {
    const g = pick(['a', 'b', 'c', 'none']);
    cols.g.push(g);
    cols.x.push(g === 'none' || rnd() < 0.2 ? NaN : Math.round(rnd() * 100) - 30); // group 'none': every x missing
    cols.y.push(rnd() < 0.1 ? NaN : Math.round(rnd() * 10));
  }
  return cols;
}
function missingRef(rows) {
  const xs = rows.map((r) => r.x).filter(present);
  return {
    s: xs.reduce((a, v) => a + v, 0),
    m: xs.length ? xs.reduce((a, v) => a + v, 0) / xs.length : NaN,
    lo: xs.length ? Math.min(...xs) : NaN,
    hi: xs.length ? Math.max(...xs) : NaN,
    c: xs.length,
    pos: rows.filter((r) => r.x > 0).length,
    miss: rows.filter((r) => !present(r.x)).length,
    co: rows.reduce((a, r) => a + (present(r.x) ? r.x : present(r.y) ? r.y : 0), 0),
    n: rows.length,
  };
}
const MISSING_SPEC = { s: 'sum(x)', m: 'mean(x)', lo: 'min(x)', hi: 'max(x)', c: 'count(x)', pos: 'count(x > 0)', miss: 'count(isMissing(x))', co: 'sum(coalesce(x, y, 0))', n: 'count()' };
const same = (got, want) => Object.keys(want).every((k) => close(got[k], want[k]));

for (const [label, make, opts] of [
  ['threads', () => new Engine(), {}], ['single thread', () => new Engine({ threads: 1 }), {}],
  ['interpreter', () => new PortableEngine({ jit: false }), {}], ['float32', () => new Engine(), { type: Float32Array }],
  ['split', () => new SplitEngine({ workers: 3 }), {}],
]) {
  const e = make();
  for (const n of [500, 120_000]) {
    if (label === 'interpreter' && n > 10_000) continue;
    const cols = missingData(n);
    const rows = cols.g.map((g, i) => ({ g, x: cols.x[i], y: cols.y[i] }));
    const t = e.tableAsync ? await e.tableAsync(cols, opts) : e.table(cols, opts);
    for (const [ftext, fjs] of [[null, () => true], ['y > 3', (r) => r.y > 3]]) {
      const v = ftext ? t.filter(ftext) : t;
      const got = await v.aggAsync(MISSING_SPEC);
      const want = missingRef(rows.filter(fjs));
      checks++; if (!same(got, want)) fail(`${label} n=${n} ${ftext ?? ''} agg: ${JSON.stringify(got)} vs ${JSON.stringify(want)}`);
      const groups = (await v.groupBy('g').aggAsync(MISSING_SPEC)).toRows();
      for (const r of groups) {
        const w = missingRef(rows.filter(fjs).filter((x) => x.g === r.g));
        checks++; if (!same(r, w)) fail(`${label} n=${n} ${ftext ?? ''} group ${r.g}: ${JSON.stringify(r)} vs ${JSON.stringify(w)}`);
      }
    }
    await t.dispose();
  }
  await e.close();
}

// ---- dates ---------------------------------------------------------------------------------
const FNS = {
  year: (d) => d.getUTCFullYear(), month: (d) => d.getUTCMonth() + 1, day: (d) => d.getUTCDate(),
  hour: (d) => d.getUTCHours(), minute: (d) => d.getUTCMinutes(), second: (d) => d.getUTCSeconds(),
  weekday: (d) => ((d.getUTCDay() + 6) % 7) + 1,
  dayStart: (d) => Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()),
  monthStart: (d) => Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1),
  weekStart: (d) => Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) - ((d.getUTCDay() + 6) % 7) * 86_400_000,
  hourStart: (d) => Math.floor(d.getTime() / 3_600_000) * 3_600_000,
};
for (const [label, make, opts] of [
  ['threads', () => new Engine(), {}], ['single thread', () => new Engine({ threads: 1 }), {}],
  ['interpreter', () => new PortableEngine({ jit: false }), {}], ['split', () => new SplitEngine({ workers: 3 }), {}],
]) {
  const e = make();
  const n = label === 'interpreter' ? 3000 : 60_000;
  const ts = Array.from({ length: n }, () => (rnd() < 0.03 ? null : new Date(Math.floor((rnd() - 0.43) * 1.3e13)))); // 1800..2200, some missing
  const t = e.tableAsync ? await e.tableAsync({ ts, k: ts.map((_, i) => i) }, opts) : e.table({ ts, k: ts.map((_, i) => i) }, opts);
  // functions, through derive (normal tables) or an aggregation per function
  for (const [fn, js] of Object.entries(FNS)) {
    const want = ts.reduce((a, d) => a + (d ? js(d) : 0), 0);
    const got = (await t.aggAsync({ v: `sum(${fn}(ts))` })).v;
    checks++; if (!close(got, want)) fail(`${label} sum(${fn}(ts)): ${got} vs ${want}`);
  }
  // literals, date(), params
  const cut = new Date('2000-02-29T12:00:00Z');
  const after = ts.filter((d) => d && d >= cut).length;
  for (const [f, params] of [["ts >= '2000-02-29T12:00:00Z'", {}], ["ts >= date('2000-02-29T14:00:00+02:00')", {}], ['ts >= since', { since: cut }]]) {
    checks++;
    const got = await t.filter(f, params).countAsync();
    if (got !== after) fail(`${label} ${f}: ${got} vs ${after}`);
  }
  // rows come back as Date objects (missing: null)
  const rows = await t.toRowsAsync(50);
  checks++;
  const byK = new Map(rows.map((r) => [r.k, r.ts]));
  if ([...byK].some(([k, d]) => (ts[k] ? !(d instanceof Date) || d.getTime() !== ts[k].getTime() : d !== null))) fail(`${label} toRows dates`);
  // group by month (derived) and by a date column: keys come back as dates
  if (!e.tableAsync) {
    const g = t.derive('m', 'monthStart(ts)').groupBy('m').agg({ n: 'count()' }).toRows();
    const months = new Map();
    for (const d of ts) { const key = d ? FNS.monthStart(d) : 'none'; months.set(key, (months.get(key) ?? 0) + 1); }
    checks++;
    if (g.length !== months.size || g.some((r) => (r.m === null ? months.get('none') : months.get(r.m.getTime())) !== r.n)) fail(`${label} group by monthStart`);
    const sorted = t.sort('ts').toRows(5);
    const firsts = ts.filter(Boolean).map((d) => d.getTime()).sort((a, b) => a - b).slice(0, 5);
    checks++; if (sorted.some((r, i) => r.ts.getTime() !== firsts[i])) fail(`${label} sort by date`);
    // Arrow keeps dates
    const back = readArrow(t.toArrow());
    checks++; if (!back.dates.includes('ts')) fail(`${label} Arrow date type`);
  }
  await e.close();
}

// CSV: ISO columns are found and parsed; JSON-line streams with a date column
{
  const e = new Engine();
  const csv = ['when,region,amount', '2024-01-31T10:00:00Z,EU,5', '"2024-02-01 08:30",US,6', ',EU,7', '2023-12-31,EU,', '2024-03-10T01:02:03+05:30,US,8'].join('\n');
  const t = e.parseCSV(csv);
  const want = ['2024-01-31T10:00:00.000Z', '2024-02-01T08:30:00.000Z', null, '2023-12-31T00:00:00.000Z', '2024-03-09T19:32:03.000Z'];
  checks++;
  if (JSON.stringify(t.toRows().map((r) => r.when && r.when.toISOString())) !== JSON.stringify(want)) fail(`CSV dates: ${JSON.stringify(t.toRows())}`);
  checks++; if (t.filter("when >= '2024-02-01'").count() !== 2) fail('CSV date filter');
  checks++; if (t.agg({ s: 'sum(amount)', c: 'count(amount)' }).s !== 26) fail('CSV missing amount');
  await e.close();
  const se = new SplitEngine({ workers: 2 });
  const s = await se.streamAsync({ format: 'ndjson', columns: { at: 'date', v: 'number' } });
  await s.push('{"at":"2024-01-01T00:00:00Z","v":1}\n{"at":1704153600000,"v":2}\n{"at":null,"v":3}\n{"at":"2024-01-03","v":4}\n');
  await s.flushAsync();
  checks++; if ((await s.filter("at >= '2024-01-02'").countAsync()) !== 2) fail('stream date filter');
  const w = await s.windowAsync({ every: 86_400_000, last: 10 * 86_400_000, timeColumn: 'at', agg: { v: 'sum(v)' } });
  const series = await w.readAsync({ series: true });
  checks++; if (series.map((r) => r.v).join() !== '1,2,4') fail(`stream date window: ${JSON.stringify(series)}`);
  await se.close();
}

// bad date text is refused clearly
{
  const e = new Engine();
  const t = e.table({ ts: [new Date(0)] });
  for (const [f, text] of [["ts > 'yesterday'", "isn't a date"], ["date('soon') > 0", 'date() needs ISO text']]) {
    let msg = '';
    try { t.filter(f).count(); } catch (err) { msg = err.message; }
    checks++; if (!msg.includes(text)) fail(`${f}: ${msg}`);
  }
  await e.close();
}
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} type checks passed`);
process.exit(failures ? 1 : 0);
