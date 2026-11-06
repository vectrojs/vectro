// Audit round 5 (streams): CSV and JSON lines pushed whole, in random pieces,
// a byte at a time and with empty pushes; keepRows (the newest rows across
// workers), keepMs (rows newer than the newest time minus keepMs), windows
// (whole slices ending at the newest time: count, sum, mean, approxDistinct,
// series) and queries while pushing, with 1 and 3 workers, checked against a
// plain-JS model of the docs (vectro-qa-2026-11-05/stream, 1 / 3 / 7 workers).
// Also: a row that arrives late with an old time is dropped by keepMs (it stayed:
// rows were dropped only from the front), and a blank line in a CSV stream is
// not a row.
import { MODE } from './test-mode.mjs';
if (MODE !== 'node') { console.log('streams run on SplitEngine workers here (Node): skipped in this mode'); process.exit(0); }
const { SplitEngine } = await import('../src/node.mjs');

let seed = 11; const rnd = () => (seed = (seed * 16807) % 2147483647) / 2147483647;
let pass = 0, fail = 0; const bad = [];
const rep = (k, v) => (typeof v === 'number' && !Number.isFinite(v) ? String(v) : Object.is(v, -0) ? 0 : typeof v === 'number' ? +v.toPrecision(10) : v);
const ok = (got, want, msg) => { const g = JSON.stringify(got, rep), w = JSON.stringify(want, rep); if (g === w) pass++; else { fail++; bad.push(`${msg}\n     got  ${g.slice(0, 260)}\n     want ${w.slice(0, 260)}`); } };
const miss = (x) => x === '' || x === null || x === undefined || Number.isNaN(x);
// rows: time mostly rising with some out of order and duplicates on slice edges; prices with missing; labels that keep growing
function makeRows(n, { grow = true } = {}) {
  const rows = [];
  for (let i = 0; i < n; i++) {
    const ts = rnd() < 0.05 ? Math.max(0, i * 10 - Math.floor(rnd() * 3000)) : rnd() < 0.05 ? Math.floor(i / 100) * 1000 : i * 10; // late rows; rows exactly on a 1-second edge
    const region = rnd() < 0.05 ? '' : grow && rnd() < 0.3 ? `R${i}` : ['EU', 'US', 'APAC', 'é😀', 'a,b'][Math.floor(rnd() * 5)];
    rows.push({ ts, cust: Math.floor(rnd() * 300), price: rnd() < 0.05 ? NaN : Math.round(rnd() * 10000) / 100, region });
  }
  return rows;
}
const csvField = (s) => (/[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s);
const toCSV = (rows) => rows.map((r) => `${r.ts},${r.cust},${Number.isNaN(r.price) ? '' : r.price},${csvField(r.region)}`).join('\n') + '\n';
const toNDJSON = (rows) => rows.map((r) => JSON.stringify({ ts: r.ts, cust: r.cust, price: Number.isNaN(r.price) ? null : r.price, region: r.region === '' ? null : r.region })).join('\n') + '\n';
const COLS = { ts: 'number', cust: 'number', price: 'number', region: 'category' };
const stats = (rows) => { const p = rows.map((r) => r.price).filter((x) => !miss(x)); return { n: rows.length, np: p.length, s: p.reduce((a, b) => a + b, 0), m: p.length ? p.reduce((a, b) => a + b, 0) / p.length : NaN, lo: p.length ? Math.min(...p) : NaN, hi: p.length ? Math.max(...p) : NaN, dc: new Set(rows.map((r) => r.cust)).size }; };
const AGG = { n: 'count()', np: 'count(price)', s: 'sum(price)', m: 'mean(price)', lo: 'min(price)', hi: 'max(price)', dc: 'countDistinct(cust)' };
const groups = (rows) => { const g = new Map(); for (const r of rows) { const k = r.region; if (!g.has(k)) g.set(k, []); g.get(k).push(r); } return [...g].map(([k, rs]) => ({ region: k, ...stats(rs) })).map(({ region, n, s }) => ({ region, n, s })).sort((a, b) => (a.region < b.region ? -1 : a.region > b.region ? 1 : 0)); };
const norm = (r) => ({ ts: r.ts, cust: r.cust, price: Number.isNaN(r.price) ? null : r.price, region: r.region === '' ? null : r.region });
const multiset = (rows) => rows.map((r) => JSON.stringify(norm(r))).sort();
async function check(label, st, rows) {
  ok(await st.aggAsync(AGG), stats(rows), `${label}: aggAsync`);
  ok(await st.countAsync(), rows.length, `${label}: countAsync`);
  ok(await st.filter("region == 'EU' && price > 50").aggAsync({ n: 'count()', s: 'sum(price)' }), (() => { const k = rows.filter((r) => r.region === 'EU' && r.price > 50); return { n: k.length, s: k.reduce((a, r) => a + r.price, 0) }; })(), `${label}: filter().aggAsync`);
  const g = (await st.groupBy('region').aggAsync({ n: 'count()', s: 'sum(price)' })).toRows().map((r) => ({ region: r.region, n: r.n, s: r.s })).sort((a, b) => (a.region < b.region ? -1 : a.region > b.region ? 1 : 0));
  ok(g, groups(rows), `${label}: groupBy(region)`);
  ok((await st.topAsync(5, '-price')).toRows().map((r) => r.price), rows.map((r) => r.price).sort((a, b) => (miss(a) ? 1 : miss(b) ? -1 : b - a)).slice(0, 5), `${label}: topAsync(5, -price) values (missing last)`);
  ok(multiset((await st.toRowsAsync()).map((r) => ({ ...r, price: r.price ?? NaN, region: r.region ?? '' }))), multiset(rows), `${label}: toRowsAsync (as a multiset)`);
}
const cut = (text, mode) => { const out = []; let i = 0; const enc = new TextEncoder().encode(text); while (i < enc.length) { const k = mode === 'bytes' ? 1 : mode === 'one' ? enc.length : 1 + Math.floor(rnd() * (mode === 'small' ? 40 : 30000)); out.push(enc.slice(i, i + k)); if (rnd() < 0.05) out.push(new Uint8Array(0)); i += k; } return out; };

for (const W of [1, 3]) {
const se = new SplitEngine({ workers: W });
for (const format of ['csv', 'ndjson']) {
  const enc = format === 'csv' ? toCSV : toNDJSON;
  // 1. whole feed, cut at random places, small pieces, and every byte (small feed)
  for (const [mode, n] of [['big', 8000], ['small', 1000], ['bytes', 60], ['one', 1]]) {
    const rows = makeRows(n), st = await se.streamAsync({ format, columns: COLS });
    for (const piece of cut(enc(rows), mode)) await st.push(piece);
    const f = await st.flushAsync();
    ok([f.rows, f.badLines, f.unparsedBytes], [rows.length, 0, 0], `${format} ${mode}: flushAsync rows / badLines / unparsedBytes`);
    await check(`${format} ${mode} (${n} rows)`, st, rows);
    await st.dispose?.();
  }
  // 2. keepRows: the newest k rows by arrival, across workers
  for (const k of [1, 7, 1000, 7999, 8000, 50000]) {
    const rows = makeRows(8000), st = await se.streamAsync({ format, columns: COLS, keepRows: k });
    for (const piece of cut(enc(rows), 'big')) await st.push(piece);
    await st.flushAsync();
    await check(`${format} keepRows ${k}`, st, rows.slice(Math.max(0, rows.length - k)));
    await st.dispose?.();
  }
  // 3. keepMs: rows newer than the newest time minus keepMs (rows exactly on the edge are in the data)
  for (const keepMs of [1000, 12345, 100000, 1e9]) {
    const rows = makeRows(8000), st = await se.streamAsync({ format, columns: COLS, keepMs, timeColumn: 'ts' });
    for (const piece of cut(enc(rows), 'big')) await st.push(piece);
    await st.flushAsync();
    const max = Math.max(...rows.map((r) => r.ts));
    const inGt = rows.filter((r) => r.ts > max - keepMs), inGe = rows.filter((r) => r.ts >= max - keepMs);
    const got = await st.countAsync();
    if (got === inGt.length || got === inGe.length) { pass++; await check(`${format} keepMs ${keepMs} (edge rows ${got === inGe.length ? 'kept' : 'dropped'})`, st, got === inGe.length ? inGe : inGt); }
    else { fail++; bad.push(`${format} keepMs ${keepMs}: count ${got}, want ${inGt.length} (ts > edge) or ${inGe.length} (ts >= edge); all ${rows.length}`); }
    await st.dispose?.();
  }
  // 4. windows: whole slices of `every` ms ending at the newest time; totals outlive keepRows
  for (const [every, last, keepRows] of [[1000, 10000, undefined], [1000, 1000, undefined], [500, 7000, 100], [3000, 60000, undefined]]) {
    const rows = makeRows(8000, { grow: false }), st = await se.streamAsync({ format, columns: COLS, ...(keepRows ? { keepRows } : {}) });
    let live; try { live = await st.windowAsync({ by: 'region', every, last, timeColumn: 'ts', agg: { n: 'count()', s: 'sum(price)', m: 'mean(price)', dc: 'approxDistinct(cust)' } }); } catch (er) { fail++; bad.push(`${format} window ${every}/${last}: ERROR ${er.message.slice(0, 120)}`); continue; }
    for (const piece of cut(enc(rows), 'big')) await st.push(piece);
    await st.flushAsync();
    const maxSlice = Math.floor(Math.max(...rows.map((r) => r.ts)) / every), first = maxSlice - Math.round(last / every) + 1;
    const inWin = rows.filter((r) => Math.floor(r.ts / every) >= first && Math.floor(r.ts / every) <= maxSlice);
    const want = new Map();
    for (const r of inWin) { const k = r.region; const g = want.get(k) ?? { region: k, n: 0, s: 0, np: 0, c: new Set() }; g.n++; if (!miss(r.price)) { g.s += r.price; g.np++; } g.c.add(r.cust); want.set(k, g); }
    const got = (await live.readAsync()).map((r) => ({ region: r.region ?? '', n: r.n, s: r.s, m: r.m, dc: r.dc })).sort((a, b) => (a.region < b.region ? -1 : 1));
    const w = [...want.values()].map((g) => ({ region: g.region, n: g.n, s: g.s, m: g.np ? g.s / g.np : NaN, dc: g.c.size })).sort((a, b) => (a.region < b.region ? -1 : 1));
    ok(got.map(({ dc, ...x }) => x), w.map(({ dc, ...x }) => x), `${format} window every ${every} last ${last}${keepRows ? ' keepRows ' + keepRows : ''}: readAsync`);
    const dcOk = got.length === w.length && got.every((g, i) => Math.abs(g.dc - w[i].dc) <= Math.max(2, 0.05 * w[i].dc));
    if (dcOk) pass++; else { fail++; bad.push(`${format} window approxDistinct: ${JSON.stringify(got.map((g) => g.dc))} vs ${JSON.stringify(w.map((g) => g.dc))}`); }
    const series = await live.readAsync({ series: true });
    const ws = new Map(); for (const r of inWin) { const k = `${Math.floor(r.ts / every) * every}|${r.region}`; ws.set(k, (ws.get(k) ?? 0) + 1); }
    ok(series.map((r) => `${r.t}|${r.region ?? ''}=${r.n}`).sort(), [...ws].map(([k, v]) => `${k}=${v}`).sort(), `${format} window every ${every} last ${last}: series`);
    await live.dispose(); await st.dispose?.();
  }
  // 5. queries while pushing: none may fail, counts only grow, the final answer is exact
  {
    const rows = makeRows(6000), st = await se.streamAsync({ format, columns: COLS }), pieces = cut(enc(rows), 'small');
    let last = 0, monotone = true, errs = 0; const qs = [];
    for (let i = 0; i < pieces.length; i++) {
      const p = st.push(pieces[i]);
      if (i % 200 === 0) qs.push(st.countAsync().then((c) => { if (c < last) monotone = false; last = c; }).catch(() => { errs++; }), st.groupBy('region').aggAsync({ n: 'count()' }).catch(() => { errs++; }));
      await p;
    }
    await Promise.all(qs); await st.flushAsync();
    ok([errs, monotone], [0, true], `${format}: ${qs.length} queries during pushes: no errors, counts only grow`);
    await check(`${format} after queries during pushes`, st, rows);
    await st.dispose?.();
  }
}
// a blank line in a CSV stream is not a row (feeds send bare newlines as heartbeats)
{
  const st = await se.streamAsync({ format: 'csv', columns: { ts: 'number', v: 'number' } });
  await st.push('1,10\n'); for (let i = 0; i < 3; i++) await st.push('\n'); await st.push('2,20\n'); await st.flushAsync();
  ok(await st.countAsync(), 2, `workers ${W}: blank lines in a CSV stream are not rows`);
}
// a late row from long ago doesn't stay in a keepMs window
{
  const st = await se.streamAsync({ format: 'csv', columns: { ts: 'number', v: 'number' }, keepMs: 1000, timeColumn: 'ts' });
  let csv = ''; for (let i = 0; i <= 102; i++) csv += `${i * 1000},1\n`;
  await st.push(csv + '5,1000000\n103000,1\n'); await st.flushAsync();
  ok((await st.aggAsync({ s: 'sum(v)' })).s <= 2, true, `workers ${W}: keepMs drops a row that arrived late with an old time`);
}
await se.close();
}
for (const b of bad.slice(0, 30)) console.log('FAIL ' + b);
console.log(fail ? `${fail} failures out of ${pass + fail} checks` : `all ${pass} round-5 stream checks passed`);
process.exit(fail ? 1 : 0);
