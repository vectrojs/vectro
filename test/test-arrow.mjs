// Arrow IPC: files written by pyarrow (test/fixtures/arrow, made by
// scripts/make-arrow-fixtures.py) must read back exactly; what Vectro writes
// must read back exactly too, whole or in pieces cut anywhere; tables, split
// tables and streams load and save Arrow.
import { readFileSync } from 'node:fs';
import { readArrow, writeArrow, ArrowReader, ArrowEncoder, Engine, SplitEngine, Emitter } from '../src/node.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
const same = (a, b) => Object.is(a, b) || (a === 0 && b === 0) || (Number.isNaN(a) && Number.isNaN(b));
let seed = 21;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);

const dir = new URL('./fixtures/arrow/', import.meta.url);
const file = (name) => readFileSync(new URL(name, dir));
const expected = JSON.parse(readFileSync(new URL('expected.json', dir), 'utf8'));
const fromJSON = (v) => (v === null || v === 'NaN' ? NaN : v === 'Infinity' ? Infinity : v === '-Infinity' ? -Infinity : v);
const valuesOf = (c) => (c.codes ? Array.from(c.codes, (k) => c.labels[k]) : Array.from(ArrayBuffer.isView(c) ? c : c.values));

function compare(label, columns, want) {
  for (const [name, list] of Object.entries(want)) {
    checks++;
    const got = columns[name] ? valuesOf(columns[name]) : null;
    if (!got || got.length !== list.length) { fail(`${label} ${name}: ${got?.length} rows, want ${list.length}`); continue; }
    const i = list.findIndex((v, i) => !same(got[i], fromJSON(v)));
    if (i >= 0) fail(`${label} ${name} row ${i}: got ${got[i]}, want ${list[i]}`);
  }
}

// ---- files made by pyarrow ---------------------------------------------------------
const readable = Object.keys(expected.columns);
for (const name of ['types.arrow', 'types.feather', 'types-lz4.feather']) {
  const { rows, columns } = readArrow(file(name), { columns: readable });
  checks++; if (rows !== 300) fail(`${name}: ${rows} rows`);
  compare(name, columns, expected.columns);
  // kinds as the stream sees them
  let kinds = null;
  new ArrowReader({ onSchema: (k) => { kinds = k; }, onBatch() {} }).push(file(name));
  checks++;
  for (const [col, kind] of Object.entries(expected.kinds)) if (kinds[col] !== kind) fail(`${name} kind of ${col}: ${kinds[col]}`);
  if (kinds.tags !== null || kinds.point !== null) fail(`${name}: list/struct should be unreadable`);
  // unreadable types are refused unless left out
  let msg = '';
  try { readArrow(file(name)); } catch (err) { msg = err.message; }
  checks++; if (!msg.includes('"tags"') || !msg.includes('List')) fail(`${name} list column accepted: ${msg}`);
}
compare('delta.arrow', readArrow(file('delta.arrow')).columns, expected.delta);
compare('replaced.arrow', readArrow(file('replaced.arrow')).columns, expected.replaced);
compare('pandas.feather', readArrow(file('pandas.feather')).columns, expected.pandas);
{
  let msg = '';
  try { readArrow(file('zstd.feather')); } catch (err) { msg = err.message; }
  checks++; if (!msg.includes('ZSTD')) fail(`zstd: ${msg}`);
  msg = '';
  try { readArrow(file('types.arrow').subarray(0, 5000), { columns: ['i8'] }); } catch (err) { msg = err.message; }
  checks++; if (!msg.includes('truncated')) fail(`truncated: ${msg}`);
  msg = '';
  try { readArrow(new TextEncoder().encode('a,b\n1,2\n')); } catch (err) { msg = err.message; }
  checks++; if (!msg.includes('Arrow')) fail(`not arrow: ${msg}`);
}

// pieces cut anywhere give the same columns as reading at once
for (const name of ['types.arrow', 'types-lz4.feather', 'delta.arrow', 'replaced.arrow']) {
  const bytes = file(name);
  const whole = readArrow(bytes, name.startsWith('types') ? { columns: readable } : {});
  for (let trial = 0; trial < 6; trial++) {
    const got = {};
    const reader = new ArrowReader({
      onSchema: (kinds) => Object.keys(kinds).filter((k) => kinds[k]),
      onBatch(rows, cols) {
        reader.fields.forEach((f, k) => { if (cols[k]) (got[f.name] ??= []).push(...valuesOf(cols[k])); });
      },
    });
    const most = trial < 2 ? 3 : 4000;
    for (let i = 0; i < bytes.length; ) { const k = 1 + Math.floor(rnd() * most); reader.push(bytes.subarray(i, i + k)); i += k; }
    checks++;
    for (const [col, c] of Object.entries(whole.columns)) {
      const want = valuesOf(c);
      if (!got[col] || got[col].length !== want.length || want.some((v, i) => !same(v, got[col][i]))) { fail(`${name} in pieces (trial ${trial}): column ${col} differs ${got[col]?.length} vs ${want.length}: ${got[col]?.slice(0, 4)} vs ${want.slice(0, 4)}`); break; }
    }
  }
}

// ---- written by Vectro, read back --------------------------------------------------
const LABELS = ['EU', 'US', 'Zürich', '日本', '', 'a "quote"', '🚀'];
for (let trial = 0; trial < 40; trial++) {
  const n = trial === 0 ? 0 : Math.floor(rnd() * 3000);
  const cols = {};
  const width = 1 + Math.floor(rnd() * 5);
  for (let k = 0; k < width; k++) {
    const r = rnd();
    if (r < 0.3) cols[`n${k}`] = Float64Array.from({ length: n }, () => [NaN, -0, Infinity, 1e-300, rnd() * 1e9 - 5e8][Math.floor(rnd() * 5)]);
    else if (r < 0.45) cols[`f${k}`] = Float32Array.from({ length: n }, () => rnd() * 100);
    else if (r < 0.7) cols[`s${k}`] = Array.from({ length: n }, () => LABELS[Math.floor(rnd() * LABELS.length)]);
    else cols[`c${k}`] = { codes: Uint32Array.from({ length: n }, () => Math.floor(rnd() * 3)), labels: ['x', 'y', 'x'] };
  }
  if (n === 0) for (const k of Object.keys(cols)) if (Array.isArray(cols[k])) delete cols[k]; // an empty string[] looks like numbers
  if (!Object.keys(cols).length) cols.empty = new Float64Array(0);
  for (const asFile of [false, true]) {
    const bytes = writeArrow(cols, { file: asFile });
    let back;
    try { back = readArrow(bytes); } catch (err) { fail(`round trip ${trial} file=${asFile} ${Object.entries(cols).map(([k, v]) => `${k}:${v.constructor.name}`)} n=${n}: ${err.message}`); continue; }
    checks++;
    if (back.rows !== n) { fail(`round trip ${trial}: ${back.rows} rows, want ${n}`); continue; }
    for (const [name, c] of Object.entries(cols)) {
      const want = c.codes ? Array.from(c.codes, (k) => c.labels[k]) : Array.from(c);
      const got = valuesOf(back.columns[name]);
      const i = want.findIndex((v, i) => !Object.is(v, got[i]) && !(Number.isNaN(v) && Number.isNaN(got[i])));
      if (i >= 0) { fail(`round trip ${trial} file=${asFile} ${name} row ${i}: ${got[i]} vs ${want[i]}`); break; }
    }
  }
}

// an encoder sends labels once, as delta dictionaries, batch after batch
{
  const e = new ArrowEncoder({ region: 'category', x: 'number' });
  const parts = [e.schema()];
  const all = [];
  for (let b = 0; b < 5; b++) {
    const region = Array.from({ length: 50 }, (_, i) => `r${(i * 7 + b * 3) % (4 + b * 4)}`);
    all.push(...region);
    parts.push(e.encode({ region, x: Float64Array.from(region, (_, i) => i) }));
  }
  parts.push(e.end());
  const bytes = new Uint8Array(parts.reduce((a, p) => a + p.length, 0));
  let at = 0;
  for (const p of parts) { bytes.set(p, at); at += p.length; }
  const back = readArrow(bytes);
  checks++; if (JSON.stringify(valuesOf(back.columns.region)) !== JSON.stringify(all)) fail('encoder delta dictionaries');
  checks++; if (back.columns.region.labels.length !== new Set(all).size) fail(`labels sent more than once: ${back.columns.region.labels.length}`);
}

// ---- tables, split tables and streams -------------------------------------------------
const N = 50_000;
const REG = ['EU', 'US', 'APAC', 'LATAM'];
const src = {
  ts: Float64Array.from({ length: N }, (_, i) => 1_700_000_000_000 + i * 1000),
  price: Float64Array.from({ length: N }, () => Math.round(rnd() * 1e5) / 100),
  region: Array.from({ length: N }, () => REG[Math.floor(rnd() * 4)]),
};
const want = {
  n: src.region.filter((r) => r === 'EU').length,
  s: src.price.reduce((a, p, i) => a + (src.region[i] === 'EU' && p > 500 ? p : 0), 0),
};
const bytes = writeArrow(src, { file: true });
for (const [label, make] of [['threads', () => new Engine()], ['split, 3 workers', () => new SplitEngine({ workers: 3 })]]) {
  const e = make();
  const t = e.fromArrowAsync ? await e.fromArrowAsync(bytes) : e.fromArrow(bytes);
  const n = await t.filter("region == 'EU'").countAsync();
  const s = await t.filter("region == 'EU' && price > 500").sumAsync('price');
  checks++; if (n !== want.n || Math.abs(s - want.s) > 1e-6 * want.s) fail(`${label} fromArrow: ${n} ${s} vs ${want.n} ${want.s}`);
  // filtered rows out as Arrow, in table order
  const out = readArrow(await t.filter('price > 900').toArrowAsync());
  const keep = [...src.price.keys()].filter((i) => src.price[i] > 900);
  checks++;
  if (out.rows !== keep.length || keep.some((i, j) => out.columns.ts[j] !== src.ts[i] || out.columns.price[j] !== src.price[i] || out.columns.region.labels[out.columns.region.codes[j]] !== src.region[i])) fail(`${label} toArrow of a filter`);
  await t.dispose();
  await e.close();
}

// a stream fed by an Emitter speaking Arrow, and pyarrow-made bytes over TCP
{
  const columns = { ts: 'number', price: 'number', region: 'category' };
  const feed = await Emitter.listen({ columns, format: 'arrow' });
  const engine = new SplitEngine({ workers: 3 });
  const s = await engine.streamAsync({ format: 'arrow', columns });
  await Promise.all(engine.workers.map((_, w) => s.connect(w, { port: feed.port })));
  await feed.waitForConnections(3);
  for (let at = 0; at < N; at += 5000) {
    const part = { ts: src.ts.subarray(at, at + 5000), price: src.price.subarray(at, at + 5000), region: src.region.slice(at, at + 5000) };
    await feed.write(part);
  }
  await feed.close();
  await new Promise((r) => setTimeout(r, 300));
  const st = await s.flushAsync();
  const n = await s.filter("region == 'EU'").countAsync();
  const sum = await s.filter("region == 'EU' && price > 500").sumAsync('price');
  checks++; if (st.rows !== N || n !== want.n || Math.abs(sum - want.s) > 1e-6 * want.s) fail(`arrow stream: rows ${st.rows}, EU ${n}/${want.n}, sum ${sum}/${want.s}, error ${st.error}`);
  // and the stream's rows back out as Arrow
  const back = readArrow(await s.toArrowAsync());
  checks++; if (back.rows !== N || back.columns.region.labels.length !== 4) fail(`stream toArrow: ${back.rows} rows, ${back.columns.region.labels}`);
  await s.dispose();
  // pyarrow's delta-dictionary stream, served raw over TCP
  const { createServer } = await import('node:net');
  const raw = file('delta.arrow');
  const server = createServer((sock) => { for (let i = 0; i < raw.length; i += 97) sock.write(raw.subarray(i, i + 97)); sock.end(); });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  const s2 = await engine.streamAsync({ format: 'arrow', columns: { k: 'category', v: 'number' } });
  await s2.connect(0, { port: server.address().port });
  await new Promise((r) => setTimeout(r, 300));
  await s2.flushAsync();
  const g = (await s2.groupBy('k').aggAsync({ n: 'count()' })).toRows();
  const ref = new Map();
  for (const k of expected.delta.k) ref.set(k, (ref.get(k) ?? 0) + 1);
  checks++; if (g.length !== ref.size || g.some((r) => ref.get(r.k) !== r.n)) fail(`pyarrow stream over TCP: ${JSON.stringify(g).slice(0, 200)}`);
  // a column the stream wants but the sender doesn't have
  const s3 = await engine.streamAsync({ format: 'arrow', columns: { nope: 'number' } });
  const server2 = createServer((sock) => sock.end(raw));
  await new Promise((r) => server2.listen(0, '127.0.0.1', r));
  await s3.connect(0, { port: server2.address().port });
  await new Promise((r) => setTimeout(r, 200));
  const st3 = await s3.flushAsync();
  checks++; if (!String(st3.errors).includes('"nope"')) fail(`missing column accepted: ${st3.errors}`);
  server.close(); server2.close();
  await engine.close();
}

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} Arrow checks passed`);
process.exit(failures ? 1 : 0);
