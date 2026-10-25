// JSON lines: the byte-level parser against JSON.parse on random lines, then
// JSON-line streams end to end (pushed in chunks cut anywhere).
import { parseNDJSON } from '../src/ndjson.mjs';
import { SplitEngine } from '../src/node.mjs';
import { Engine } from '../src/node.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
const same = (a, b) => a === b || (Number.isNaN(a) && Number.isNaN(b));

let seed = 77;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const pick = (xs) => xs[Math.floor(rnd() * xs.length)];

// ---- reference: what each column should get from one line -------------------
const NAMES = ['ts', 'price', 'region', 'flag'];
const KINDS = ['number', 'number', 'category', 'category'];
function reference(line) {
  const t = line.trim();
  if (!t) return null; // blank: no row
  let obj;
  try { obj = JSON.parse(t); } catch { return 'bad'; }
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return 'bad';
  return NAMES.map((name, k) => {
    const v = obj[name];
    if (KINDS[k] === 'number') {
      if (typeof v === 'number') return v;
      if (typeof v === 'boolean') return v ? 1 : 0;
      if (typeof v === 'string') return v.trim() === '' ? NaN : Number(v);
      return NaN;
    }
    if (typeof v === 'string') return v;
    if (typeof v === 'number' || typeof v === 'boolean') return String(v);
    return '';
  });
}

// ---- random lines ------------------------------------------------------------------
const LABELS = ['EU', 'US', 'Zürich', 'say "hi"', 'tab\there', '日本', '🚀', 'back\\slash', ''];
const numberText = () => pick(['0', '-3', '17', '2.5', '-0.125', '1e3', '-2.5E-2', '123456789', '0.1', '99.99']);
function value(kind) {
  const r = rnd();
  if (r < 0.06) return 'null';
  if (r < 0.1) return pick(['true', 'false']);
  if (r < 0.14) return pick(['{"a":1,"b":"}"}', '[1,[2,"]"]]', '{}', '[]']);
  if (kind === 'number') return r < 0.22 ? JSON.stringify(pick(['12.5', ' 7 ', 'abc', ''])) : numberText();
  return r < 0.2 ? String(pick([3, 42, 7.5])) : JSON.stringify(pick(LABELS));
}
function line() {
  const r = rnd();
  if (r < 0.03) return pick(['not json', '{"price":', '[1,2]', '42', 'null', '{"price" 1}', '{"price":1,}', '"text"']);
  if (r < 0.05) return pick(['', '   ', '\t']);
  const keys = NAMES.filter(() => rnd() < 0.9);
  if (rnd() < 0.3) keys.push(pick(['extra', 'other', 'n']));
  for (let i = keys.length - 1; i > 0; i--) { const j = Math.floor(rnd() * (i + 1)); [keys[i], keys[j]] = [keys[j], keys[i]]; }
  const sp = () => pick(['', '', ' ']);
  const key = (k) => (k === 'price' && rnd() < 0.05 ? '"pri\\u0063e"' : JSON.stringify(k));
  return `${sp()}{${keys.map((k) => `${sp()}${key(k)}${sp()}:${sp()}${value(KINDS[NAMES.indexOf(k)] ?? 'number')}`).join(',')}${sp()}}${sp()}`;
}

// ---- parser vs reference -------------------------------------------------------------
const enc = new TextEncoder();
for (let trial = 0; trial < 300; trial++) {
  const lines = Array.from({ length: 1 + Math.floor(rnd() * 60) }, line);
  const text = lines.join(rnd() < 0.3 ? '\r\n' : '\n') + (rnd() < 0.5 ? '\n' : '');
  const bytes = enc.encode(text);
  const got = parseNDJSON(bytes, 0, bytes.length, NAMES, KINDS);
  const want = lines.map(reference).filter((r) => r !== null);
  const good = want.filter((r) => r !== 'bad');
  checks++;
  if (got.rows !== good.length || got.bad !== want.length - good.length) { fail(`trial ${trial}: rows ${got.rows}/${good.length}, bad ${got.bad}/${want.length - good.length}\n${text.slice(0, 400)}`); continue; }
  for (let r = 0; r < good.length; r++) {
    for (let k = 0; k < NAMES.length; k++) {
      checks++;
      const v = got.cols[k][r];
      const g = KINDS[k] === 'number' ? v : got.dicts[k][v];
      if (!same(g, good[r][k])) { fail(`trial ${trial} row ${r} ${NAMES[k]}: got ${JSON.stringify(g)}, want ${JSON.stringify(good[r][k])}`); break; }
    }
  }
}

// ---- streams with format: 'ndjson' ----------------------------------------------------------
const REG = ['EU', 'US', 'APAC', 'LATAM'];
const rows = Array.from({ length: 40_000 }, (_, i) => ({ ts: i, price: Math.round(rnd() * 1e5) / 100, qty: 1 + Math.floor(rnd() * 9), region: REG[Math.floor(rnd() * 4)], note: 'ignored' }));
const ndjson = rows.map((r) => JSON.stringify(r)).join('\n') + '\n';
const ref = new Engine();
const t = ref.table({ price: rows.map((r) => r.price), qty: rows.map((r) => r.qty), region: rows.map((r) => r.region) });
const want = {
  agg: t.agg({ n: 'count()', s: 'sum(price * qty)', hi: 'max(price)' }),
  eu: t.filter("region == 'EU'").agg({ n: 'count()', s: 'sum(qty)' }),
  groups: t.groupBy('region').agg({ n: 'count()', s: 'sum(price)' }).toRows().sort((a, b) => (a.region < b.region ? -1 : 1)),
};
const close = (a, b) => JSON.stringify(a, (k, x) => (typeof x === 'number' ? Number(x.toPrecision(11)) : x)) === JSON.stringify(b, (k, x) => (typeof x === 'number' ? Number(x.toPrecision(11)) : x));
for (const workers of [1, 4]) {
  const engine = new SplitEngine({ workers });
  const s = await engine.streamAsync({ format: 'ndjson', columns: { price: 'number', qty: 'number', region: 'category' } });
  for (let i = 0; i < ndjson.length; ) { const k = Math.floor(rnd() * 9000); await s.push(ndjson.slice(i, i + k)); i += k; }
  await s.push('this line is not json\n');
  const st = await s.flushAsync();
  checks++; if (st.rows !== rows.length || st.badLines !== 1) fail(`w=${workers} stream rows ${st.rows}, bad ${st.badLines}`);
  const got = {
    agg: await s.aggAsync({ n: 'count()', s: 'sum(price * qty)', hi: 'max(price)' }),
    eu: await s.filter("region == 'EU'").aggAsync({ n: 'count()', s: 'sum(qty)' }),
    groups: (await s.groupBy('region').aggAsync({ n: 'count()', s: 'sum(price)' })).toRows(),
  };
  for (const k of Object.keys(want)) { checks++; if (!close(got[k], want[k])) fail(`w=${workers} stream ${k}: ${JSON.stringify(got[k])} vs ${JSON.stringify(want[k])}`); }
  let msg = '';
  try { await engine.streamAsync({ format: 'xml', columns: { a: 'number' } }); } catch (err) { msg = err.message; }
  checks++; if (!msg.includes("'ndjson' or 'vectro'")) fail(`w=${workers} bad format accepted: ${msg}`);
  await engine.close();
}
await ref.close();
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} JSON-lines checks passed`);
process.exit(failures ? 1 : 0);
