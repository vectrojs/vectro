// Text functions on category columns (startsWith, endsWith, contains, in,
// like), evaluated once per label: every answer must equal plain JavaScript.
import { Engine, SplitEngine } from '../src/node.mjs';
import { Engine as PortableEngine } from '../src/portable.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);

let seed = 8;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const pick = (xs) => xs[Math.floor(rnd() * xs.length)];
const PARTS = ['ab', 'Ab', 'a.b', '(x)', 'x*', '50%', 'a_b', 'Zürich', '東京', ' ', '', 'end', 'sale', 'SALE'];
const labels = [...new Set(Array.from({ length: 1500 }, () => pick(PARTS) + pick(PARTS) + Math.floor(rnd() * 40)))];
const N = 60_000;
const rows = Array.from({ length: N }, () => ({ tag: pick(labels), n: Math.floor(rnd() * 100) }));

// an independent LIKE: % = any run, _ = one character
function like(s, p) {
  const S = [...s], P = [...p];
  const memo = new Map();
  const m = (i, j) => {
    const key = i * 1000 + j;
    if (memo.has(key)) return memo.get(key);
    let r;
    if (j === P.length) r = i === S.length;
    else if (P[j] === '%') r = m(i, j + 1) || (i < S.length && m(i + 1, j));
    else r = i < S.length && (P[j] === '_' || P[j] === S[i]) && m(i + 1, j + 1);
    memo.set(key, r);
    return r;
  };
  return m(0, 0);
}
const CASES = [
  ["startsWith(tag, 'ab')", (r) => r.tag.startsWith('ab')],
  ["endsWith(tag, '7')", (r) => r.tag.endsWith('7')],
  ["contains(tag, 'a.b')", (r) => r.tag.includes('a.b')],
  ["contains(tag, '(x)')", (r) => r.tag.includes('(x)')],
  ["contains(tag, 'Zürich')", (r) => r.tag.includes('Zürich')],
  [`in(tag, '${labels[3]}', '${labels[10]}', 'not a label')`, (r) => r.tag === labels[3] || r.tag === labels[10]],
  ["like(tag, 'a_b%')", (r) => like(r.tag, 'a_b%')],
  ["like(tag, '%50\\%%')".replace('\\%', '%'), (r) => like(r.tag, '%50%%')],
  ["like(tag, '%sale_')", (r) => like(r.tag, '%sale_')],
  ["!contains(tag, 'sale') && n >= 50", (r) => !r.tag.includes('sale') && r.n >= 50],
  ["startsWith(tag, 'nothing') || n == 3", (r) => r.n === 3],
  ["contains(tag, '')", () => true],
];
const ref = (keep) => {
  const sel = rows.filter(keep);
  const byTag = new Map();
  for (const r of sel) byTag.set(r.tag, (byTag.get(r.tag) ?? 0) + 1);
  return { count: sel.length, total: sel.reduce((a, r) => a + r.n, 0), groups: [...byTag].sort((a, b) => (a[0] < b[0] ? -1 : 1)).slice(0, 20) };
};
const engines = [['threads', () => new Engine()], ['interpreter', () => new PortableEngine({ jit: false })], ['split, 3 workers', () => new SplitEngine({ workers: 3 })]];
for (const [name, make] of engines) {
  const e = make();
  const cols = { tag: rows.map((r) => r.tag), n: rows.map((r) => r.n) };
  const t = e.tableAsync ? await e.tableAsync(cols) : e.table(cols);
  for (const [f, js] of CASES) {
    const want = ref(js);
    const v = t.filter(f);
    const g = await v.groupBy('tag').aggAsync({ c: 'count()' });
    const got = {
      count: await v.countAsync(),
      total: await v.sumAsync('n'),
      groups: g.toRows().map((r) => [r.tag, r.c]).sort((a, b) => (a[0] < b[0] ? -1 : 1)).slice(0, 20),
    };
    checks++;
    if (!same(got, want)) fail(`${name} ${f}: ${JSON.stringify(got).slice(0, 200)} vs ${JSON.stringify(want).slice(0, 200)}`);
  }
  // mistakes are refused with a clear message
  for (const [f, text] of [["startsWith(n, 'a')", 'needs a text column'], ['startsWith(tag)', 'needs text in quotes'], ["startsWith(tag, 'a', 'b')", 'one text'], ['contains(tag, n)', 'needs text in quotes']]) {
    let msg = '';
    try { await t.filter(f).countAsync(); } catch (err) { msg = err.message; }
    checks++; if (!msg.includes(text)) fail(`${name} bad call "${f}" accepted: ${msg}`);
  }
  await t.dispose();
  await e.close();
}
// matches scattered over many codes: a 0 / 1 per row instead of a huge filter (test-edges.mjs has more)
{
  const e = new Engine();
  const alt = Array.from({ length: 1200 }, (_, i) => (i % 2 ? 'b' : 'a') + i);
  const t = e.table({ tag: alt, n: alt.map((_, i) => i) });
  checks++; if (t.filter("startsWith(tag, 'a')").count() !== 600) fail(`scattered matches: ${t.filter("startsWith(tag, 'a')").count()}`);
  let msg = '';
  // free text columns can't use them
  const csv = e.parseCSV('note\nhello\nworld\n', { types: { note: 'string' } });
  msg = '';
  try { csv.filter("contains(note, 'o')").count(); } catch (err) { msg = err.message; }
  checks++; if (!msg.includes('free-text')) fail(`free text: ${msg}`);
  await e.close();
}
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} text-function checks passed`);
process.exit(failures ? 1 : 0);
