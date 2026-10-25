// CSV loader tests: hand-written edge cases, then random CSVs (quotes, commas
// and newlines inside fields, CRLF, odd number formats, unicode) compared cell
// by cell against a simple sequential reference parser.
import { Engine, MODE } from './test-mode.mjs';
import { writeFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 20) console.log('FAIL', msg); };
const same = (a, b) => Object.is(a, b) || (Number.isNaN(a) && Number.isNaN(b));
const eq = (label, got, want) => { checks++; if (!same(got, want) && JSON.stringify(got) !== JSON.stringify(want)) fail(`${label}: got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`); };

// Reference: RFC 4180 parser, one character at a time.
function refParse(text) {
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  const rows = [];
  let row = [], field = '', i = 0, quoted = false, wasQuoted = false;
  const endField = () => { row.push(wasQuoted ? field : field.replace(/\r$/, '')); field = ''; wasQuoted = false; };
  while (i < text.length) {
    const c = text[i];
    if (quoted) {
      if (c === '"') { if (text[i + 1] === '"') { field += '"'; i += 2; continue; } quoted = false; i++; continue; }
      field += c; i++; continue;
    }
    if (c === '"' && field === '') { quoted = true; wasQuoted = true; i++; continue; }
    if (c === ',') { endField(); i++; continue; }
    if (c === '\n') { endField(); rows.push(row); row = []; i++; continue; }
    if (wasQuoted) { i++; continue; } // junk after a closing quote is ignored
    field += c; i++;
  }
  if (field !== '' || row.length || wasQuoted) { endField(); rows.push(row); }
  while (rows.length && rows[rows.length - 1].length === 1 && rows[rows.length - 1][0] === '') rows.pop();
  return rows;
}
const refNumber = (s) => (s === undefined || s.trim() === '' ? NaN : Number(s));

const e = new Engine();
const dir = mkdtempSync(join(tmpdir(), 'vectro-csv-test-'));

// ---- hand-written cases ------------------------------------------------------
{
  const t = e.parseCSV('﻿id,price,name,Order Date\r\n1,12.5,"Smith, J",2024\r\n2,-3e2,"say ""hi""",2025\r\n3,,plain,\r\n4,.5,"multi\nline",7\n\n\n');
  eq('names', t.columnNames, ['id', 'price', 'name', 'Order_Date']);
  eq('rows', t.length, 4);
  const rows = t.toRows();
  eq('quoted comma', rows[0].name, 'Smith, J');
  eq('escaped quotes', rows[1].name, 'say "hi"');
  eq('exponent', rows[1].price, -300);
  eq('empty number is NaN', rows[2].price, NaN);
  eq('empty trailing field', rows[2].Order_Date, NaN);
  eq('leading dot', rows[3].price, 0.5);
  eq('newline in quotes', rows[3].name, 'multi\nline');
  eq('CRLF stripped', rows[0].Order_Date, 2024);
  eq('sum works on loaded table', t.sum('id'), 10);
}
{
  const nums = ['0', '-0', '007', '5.', '+4', ' 12 ', '1e5', '2.5E-3', '123456789012345678901234', '0.1', '9007199254740993', '1e-30', '1e30', 'NaN', 'Infinity', '-Infinity', '0x1F', '3.14159265358979323846'];
  const t = e.parseCSV('v\n' + nums.join('\n'), { types: { v: 'number' } });
  const got = Array.from(t.column('v'));
  nums.forEach((s, i) => eq(`number "${s}"`, got[i], refNumber(s)));
}
{
  const t = e.parseCSV('a;b;c\n1;x;2\n3;y\n4;z;5;extra', { delimiter: ';' });
  eq('semicolon rows', t.length, 3);
  eq('missing field -> NaN', t.toRows()[1].c, NaN);
  eq('extra field ignored', t.toRows()[2].c, 5);
  const h = e.parseCSV('1,2\n3,4', { header: false });
  eq('no header names', h.columnNames, ['col0', 'col1']);
  eq('no header sum', h.sum('col0 + col1'), 10);
  const sub = e.parseCSV('a,b,c\n1,2,3\n4,5,6', { columns: ['c', 'a'] });
  eq('column subset', sub.columnNames, ['a', 'c']);
  eq('subset values', sub.sum('c'), 9);
  const forced = e.parseCSV('zip,n\n02139,1\n10001,2', { types: { zip: 'text' } });
  eq('forced text keeps leading zero', forced.toRows()[0].zip, '02139');
  const guess = e.parseCSV('code,n\n12,1\nA7,2');
  eq('mixed column guessed as text', guess.toRows()[1].code, 'A7');
  const tsv = e.parseCSV('a\tb\n1\t2', { delimiter: '\t' });
  eq('tab delimiter', tsv.sum('a * b'), 2);
  eq('header only', e.parseCSV('a,b\n').length, 0);
}

// ---- free text, type guessing, errors from worker threads -------------------------
{
  const lines = ['id,email,status'];
  for (let i = 0; i < 5000; i++) lines.push(`${i},user${i}@example.com,${['active', 'paused', 'closed'][i % 3]}`);
  const t = e.parseCSV(lines.join('\n'));
  eq('unique text guessed as free text', t.cols.get('email').strings !== null, true);
  eq('repeating text guessed as category', t.cols.get('status').dict?.length, 3);
  eq('free text value', t.toRows(3)[2].email, 'user2@example.com');
  eq('filter still works next to free text', t.filter("status == 'paused'").count(), 1667);
  const collected = t.filter("status == 'closed' && id > 4990").collect().toRows();
  eq('free text survives collect', collected.map((r) => r.email), ['user4991@example.com', 'user4994@example.com', 'user4997@example.com']);
  let msg = '';
  try { t.filter("email == 'x'").count(); } catch (err) { msg = err.message; }
  eq('free text in an expression explains itself', msg.includes('free-text column'), true);
  t.dispose();
}
{
  const big = 'x'.repeat(600_000);
  const lines = ['id,note'];
  for (let i = 0; i < 2000; i++) lines.push(`${i},${i === 1500 ? big : `note ${i}`}`);
  let msg = '';
  try { e.parseCSV(lines.join('\n'), { types: { note: 'string' }, chunkBytes: 1024 }); } catch (err) { msg = err.message; }
  eq('error on a worker thread is reported, not a hang', msg.includes('longer than'), true);
  const after = e.parseCSV('a\n1\n2');
  eq('engine still works after a worker error', after.sum('a'), 3);
}

// ---- random CSVs vs the reference parser -----------------------------------------
let seed = Number(process.env.SEED ?? 4242);
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const pick = (xs) => xs[Math.floor(rnd() * xs.length)];
const TEXTS = ['EU', 'US', 'plain', 'with, comma', 'say "hi"', 'two\nlines', 'crlf\r\ninside', 'ünïcødé ✓', '', ' padded ', '日本', 'a""b', ',', '"'];
const randNumber = () => pick([
  () => String(Math.floor(rnd() * 1e6)),
  () => (rnd() * 1000 - 500).toFixed(Math.floor(rnd() * 6)),
  () => (rnd() * 1e6).toExponential(Math.floor(rnd() * 8)),
  () => String(rnd() * 1e-5),
  () => '',
  () => pick(['NaN', '-0', '1e400', '123456789012345678', ' 42', '.25']),
])();
const quote = (s) => (/[",\r\n]/.test(s) || rnd() < 0.1 ? `"${s.replaceAll('"', '""')}"` : s);

for (let trial = 0; trial < 6; trial++) {
  const rowsN = trial < 3 ? 2_000 : 120_000;
  const eol = trial % 2 ? '\r\n' : '\n';
  const lines = ['num,txt,num2,cat'];
  for (let r = 0; r < rowsN; r++) {
    lines.push([quote(randNumber()), quote(pick(TEXTS)), quote(randNumber()), quote(pick(['A', 'B', 'C']))].join(','));
  }
  const text = lines.join(eol) + (rnd() < 0.5 ? eol : '');
  const path = join(dir, `t${trial}.csv`);
  writeFileSync(path, text);
  // odd trials load txt as free text (byte references), even as a category
  const t = e.readCSV(path, { types: { num: 'number', num2: 'number', txt: trial % 2 ? 'string' : 'category', cat: 'text' }, chunkBytes: 4096 });
  const ref = refParse(text).slice(1);
  eq(`trial ${trial} row count`, t.length, ref.length);
  const got = t.toRows();
  for (let r = 0; r < Math.min(ref.length, got.length); r++) {
    const [num, txt, num2, cat] = ref[r];
    const g = got[r];
    checks++;
    if (!same(g.num, refNumber(num)) || g.txt !== txt || !same(g.num2, refNumber(num2)) || g.cat !== cat) {
      fail(`trial ${trial} row ${r}: got ${JSON.stringify(g)} want ${JSON.stringify({ num: refNumber(num), txt, num2: refNumber(num2), cat })}`);
      break;
    }
  }
  // the loaded table works with the query engine
  const want = ref.filter((x) => x[3] === 'B').reduce((s, x) => s + (Number.isNaN(refNumber(x[0])) ? 0 : refNumber(x[0])), 0);
  const gotSum = t.filter("cat == 'B' && num == num").sum('num');
  checks++;
  if (!(gotSum === want || Math.abs(gotSum - want) <= 1e-9 * Math.abs(want)) && !(Number.isNaN(gotSum) && Number.isNaN(want))) fail(`trial ${trial} query: ${gotSum} vs ${want}`);
  t.dispose();
}

rmSync(dir, { recursive: true });
await e.close();
console.log(failures ? `${failures} failures out of ${checks} checks` : `[${MODE}] all ${checks} csv checks passed`);
process.exit(failures ? 1 : 0);
