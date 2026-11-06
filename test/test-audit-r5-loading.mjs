// Audit round 5 (loading): parseCSV on nasty files (BOM + CRLF, quoted commas /
// newlines / doubled quotes, quoted empty, numbers like 1e5 .5 -0 1e400 -.5e2,
// a header only, no final newline, a trailing delimiter, unicode values), the
// same files through readCSV, readCSVAsync and csvToDataset read 7 bytes at a
// time (Node), Arrow round trips of the edge kit (stream and file, Float32,
// compressed, empty, all missing, a filtered view); the values were checked
// against DuckDB's read_csv / read_json in vectro-qa-2026-11-05/load. Also: a
// CSV with CR-only line ends (it became one header line and no rows); JSON lines
// with a numeric string in a number column (the whole column became text), a BOM
// (the first record was lost), 1e400 (missing, not Infinity) and a key whose
// value is nested in some rows only (the key was dropped); writeArrow of plain
// text columns holding null (written as "null", or as numbers when null came first).
import { Engine, MODE } from './test-mode.mjs';
import { readArrow, writeArrow } from '../src/arrow.mjs';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let failures = 0, checks = 0;
const show = (v) => (v instanceof Date ? (Number.isNaN(+v) ? null : v.toISOString()) : typeof v === 'number' ? (Number.isNaN(v) ? null : Object.is(v, -0) ? '-0' : Number.isFinite(v) ? v : String(v)) : v === '' || v === undefined ? null : v);
const rowsOf = (t) => t.toRows().map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, show(v)])));
const same = (got, want, msg) => { checks++; const g = JSON.stringify(got), w = JSON.stringify(want); if (g !== w) { failures++; console.log(`FAIL ${msg}\n  got  ${g.slice(0, 400)}\n  want ${w.slice(0, 400)}`); } };
const tryRows = (f) => { try { return rowsOf(f()); } catch (err) { return 'error: ' + err.message.slice(0, 120); } };

// file text -> the rows DuckDB's read_csv gives (missing as null; Vectro keeps -0, DuckDB's BIGINT makes it 0)
const CSV = {
  bomCrlf: ['﻿a,b\r\n1,x\r\n2,y\r\n', [{ a: 1, b: 'x' }, { a: 2, b: 'y' }]],
  oneRowNoNewline: ['a,b\n1,2', [{ a: 1, b: 2 }]],
  headerOnly: ['a,b\n', []],
  quoting: ['id,q\n1,"a""b"\n2,"x,y"\n3,"line\nbreak"\n4,""\n5,\n6,  spaced  \n7,"  quoted spaced "\n8,mid"quote\n9,"crlf\r\ninside"\n10,\'single\'\n',
    [{ id: 1, q: 'a"b' }, { id: 2, q: 'x,y' }, { id: 3, q: 'line\nbreak' }, { id: 4, q: null }, { id: 5, q: null }, { id: 6, q: '  spaced  ' }, { id: 7, q: '  quoted spaced ' }, { id: 8, q: 'mid"quote' }, { id: 9, q: 'crlf\r\ninside' }, { id: 10, q: "'single'" }]],
  quotedNumbers: ['id,v\n1,"12"\n2,""\n3,\n4," 7 "\n', [{ id: 1, v: 12 }, { id: 2, v: null }, { id: 3, v: null }, { id: 4, v: 7 }]],
  numbers: ['a,b,c,d,e,f\n1e5,.5,-0,1e400,-.5e2,1.5e-7\n1,1,1,1,1,1\n', [{ a: 100000, b: 0.5, c: '-0', d: 'Infinity', e: -50, f: 1.5e-7 }, { a: 1, b: 1, c: 1, d: 1, e: 1, f: 1 }]],
  trailingDelim: ['a,b,\n1,2,\n3,4,\n', [{ a: 1, b: 2, col2: null }, { a: 3, b: 4, col2: null }]],
  unicodeVals: ['k,v\né,1\né,2\n😀,3\nİ,4\n', [{ k: 'é', v: 1 }, { k: 'é', v: 2 }, { k: '😀', v: 3 }, { k: 'İ', v: 4 }]],
  crOnly: ['a,b\r1,x\r2,y\r', [{ a: 1, b: 'x' }, { a: 2, b: 'y' }]], // (old Mac line ends: it became one header line)
};
const e = new Engine();
for (const [name, [text, want]] of Object.entries(CSV)) {
  same(tryRows(() => e.parseCSV(text)), want, `parseCSV ${name}`);
  same(tryRows(() => e.parseCSV(new TextEncoder().encode(text))), want, `parseCSV ${name} (bytes)`);
}
same(e.parseCSV('a,b\n').columnNames, ['a', 'b'], 'a header only keeps its column names');

// Arrow round trips
const KIT = {
  n: Float64Array.from([0, -0, NaN, Infinity, -Infinity, 1e308, 5e-324, 2 ** 53, -1, 0.1]),
  s: ['a', '', 'é', 'é', '😀', 'a', '', 'İ', 'x'.repeat(1000), 'z'],
  d: [0, -1, NaN, Date.UTC(1900, 1, 28), Date.UTC(9999, 11, 31), Date.UTC(1, 0, 1), 1, Date.UTC(2024, 1, 29, 12, 0, 0, 5), -86400000, NaN].map((x) => new Date(x)),
};
const ARROW = {
  kit: () => e.table(KIT), float32: () => e.table({ n: KIT.n, s: KIT.s }, { type: Float32Array }), compressed: () => e.table(KIT, { compress: true }),
  empty: () => e.table(KIT).filter('n > 1e309').collect(), 'all missing': () => e.table({ n: Float64Array.from([NaN, NaN]), s: ['', ''] }), 'filtered view': () => e.table(KIT).filter('n > 0'),
};
for (const [label, mk] of Object.entries(ARROW)) for (const file of [false, true]) {
  const t = mk(), want = rowsOf(t);
  same(tryRows(() => e.fromArrow(t.toArrow({ file }))), want, `Arrow ${file ? 'file' : 'stream'} round trip: ${label}`);
  let got; try { got = rowsOf(e.fromArrow(await t.toArrowAsync({ file }))); } catch (err) { got = 'error: ' + err.message; }
  same(got, want, `Arrow ${file ? 'file' : 'stream'} round trip (async): ${label}`);
}
const text = (r, c) => (r.columns[c].labels ? Array.from(r.columns[c].codes, (k) => r.columns[c].labels[k]) : Array.from(r.columns[c]));
for (const s of [['a', null, 'b'], [null, 'a', 'b'], [undefined, 'x', '']]) {
  let got; try { got = text(readArrow(writeArrow({ s })), 's'); } catch (err) { got = 'error: ' + err.message; }
  same(got, s.map((x) => x ?? ''), `writeArrow / readArrow of plain text ${JSON.stringify(s)}: null is missing`);
}
await e.close();

if (MODE === 'node') {
  const dir = mkdtempSync(join(tmpdir(), 'vectro-r5-load-'));
  const en = new Engine(), opened = [];
  for (const [name, [csv, want]] of Object.entries(CSV)) {
    const p = join(dir, name + '.csv'); writeFileSync(p, csv);
    same(tryRows(() => en.readCSV(p)), want, `readCSV ${name}`);
    let got; try { got = rowsOf(await en.readCSVAsync(p)); } catch (err) { got = 'error: ' + err.message; }
    same(got, want, `readCSVAsync ${name}`);
    same(tryRows(() => { en.csvToDataset(p, join(dir, name + '.vectro'), { pieceBytes: 7 }); const d = en.openDataset(join(dir, name + '.vectro')); opened.push(d); return d; }), want, `csvToDataset ${name} (7-byte pieces)`);
  }
  for (const d of opened) d.close();
  await en.close();
  // JSON lines through openTables
  const { openTables } = await import('../src/serve/index.mjs');
  const jsonl = async (lines) => { const d = mkdtempSync(join(dir, 'j-')); writeFileSync(join(d, 't.jsonl'), lines.join('\n') + '\n'); const tabs = await openTables({ root: d }); try { return rowsOf(tabs.named().t); } finally { await tabs.close(); } };
  same(await jsonl(['{"x":1}', '{"x":"2"}', '{"x":3}']), [{ x: 1 }, { x: 2 }, { x: 3 }], 'JSON lines: a numeric string goes into a number column');
  same(await jsonl(['{"x":1}', '{"x":"1e3"}', '{"x":" 7 "}']), [{ x: 1 }, { x: 1000 }, { x: 7 }], 'JSON lines: numeric strings with an exponent or spaces');
  same(await jsonl(['﻿{"x":1}', '{"x":2}']), [{ x: 1 }, { x: 2 }], 'JSON lines: a BOM before the first record');
  same(await jsonl(['{"x":1}', '{"x":1e400}', '{"x":-1e400}']), [{ x: 1 }, { x: 'Infinity' }, { x: '-Infinity' }], 'JSON lines: 1e400 is Infinity (as in CSV)');
  same(await jsonl(['{"a":[1,2],"z":1}', '{"a":"txt","z":2}', '{"a":null,"z":3}']), [{ a: '[1,2]', z: 1 }, { a: 'txt', z: 2 }, { a: null, z: 3 }], 'JSON lines: a key nested in some rows keeps its plain values, nested ones as JSON text');
  same(await jsonl(['{"user":"a","ms":12}', 'not json', '{"user":"b","ms":null}']), [{ user: 'a', ms: 12 }, { user: 'b', ms: null }], 'JSON lines: a line that is not an object is skipped');
  same(await jsonl(['{"x":"1"}', '{"x":""}', '{"x":"\\u0030"}']), [{ x: 1 }, { x: null }, { x: 0 }], 'JSON lines: blank text is missing, an escaped "0" is 0');
  same(await jsonl([...Array.from({ length: 1200 }, (_, i) => `{"x":${i}}`), '{"x":1,"y":"7"}']).then((r) => r.at(-1)), { x: 1, y: 7 }, 'JSON lines: a key first seen after the first lines, with a numeric string, is a number');
  // streams: a BOM before the first pushed row
  const { SplitEngine } = await import('../src/node.mjs');
  const se = new SplitEngine({ workers: 2 });
  for (const [format, text] of [['csv', '\uFEFF1,x\n2,y\n'], ['ndjson', '\uFEFF{"a":1,"b":"x"}\n{"a":2,"b":"y"}\n']]) {
    const st = await se.streamAsync({ format, columns: { a: 'number', b: 'category' } });
    await st.push(text);
    same(await (await st.snapshotAsync()).toRowsAsync(), [{ a: 1, b: 'x' }, { a: 2, b: 'y' }], `${format} stream: a BOM before the first row`);
  }
  const cr = await se.parseCSVAsync('a,b\r1,x\r2,y\r');
  same(await cr.toRowsAsync(), [{ a: 1, b: 'x' }, { a: 2, b: 'y' }], 'split table: CSV with CR-only line ends');
  await se.close();
  rmSync(dir, { recursive: true, force: true });
}

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} round-5 loading checks passed`);
process.exit(failures ? 1 : 0);
