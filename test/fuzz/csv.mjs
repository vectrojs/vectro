// CSV fuzz (RFC 4180): random tables -> CSV text -> vectro parseCSV/readCSV(Async) with many chunkBytes / threads; cell-by-cell compare.
import { Engine } from '../../src/node.mjs';
import { writeFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
const D = mkdtempSync(join(tmpdir(), 'vectro-fuzz-csv-'));
let seed = +(process.argv[2] ?? 1);
const rnd = () => { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed / 0x80000000; };
const pick = (a) => a[Math.floor(rnd() * a.length)];
const ATOMS = ['a', 'b', 'EU', ',', '"', '""', '\n', '\r\n', ' ', 'é', '😀', '中文', 'x y', "'", '\t', ';'];
const text = () => { const n = Math.floor(rnd() * 6); let s = ''; for (let i = 0; i < n; i++) s += pick(ATOMS); return s; };
const quoteIf = (s, force) => (force || /[",\n\r]/.test(s) || s === '' && false ? '"' + s.replaceAll('"', '""') + '"' : s);
const engines = [new Engine({ threads: 1 }), new Engine({ threads: 4 }), new Engine()];
let checks = 0, fails = 0;
const iters = +(process.argv[3] ?? 200);
for (let it = 0; it < iters; it++) {
  const nCols = 1 + Math.floor(rnd() * 5), nRows = Math.floor(rnd() * 300);
  const kinds = Array.from({ length: nCols }, () => pick(['number', 'category', 'string']));
  const names = kinds.map((k, i) => `c${i}`);
  const rows = Array.from({ length: nRows }, () => kinds.map((k) => (k === 'number' ? (rnd() < 0.1 ? NaN : Math.round((rnd() - 0.5) * 1e6) / 100) : text())));
  const eol = pick(['\n', '\r\n', 'mixed']);
  const forceQuote = rnd() < 0.3;
  let csv = names.join(',') + (eol === 'mixed' ? '\r\n' : eol);
  rows.forEach((r, i) => {
    csv += r.map((v, j) => (kinds[j] === 'number' ? (Number.isNaN(v) ? '' : (forceQuote ? `"${v}"` : String(v))) : quoteIf(v, forceQuote))).join(',');
    if (i < nRows - 1 || rnd() < 0.5) csv += eol === 'mixed' ? pick(['\n', '\r\n']) : eol;
  });
  // A row of all-empty fields in a single-column table is a blank line; skip that ambiguity.
  if (nCols === 1 && rows.some((r) => r[0] === '' || Number.isNaN(r[0]))) continue;
  // trailing-row ambiguity: last row's last field empty unquoted at end of file -> fine
  const types = Object.fromEntries(names.map((n, i) => [n, kinds[i]]));
  const file = `${D}/f${it}.csv`; writeFileSync(file, csv);
  const configs = [];
  for (const cb of [1, 7, 64, 1000, 1 << 21]) for (const ei of [0, 1, 2]) configs.push({ cb, ei, how: pick(['parse', 'parseBytes', 'read', 'readAsync', 'parseAsync']) });
  for (const { cb, ei, how } of configs) {
    const e = engines[ei];
    const o = { types, chunkBytes: cb };
    let t;
    try {
      t = how === 'parse' ? e.parseCSV(csv, o) : how === 'parseBytes' ? e.parseCSV(Buffer.from(csv), o) : how === 'read' ? e.readCSV(file, o) : how === 'readAsync' ? await e.readCSVAsync(file, o) : await e.parseCSVAsync(csv, o);
    } catch (err) { fails++; console.log('ERR', it, cb, ei, how, err.message); continue; }
    checks++;
    let bad = null;
    if (t.length !== nRows) bad = `rows ${t.length} != ${nRows}`;
    else {
      const got = t.toRows();
      outer: for (let i = 0; i < nRows; i++) for (let j = 0; j < nCols; j++) {
        const exp = rows[i][j], g = got[i][names[j]];
        if (!(Object.is(exp, g) || (exp === 0 && g === 0))) { bad = `r${i} c${j} exp ${JSON.stringify(exp)} got ${JSON.stringify(g)}`; break outer; }
      }
    }
    if (bad) { fails++; if (fails < 30) console.log('FAIL seed', process.argv[2], 'it', it, 'cb', cb, 'threads', ei, how, eol, forceQuote, bad); writeFileSync(`${D}/fail-${it}.csv`, csv); }
    t.dispose();
  }
}
console.log(fails ? `${fails} failures out of ${checks} CSV fuzz checks (files kept in ${D})` : `all ${checks} CSV fuzz checks passed`);
for (const e of engines) await e.close();
if (!fails) rmSync(D, { recursive: true, force: true });
process.exit(fails ? 1 : 0);
