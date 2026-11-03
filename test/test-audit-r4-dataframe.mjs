// Audit round 4 (dataframe): text filters on a live stream free their masks after each query
// (they used to pile up until the worker's memory grew and the query failed); csvToDataset in
// small pieces loads what readCSV loads (blank lines at a piece's end, a blank line after the
// header, a row cut at the first piece's end).
import { Engine, SplitEngine } from '../src/node.mjs';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';

let failures = 0, checks = 0;
const same = (a, b, msg) => { checks++; const x = JSON.stringify(a), y = JSON.stringify(b); if (x !== y) { failures++; if (failures <= 20) console.log(`FAIL ${msg}\n  got  ${x.slice(0, 300)}\n  want ${y.slice(0, 300)}`); } };

// stream text filters, many queries
{
  const s = new SplitEngine({ workers: 1 });
  const st = await s.streamAsync({ columns: { tag: 'category', bill: 'category', v: 'number' } });
  let csv = '';
  const tag = [], bill = [];
  for (let i = 0; i < 20000; i++) { tag.push(`t${(i * 7919) % 5000}`); bill.push(`t${(i * 31) % 5000}`); csv += `${tag[i]},${bill[i]},${i}\n`; }
  await st.push(csv);
  for (let k = 0; k < 50 && (await st.countAsync()) < 20000; k++) await new Promise((r) => setTimeout(r, 20));
  const want = { "contains(tag, '7')": tag.filter((x) => x.includes('7')).length, 'tag == bill': tag.filter((x, i) => x === bill[i]).length };
  for (const [q, n] of Object.entries(want)) {
    const got = [];
    for (let k = 0; k < 300; k++) { try { got.push(await st.filter(q).countAsync()); } catch (err) { got.push(err.message); break; } }
    same([...new Set(got)], [n], `stream ${q}, 300 queries`);
  }
  await s.close();
}

// csvToDataset in pieces against readCSV
{
  const D = mkdtempSync(tmpdir() + '/vectro-r4-');
  const e = new Engine();
  const run = (label, text, pieces) => {
    writeFileSync(D + '/a.csv', text);
    const want = e.readCSV(D + '/a.csv').toRows();
    for (const pieceBytes of pieces) {
      let got;
      try { e.csvToDataset(D + '/a.csv', D + '/a.vectro', { pieceBytes }); const ds = e.openDataset(D + '/a.vectro'); got = ds.toRows(); ds.close(); }
      catch (err) { got = err.message; }
      same(got, want, `${label}, pieceBytes ${pieceBytes}`);
    }
  };
  const all = (text) => Array.from({ length: text.length + 1 }, (_, i) => i + 1);
  for (const text of ['a,b\n1,x\n\n\n', 'a,b\r\n1,x\r\n\r\n\r\n', 'a,b\n1,x\n2,y\n\n\n\n', 'a,b\n1,x\n\n2,y\n\n']) run(`blank lines ${JSON.stringify(text)}`, text, all(text));
  run('blank line after the header', 'c0,c1\n\n-37,b\n5,c\n', all('c0,c1\n\n-37,b\n5,c\n'));
  run('cut number in the first piece', 'c0,c1\n1,-41\n2,-5\n3,7\n4,8\n', all('c0,c1\n1,-41\n2,-5\n3,7\n4,8\n'));
  let big = 'id,s,v\n';
  for (let i = 0; i < 3000; i++) big += `${i},${i === 499 ? 'x' : i % 7},${i % 11 ? i / 4 : ''}\n${i % 500 ? '' : '\n'}`;
  run('3000 rows, text at row 499', big, [64, 1000, 9000, 50000]);
  await e.close();
  rmSync(D, { recursive: true, force: true });
}

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} round-4 dataframe checks passed`);
process.exit(failures ? 1 : 0);
