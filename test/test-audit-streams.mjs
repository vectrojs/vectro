// Audit fixes for streams and CSV to dataset, each checked against plain JS:
// - keepRows keeps the newest keepRows rows of the whole stream (not a share
//   per worker), with chunks of even and uneven sizes;
// - keepMs measures from the stream's newest time (over all workers);
// - CSV chunks cut inside a quoted field (newlines, commas, "") lose no rows,
//   by push() and over TCP;
// - csvToDataset with small pieces is about as fast as with large ones, and
//   works with pieces shorter than the header.
import { Worker } from 'node:worker_threads';
import { createServer } from 'node:net';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine } from '../src/node.mjs';
import { SplitEngine } from '../src/split.mjs';

let failures = 0, checks = 0;
const norm = (v) => JSON.stringify(v, (k, x) => (typeof x === 'number' && !Number.isInteger(x) ? Number(x.toPrecision(11)) : x));
const expect = (label, got, want) => { checks++; if (norm(got) !== norm(want)) { failures++; if (failures <= 15) console.log('FAIL', label, norm(got).slice(0, 300), 'vs', norm(want).slice(0, 300)); } };
const spawn = () => {
  const w = new Worker(new URL('../src/split-worker-node.mjs', import.meta.url));
  w.unref();
  return { post: (m, t) => w.postMessage(m, t), onMessage: (fn) => w.on('message', fn), terminate: () => w.terminate() };
};

let seed = 33;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const REG = ['EU', 'US', 'APAC'];
const rows = (from, n) => Array.from({ length: n }, (_, i) => ({ ts: from + i, v: Math.round(rnd() * 1000) / 10, region: REG[(from + i) % 3] }));
const csv = (rs) => rs.map((r) => `${r.ts},${r.v},${r.region}`).join('\n') + '\n';
const ndjson = (rs) => rs.map((r) => JSON.stringify(r)).join('\n') + '\n';
const COLUMNS = { ts: 'number', v: 'number', region: 'category' };
// what a stream reports, and the same from plain JS rows
const AGG = { n: 'count()', lo: 'min(ts)', hi: 'max(ts)', s: 'sum(v)', eu: "count(region == 'EU')" };
const reference = (rs) => ({ n: rs.length, lo: rs.length ? Math.min(...rs.map((r) => r.ts)) : null, hi: rs.length ? Math.max(...rs.map((r) => r.ts)) : null, s: rs.reduce((a, r) => a + r.v, 0), eu: rs.filter((r) => r.region === 'EU').length });
const answer = (a) => ({ ...a, lo: a.n ? a.lo : null, hi: a.n ? a.hi : null });
// chunk sizes in rows: even, or anything from 1 to 3000
const sizes = (total, even) => { const out = []; for (let i = 0; i < total; ) { const k = even ? even : 1 + Math.floor(rnd() * 3000); out.push(Math.min(k, total - i)); i += k; } return out; };

for (const workers of [1, 3, 4, 8]) {
  const split = new SplitEngine({ workers, spawn });

  // 1. keepRows over the whole stream: a plain sliding window of the newest K rows
  for (const [format, text] of [['csv', csv], ['ndjson', ndjson]]) {
    for (const even of [100, 1000, 0]) {
      for (const K of [1000, 9000]) {
        const s = await split.streamAsync({ format, columns: COLUMNS, keepRows: K });
        const all = [];
        let step = 0;
        for (const k of sizes(30_000, even)) {
          const rs = rows(all.length, k);
          all.push(...rs);
          await s.push(text(rs));
          if (++step % 7 === 0 || all.length === 30_000) { // check along the way, not only at the end
            await s.flushAsync(); // (queries see the rows parsed so far)
            const want = reference(all.slice(-K));
            expect(`w=${workers} ${format} keepRows ${K}, chunks ${even || 'uneven'}, after ${all.length} rows`, answer(await s.aggAsync(AGG)), want);
          }
        }
        const st = await s.flushAsync();
        expect(`w=${workers} ${format} keepRows ${K} flush rows`, [st.rows, st.rowsIn], [K, 30_000]);
        expect(`w=${workers} ${format} keepRows ${K} filtered`, await s.filter("region == 'US'").countAsync(), all.slice(-K).filter((r) => r.region === 'US').length);
        const snap = await s.snapshotAsync();
        expect(`w=${workers} ${format} keepRows ${K} snapshot`, await snap.countAsync(), K);
        await snap.dispose();
        await s.dispose();
      }
    }
  }
  // the audit's case: 1000 rows in one push with keepRows 1000, then three more pushes
  {
    const s = await split.streamAsync({ columns: { ts: 'number', v: 'number' }, keepRows: 1000 });
    const lines = (from, n) => Array.from({ length: n }, (_, i) => `${from + i},1`).join('\n') + '\n';
    await s.push(lines(0, 1000));
    await s.flushAsync();
    expect(`w=${workers} one push of keepRows rows`, await s.countAsync(), 1000);
    for (const from of [1000, 1100, 1200]) await s.push(lines(from, 100));
    await s.flushAsync();
    expect(`w=${workers} newest keepRows after more pushes`, await s.aggAsync({ n: 'count()', lo: 'min(ts)', hi: 'max(ts)' }), { n: 1000, lo: 300, hi: 1299 });
    await s.dispose();
  }

  // 2. keepMs from the stream's newest time; with keepRows too; windows unchanged
  for (const keepRows of [Infinity, 5000]) {
    const s = await split.streamAsync({ columns: COLUMNS, keepMs: 2000, timeColumn: 'ts', keepRows });
    const live = await s.windowAsync({ every: 100, last: 1000, timeColumn: 'ts', agg: { n: 'count()', s: 'sum(v)' } });
    const all = [];
    let t = 0, step = 0;
    for (const k of sizes(20_000, 0)) {
      const rs = rows(0, k).map((r, i) => ({ ...r, ts: t + i * (1 + (step % 3)) })); // times rise at different rates
      t = rs[rs.length - 1].ts + 1;
      all.push(...rs);
      await s.push(csv(rs));
      if (++step % 5 === 0) {
        await s.flushAsync();
        const newest = all[all.length - 1].ts, kept = all.slice(-keepRows).filter((r) => r.ts >= newest - 2000);
        expect(`w=${workers} keepMs (keepRows ${keepRows}) after ${all.length} rows`, answer(await s.aggAsync(AGG)), reference(kept));
      }
    }
    await s.flushAsync();
    const newest = all[all.length - 1].ts, kept = all.slice(-keepRows).filter((r) => r.ts >= newest - 2000);
    expect(`w=${workers} keepMs (keepRows ${keepRows}) at the end`, answer(await s.aggAsync(AGG)), reference(kept));
    const last = Math.floor(newest / 100), inWindow = all.filter((r) => Math.floor(r.ts / 100) > last - 10);
    const [w] = await live.readAsync();
    expect(`w=${workers} window over the last second (keepRows ${keepRows})`, w, { n: inWindow.length, s: inWindow.reduce((a, r) => a + r.v, 0) });
    await live.dispose();
    await s.dispose();
  }
  // the audit's case: an old chunk on one worker, a newer one on another
  {
    const s = await split.streamAsync({ columns: { ts: 'number', v: 'number' }, keepMs: 1000, timeColumn: 'ts' });
    await s.push('0,1\n100,1\n200,1\n300,1\n');
    await s.push('10000,1\n10500,1\n');
    await s.flushAsync();
    expect(`w=${workers} keepMs from the newest time of all workers`, await s.aggAsync({ n: 'count()', lo: 'min(ts)' }), { n: 2, lo: 10000 });
    await s.dispose();
  }

  // 3. CSV cut inside quoted fields
  {
    const notes = (i) => [`n${i}`, `two\nlines ${i}`, `a, "b" ${i}`, `27" wide`, `O"Brien ${i}`, `x\n\ny\r`, `"quoted" start`, ''][i % 8];
    const field = (s) => (/^[^",\n\r]*$/.test(s) && s !== '' ? s : s.includes('"') && !/^"|[,\n\r]/.test(s) ? s : `"${s.replace(/"/g, '""')}"`); // 27" wide stays unquoted
    const make = (n) => Array.from({ length: n }, (_, i) => ({ id: i, note: notes(i) }));
    const text = (rs) => rs.map((r) => `${r.id},${field(r.note)}`).join('\n') + '\n';
    const cols = { id: 'number', note: 'category' };
    const sorted = (rs) => [...rs].sort((a, b) => a.id - b.id);
    // a small text cut at every byte, in two pushes
    const small = make(8), t8 = text(small);
    let bad = 0;
    for (let cut = 0; cut <= t8.length; cut++) {
      const s = await split.streamAsync({ columns: cols });
      await s.push(t8.slice(0, cut));
      await s.push(t8.slice(cut));
      const st = await s.flushAsync();
      if (norm(sorted(await s.toRowsAsync())) !== norm(small) || st.badLines) bad++;
      await s.dispose();
    }
    expect(`w=${workers} quoted fields cut at every byte`, bad, 0);
    // 10,000 rows in pieces of 1000 bytes, and of random sizes
    const big = make(10_000), tb = text(big);
    for (const piece of [1000, 0]) {
      const s = await split.streamAsync({ columns: cols });
      for (let i = 0; i < tb.length; ) { const k = piece || Math.floor(rnd() * 3000); await s.push(tb.slice(i, i + k)); i += k; }
      const st = await s.flushAsync();
      expect(`w=${workers} quoted fields, pieces of ${piece || 'random'} bytes: rows`, [st.rows, st.badLines, st.unparsedBytes], [10_000, 0, 0]);
      expect(`w=${workers} quoted fields, pieces of ${piece || 'random'} bytes: values`, sorted(await s.toRowsAsync()), big);
      await s.dispose();
    }
    // a field still open at flushAsync waits for the rest of it
    {
      const s = await split.streamAsync({ columns: cols });
      await s.push('1,"open\nstill');
      expect(`w=${workers} open quote at flush: not yet a row`, (await s.flushAsync()).rows, 0);
      await s.push(' open"\n2,b');
      await s.flushAsync(); // (the last row has no newline: flush ends it)
      expect(`w=${workers} open quote, finished later`, sorted(await s.toRowsAsync()), [{ id: 1, note: 'open\nstill open' }, { id: 2, note: 'b' }]);
      await s.dispose();
    }
    // keepRows counts quoted rows, not newlines
    {
      const s = await split.streamAsync({ columns: cols, keepRows: 1500 });
      for (let i = 0; i < tb.length; i += 1000) await s.push(tb.slice(i, i + 1000));
      await s.flushAsync();
      expect(`w=${workers} keepRows with quoted newlines`, sorted(await s.toRowsAsync()), big.slice(-1500));
      await s.dispose();
    }
    // over TCP, cut anywhere (each worker its own connection)
    const parts = Array.from({ length: workers }, (_, w) => make(3000).map((r) => ({ ...r, id: w * 3000 + r.id })));
    const servers = await Promise.all(parts.map((rs) => new Promise((resolve) => {
      const server = createServer((socket) => {
        const tt = text(rs);
        for (let i = 0; i < tt.length; ) { const k = 1 + Math.floor(rnd() * 700); socket.write(tt.slice(i, i + k)); i += k; }
        socket.end();
      });
      server.listen(0, '127.0.0.1', () => resolve(server));
    })));
    const s = await split.streamAsync({ columns: cols });
    await Promise.all(servers.map((server, w) => s.connect(w, { port: server.address().port })));
    let st;
    for (let k = 0; k < 200; k++) { st = await s.flushAsync(); if (st.rowsIn === 3000 * workers) break; await new Promise((r) => setTimeout(r, 25)); }
    expect(`w=${workers} tcp quoted fields`, [st.rowsIn, st.badLines, st.errors], [3000 * workers, 0, []]);
    expect(`w=${workers} tcp quoted values`, sorted(await s.toRowsAsync()), sorted(parts.flat()));
    await s.dispose();
    for (const server of servers) server.close();
  }
  await split.close();
}

// 4. csvToDataset: small pieces give the same rows, in about the same time
{
  const e = new Engine();
  const dir = mkdtempSync(join(tmpdir(), 'vectro-audit-'));
  try {
    const n = 20_000;
    const ref = Array.from({ length: n }, (_, i) => ({ id: i, price: (i % 997) / 10, ref: i % 50 ? `ref-${i}` : `line\nbreak, "${i}"`, region: REG[i % 3] }));
    const q = (s) => (/[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s);
    writeFileSync(join(dir, 'a.csv'), 'id,price,ref,region\n' + ref.map((r) => `${r.id},${r.price},${q(r.ref)},${r.region}`).join('\n') + '\n');
    for (const pieceBytes of [7, 50, 1000, 30_000, 64 << 20]) {
      for (const rowsPerChunk of [3000, undefined]) {
        if (pieceBytes < 1000 && rowsPerChunk === undefined) continue;
        const out = join(dir, `a-${pieceBytes}-${rowsPerChunk}.vectro`);
        e.csvToDataset(join(dir, 'a.csv'), out, { pieceBytes, rowsPerChunk, types: { ref: 'string' } });
        const ds = e.openDataset(out);
        expect(`csvToDataset pieces of ${pieceBytes} bytes, chunks of ${rowsPerChunk ?? 'default'} rows`, ds.toRows(), ref);
        await ds.close?.();
      }
    }
    // a header longer than the pieces
    writeFileSync(join(dir, 'h.csv'), 'first_column,second_column\n1,2\n3,4\n');
    for (const pieceBytes of [1, 10, 30]) {
      let got;
      try { e.csvToDataset(join(dir, 'h.csv'), join(dir, 'h.vectro'), { pieceBytes }); const ds = e.openDataset(join(dir, 'h.vectro')); got = ds.toRows(); await ds.close?.(); } catch (err) { got = err.message; }
      expect(`csvToDataset pieces of ${pieceBytes} bytes, shorter than the header`, got, [{ first_column: 1, second_column: 2 }, { first_column: 3, second_column: 4 }]);
    }
    // time: unique text per row, one piece vs about 300 pieces (was quadratic in pieces)
    let big = 'id,price,ref\n';
    for (let i = 0; i < 200_000; i++) big += `${i},${(i % 9999) / 100},ref-${i}-${(i * 7919) % 100000}\n`;
    writeFileSync(join(dir, 'b.csv'), big);
    const time = (pieceBytes) => { const t0 = performance.now(); e.csvToDataset(join(dir, 'b.csv'), join(dir, 'b.vectro'), { pieceBytes, types: { ref: 'string' } }); return performance.now() - t0; };
    time(64 << 20); // (warm up)
    const one = time(64 << 20), many = time(16_000);
    checks++;
    if (!(many < 4 * one + 1500)) { failures++; console.log(`FAIL csvToDataset small pieces: ${Math.round(many)} ms vs ${Math.round(one)} ms in one piece`); }
  } finally {
    rmSync(dir, { recursive: true, force: true });
    await e.close?.();
  }
}

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} audit stream checks passed`);
process.exit(failures ? 1 : 0);
