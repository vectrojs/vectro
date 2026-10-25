// Vectro frames: batches encoded on one side and read back exactly on the
// other, whatever sizes the bytes arrive in.
import { encodeSchema, encodeEnd, BatchEncoder, FrameReader } from '../src/frames.mjs';

let failures = 0, checks = 0;
const fail = (msg) => { failures++; if (failures <= 15) console.log('FAIL', msg); };
const sameNum = (a, b) => Object.is(a, b);

let seed = 13;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const pick = (xs) => xs[Math.floor(rnd() * xs.length)];
const SPECIAL = [NaN, -0, 0, Infinity, -Infinity, 1e308, -1e-308, 2 ** 53 + 2, 0.1];
const LABELS = ['EU', 'US', '', 'Zürich', '東京', '🚀 launch', 'a,b', 'line\nbreak', 'x'.repeat(300)];

for (let trial = 0; trial < 60; trial++) {
  const columns = {};
  const ncols = 1 + Math.floor(rnd() * 5);
  for (let c = 0; c < ncols; c++) columns[`c${c}`] = rnd() < 0.6 ? 'number' : 'category';
  const encoder = new BatchEncoder(columns);
  const batches = [];
  const frames = [encodeSchema(columns)];
  for (let b = 0; b < 1 + Math.floor(rnd() * 6); b++) {
    const rows = pick([0, 1, 3, 100, 5000]);
    const batch = {};
    for (const [name, kind] of Object.entries(columns)) {
      if (kind === 'number') {
        const a = new Float64Array(rows);
        for (let i = 0; i < rows; i++) a[i] = rnd() < 0.1 ? pick(SPECIAL) : (rnd() - 0.5) * 1e6;
        batch[name] = rnd() < 0.5 ? a : Array.from(a); // typed arrays or plain arrays
      } else {
        batch[name] = Array.from({ length: rows }, () => (rnd() < 0.5 ? pick(LABELS) : `L${Math.floor(rnd() * 50)}`));
      }
    }
    batches.push(batch);
    frames.push(encoder.encode(batch));
  }
  frames.push(encodeEnd());
  const all = new Uint8Array(frames.reduce((a, f) => a + f.length, 0));
  let at = 0;
  for (const f of frames) { all.set(f, at); at += f.length; }

  // read back, in pieces of random size (sometimes one byte at a time)
  const got = [];
  let schema = null, ended = false;
  const reader = new FrameReader({
    onSchema: (cols) => { schema = cols; },
    onBatch: (rows, cols) => got.push({ rows, cols: cols.map((c) => (c.values ? { values: Float64Array.from(c.values) } : { labels: Array.from(c.codes, (code) => c.labels[code]) })) }),
    onEnd: () => { ended = true; },
  });
  const tiny = rnd() < 0.2;
  for (let i = 0; i < all.length; ) { const k = tiny ? 1 : 1 + Math.floor(rnd() * 9000); reader.push(all.subarray(i, i + k)); i += k; }
  checks++;
  if (JSON.stringify(schema) !== JSON.stringify(columns) || !ended || got.length !== batches.length) { fail(`trial ${trial}: schema/end/batch count`); continue; }
  batches.forEach((batch, b) => {
    Object.entries(columns).forEach(([name, kind], k) => {
      checks++;
      const want = batch[name], g = got[b].cols[k];
      const ok = kind === 'number' ? want.length === g.values.length && Array.from(want).every((v, i) => sameNum(v, g.values[i])) : JSON.stringify(want) === JSON.stringify(g.labels);
      if (!ok) fail(`trial ${trial} batch ${b} column ${name}`);
    });
  });
}

// category columns given as codes + labels decode to the same labels
{
  const e = new BatchEncoder({ r: 'category', x: 'number' });
  const labels = ['EU', 'US', 'Zürich', ''];
  const codes = Uint32Array.from({ length: 1000 }, (_, i) => (i * 7) % 4);
  const frames = [encodeSchema({ r: 'category', x: 'number' }), e.encode({ r: { codes, labels }, x: new Float64Array(1000) }), e.encode({ r: ['US', 'new'], x: [1, 2] }), e.encode({ r: { codes: [3, 0], labels }, x: [1, 2] })];
  const got = [];
  const reader = new FrameReader({ onSchema() {}, onBatch: (rows, cols) => got.push(Array.from(cols[0].codes, (c) => cols[0].labels[c])) });
  for (const f of frames) reader.push(f);
  checks++;
  if (JSON.stringify(got[0]) !== JSON.stringify(Array.from(codes, (c) => labels[c])) || JSON.stringify(got[1]) !== '["US","new"]' || JSON.stringify(got[2]) !== '["","EU"]') fail('codes + labels input');
}
// frames come from memory that isn't zero-filled: every padding byte must
// still be zero (no stale memory goes out on the network)
{
  for (let trial = 0; trial < 50; trial++) {
    globalThis.Buffer.allocUnsafeSlow(1 << 20).fill(0xab); // dirty some memory first
    const e = new BatchEncoder({ x: 'number', r: 'category', y: 'number' });
    const n = 1 + (trial * 37) % 101;
    const labels = Array.from({ length: n }, (_, i) => 'L'.repeat(1 + ((i * 13 + trial) % 11)) + i);
    const f = e.encode({ x: Float64Array.from({ length: n }, (_, i) => i), r: labels, y: new Float64Array(n) });
    const dv = new DataView(f.buffer, f.byteOffset, f.byteLength);
    const pad = [];
    pad.push([5, 8], [12, 16]);
    let at = 16 + n * 8; // x (n*8 is a multiple of 8)
    const count = dv.getUint32(at, true);
    pad.push([at + 4, at + 8]);
    at += 8;
    for (let i = 0; i < count; i++) { const len = dv.getUint32(at, true); pad.push([at + 4 + len, at + ((4 + len + 7) & ~7)]); at += (4 + len + 7) & ~7; }
    pad.push([at + n * 4, at + ((n * 4 + 7) & ~7)]);
    checks++;
    if (pad.some(([a, z]) => f.subarray(a, z).some((v) => v !== 0))) fail(`padding not zero (trial ${trial})`);
  }
}
// labels are sent once per connection: the second batch carries only new ones
{
  const e = new BatchEncoder({ r: 'category' });
  const first = e.encode({ r: ['EU', 'US', 'EU'] }).length, second = e.encode({ r: ['EU', 'US', 'EU'] }).length;
  checks++; if (!(second < first)) fail(`labels resent: ${first} then ${second} bytes`);
}
// mistakes are refused
for (const [label, run, text] of [
  ['bad kind', () => new BatchEncoder({ a: 'text' }), "'number' or 'category'"],
  ['missing column', () => new BatchEncoder({ a: 'number', b: 'number' }).encode({ a: [1] }), 'missing column "b"'],
  ['length mismatch', () => new BatchEncoder({ a: 'number', b: 'number' }).encode({ a: [1, 2], b: [1] }), 'expected 2'],
  ['batch before schema', () => new FrameReader({ onSchema() {}, onBatch() {} }).push(new BatchEncoder({ a: 'number' }).encode({ a: [1] })), 'before the schema'],
]) {
  let msg = '';
  try { run(); } catch (err) { msg = err.message; }
  checks++; if (!msg.includes(text)) fail(`${label}: ${msg}`);
}
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} frame checks passed`);
process.exit(failures ? 1 : 0);
