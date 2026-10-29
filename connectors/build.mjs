// Builds the packaged connectors into connectors/dist (no dependencies):
//   Vectro.mez   Power BI custom connector (Power Query): Vectro.pq, resources.resx, icons
//   vectro.taco  Tableau connector plugin (a jar): the files in tableau/vectro
//   node connectors/build.mjs [--icons]     (--icons: draw the PNG icons again)
import { readFileSync, writeFileSync, mkdirSync, readdirSync, existsSync } from 'node:fs';
import { deflateRawSync, deflateSync } from 'node:zlib';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const pbi = join(here, 'powerbi', 'Vectro'), tab = join(here, 'tableau', 'vectro'), dist = join(here, 'dist');
const SIZES = [16, 20, 24, 32, 40, 48, 64, 80];

// ---- CRC-32, PNG, zip --------------------------------------------------------------------
const CRC = new Uint32Array(256).map((_, n) => { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; return c >>> 0; });
const crc32 = (buf) => { let c = 0xffffffff; for (const b of buf) c = CRC[(c ^ b) & 0xff] ^ (c >>> 8); return (c ^ 0xffffffff) >>> 0; };

function png(size, rgba) {
  const chunk = (type, data) => { const len = Buffer.alloc(4); len.writeUInt32BE(data.length); const td = Buffer.concat([Buffer.from(type), data]); const crc = Buffer.alloc(4); crc.writeUInt32BE(crc32(td)); return Buffer.concat([len, td, crc]); };
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(size, 0); ihdr.writeUInt32BE(size, 4); ihdr[8] = 8; ihdr[9] = 6; // 8-bit RGBA
  const raw = Buffer.alloc(size * (size * 4 + 1));
  for (let y = 0; y < size; y++) { raw[y * (size * 4 + 1)] = 0; Buffer.from(rgba.buffer, y * size * 4, size * 4).copy(raw, y * (size * 4 + 1) + 1); }
  return Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk('IHDR', ihdr), chunk('IDAT', deflateSync(raw)), chunk('IEND', Buffer.alloc(0))]);
}

// The icon: a white "V" on a blue rounded square, drawn with 4x4 samples per pixel.
function icon(size) {
  const out = new Uint8Array(size * size * 4), S = 4;
  const dist = (px, py, ax, ay, bx, by) => { const dx = bx - ax, dy = by - ay, t = Math.max(0, Math.min(1, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy))); return Math.hypot(px - ax - t * dx, py - ay - t * dy); };
  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) {
      let bg = 0, fg = 0;
      for (let sy = 0; sy < S; sy++) for (let sx = 0; sx < S; sx++) {
        const u = (x + (sx + 0.5) / S) / size, v = (y + (sy + 0.5) / S) / size;
        const r = 0.2, cx = Math.min(Math.max(u, r), 1 - r), cy = Math.min(Math.max(v, r), 1 - r);
        if (Math.hypot(u - cx, v - cy) > r) continue; // outside the rounded square
        bg++;
        const w = 0.085; // the V: two strokes meeting at the bottom
        if (dist(u, v, 0.24, 0.26, 0.5, 0.76) < w || dist(u, v, 0.76, 0.26, 0.5, 0.76) < w) fg++;
      }
      const a = bg / (S * S), f = bg ? fg / bg : 0, o = (y * size + x) * 4;
      out[o] = Math.round(37 + (255 - 37) * f); out[o + 1] = Math.round(99 + (255 - 99) * f); out[o + 2] = Math.round(235 + (255 - 235) * f); out[o + 3] = Math.round(a * 255);
    }
  }
  return png(size, out);
}

// A zip of [name, bytes] (deflated), as .mez and .taco (jar) files are.
function zip(files) {
  const local = [], central = [];
  let offset = 0;
  for (const [name, data] of files) {
    const nameBuf = Buffer.from(name), body = deflateRawSync(data), crc = crc32(data);
    const h = Buffer.alloc(30);
    h.writeUInt32LE(0x04034b50, 0); h.writeUInt16LE(20, 4); h.writeUInt16LE(0x0800, 6); h.writeUInt16LE(8, 8);
    h.writeUInt16LE(0, 10); h.writeUInt16LE(0x5b21, 12); // (a fixed date: the same files make the same archive)
    h.writeUInt32LE(crc, 14); h.writeUInt32LE(body.length, 18); h.writeUInt32LE(data.length, 22); h.writeUInt16LE(nameBuf.length, 26);
    local.push(h, nameBuf, body);
    const c = Buffer.alloc(46);
    c.writeUInt32LE(0x02014b50, 0); c.writeUInt16LE(20, 4); c.writeUInt16LE(20, 6); c.writeUInt16LE(0x0800, 8); c.writeUInt16LE(8, 10);
    c.writeUInt16LE(0, 12); c.writeUInt16LE(0x5b21, 14); c.writeUInt32LE(crc, 16); c.writeUInt32LE(body.length, 20); c.writeUInt32LE(data.length, 24);
    c.writeUInt16LE(nameBuf.length, 28); c.writeUInt32LE(offset, 42);
    central.push(c, nameBuf);
    offset += 30 + nameBuf.length + body.length;
  }
  const cd = Buffer.concat(central), end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0); end.writeUInt16LE(files.length, 8); end.writeUInt16LE(files.length, 10);
  end.writeUInt32LE(cd.length, 12); end.writeUInt32LE(offset, 16);
  return Buffer.concat([...local, cd, end]);
}

// ---- build ---------------------------------------------------------------------------------
if (process.argv.includes('--icons') || !SIZES.every((s) => existsSync(join(pbi, `Vectro${s}.png`)))) {
  for (const s of SIZES) writeFileSync(join(pbi, `Vectro${s}.png`), icon(s));
}
mkdirSync(dist, { recursive: true });
const mez = zip([
  ['Vectro.pq', readFileSync(join(pbi, 'Vectro.pq'))],
  ['resources.resx', readFileSync(join(pbi, 'resources.resx'))],
  ...SIZES.map((s) => [`Vectro${s}.png`, readFileSync(join(pbi, `Vectro${s}.png`))]),
]);
writeFileSync(join(dist, 'Vectro.mez'), mez);
const taco = zip([
  ['META-INF/MANIFEST.MF', Buffer.from('Manifest-Version: 1.0\r\nCreated-By: Vectro connectors/build.mjs\r\n\r\n')],
  ...readdirSync(tab).sort().map((f) => [f, readFileSync(join(tab, f))]),
]);
writeFileSync(join(dist, 'vectro.taco'), taco);
console.log(`connectors/dist: Vectro.mez (${mez.length} bytes), vectro.taco (${taco.length} bytes)`);
