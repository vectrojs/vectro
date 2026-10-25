// Text and byte helpers that work everywhere: Node's Buffer when it exists
// (fast, and accepts shared memory), standard TextDecoder otherwise.

const NodeBuffer = globalThis.Buffer;
const decoders = { utf8: new TextDecoder('utf-8'), latin1: new TextDecoder('latin1') };
const encoder = new TextEncoder();
const isShared = (buf) => typeof SharedArrayBuffer === 'function' && buf instanceof SharedArrayBuffer;

// Decode bytes [s, e) of a Uint8Array.
export function decode(u8, s, e, enc = 'utf8') {
  if (NodeBuffer) return NodeBuffer.from(u8.buffer, u8.byteOffset, u8.length).toString(enc, s, e);
  const part = u8.subarray(s, e);
  return decoders[enc].decode(isShared(u8.buffer) ? part.slice() : part); // TextDecoder refuses shared memory
}

export const encode = (str) => encoder.encode(str);

// Node's Buffer.indexOf uses memchr/memmem and is ~6x faster than
// TypedArray.indexOf, so it's used whenever Buffer exists.
const asBuffer = (u8) => NodeBuffer.from(u8.buffer, u8.byteOffset, u8.length);

// First index of `byte` in [s, e), or -1.
export function indexOfByte(u8, byte, s, e = u8.length) {
  const i = NodeBuffer ? asBuffer(u8).indexOf(byte, s) : u8.indexOf(byte, s);
  return i !== -1 && i < e ? i : -1;
}

// Number of times `byte` occurs in [s, e).
export function countByte(u8, byte, s, e) {
  let n = 0;
  if (NodeBuffer) {
    const b = asBuffer(u8);
    for (let i = b.indexOf(byte, s); i !== -1 && i < e; i = b.indexOf(byte, i + 1)) n++;
  } else {
    for (let i = s; i < e; i++) if (u8[i] === byte) n++;
  }
  return n;
}

// Occurrences of the byte pattern starting inside [s, e) (overlapping matches count).
export function countPattern(u8, pat, s, e) {
  const m = pat.length;
  if (m === 0) return 0;
  let n = 0;
  if (NodeBuffer) {
    const hi = Math.min(u8.length, e + m - 1);
    if (hi <= s) return 0;
    const b = NodeBuffer.from(u8.buffer, u8.byteOffset + s, hi - s), p = NodeBuffer.from(pat.buffer, pat.byteOffset, m);
    for (let i = b.indexOf(p, 0); i !== -1 && i < e - s; i = b.indexOf(p, i + 1)) n++;
    return n;
  }
  const first = pat[0], last = Math.min(e - 1, u8.length - m);
  for (let i = s; i <= last; i++) {
    if (u8[i] !== first) continue;
    let k = 1;
    while (k < m && u8[i + k] === pat[k]) k++;
    if (k === m) n++;
  }
  return n;
}
