// Portable entry for browsers, Deno, Bun and edge runtimes: no Node APIs,
// single-threaded, plain memory (no SharedArrayBuffer needed).
//
//   import { Engine } from 'vectrojs/portable';
//   const engine = new Engine();                  // browsers, Deno, Bun
//
// Cloudflare Workers (no compiling WebAssembly at runtime): bundle the
// precompiled module and pass it in. Expressions then run on the
// vectorized interpreter instead of compiled kernels.
//   import wasmModule from 'vectrojs/dist/vectro.wasm';
//   const engine = new Engine({ wasmModule });
export { Engine, canCompileWasm } from './engine.mjs';
export { Table } from './table.mjs';

export { VectorIndex } from './vectors.mjs';
export { BatchEncoder, FrameReader, encodeSchema, encodeEnd } from './frames.mjs';
export { readArrow, writeArrow, ArrowReader, ArrowEncoder } from './arrow.mjs';
export { DistinctSketch } from './sketch.mjs';
