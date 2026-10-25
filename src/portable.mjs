// Portable entry for browsers, Deno, Bun and edge runtimes: no Node APIs,
// single-threaded, plain memory (no SharedArrayBuffer needed).
//
//   import { Engine } from 'vectro/portable';
//   const engine = new Engine();                  // browsers, Deno, Bun
//
// Cloudflare Workers (no compiling WebAssembly at runtime): bundle the
// precompiled module and pass it in. Expressions then run on the
// vectorized interpreter instead of compiled kernels.
//   import wasmModule from 'vectro/dist/vectro.wasm';
//   const engine = new Engine({ wasmModule });
export { Engine, canCompileWasm } from './engine.mjs';
export { Table } from './table.mjs';
