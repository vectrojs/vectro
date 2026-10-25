// Writes dist/vectro.wasm: the engine's fixed kernels, precompiled for
// runtimes that can't compile WebAssembly themselves (Cloudflare Workers).
// Single-threaded build (plain, non-shared memory).
import { writeFileSync, mkdirSync } from 'node:fs';
import { buildStaticModule } from '../src/static-kernels.mjs';

const bytes = buildStaticModule(false);
if (!WebAssembly.validate(bytes)) throw new Error('generated module is invalid');
mkdirSync(new URL('../dist/', import.meta.url), { recursive: true });
writeFileSync(new URL('../dist/vectro.wasm', import.meta.url), bytes);
console.log(`dist/vectro.wasm: ${bytes.length.toLocaleString()} bytes`);
