// vectro on Cloudflare Workers.
// Workers can't compile WebAssembly at runtime, so the precompiled kernels
// are bundled (Wrangler turns a .wasm import into a WebAssembly.Module) and
// expressions run on the vectorized interpreter. Single thread, plain memory.
//
// Run `npm run build` in the package root first to create dist/vectro.wasm.
import { Engine } from '../../../src/portable.mjs';
import wasmModule from '../../../dist/vectro.wasm';

let engine, orders;

export default {
  async fetch(request) {
    // One engine and table per isolate, reused across requests.
    engine ??= new Engine({ wasmModule });
    orders ??= engine.parseCSV(await sampleCSV());

    const url = new URL(request.url);
    const region = url.searchParams.get('region') ?? 'EU';
    const summary = orders.filter(`region == ${JSON.stringify(region)}`).agg({
      orders: 'count()',
      revenue: 'sum(price * qty)',
      avgPrice: 'mean(price)',
    });
    const grouped = orders.groupBy('region').agg({ revenue: 'sum(price * qty)' });
    const byRegion = grouped.toRows();
    grouped.dispose(); // free the result table's memory; the isolate lives on
    // (no timing here: Workers freeze performance.now() during a request)
    return Response.json({ region, summary, byRegion, mode: { jit: engine.jit, threads: engine.T } });
  },
};

// Demo data. In a real Worker this would come from R2, KV or a fetch.
async function sampleCSV() {
  const regions = ['EU', 'US', 'APAC', 'LATAM'];
  const lines = ['price,qty,region'];
  let seed = 1;
  const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
  for (let i = 0; i < 200_000; i++) lines.push(`${(rnd() * 1000).toFixed(2)},${1 + Math.floor(rnd() * 9)},${regions[i % 4]}`);
  return lines.join('\n');
}
