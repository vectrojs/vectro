// Correctness: every op, WASM and JS paths, odd sizes, both thread modes.
import { Engine, MODE } from './test-mode.mjs';

let failures = 0;
const check = (label, got, want) => {
  const ok = got === want || Math.abs(got - want) <= 1e-9 * Math.max(1, Math.abs(want));
  if (!ok) { failures++; console.log(`FAIL ${label}: got ${got}, want ${want}`); }
};

const ref = {
  sum: (a) => { let s = 0; for (const v of a) s += v; return s; },
  min: (a) => { let m = Infinity; for (const v of a) if (v < m) m = v; return m; },
  max: (a) => { let m = -Infinity; for (const v of a) if (v > m) m = v; return m; },
  dot: (a, b) => { let s = 0; for (let i = 0; i < a.length; i++) s += a[i] * b[i]; return s; },
  gt: (a, x) => { let c = 0; for (const v of a) if (v > x) c++; return c; },
};

for (const wasm of [true, false]) {
  for (const threads of [1, 4, 8]) {
    const e = new Engine({ threads, wasm, minParallel: {} });
    for (const n of [0, 1, 3, 7, 8, 9, 1001, 16_384, 100_003, 1_000_001]) {
      for (const Type of [Float64Array, Float32Array]) {
        const tag = `wasm=${wasm} threads=${threads} n=${n} ${Type.name}`;
        let a = e.alloc(Type, n), b = e.alloc(Type, n), out = e.alloc(Type, n);
        [a, b, out] = [a, b, out].map((x) => e.live(x)); // single-thread memory may have grown
        for (let i = 0; i < n; i++) { a[i] = ((i * 7919) % 1000) / 8 - 40; b[i] = (i % 13) / 4; }
        if (Type === Float64Array) {
          check(`sum ${tag}`, e.sum(a), ref.sum(a));
          check(`dot ${tag}`, e.dot(a, b), ref.dot(a, b));
        }
        check(`min ${tag}`, e.min(a), ref.min(a));
        check(`max ${tag}`, e.max(a), ref.max(a));
        check(`countGreater ${tag}`, e.countGreater(a, 3.5), ref.gt(a, 3.5));
        e.add(a, b, out);
        for (let i = 0; i < n; i++) if (out[i] !== Math.fround(a[i] + b[i]) && out[i] !== a[i] + b[i]) { failures++; console.log(`FAIL add ${tag} at ${i}`); break; }
        e.scale(a, 2, 1, out);
        for (let i = 0; i < n; i++) if (out[i] !== new Type([a[i] * 2 + 1])[0]) { failures++; console.log(`FAIL scale ${tag} at ${i}`); break; }
        e.free(out); e.free(b); e.free(a);
      }
    }
    // sort + search still work on WASM-memory arrays
    const s = e.alloc(Float64Array, 300_001);
    for (let i = 0; i < s.length; i++) s[i] = Math.random();
    const want = Float64Array.from(s).sort();
    e.sort(s);
    for (let i = 0; i < s.length; i++) if (s[i] !== want[i]) { failures++; console.log(`FAIL sort wasm=${wasm} threads=${threads}`); break; }
    const t = e.alloc(Uint8Array, 1_000_000);
    t.set(new TextEncoder().encode('xxERRORxx'.repeat(111_111)));
    check(`countBytes wasm=${wasm} threads=${threads}`, e.countBytes(t, 'ERROR'), 111_111);
    await e.close();
  }
}

// allocator: freed space is reused and memory grows past the initial size
{
  const e = new Engine({ threads: 1 });
  const x = e.alloc(Float64Array, 1000); e.free(x);
  const y = e.alloc(Float64Array, 1000);
  if (y.byteOffset !== x.byteOffset) { failures++; console.log('FAIL allocator reuse'); }
  const big = e.alloc(Float64Array, 50_000_000); // 400MB, forces memory.grow
  big[big.length - 1] = 42;
  check('grown memory', e.max(big), 42);
  await e.close();
}

console.log(failures ? `${failures} failures` : `[${MODE}] all op tests passed`);
process.exit(failures ? 1 : 0);
