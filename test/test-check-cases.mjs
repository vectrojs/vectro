// The browser check page's cases, run in Node on split tables and in
// single-thread mode: all must match examples/check/expected.json.
import { readFileSync } from 'node:fs';
import { Worker } from 'node:worker_threads';
import { Engine } from '../src/portable.mjs';
import { SplitEngine } from '../src/split.mjs';
import { asyncEngine, queryCases, fuzzCases } from '../examples/check/cases.mjs';

const expected = JSON.parse(readFileSync(new URL('../examples/check/expected.json', import.meta.url)));
const spawn = () => {
  const w = new Worker(new URL('../src/split-worker-node.mjs', import.meta.url));
  w.unref();
  return { post: (m, t) => w.postMessage(m, t), onMessage: (fn) => w.on('message', fn), terminate: () => w.terminate() };
};
let failures = 0, checks = 0;
for (const [name, make] of [['single thread', () => new Engine()], ['split, 3 workers', () => new SplitEngine({ workers: 3, spawn })]]) {
  const e = make();
  const eng = e instanceof SplitEngine ? e : asyncEngine(e);
  const got = await queryCases(eng);
  for (const k in expected) { checks++; if (got[k] !== expected[k]) { failures++; console.log('FAIL', name, k, String(got[k]).slice(0, 200)); } }
  const fz = await fuzzCases(eng);
  checks += fz.checks; failures += fz.failures.length;
  for (const f of fz.failures) console.log('FAIL', name, f);
  await e.close();
}
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} check-page cases passed`);
process.exit(failures ? 1 : 0);
