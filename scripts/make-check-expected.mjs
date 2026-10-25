// Writes examples/check/expected.json: Node's answers to the shared query
// cases. Browsers must reproduce them (examples/check/index.html).
import { writeFileSync } from 'node:fs';
import { Engine } from '../src/node.mjs';
import { asyncEngine, queryCases, fuzzCases } from '../examples/check/cases.mjs';

const engine = new Engine();
const expected = await queryCases(asyncEngine(engine));
const fuzz = await fuzzCases(asyncEngine(engine));
await engine.close();
if (fuzz.failures.length) { console.log(fuzz.failures.join('\n')); process.exit(1); }
writeFileSync(new URL('../examples/check/expected.json', import.meta.url), JSON.stringify(expected, null, 1));
console.log(`${Object.keys(expected).length} expected answers written; ${fuzz.checks} fuzz checks passed in Node`);
