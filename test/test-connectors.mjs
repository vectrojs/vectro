// The packaged connectors: connectors/build.mjs makes Vectro.mez and
// vectro.taco with every file their code refers to, and the Power BI
// connector calls endpoints the server has.
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { inflateRawSync } from 'node:zlib';
import { fileURLToPath } from 'node:url';

let failures = 0, checks = 0;
const check = (ok, msg) => { checks++; if (!ok) { failures++; console.log('FAIL', msg); } };
const at = (p) => fileURLToPath(new URL(`../connectors/${p}`, import.meta.url));
execFileSync(process.execPath, [at('build.mjs')]);

// a zip's files: name -> bytes (from the central directory)
function unzip(buf) {
  const out = new Map();
  let end = buf.length - 22;
  while (buf.readUInt32LE(end) !== 0x06054b50) end--;
  let p = buf.readUInt32LE(end + 16);
  for (let k = 0; k < buf.readUInt16LE(end + 10); k++) {
    const method = buf.readUInt16LE(p + 10), size = buf.readUInt32LE(p + 20), nameLen = buf.readUInt16LE(p + 28), extra = buf.readUInt16LE(p + 30), comment = buf.readUInt16LE(p + 32), local = buf.readUInt32LE(p + 42);
    const name = buf.subarray(p + 46, p + 46 + nameLen).toString();
    const start = local + 30 + buf.readUInt16LE(local + 26) + buf.readUInt16LE(local + 28);
    const body = buf.subarray(start, start + size);
    out.set(name, method === 8 ? inflateRawSync(body) : body);
    p += 46 + nameLen + extra + comment;
  }
  return out;
}

const mez = unzip(readFileSync(at('dist/Vectro.mez')));
const pq = mez.get('Vectro.pq')?.toString() ?? '';
check(pq.startsWith('// Vectro') && /section Vectro;/.test(pq) && /shared Vectro\.Contents/.test(pq), 'Vectro.mez holds Vectro.pq');
for (const [, file] of pq.matchAll(/Extension\.Contents\("([^"]+)"\)/g)) check(mez.has(file), `Vectro.mez holds ${file}`);
const resx = mez.get('resources.resx')?.toString() ?? '';
for (const [, name] of pq.matchAll(/Extension\.LoadString\("([^"]+)"\)/g)) check(resx.includes(`<data name="${name}"`), `resources.resx has ${name}`);
check(/OData\.Feed\(base & "odata\/"/.test(pq) && /RelativePath = "sql", Query = \[q = query, format = "json"\]/.test(pq), 'the connector calls /odata/ and /sql?format=json');
check(/if kind = "number" then type number else if kind = "date" then type datetimezone/.test(pq), 'column types as /sql gives them (number, text, date)');

const taco = unzip(readFileSync(at('dist/vectro.taco')));
check(taco.get('META-INF/MANIFEST.MF')?.toString().startsWith('Manifest-Version: 1.0'), 'vectro.taco is a jar');
const manifest = taco.get('manifest.xml')?.toString() ?? '';
check(/<connector-plugin class='vectro' superclass='jdbc'/.test(manifest), 'the plugin class');
for (const [, file] of manifest.matchAll(/file=['"]([^'"]+)['"]/g)) check(taco.has(file), `vectro.taco holds ${file}`);
const tdr = taco.get('connectionResolver.tdr')?.toString() ?? '';
for (const [, file] of tdr.matchAll(/file=['"]([^'"]+)['"]/g)) check(taco.has(file), `vectro.taco holds ${file}`);
check(/class='vectro'/.test(tdr) && /class='vectro'/.test(taco.get('dialect.tdd')?.toString() ?? ''), 'one class in every file');
check(/CAP_CREATE_TEMP_TABLES" value="no"/.test(manifest) && /CAP_SELECT_INTO" value="no"/.test(manifest), 'read only: no temporary tables, no SELECT INTO');
check(/default-value="5433"/.test(taco.get('connectionFields.xml')?.toString() ?? ''), 'port 5433 by default');
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} connector checks passed`);
process.exit(failures ? 1 : 0);
