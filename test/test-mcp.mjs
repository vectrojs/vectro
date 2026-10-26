// The MCP server as a real subprocess: initialize, list tools, load files,
// query, and refuse files outside its data folder.
import { spawn } from 'node:child_process';
import { mkdtempSync, writeFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';

let failures = 0, checks = 0;
const expect = (label, ok, detail = '') => { checks++; if (!ok) { failures++; console.log('FAIL', label, detail); } };

// data folder with a CSV, a JSON-lines file and a file outside it
const dir = mkdtempSync(join(tmpdir(), 'vectro-mcp-'));
mkdirSync(join(dir, 'data'));
const REG = ['EU', 'US', 'APAC'];
const rows = Array.from({ length: 5000 }, (_, i) => ({ id: i, price: (i * 37) % 1000, qty: 1 + (i % 5), region: REG[i % 3] }));
writeFileSync(join(dir, 'data', 'orders.csv'), 'id,price,qty,region\n' + rows.map((r) => `${r.id},${r.price},${r.qty},${r.region}`).join('\n') + '\n');
writeFileSync(join(dir, 'data', 'events.jsonl'), rows.map((r) => JSON.stringify({ ...r, extra: { nested: true } })).join('\n') + '\nnot json\n');
writeFileSync(join(dir, 'secret.csv'), 'a\n1\n');
// the same orders as a dataset on disk (small chunks: several of them)
{
  const { Engine } = await import('../src/node.mjs');
  const e = new Engine({ threads: 1 });
  e.writeDataset(join(dir, 'data', 'big.vectro'), e.table({ id: rows.map((r) => r.id), price: rows.map((r) => r.price), qty: rows.map((r) => r.qty), region: rows.map((r) => r.region) }), { rowsPerChunk: 700 });
  await e.close();
}
// documents with 8-number embeddings
let seed = 3;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff) - 0.5;
const docs = Array.from({ length: 2000 }, (_, i) => ({ id: i, topic: ['ai', 'db', 'web'][i % 3], year: 2015 + (i % 10), embedding: Array.from({ length: 8 }, rnd) }));
writeFileSync(join(dir, 'data', 'docs.jsonl'), docs.map((d) => JSON.stringify(d)).join('\n') + '\n');
const cosine = (a, b) => { let d = 0, x = 0, y = 0; for (let j = 0; j < a.length; j++) { const p = Math.fround(a[j]), q = Math.fround(b[j]); d += p * q; x += p * p; y += q * q; } return d / Math.sqrt(x * y); };

const server = spawn(process.execPath, [new URL('../mcp/server.mjs', import.meta.url).pathname, '--root', join(dir, 'data')], { stdio: ['pipe', 'pipe', 'pipe'] });
const pending = new Map();
createInterface({ input: server.stdout }).on('line', (line) => { const m = JSON.parse(line); pending.get(m.id)?.(m); pending.delete(m.id); });
let nextId = 0;
const rpc = (method, params) => new Promise((resolve) => { const id = ++nextId; pending.set(id, resolve); server.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); });
const call = async (name, args) => { const r = await rpc('tools/call', { name, arguments: args }); return { error: r.result?.isError, text: r.result?.content?.[0]?.text, data: r.result?.structuredContent }; };

const init = await rpc('initialize', { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'test', version: '1' } });
expect('initialize', init.result?.protocolVersion === '2025-06-18' && init.result?.capabilities?.tools && init.result?.serverInfo?.name === 'vectro', JSON.stringify(init));
server.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n');
const old = await rpc('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 't', version: '1' } });
expect('older protocol accepted', old.result?.protocolVersion === '2024-11-05');
const list = await rpc('tools/list', {});
expect('tools listed', ['load_file', 'list_tables', 'describe_table', 'query', 'sql', 'search_similar', 'add_column', 'drop_table'].every((n) => list.result.tools.some((t) => t.name === n)));
expect('unknown method', (await rpc('no/such', {})).error?.code === -32601);

let r = await call('load_file', { path: 'orders.csv' });
expect('load csv', !r.error && r.data.rows === 5000 && r.data.columns.region === 'category' && r.data.sample.length === 5, r.text);
r = await call('load_file', { path: 'events.jsonl', name: 'events' });
expect('load jsonl', !r.error && r.data.rows === 5000 && r.data.skipped_lines === 1 && !('extra' in r.data.columns), r.text);

const sum = (f) => rows.filter(f).reduce((a, x) => a + x.price * x.qty, 0);
r = await call('query', { table: 'orders', filter: "region == 'EU' && price > 500", aggregates: { revenue: 'sum(price * qty)', n: 'count()' } });
expect('aggregate', !r.error && r.data.rows[0].revenue === sum((x) => x.region === 'EU' && x.price > 500) && r.data.rows[0].n === rows.filter((x) => x.region === 'EU' && x.price > 500).length, r.text);
r = await call('query', { table: 'events', group_by: 'region', aggregates: { revenue: 'sum(price * qty)' }, sort: '-revenue' });
const byRegion = REG.map((g) => ({ region: g, revenue: sum((x) => x.region === g) })).sort((a, b) => b.revenue - a.revenue);
expect('group + sort', !r.error && JSON.stringify(r.data.rows) === JSON.stringify(byRegion), r.text);
r = await call('query', { table: 'orders', filter: 'qty == 5', sort: ['-price', 'id'], limit: 3 });
const top3 = rows.filter((x) => x.qty === 5).sort((a, b) => b.price - a.price || a.id - b.id).slice(0, 3).map((x) => x.id);
expect('rows sorted', !r.error && JSON.stringify(r.data.rows.map((x) => x.id)) === JSON.stringify(top3) && r.data.matching_rows === 1000, r.text);
r = await call('sql', { query: "SELECT region, SUM(price * qty) AS revenue FROM events GROUP BY region ORDER BY revenue DESC" });
expect('sql group', !r.error && JSON.stringify(r.data.rows) === JSON.stringify(byRegion) && r.data.rows_in_answer === REG.length, r.text);
r = await call('sql', { query: 'SELECT o.id FROM orders o JOIN events e ON o.id = e.id WHERE o.qty = 5 ORDER BY o.price DESC, o.id LIMIT 3' });
expect('sql join', !r.error && JSON.stringify(r.data.rows.map((x) => x.id)) === JSON.stringify(top3), r.text);
r = await call('sql', { query: 'SELECT id FROM orders' });
expect('sql rows capped', !r.error && r.data.rows_in_answer === 5000 && r.data.returned === 500, r.text);
r = await call('sql', { query: 'SELECT nope FROM orders' });
expect('sql error', r.error && r.text.includes('no column "nope"'), r.text);
r = await call('add_column', { table: 'orders', name: 'revenue', expression: 'price * qty' });
expect('add column', !r.error && r.data.columns.revenue === 'number', r.text);
r = await call('query', { table: 'orders', aggregates: { total: 'sum(revenue)' } });
expect('use added column', !r.error && r.data.rows[0].total === sum(() => true), r.text);
r = await call('describe_table', { table: 'orders' });
expect('describe', !r.error && r.data.columns.region.distinct === 3 && r.data.columns.price.min === 0, r.text);

// a dataset on disk answers like the loaded table
r = await call('load_file', { path: 'big.vectro' });
expect('open dataset', !r.error && r.data.rows === 5000 && r.data.on_disk && r.data.columns.region === 'category' && r.data.sample.length === 5, r.text);
r = await call('query', { table: 'big', filter: "region == 'EU' && price > 500", aggregates: { revenue: 'sum(price * qty)', n: 'count()' } });
expect('dataset aggregate', !r.error && r.data.rows[0].revenue === sum((x) => x.region === 'EU' && x.price > 500), r.text);
r = await call('query', { table: 'big', group_by: 'region', aggregates: { revenue: 'sum(price * qty)' }, sort: '-revenue' });
expect('dataset group + sort', !r.error && JSON.stringify(r.data.rows) === JSON.stringify(byRegion) && r.data.groups === 3, r.text);
r = await call('query', { table: 'big', filter: 'qty == 5', sort: ['-price', 'id'], limit: 3 });
expect('dataset rows sorted', !r.error && JSON.stringify(r.data.rows.map((x) => x.id)) === JSON.stringify(top3) && r.data.matching_rows === 1000, r.text);
r = await call('sql', { query: 'SELECT region, SUM(price * qty) AS revenue FROM big GROUP BY region ORDER BY revenue DESC' });
expect('dataset sql', !r.error && JSON.stringify(r.data.rows) === JSON.stringify(byRegion), r.text);
r = await call('describe_table', { table: 'big' });
expect('dataset describe', !r.error && r.data.columns.region.distinct === 3 && r.data.columns.price.min === 0, r.text);
r = await call('add_column', { table: 'big', name: 'x', expression: 'price * 2' });
expect('dataset add_column refused', r.error && r.text.includes('dataset on disk'), r.text);
r = await call('drop_table', { table: 'big' });

// mistakes come back as tool errors the model can read
for (const [label, name, args, text] of [
  ['outside folder', 'load_file', { path: '../secret.csv' }, 'outside the data folder'],
  ['missing file', 'load_file', { path: 'nope.csv' }, 'no such file'],
  ['unknown table', 'query', { table: 'nope' }, 'no table "nope"'],
  ['bad expression', 'query', { table: 'orders', filter: 'price >' }, 'vectro:'],
  ['unknown column', 'query', { table: 'orders', aggregates: { x: 'sum(nope)' } }, 'unknown column'],
  ['group without aggregates', 'query', { table: 'orders', group_by: 'region' }, 'needs aggregates'],
]) {
  r = await call(name, args);
  expect(`error: ${label}`, r.error && r.text.includes(text), r.text);
}
// embeddings: load, then similar rows by row, by vector, with a filter
r = await call('load_file', { path: 'docs.jsonl' });
expect('load embeddings', !r.error && r.data.rows === 2000 && r.data.embedding?.dim === 8 && r.data.columns.topic === 'category', r.text);
const nearest = (q, keep = () => true) => docs.filter(keep).map((d) => ({ id: d.id, s: cosine(d.embedding, q) })).sort((a, b) => b.s - a.s);
r = await call('search_similar', { table: 'docs', like: { column: 'id', value: 7 }, k: 5 });
const want = nearest(docs[7].embedding).slice(0, 5);
expect('similar to a row', !r.error && r.data.rows[0].id === 7 && r.data.rows.every((h, j) => Math.abs(cosine(docs[h.id].embedding, docs[7].embedding) - want[j].s) < 1e-5), r.text);
const qv = Array.from({ length: 8 }, rnd);
r = await call('search_similar', { table: 'docs', vector: qv, k: 4, filter: "topic == 'ai' && year >= 2020" });
const wantF = nearest(qv, (d) => d.topic === 'ai' && d.year >= 2020).slice(0, 4);
expect('similar to a vector, filtered', !r.error && r.data.rows.length === 4 && r.data.rows.every((h, j) => h.topic === 'ai' && h.year >= 2020 && Math.abs(cosine(docs[h.id].embedding, qv) - wantF[j].s) < 1e-5), r.text);
r = await call('search_similar', { table: 'orders', like: { column: 'id', value: 1 } });
expect('error: no embeddings', r.error && r.text.includes('has no embeddings'), r.text);
r = await call('search_similar', { table: 'docs', like: { column: 'id', value: 99999 } });
expect('error: no such row', r.error && r.text.includes('no row where'), r.text);
r = await call('drop_table', { table: 'docs' });

r = await call('drop_table', { table: 'events' });
r = await call('list_tables', {});
expect('drop + list', !r.error && r.data.tables.length === 1 && r.data.tables[0].table === 'orders', r.text);

server.stdin.end();
await new Promise((res) => server.on('exit', res));
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} MCP checks passed`);
process.exit(failures ? 1 : 0);
