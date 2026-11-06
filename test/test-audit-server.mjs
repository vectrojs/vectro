// Servers, the engine's life and error messages (QA audit, 3 Nov 2026):
// POST /reload while queries run; column types (PostgreSQL, OData) while
// an async query runs; Engine / SplitEngine close() with queries queued;
// failed table() calls giving their memory back; malformed %-escapes in
// HTTP paths; clear messages for bad top / filter / agg arguments and very
// deep expressions; psql's \d name; the MCP server's argument checks.
// Expected values are worked out here in plain JavaScript.
import { mkdtempSync, rmSync, writeFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createInterface } from 'node:readline';
import { Engine, SplitEngine, sql } from '../src/node.mjs';
import { serve, servePg, openTables } from '../src/serve/index.mjs';

let failures = 0, checks = 0;
const check = (ok, msg) => { checks++; if (!ok) { failures++; if (failures <= 20) console.log('FAIL', msg); } };
const same = (a, b, msg) => { const x = JSON.stringify(a), y = JSON.stringify(b); check(x === y, `${msg}\n  got  ${x.slice(0, 300)}\n  want ${y.slice(0, 300)}`); };
const throwsLike = (fn, re, msg) => { try { fn(); check(false, `${msg}: no error`); } catch (err) { check(re.test(err.message), `${msg}: ${err.message}`); } };
const rejectsLike = async (p, re, msg) => { try { await p; check(false, `${msg}: no error`); } catch (err) { check(re.test(err.message), `${msg}: ${err.message}`); } };
const used = (e) => e.heapTop - e.freeList.reduce((s, b) => s + b.size, 0); // engine bytes in use
const until = async (cond, ms = 10_000) => { const end = Date.now() + ms; while (!cond() && Date.now() < end) await new Promise((r) => setImmediate(r)); return cond(); };
const run = (cmd, args, opts = {}) => new Promise((done) => { // (spawned, not spawnSync: servers run in this process)
  const p = spawn(cmd, args, opts);
  let stdout = '', stderr = '';
  p.stdout?.on('data', (d) => { stdout += d; });
  p.stderr?.on('data', (d) => { stderr += d; });
  p.on('error', (error) => done({ error }));
  p.on('close', (code, signal) => done({ code, signal, stdout, stderr }));
});
const dir = mkdtempSync(join(tmpdir(), 'vectro-audit-server-'));

// ---- 1. POST /reload while queries are running ------------------------------------------
{
  const N = 400_000, qty = (i) => (i * 7919) % 50;
  writeFileSync(join(dir, 'orders.csv'), 'id,qty\n' + Array.from({ length: N }, (_, i) => `${i},${qty(i)}`).join('\n') + '\n');
  const ids = []; // ORDER BY qty DESC, id LIMIT 3
  for (let q = 49; ids.length < 3; q--) for (let i = 0; i < N && ids.length < 3; i++) if (qty(i) === q) ids.push(i);
  const want = ids.map((i) => [i, qty(i)]);

  const http = await serve({ root: dir, port: 0 });
  const e = http.engine;
  const q = encodeURIComponent('SELECT id, qty FROM orders ORDER BY qty DESC, id LIMIT 3');
  same((await (await fetch(`${http.url}/sql?q=${q}`)).json()).rows, want, 'query before the reload');
  const before = used(e);
  let finished = 0;
  const inflight = Array.from({ length: 20 }, () => fetch(`${http.url}/sql?q=${q}`).then((r) => r.json()).then((j) => { finished++; return j; }));
  await until(() => e.busy);
  const reload = await (await fetch(`${http.url}/reload`, { method: 'POST' })).json();
  same(reload, { loaded: ['orders'], failed: [] }, 'the reload succeeds while queries run');
  const answers = await Promise.all(inflight);
  check(answers.every((j) => !j.error && JSON.stringify(j.rows) === JSON.stringify(want)), `in-flight queries answer from the old table: ${JSON.stringify(answers.find((j) => j.error || JSON.stringify(j.rows) !== JSON.stringify(want)))}`);
  same((await (await fetch(`${http.url}/sql?q=${q}`)).json()).rows, want, 'query after the reload');
  check(used(e) === before, `the old table is freed once its queries end: ${used(e)} bytes in use, ${before} before`);
  await http.close();

  // the holds, step by step: tables replaced by a reload are freed when the holds taken before it end
  const catalog = await openTables({ root: dir });
  const oneTable = used(catalog.engine);
  const done = catalog.hold();
  const old = catalog.entries.get('orders').table;
  const running = old.lazy().sort('-qty', 'id').limit(3).collectAsync();
  same(await catalog.reload(), { loaded: ['orders'], failed: [] }, 'reload with a hold taken');
  const fresh = catalog.entries.get('orders').table;
  check(fresh !== old, 'a new table is served');
  const top = await running;
  same(top.toRows().map((r) => [r.id, r.qty]), want, 'a query that started before the reload');
  top.dispose();
  check(used(catalog.engine) > oneTable, `both tables kept while the hold lasts: ${used(catalog.engine)}, one table ${oneTable}`);
  same(old.lazy().sort('-qty', 'id').limit(3).toRows().map((r) => [r.id, r.qty]), want, 'the old table still reads right while held');
  done();
  done(); // (twice: no effect)
  check(used(catalog.engine) === oneTable, `the old table freed when the hold ends: ${used(catalog.engine)}, want ${oneTable}`);
  same(fresh.lazy().sort('-qty', 'id').limit(3).toRows().map((r) => [r.id, r.qty]), want, 'the new table');
  // two reloads at once run one after the other
  const [r1, r2] = await Promise.all([catalog.reload(), catalog.reload()]);
  same([r1.loaded, r2.loaded], [['orders'], ['orders']], 'two reloads at once');
  check(used(catalog.engine) === oneTable, `after two reloads, one table in memory: ${used(catalog.engine)}`);
  await catalog.close();
  rmSync(join(dir, 'orders.csv'));
}

// ---- 2. column types while an async query is running ------------------------------------
{
  const e = new Engine();
  const N = 300_000;
  const DAY = 86_400_000, t0 = Date.UTC(2024, 0, 1);
  const cols = {
    k: Float64Array.from({ length: N }, (_, i) => i % 5000),                 // whole numbers
    x: Float64Array.from({ length: N }, (_, i) => (i === N - 1 ? 0.5 : i)),  // one fraction, at the end
    big: Float64Array.from({ length: N }, (_, i) => (i ? i : 2 ** 53)),      // whole, but too big for int8
    m: Float64Array.from({ length: N }, (_, i) => (i % 7 ? i : NaN)),        // whole, some missing
    day: Array.from({ length: N }, (_, i) => new Date(t0 + (i % 400) * DAY)),
    ts: Array.from({ length: N }, (_, i) => new Date(t0 + (i % 400) * DAY + (i === 5 ? 1000 : 0))),
    r: Array.from({ length: N }, (_, i) => ['EU', 'US'][i % 2]),
  };
  // the types, from the values
  const numType = (a) => { let hi = 0; for (const v of a) { if (v !== v) continue; if (v !== Math.floor(v)) return 'float'; hi = Math.max(hi, Math.abs(v)); } return hi >= 2 ** 53 ? 'float' : 'int'; };
  const dateType = (a) => (a.every((d) => d.getTime() % DAY === 0) ? 'day' : 'date');
  const want = { k: numType(cols.k), x: numType(cols.x), big: numType(cols.big), m: numType(cols.m), day: dateType(cols.day), ts: dateType(cols.ts), r: 'text' };
  same(want, { k: 'int', x: 'float', big: 'float', m: 'int', day: 'day', ts: 'date', r: 'text' }, 'the expected types themselves');
  const load = e.table({ v: Float64Array.from({ length: 2_000_000 }, (_, i) => i % 1000) });
  for (const packed of [false, true]) {
    const t = packed ? e.table(cols).compress() : e.table(cols);
    if (packed) check([...t.cols.values()].some((c) => c.enc), 'some columns packed');
    // (more work until the engine is seen busy: on a slow runner one round can end before the check looks)
    const busy = [];
    let seen = false;
    for (let round = 0; round < 5 && !seen; round++) { busy.push(...Array.from({ length: 8 }, () => load.aggAsync({ s: 'sum(v * v)' }))); seen = await until(() => e.busy, 2000); }
    check(seen, 'the engine is busy');
    let types = null, wasBusy = false;
    try {
      wasBusy = e.busy;
      types = Object.fromEntries(sql('SELECT * FROM t', { t }, { types: true }).columns.map((c) => [c.name, c.type]));
    } catch (err) { types = err.message; }
    check(wasBusy, 'asked while busy');
    same(types, want, `types while an async query runs${packed ? ' (packed columns)' : ''}`);
    await Promise.all(busy);
    const idle = new Engine({ threads: 1 }); // the same table on an idle engine: the types as before
    const t2 = idle.table(cols);
    same(Object.fromEntries(sql('SELECT * FROM t', { t: t2 }, { types: true }).columns.map((c) => [c.name, c.type])), want, 'types on an idle engine');
    await idle.close();
    t.dispose();
  }

  // OData $metadata and a PostgreSQL query while /sql is busy
  const orders = e.table({ id: Float64Array.from({ length: 1_000_000 }, (_, i) => i), amount: Float64Array.from({ length: 1_000_000 }, (_, i) => (i % 997) + 0.5) });
  const http = await serve({ tables: { orders }, port: 0 });
  const big = encodeURIComponent('SELECT COUNT(DISTINCT id) AS d FROM orders');
  let load2 = Promise.all(Array.from({ length: 6 }, () => fetch(`${http.url}/sql?q=${big}`).then((r) => r.status)));
  await until(() => e.busy);
  const meta = await fetch(`${http.url}/odata/$metadata`);
  const xml = await meta.text();
  check(meta.status === 200 && /Name="id" Type="Edm.Int64"/.test(xml) && /Name="amount" Type="Edm.Double"/.test(xml), `OData $metadata while /sql is busy: ${meta.status} ${xml.slice(0, 200)}`);
  same(await load2, [200, 200, 200, 200, 200, 200], '/sql answers');
  const pgs = await servePg({ tables: { orders }, port: 0 });
  load2 = Promise.all(Array.from({ length: 6 }, () => fetch(`${http.url}/sql?q=${big}`).then((r) => r.status)));
  await until(() => e.busy);
  const psql = await run('psql', ['-h', '127.0.0.1', '-p', String(pgs.port), '-U', 'x', '-d', 'vectro', '-At', '-c', 'SELECT SUM(amount) AS s FROM orders']);
  let s = 0;
  for (let i = 0; i < 1_000_000; i++) s += (i % 997) + 0.5;
  if (!psql.error) check(Number(psql.stdout.trim()) === s, `psql while /sql is busy: ${psql.stdout} ${psql.stderr}`);
  await load2;
  await pgs.close();
  await http.close();
  await e.close();
}

// ---- 3. close() with async queries queued; every call after close -----------------------
{
  // a child process: it must exit by itself, and every query settles
  const src = `
    import { Engine } from ${JSON.stringify(pathToFileURL(fileURLToPath(new URL('../src/node.mjs', import.meta.url))).href)};
    const e = new Engine();
    const t = e.table({ x: Float64Array.from({ length: 5_000_000 }, (_, i) => i % 1000) });
    const qs = Array.from({ length: 10 }, () => t.aggAsync({ s: 'sum(x)' }));
    await new Promise((r) => setTimeout(r, 5));
    await e.close();
    const states = await Promise.all(qs.map((q) => q.then((v) => v.s === 499500 * 5000 ? 'right' : 'WRONG ' + v.s, (err) => err.message)));
    let sync; try { t.agg({ s: 'sum(x)' }); sync = 'ran'; } catch (err) { sync = err.message; }
    console.log(JSON.stringify({ states, sync }));`;
  writeFileSync(join(dir, 'close-child.mjs'), src);
  const r = await run(process.execPath, [join(dir, 'close-child.mjs')], { timeout: 60_000 }); // (killed if it hangs)
  let out = null;
  try { out = JSON.parse(r.stdout.trim().split('\n').pop()); } catch { /* reported below */ }
  check(r.code === 0 && out, `the process exits after close(): code ${r.code} ${r.signal ?? ''} ${r.stderr.slice(0, 300)}`);
  if (out) {
    check(out.states.length === 10 && out.states.every((s) => s === 'right' || /^vectro: this engine is closed/.test(s)), `queued queries: ${JSON.stringify(out.states)}`);
    check(out.states.some((s) => s !== 'right'), `some queries were stopped by close(): ${JSON.stringify(out.states)}`);
    check(/^vectro: this engine is closed/.test(out.sync), `sync call after close: ${out.sync}`);
  }

  const e = new Engine();
  const t = e.table({ a: [1, 2, 3] });
  const u = e.table({ a: [4, 5] });
  await e.close();
  await e.close(); // (twice: fine)
  const closed = /^vectro: this engine is closed/;
  throwsLike(() => e.table({ a: [1, 2, 3] }), closed, 'table() after close');
  throwsLike(() => e.alloc(Float64Array, 4), closed, 'alloc() after close');
  throwsLike(() => t.toRows(), closed, 'toRows() after close');
  throwsLike(() => t.sum('a'), closed, 'sum() after close');
  throwsLike(() => t.filter('a > 1').count(), closed, 'filter().count() after close');
  await rejectsLike(t.aggAsync({ s: 'sum(a)' }), closed, 'aggAsync() after close');
  await rejectsLike(e.parseCSVAsync('a\n1\n'), closed, 'parseCSVAsync() after close');
  u.dispose(); // (freeing still works: cleanup code after close)
  check(true, 'dispose after close');

  const se = new SplitEngine({ workers: 2 });
  await se.ready;
  const st = await se.tableAsync({ a: [1, 2, 3] });
  await se.close();
  await rejectsLike(se.streamAsync({ columns: { a: 'number' } }), closed, 'SplitEngine.streamAsync after close');
  await rejectsLike(se.tableAsync({ a: [1] }), closed, 'SplitEngine.tableAsync after close');
  await rejectsLike(se.parseCSVAsync('a\n1\n'), closed, 'SplitEngine.parseCSVAsync after close');
  await rejectsLike(st.countAsync(), closed, 'a split table after close');
  const se2 = new SplitEngine({ workers: 2 });
  await se2.close(); // (before it was ready: no unhandled rejection)
  await rejectsLike(se2.ready, closed, 'SplitEngine.ready after an early close');
}

// ---- 4. a failed table() gives back what it copied ---------------------------------------
{
  const e = new Engine();
  const big = new Float64Array(2_000_000);
  const before = used(e);
  for (const [what, cols] of [
    ['rows that differ', { a: big, b: [1, 2, 3] }],
    ['a bad name', { a: big, 'bad name': big }],
    ['a code with no label', { a: big, c: { codes: Uint32Array.from({ length: big.length }, (_, i) => (i === big.length - 1 ? 9 : 0)), labels: ['x'] } }],
  ]) {
    try { e.table(cols); check(false, `${what}: no error`); } catch (err) { check(/^vectro:/.test(err.message), `${what}: ${err.message}`); }
    check(used(e) === before, `${what}: ${used(e) - before} bytes still in use`);
  }
  for (const [what, rows, opts] of [
    ['fromRows, a bad name', [{ a: 1, 'b c': 2 }], {}],
    ['fromRows, dates in float32', [{ a: 1, d: new Date(0) }], { type: Float32Array }],
  ]) {
    try { e.tableFromRows(rows, opts); check(false, `${what}: no error`); } catch (err) { check(/^vectro:/.test(err.message), `${what}: ${err.message}`); }
    check(used(e) === before, `${what}: ${used(e) - before} bytes still in use`);
  }
  const t = e.table({ a: big, b: big }); // (and a good table still loads)
  check(t.length === big.length, 'a good table after the failed ones');
  t.dispose();
  check(used(e) === before, 'and its memory back');
  await e.close();
}

// ---- 5. malformed %-escapes in HTTP paths: 400 ------------------------------------------
{
  const e = new Engine();
  const s = await serve({ tables: { t: e.table({ a: [1] }) }, port: 0 });
  for (const p of ['/tables/%ZZ', '/tables/%E0%A4%A/rows', '/odata/%ZZ', '/odata/t(%ZZ)']) {
    const r = await fetch(s.url + p), text = await r.text();
    check(r.status === 400 && /malformed %-escape/.test(text), `${p}: ${r.status} ${text}`);
  }
  const ok = await fetch(`${s.url}/tables/%74`); // (a good escape: "t")
  check(ok.status === 200, `a good escape: ${ok.status}`);
  await s.close();
  await e.close();
}

// ---- 6. clear messages ----------------------------------------------------------------
{
  const e = new Engine();
  const N = 200_000;
  const t = e.table({ id: Float64Array.from({ length: N }, (_, i) => i) });
  for (const [what, f] of [
    ['top on 200k rows', () => t.top(50, 'nope')],
    ['lazy sort + limit', () => t.lazy().sort('nope').limit(5).toRows()],
    ['top on a small table', () => t.head(10).top(5, 'nope')],
    ['top with an expression', () => t.top(50, '-(nope * 2)')],
  ]) throwsLike(f, /^vectro: unknown column "nope".*\(columns: id\)$/, what);
  throwsLike(() => t.top(NaN, 'id'), /^vectro: top\(k, by\) needs k, the number of rows to keep, got NaN$/, 'top(NaN)');
  throwsLike(() => t.top('5', 'id'), /^vectro: top\(k, by\) needs k/, 'top("5")');
  throwsLike(() => t.top(undefined, 'id'), /^vectro: top\(k, by\) needs k/, 'top(undefined)');
  same(t.top(3, '-id').toRows(), [{ id: N - 1 }, { id: N - 2 }, { id: N - 3 }], 'top(3) still works');
  throwsLike(() => t.filter(null).count(), /^vectro: an expression must be text/, 'filter(null)');
  throwsLike(() => t.filter(undefined).count(), /^vectro: an expression must be text/, 'filter(undefined)');
  throwsLike(() => t.agg(null), /^vectro: agg\(\) needs an object of aggregations/, 'agg(null)');
  throwsLike(() => t.agg(['sum(id)']), /^vectro: agg\(\) needs an object.*got an array/, 'agg([...])');
  throwsLike(() => t.lazy().agg(null), /^vectro: agg\(\) needs an object of aggregations/, 'lazy agg(null)');
  throwsLike(() => t.derive('d', null), /^vectro: an expression must be text/, 'derive(name, null)');
  // deep expressions: a clear limit; just under it still runs, and right
  const sumX = N * (N - 1) / 2;
  throwsLike(() => t.agg({ a: 'sum(' + 'id+'.repeat(5000) + 'id)' }), /^vectro: expression too deep/, 'sum of 5000 terms');
  throwsLike(() => t.filter('('.repeat(10_000) + 'id > 1' + ')'.repeat(10_000)).count(), /^vectro: expression too deep/, '10,000 nested brackets');
  throwsLike(() => t.filter('-'.repeat(5000) + 'id > 1').count(), /^vectro: expression too deep/, '5000 minus signs');
  same(t.agg({ a: 'sum(' + 'id+'.repeat(900) + 'id)' }).a, 901 * sumX, 'a sum of 901 terms');
  same(t.filter('('.repeat(900) + 'id > 1' + ')'.repeat(900)).count(), N - 2, '900 nested brackets');
  throwsLike(() => sql('SELECT SUM(' + 'id+'.repeat(5000) + 'id) AS s FROM t', { t }), /^vectro( SQL)?: (expression too deep|the query is nested too deeply)/, 'SQL: 5000 terms');
  throwsLike(() => sql('SELECT COUNT(*) AS c FROM t WHERE ' + '('.repeat(5000) + 'id > 1' + ')'.repeat(5000), { t }), /^vectro SQL: the query is nested too deeply/, 'SQL: 5000 brackets');
  same(sql('SELECT COUNT(*) AS c FROM t WHERE ' + '('.repeat(100) + 'id > 1' + ')'.repeat(100), { t }).toRows(), [{ c: N - 2 }], 'SQL: 100 brackets');
  const s = await serve({ tables: { t }, port: 0 });
  const r = await fetch(`${s.url}/sql`, { method: 'POST', body: 'SELECT COUNT(*) AS c FROM t WHERE ' + '('.repeat(5000) + 'id > 1' + ')'.repeat(5000) });
  const j = await r.json();
  check(r.status === 400 && /nested too deeply/.test(j.error), `HTTP /sql, a deep query: ${r.status} ${j.error}`);
  await s.close();
  same(t.sum('id'), sumX, 'the engine is still fine');
  await e.close();
}

// ---- 7. psql's \d name and \d+ name ------------------------------------------------------
{
  const e = new Engine();
  const orders = e.table({ id: [1, 2, 3], region: ['EU', 'US', 'EU'], amount: [1.5, 2, 3.25], ts: [new Date('2024-01-01'), new Date('2024-01-02T03:00:00Z'), new Date('2024-01-03')] });
  const s = await servePg({ tables: { orders, other_t: e.table({ k: [1] }) }, port: 0 });
  const psql = (cmd, flags = '-At') => run('psql', ['-h', '127.0.0.1', '-p', String(s.port), '-U', 'x', '-d', 'vectro', flags, '-c', cmd]);
  const d = await psql('\\d orders');
  if (!d.error) { // (psql installed)
    same(d.stdout.trim().split('\n'), ['id|bigint|||', 'region|text|||', 'amount|double precision|||', 'ts|timestamp without time zone|||'], `psql \\d orders (${d.stderr.trim()})`);
    const plus = await psql('\\d+ orders');
    same(plus.stdout.trim().split('\n').slice(0, 4).map((l) => l.split('|').slice(0, 2).concat(l.split('|')[5])), [['id', 'bigint', 'plain'], ['region', 'text', 'extended'], ['amount', 'double precision', 'plain'], ['ts', 'timestamp without time zone', 'plain']], `psql \\d+ orders (${plus.stderr.trim()})`);
    const footer = await psql('\\d+ orders', '-A'); // (with -t, psql leaves the footer out)
    check(/Access method: heap/.test(footer.stdout), `\\d+ shows the access method: ${footer.stdout}`);
    const other = await psql('\\d other_t');
    same(other.stdout.trim().split('\n'), ['k|bigint|||'], 'psql \\d other_t');
    const pattern = await psql('\\d ord*');
    same(pattern.stdout.trim().split('\n'), ['id|bigint|||', 'region|text|||', 'amount|double precision|||', 'ts|timestamp without time zone|||'], 'psql \\d ord*');
    const none = await psql('\\d nope');
    check(/Did not find any relation named "nope"/.test(none.stderr), `psql \\d nope: ${none.stderr}`);
    const dt = await psql('\\dt');
    same(dt.stdout.trim().split('\n'), ['public|orders|table|x', 'public|other_t|table|x'], 'psql \\dt still works');
  } else console.log('(psql not installed: \\d checks skipped)');
  await s.close();
  await e.close();
}

// ---- 8. the MCP server checks limit, k and aggregates ------------------------------------
{
  mkdirSync(join(dir, 'mcp'));
  const rows = Array.from({ length: 100 }, (_, i) => ({ id: i, price: (i * 37) % 100, region: ['EU', 'US'][i % 2] }));
  writeFileSync(join(dir, 'mcp', 'orders.csv'), 'id,price,region\n' + rows.map((r) => `${r.id},${r.price},${r.region}`).join('\n') + '\n');
  const server = spawn(process.execPath, [fileURLToPath(new URL('../mcp/server.mjs', import.meta.url)), '--root', join(dir, 'mcp')], { stdio: ['pipe', 'pipe', 'pipe'] });
  const pending = new Map();
  createInterface({ input: server.stdout }).on('line', (line) => { const m = JSON.parse(line); pending.get(m.id)?.(m); pending.delete(m.id); });
  let nextId = 0;
  const rpc = (method, params) => new Promise((resolve) => { const id = ++nextId; pending.set(id, resolve); server.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); });
  const call = async (name, args) => { const r = await rpc('tools/call', { name, arguments: args }); return { error: r.result?.isError, text: r.result?.content?.[0]?.text, data: r.result?.structuredContent }; };
  await rpc('initialize', { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'test', version: '1' } });
  let r = await call('load_file', { path: 'orders.csv' });
  check(!r.error && r.data.rows === 100, `load: ${r.text}`);
  for (const limit of [-3, 0, '10', 2.5, 'all']) {
    r = await call('query', { table: 'orders', limit });
    check(r.error && /^limit must be a whole number from 1 to 500, got /.test(r.text), `limit ${JSON.stringify(limit)}: ${r.text}`);
  }
  for (const aggregates of [['sum(price)'], 'sum(price)', { s: 5 }, 7]) {
    r = await call('query', { table: 'orders', aggregates });
    check(r.error && /^aggregates must be an object of names and expressions/.test(r.text), `aggregates ${JSON.stringify(aggregates)}: ${r.text}`);
  }
  r = await call('query', { table: 'orders', limit: null, aggregates: null }); // (null: not given)
  check(!r.error && r.data.returned === 50, `limit and aggregates null: ${r.text?.slice(0, 100)}`);
  r = await call('query', { table: 'orders', sort: '-price', limit: 3 });
  same(r.data?.rows?.map((x) => x.price), rows.map((x) => x.price).sort((a, b) => b - a).slice(0, 3), 'a good limit');
  r = await call('query', { table: 'orders', limit: 10_000 });
  check(!r.error && r.data.returned === 100, `a limit over 500 gives at most 500: ${r.text?.slice(0, 100)}`);
  r = await call('query', { table: 'orders', aggregates: { s: 'sum(price)' } });
  same(r.data?.rows, [{ s: rows.reduce((a, x) => a + x.price, 0) }], 'good aggregates');
  server.stdin.end();
  await new Promise((done) => server.on('close', done));
}

// round 3: close() during async wrappers and split calls never crashes the process (strict unhandled rejections)
{
  const node = JSON.stringify(pathToFileURL(fileURLToPath(new URL('../src/node.mjs', import.meta.url))).href);
  const src = `
    import { Engine, SplitEngine, sql } from ${node};
    import { writeFileSync } from 'node:fs';
    const csv = ${JSON.stringify(join(dir, 'r3.csv'))};
    writeFileSync(csv, 'k,v\\n' + Array.from({ length: 200_000 }, (_, i) => i + ',' + i).join('\\n') + '\\n');
    const out = [];
    const { VectorIndex } = await import(${JSON.stringify(pathToFileURL(fileURLToPath(new URL('../src/vectors.mjs', import.meta.url))).href)});
    for (const op of ['readCSVAsync', 'parseCSVAsync', 'sqlAsync', 'countAsync', 'unionAsync', 'searchAsync']) {
      const e = new Engine(), t = e.table({ g: Float64Array.from({ length: 300_000 }, (_, i) => i % 7) });
      const index = op === 'searchAsync' ? await VectorIndex.create(e, { vectors: Array.from({ length: 50_000 }, (_, i) => [i % 3, 1, i % 5]), dim: 3 }) : null;
      const p = op === 'readCSVAsync' ? e.readCSVAsync(csv) : op === 'parseCSVAsync' ? e.parseCSVAsync('a,b\\n' + '1,2\\n'.repeat(200_000))
        : op === 'sqlAsync' ? sql('SELECT g, COUNT(*) AS n FROM t GROUP BY g', { t }).toRowsAsync()
        : op === 'unionAsync' ? sql('SELECT g FROM t UNION ALL SELECT g FROM t', { t }).toRowsAsync()
        : op === 'searchAsync' ? index.searchAsync([1, 1, 1], { k: 5 }) : t.lazy().countAsync();
      await e.close();
      out.push(await p.then(() => 'resolved', (err) => /this engine is closed/.test(err.message) ? 'closed' : err.message));
    }
    const s = new SplitEngine({ workers: 2 }); const st = await s.tableAsync({ a: [1, 2, 3] });
    st.dispose(); await s.close(); out.push('split ok');
    console.log(JSON.stringify(out));`;
  writeFileSync(join(dir, 'close-r3.mjs'), src);
  const r = await run(process.execPath, ['--unhandled-rejections=strict', join(dir, 'close-r3.mjs')], { timeout: 120_000 });
  let out = null;
  try { out = JSON.parse(r.stdout.trim().split('\n').pop()); } catch { /* reported below */ }
  check(r.code === 0 && out?.length === 7 && out.slice(0, 6).every((x) => x === 'closed' || x === 'resolved'), `close() during async calls, strict: code ${r.code} ${JSON.stringify(out)} ${r.stderr.slice(0, 300)}`);
  // the wrapped methods keep their names, and an error thrown at once still is
  const e = new Engine(), t = e.table({ a: [1, 2] });
  check(t.sumAsync.name === 'sumAsync' && e.readCSVAsync.name === 'readCSVAsync', `wrapped names: ${t.sumAsync.name}, ${e.readCSVAsync.name}`);
  let thrown = false;
  try { t.deriveAsync(123, 'a').catch(() => {}); } catch { thrown = true; }
  check(thrown, 'deriveAsync(123) still throws at once');
  await e.close();
}

// round 3: whenFree runs once the holds taken before it end; keepMs right after a push never shows more than the window
{
  const cat = await openTables({ tables: { t: new Engine().table({ a: [1] }) } });
  const release = cat.hold();
  let ran = 0;
  cat.whenFree(() => ran++);
  check(ran === 0, 'whenFree waits for a running hold');
  release();
  check(ran === 1, 'whenFree runs when that hold ends');
  cat.whenFree(() => ran++);
  check(ran === 2, 'whenFree with no hold runs at once');
  await cat.engine.close();
  const s = new SplitEngine({ workers: 3 });
  const st = await s.streamAsync({ format: 'csv', columns: { ts: 'number', v: 'number' }, keepMs: 100, timeColumn: 'ts' });
  let worst = 0;
  for (let k = 0; k < 20; k++) {
    await st.push(Array.from({ length: 50 }, (_, i) => `${k * 50 + i},1`).join('\n') + '\n');
    const n = (await st.aggAsync({ n: 'count()' })).n;
    worst = Math.max(worst, n); // (the window over what was pushed: ts within 100 of the newest)
  }
  check(worst <= 101, `keepMs before a flush never keeps more than the window: up to ${worst} rows`);
  await s.close();
}

rmSync(dir, { recursive: true, force: true });
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} server and engine-life checks passed`);
process.exit(failures ? 1 : 0);
