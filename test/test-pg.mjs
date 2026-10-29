// The PostgreSQL protocol server, spoken to by a small client written here
// (no driver needed): logins (SCRAM, md5, password, TLS), simple and extended
// queries with text and binary values, parameters, portals fetched in parts,
// cursors, transactions, errors, and the catalog queries drivers send. Every
// answer must equal what sql(...).toRows() gives. With psql installed, it
// answers too.
import { connect } from 'node:net';
import { connect as tlsConnect } from 'node:tls';
import { createHash, createHmac, pbkdf2Sync, randomBytes } from 'node:crypto';
import { readFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { Engine, sql } from '../src/node.mjs';
import { servePg } from '../src/serve/pg.mjs';
import { toPgText } from '../src/serve/pgtypes.mjs';

let failures = 0, checks = 0;
const check = (ok, msg) => { checks++; if (!ok) { failures++; if (failures <= 20) console.log('FAIL', msg); } };
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);

// ---- a small client ------------------------------------------------------------------
class Client {
  static async open(port, { user = 'tester', password = null, database = 'shop', ssl = false } = {}) {
    const c = new Client();
    c.sock = connect(port, '127.0.0.1');
    await new Promise((r, j) => { c.sock.once('connect', r); c.sock.once('error', j); });
    c.buf = Buffer.alloc(0);
    c.waiters = [];
    const feed = (sock) => sock.on('data', (d) => { c.buf = Buffer.concat([c.buf, d]); c.pump(); });
    if (ssl) {
      c.sock.write(Buffer.from([0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f]));
      const answer = await new Promise((r) => c.sock.once('data', (d) => r(String.fromCharCode(d[0]))));
      c.sslAnswer = answer;
      if (answer === 'S') { c.sock = tlsConnect({ socket: c.sock, rejectUnauthorized: false }); await new Promise((r) => c.sock.once('secureConnect', r)); }
    }
    feed(c.sock);
    const params = Buffer.from(`user\0${user}\0database\0${database}\0application_name\0test-pg\0\0`);
    const head = Buffer.alloc(8); head.writeInt32BE(8 + params.length); head.writeInt32BE(196608, 4);
    c.sock.write(Buffer.concat([head, params]));
    for (;;) {
      const m = await c.next();
      if (m.type === 'E') { c.error = c.fields(m.body); return c; }
      if (m.type === 'R') {
        const code = m.body.readInt32BE(0);
        if (code === 0) continue;
        if (code === 3) c.send('p', Buffer.from(`${password}\0`));
        else if (code === 5) { const inner = createHash('md5').update(password + user).digest('hex'); c.send('p', Buffer.from(`md5${createHash('md5').update(Buffer.concat([Buffer.from(inner), m.body.subarray(4, 8)])).digest('hex')}\0`)); }
        else if (code === 10) { // SCRAM-SHA-256
          c.cnonce = randomBytes(18).toString('base64');
          c.first = `n=${user},r=${c.cnonce}`;
          const msg = Buffer.from(`n,,${c.first}`), len = Buffer.alloc(4); len.writeInt32BE(msg.length);
          c.send('p', Buffer.concat([Buffer.from('SCRAM-SHA-256\0'), len, msg]));
        } else if (code === 11) {
          const sf = m.body.subarray(4).toString(), r = /r=([^,]*)/.exec(sf)[1], salt = Buffer.from(/s=([^,]*)/.exec(sf)[1], 'base64'), it = Number(/i=(\d+)/.exec(sf)[1]);
          const salted = pbkdf2Sync(password ?? '', salt, it, 32, 'sha256');
          const ck = createHmac('sha256', salted).update('Client Key').digest(), sk = createHash('sha256').update(ck).digest();
          const without = `c=biws,r=${r}`, auth = `${c.first},${sf},${without}`;
          const sig = createHmac('sha256', sk).update(auth).digest();
          c.serverSig = createHmac('sha256', createHmac('sha256', salted).update('Server Key').digest()).update(auth).digest('base64');
          c.send('p', Buffer.from(`${without},p=${Buffer.from(ck.map((b, i) => b ^ sig[i])).toString('base64')}`));
        } else if (code === 12) c.serverVerified = m.body.subarray(4).toString() === `v=${c.serverSig}`;
      }
      if (m.type === 'S') { const [k, v] = m.body.toString().split('\0'); (c.status ??= {})[k] = v; }
      if (m.type === 'Z') return c;
    }
  }
  pump() {
    while (this.buf.length >= 5) {
      const len = this.buf.readInt32BE(1);
      if (this.buf.length < len + 1) return;
      const m = { type: String.fromCharCode(this.buf[0]), body: this.buf.subarray(5, len + 1) };
      this.buf = this.buf.subarray(len + 1);
      (this.queue ??= []).push(m);
      this.waiters.shift()?.();
    }
  }
  async next() { while (!this.queue?.length) await new Promise((r) => this.waiters.push(r)); return this.queue.shift(); }
  send(type, body = Buffer.alloc(0)) { const h = Buffer.alloc(5); h.write(type, 0, 'latin1'); h.writeInt32BE(body.length + 4, 1); this.sock.write(Buffer.concat([h, body])); }
  fields(body) { const o = {}; let i = 0; while (body[i]) { const k = String.fromCharCode(body[i]); const e = body.indexOf(0, i + 1); o[k] = body.subarray(i + 1, e).toString(); i = e + 1; } return o; }
  // messages until ReadyForQuery: [{ columns, rows, tag, error }] per statement
  async collect(binaryTypes = null) {
    const out = [];
    let cur = null;
    for (;;) {
      const m = await this.next();
      if (m.type === 'T') { cur = { columns: [], rows: [] }; out.push(cur); const n = m.body.readInt16BE(0); let at = 2; for (let k = 0; k < n; k++) { const e = m.body.indexOf(0, at); const name = m.body.subarray(at, e).toString(); at = e + 1; const oid = m.body.readInt32BE(at + 6), fmt = m.body.readInt16BE(at + 16); cur.columns.push({ name, oid, fmt }); at += 18; } }
      else if (m.type === 'D') {
        if (!cur) { cur = { columns: binaryTypes ?? [], rows: [] }; out.push(cur); }
        const n = m.body.readInt16BE(0); let at = 2; const row = [];
        for (let k = 0; k < n; k++) { const len = m.body.readInt32BE(at); at += 4; if (len < 0) { row.push(null); continue; } const b = m.body.subarray(at, at + len); at += len; row.push(cur.columns[k]?.fmt === 1 ? decodeBinary(cur.columns[k].oid, b) : b.toString()); }
        cur.rows.push(row);
      } else if (m.type === 'C') { (cur ??= {}).tag = m.body.toString().replace(/\0$/, ''); if (!out.includes(cur)) out.push(cur); cur = null; }
      else if (m.type === 's') { (cur ??= { rows: [] }).suspended = true; if (!out.includes(cur)) out.push(cur); cur = null; }
      else if (m.type === 'E') out.push({ error: this.fields(m.body) });
      else if (m.type === 't') out.push({ params: Array.from({ length: m.body.readInt16BE(0) }, (_, i) => m.body.readInt32BE(2 + 4 * i)) });
      else if (m.type === 'Z') { this.tx = String.fromCharCode(m.body[0]); return out; }
    }
  }
  query(text) { this.send('Q', Buffer.from(`${text}\0`)); return this.collect(); }
  // extended: Parse, Bind (text params), Describe portal, Execute (maxRows), Sync
  extended(text, params = [], { binary = false, maxRows = 0, oids = [], describeStatement = false } = {}) {
    const i16 = (v) => { const b = Buffer.alloc(2); b.writeInt16BE(v); return b; }, i32 = (v) => { const b = Buffer.alloc(4); b.writeInt32BE(v); return b; };
    this.send('P', Buffer.concat([Buffer.from(`\0${text}\0`), i16(oids.length), ...oids.map(i32)]));
    if (describeStatement) this.send('D', Buffer.from('S\0'));
    this.send('B', Buffer.concat([Buffer.from('\0\0'), i16(0), i16(params.length), ...params.flatMap((p) => (p === null ? [i32(-1)] : [i32(Buffer.byteLength(String(p))), Buffer.from(String(p))])), i16(1), i16(binary ? 1 : 0)]));
    this.send('D', Buffer.from('P\0'));
    this.send('E', Buffer.concat([Buffer.from('\0'), i32(maxRows)]));
    if (maxRows) this.send('E', Buffer.concat([Buffer.from('\0'), i32(0)]));
    this.send('S');
    return this.collect();
  }
  close() { this.send('X'); this.sock.end(); }
}
const EPOCH = 946_684_800_000;
function decodeBinary(oid, b) {
  switch (oid) {
    case 20: return String(b.readBigInt64BE());
    case 23: return String(b.readInt32BE());
    case 21: return String(b.readInt16BE());
    case 701: return String(b.readDoubleBE());
    case 16: return b[0] ? 't' : 'f';
    case 1114: return `ts:${EPOCH + Number(b.readBigInt64BE()) / 1000}`;
    case 1082: return `day:${EPOCH + b.readInt32BE() * 86_400_000}`;
    default: return b.toString();
  }
}

// ---- data -----------------------------------------------------------------------------
let seed = 5;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const N = 5000;
const engine = new Engine();
const orders = engine.table({
  cust: Array.from({ length: N }, () => Math.floor(rnd() * 300)),
  price: Array.from({ length: N }, () => (rnd() < 0.05 ? NaN : Math.round(rnd() * 100000) / 100)),
  region: Array.from({ length: N }, () => ['EU', 'US', 'APAC', "O'Hare", 'Zürich'][Math.floor(rnd() * 5)]),
  ts: Array.from({ length: N }, () => new Date(Date.UTC(2024, 0, 1) + Math.floor(rnd() * 400 * 86400) * 1000)),
  day: Array.from({ length: N }, () => new Date(Date.UTC(2024, 0, 1) + Math.floor(rnd() * 400) * 86_400_000)),
});
const customers = engine.table({ cust: Array.from({ length: 300 }, (_, i) => i), name: Array.from({ length: 300 }, (_, i) => `c${i}`), tier: Array.from({ length: 300 }, (_, i) => 1 + (i % 3)) });
const tables = { orders, customers };
const TYPE = { int: 'int8', float: 'float8', bool: 'bool', text: 'text', date: 'timestamp', day: 'date' };
// what the server should send for a query, as text
function expected(text) {
  const q = sql(text, tables, { types: true, foldCase: true });
  const rows = q.toRows();
  return { names: q.columns.map((c) => c.name), types: q.columns.map((c) => TYPE[c.type]), rows: rows.map((r) => q.columns.map((c, k) => { const v = Object.values(r)[k]; return toPgText(TYPE[c.type], v instanceof Date ? v.getTime() : v); })) };
}

// a throwaway certificate for TLS (with openssl; without it, TLS isn't tried)
const certDir = mkdtempSync(join(tmpdir(), 'vectro-pg-'));
const made = await new Promise((done) => {
  const p = spawn('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', join(certDir, 'k.pem'), '-out', join(certDir, 'c.pem'), '-days', '2', '-subj', '/CN=localhost']);
  p.on('error', () => done(false));
  p.on('close', (code) => done(code === 0));
});
const tls = made ? { cert: readFileSync(join(certDir, 'c.pem')), key: readFileSync(join(certDir, 'k.pem')) } : null;
rmSync(certDir, { recursive: true, force: true });
const server = await servePg({ tables, port: 0, user: 'tester', password: 'secret', database: 'shop', engine, tls });
const PORT = server.port;

// logins
{
  const c = await Client.open(PORT, { password: 'secret' });
  check(!c.error && c.serverVerified && c.status.server_version === '16.0' && c.status.client_encoding === 'UTF8', `SCRAM login: ${JSON.stringify(c.error ?? c.status)}`);
  c.close();
  const bad = await Client.open(PORT, { password: 'wrong' });
  check(bad.error?.C === '28P01', `wrong password refused: ${JSON.stringify(bad.error)}`);
  const other = await Client.open(PORT, { user: 'someone', password: 'secret' });
  check(other.error?.C === '28P01', 'another user name refused');
  if (tls) {
    const t = await Client.open(PORT, { password: 'secret', ssl: true });
    check(t.sslAnswer === 'S' && !t.error, 'TLS');
    const r = await t.query('SELECT COUNT(*) AS n FROM orders');
    check(same(r[0].rows, [[String(N)]]), 'a query over TLS');
    t.close();
  }
}
const c = await Client.open(PORT, { password: 'secret' });
// simple queries: the same answers as sql(), as PostgreSQL text
const QUERIES = [
  'SELECT * FROM orders ORDER BY ts, cust LIMIT 50',
  'SELECT region, COUNT(*) AS n, SUM(price) AS s, AVG(price) AS a, MIN(ts) AS first, MAX(day) AS last_day FROM orders GROUP BY region ORDER BY region',
  'SELECT c.tier, COUNT(*) AS n FROM "public"."orders" o JOIN customers c ON o.cust = c.cust WHERE o.price > 500 GROUP BY c.tier ORDER BY 1',
  "SELECT UPPER(region) AS r, price > 900 AS big, CAST(cust AS VARCHAR) || '!' AS k, DATE_TRUNC('month', ts) AS m FROM orders ORDER BY ts LIMIT 20",
  'SELECT region AS "sum:region:ok", COUNT(1) AS "cnt" FROM orders GROUP BY 1 ORDER BY 1',
];
for (const text of QUERIES) {
  const want = expected(text);
  const [got] = await c.query(text);
  check(got?.columns && same(got.columns.map((x) => x.name), want.names.map((nm) => nm.startsWith('__q_') ? got.columns[want.names.indexOf(nm)]?.name : nm)), `names: ${text}\n  ${JSON.stringify(got?.columns ?? got)}`);
  check(got?.columns && same(got.columns.map((x) => x.oid), want.types.map((t) => ({ int8: 20, float8: 701, bool: 16, text: 25, timestamp: 1114, date: 1082 })[t])), `types: ${text}\n  ${JSON.stringify(got?.columns?.map((x) => x.oid))} ${want.types}`);
  check(same(got?.rows, want.rows) && got.tag === `SELECT ${want.rows.length}`, `rows: ${text}\n  got  ${JSON.stringify(got?.rows?.slice(0, 2))} ${got?.tag}\n  want ${JSON.stringify(want.rows.slice(0, 2))}`);
}
check(c.query === Client.prototype.query && (await c.query('SELECT 1 AS a; SELECT 2 AS b')).length === 2, 'two statements in one query');
// extended: a parameter, binary answers, a portal in parts, Describe of a statement
{
  const text = 'SELECT cust, price, region, ts, day, price > $2 AS big FROM orders WHERE region = $1 ORDER BY ts, cust LIMIT 7';
  const want = sql("SELECT cust, price, region, ts, day, price > 100 AS big FROM orders WHERE region = 'EU' ORDER BY ts, cust LIMIT 7", tables).toRows();
  const out = await c.extended(text, ['EU', '100'], { binary: true });
  const r = out.find((x) => x.rows);
  const wantBin = want.map((o) => [String(o.cust), Number.isNaN(o.price) ? null : String(o.price), o.region, `ts:${o.ts.getTime()}`, `day:${o.day.getTime()}`, Number.isNaN(o.price) ? null : o.price > 100 ? 't' : 'f']);
  check(same(r?.rows, wantBin), `binary answers with parameters\n  got  ${JSON.stringify(r?.rows?.slice(0, 2))}\n  want ${JSON.stringify(wantBin.slice(0, 2))}`);
  const parts = await c.extended('SELECT cust FROM orders ORDER BY ts, cust LIMIT 10', [], { maxRows: 4 });
  check(parts[0]?.suspended && parts[0].rows.length === 4 && parts[1]?.rows?.length === 6 && parts[1].tag === 'SELECT 10', `a portal in two parts: ${JSON.stringify(parts.map((p) => [p.rows?.length, p.suspended, p.tag]))}`);
  const d = await c.extended('SELECT cust, region FROM orders WHERE price > $1 LIMIT 1', ['5'], { describeStatement: true, oids: [701] });
  check(same(d[0]?.params, [701]) && d.some((x) => x.columns && same(x.columns.map((k) => k.oid), [20, 25])), `Describe statement: ${JSON.stringify(d)}`);
}
// errors: the session goes on
{
  const [e] = await c.query('SELECT nope FROM orders');
  check(e.error?.C === '42703' && /nope/.test(e.error.M), `unknown column: ${JSON.stringify(e)}`);
  const [e2] = await c.query('SELEC 1');
  check(e2.error?.C === '42601', 'syntax error');
  const [e3] = await c.query('DELETE FROM orders');
  check(e3.error?.C === '25006', 'read only');
  const ext = await c.extended('SELECT missing FROM orders', []);
  check(ext.length === 1 && ext[0].error && c.tx === 'I', 'extended: one error, then Sync');
  const [ok] = await c.query('SELECT 7 AS x');
  check(same(ok.rows, [['7']]), 'answers after errors');
}
// transactions, settings, cursors
{
  await c.query('BEGIN');
  check(c.tx === 'T', 'BEGIN');
  await c.query('SELECT nope FROM orders');
  check(c.tx === 'E', 'an error inside a transaction');
  const [ignored] = await c.query('SELECT 1');
  check(ignored.error?.C === '25P02', 'commands ignored until ROLLBACK');
  await c.query('ROLLBACK');
  check(c.tx === 'I', 'ROLLBACK');
  const [show] = await c.query('SHOW server_version');
  check(same(show.rows, [['16.0']]), 'SHOW');
  await c.query("SET application_name = 'other'");
  const [app] = await c.query('SHOW application_name');
  check(same(app.rows, [['other']]), 'SET then SHOW');
  const [v] = await c.query('SELECT version(), current_user, current_database()');
  check(/PostgreSQL 16/.test(v.rows[0][0]) && v.rows[0][1] === 'tester' && v.rows[0][2] === 'shop' && same(v.columns.map((x) => x.name), ['version', 'current_user', 'current_database']), `session functions: ${JSON.stringify(v)}`);
  const cur = await c.query('BEGIN; DECLARE c1 CURSOR FOR SELECT cust FROM orders ORDER BY ts, cust LIMIT 5; FETCH 3 FROM c1; FETCH ALL IN c1; CLOSE c1; COMMIT');
  const fetched = cur.filter((x) => x.tag?.startsWith('FETCH'));
  check(fetched.length === 2 && fetched[0].rows.length === 3 && fetched[1].rows.length === 2 && fetched[1].tag === 'FETCH 2', `cursor: ${JSON.stringify(cur.map((x) => x.tag))}`);
  const [disc] = await c.query('DISCARD ALL');
  check(disc.tag === 'DISCARD ALL', 'DISCARD ALL');
}
// the catalog
{
  const [t] = await c.query("select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE from INFORMATION_SCHEMA.tables where TABLE_SCHEMA not in ('information_schema', 'pg_catalog') order by TABLE_SCHEMA, TABLE_NAME");
  check(same(t.rows, [['public', 'customers', 'BASE TABLE'], ['public', 'orders', 'BASE TABLE']]) && same(t.columns.map((x) => x.name), ['table_schema', 'table_name', 'table_type']), `Power BI's table list: ${JSON.stringify(t)}`);
  const [cols] = await c.query("SELECT column_name, udt_name AS data_type FROM information_schema.columns WHERE table_name = 'orders' ORDER BY ordinal_position");
  check(same(cols.rows, [['cust', 'int8'], ['price', 'float8'], ['region', 'text'], ['ts', 'timestamp'], ['day', 'date']]), `columns: ${JSON.stringify(cols.rows)}`);
  const [types] = await c.query('/*** Load all supported types ***/\nSELECT ns.nspname, a.typname, a.oid FROM pg_type AS a JOIN pg_proc ON pg_proc.oid = a.typreceive ORDER BY ord');
  check(types.rows.length > 50 && types.rows.some((r) => r[1] === 'int8' && r[2] === '20' && r[5] === 'b') && types.rows.some((r) => r[1] === '_text' && r[5] === 'a' && r[6] === '25'), `Npgsql's types: ${types.rows.length}`);
  const [jt] = await c.query("SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME, CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema' WHEN true THEN 'x' END AS TABLE_TYPE, d.description AS REMARKS FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid) WHERE c.relnamespace = n.oid AND n.nspname LIKE 'public' AND c.relname LIKE 'ord%' AND (false OR ( c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ))");
  check(same(jt.rows.map((r) => r.slice(1, 4)), [['public', 'orders', 'TABLE']]), `pgJDBC's getTables: ${JSON.stringify(jt.rows)}`);
  const [ms] = await c.query("SELECT nspname AS \"TABLE_SCHEM\", current_database() AS \"TABLE_CATALOG\" FROM pg_catalog.pg_namespace WHERE nspname <> 'pg_toast' AND (nspname !~ '^pg_temp_' OR nspname = (pg_catalog.current_schemas(true))[1]) ORDER BY \"TABLE_SCHEM\"");
  check(same(ms.rows, [['information_schema', 'shop'], ['pg_catalog', 'shop'], ['public', 'shop']]), `newer pgJDBC's getSchemas (Metabase): ${JSON.stringify(ms)}`);
  const [priv] = await c.query("select t.schemaname as schema, t.tablename as table, pg_catalog.has_table_privilege(current_user, '\"' || t.schemaname || '\".\"' || t.tablename || '\"', 'select') as select, pg_catalog.has_table_privilege(current_user, t.tablename, 'insert') as insert from pg_catalog.pg_tables t where t.schemaname !~ '^pg_' order by 2");
  check(same(priv.rows, [['public', 'customers', 't', 'f'], ['public', 'orders', 't', 'f']]), `privileges: read only: ${JSON.stringify(priv)}`);
  const [empty] = await c.query("SELECT conname AS name, pg_catalog.pg_get_constraintdef(r.oid, true) AS def FROM pg_catalog.pg_constraint r WHERE r.conrelid = 'orders'::regclass AND r.contype = ANY (ARRAY['f'])");
  check(empty.rows?.length === 0 && same(empty.columns.map((x) => x.name), ['name', 'def']), `a catalog query beyond Vectro's SQL: empty: ${JSON.stringify(empty)}`);
  const [dt] = await c.query("SELECT n.nspname as \"Schema\", c.relname as \"Name\", CASE c.relkind WHEN 'r' THEN 'table' END as \"Type\", pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\" FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','p','') AND n.nspname <> 'pg_catalog' AND n.nspname !~ '^pg_toast' AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 1,2");
  check(same(dt.rows, [['public', 'customers', 'table', 'tester'], ['public', 'orders', 'table', 'tester']]), `psql's \\dt: ${JSON.stringify(dt.rows)}`);
}
c.close();

// other logins: md5, plain password, none (this machine only)
{
  const s2 = await servePg({ tables, port: 0, password: 'pw', auth: 'md5', engine });
  const a = await Client.open(s2.port, { password: 'pw' });
  check(!a.error, `md5 login: ${JSON.stringify(a.error)}`);
  a.close();
  await s2.close();
  const s3 = await servePg({ tables, port: 0, password: 'pw', auth: 'password', engine });
  const b = await Client.open(s3.port, { password: 'pw' });
  check(!b.error, 'password login');
  const nossl = await Client.open(s3.port, { password: 'pw', ssl: true });
  check(nossl.sslAnswer === 'N' && !nossl.error, 'no certificate: SSL refused, plain connection');
  b.close(); nossl.close();
  await s3.close();
  const s4 = await servePg({ tables, port: 0, engine });
  const d = await Client.open(s4.port, {});
  check(!d.error, 'no password on this machine');
  d.close();
  let refused = '';
  try { await servePg({ tables, port: 0, host: '0.0.0.0', engine }); } catch (err) { refused = err.message; }
  check(/password is needed/.test(refused), 'no password for other machines: refused');
  // psql, if this machine has it
  // (spawned, not spawnSync: the server runs in this process)
  const psql = await new Promise((done) => {
    const p = spawn('psql', ['-h', '127.0.0.1', '-p', String(s4.port), '-U', 'x', '-d', 'vectro', '-At', '-c', 'SELECT region, COUNT(*) FROM orders GROUP BY 1 ORDER BY 1']);
    let stdout = '', stderr = '';
    p.stdout.on('data', (d) => { stdout += d; });
    p.stderr.on('data', (d) => { stderr += d; });
    p.on('error', (error) => done({ error }));
    p.on('close', () => done({ stdout, stderr }));
  });
  if (!psql.error) {
    const want = sql('SELECT region, COUNT(*) AS n FROM orders GROUP BY 1 ORDER BY 1', tables).toRows().map((r) => `${r.region}|${r.n}`).join('\n');
    check(psql.stdout.trim() === want, `psql: ${psql.stdout} ${psql.stderr}`);
  }
  await s4.close();
}
await server.close();
await engine.close();
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} PostgreSQL protocol checks passed`);
process.exit(failures ? 1 : 0);
