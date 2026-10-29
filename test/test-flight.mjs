// Arrow Flight SQL, spoken to by a small gRPC client written here (node:http2):
// statements and prepared statements give the rows sql(...).toRows() gives,
// as Arrow with the right types; the catalog commands, sign-in with a bearer
// token, read-only refusals and errors (as gRPC status codes).
import { connect } from 'node:http2';
import { Engine, sql } from '../src/node.mjs';
import { serveFlight, pbEncode, pbDecode } from '../src/serve/flight.mjs';

let failures = 0, checks = 0;
const check = (ok, msg) => { checks++; if (!ok) { failures++; if (failures <= 15) console.log('FAIL', msg); } };
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);

// ---- a small Flight client --------------------------------------------------------------
const SQL = 'type.googleapis.com/arrow.flight.protocol.sql.';
const any = (type, value) => pbEncode([[1, 'b', SQL + type], [2, 'b', value]]);
const frame = (msg) => { const h = Buffer.alloc(5); h.writeUInt32BE(msg.length, 1); return Buffer.concat([h, msg]); };
function call(session, method, msg, headers = {}) {
  return new Promise((done) => {
    const s = session.request({ ':method': 'POST', ':path': `/arrow.flight.protocol.FlightService/${method}`, 'content-type': 'application/grpc', te: 'trailers', ...headers });
    const chunks = [];
    let head = {};
    s.on('response', (h) => { head = h; });
    s.on('data', (c) => chunks.push(c));
    s.on('trailers', (t) => { head = { ...head, ...t }; });
    s.on('end', () => {
      const buf = Buffer.concat(chunks), msgs = [];
      for (let i = 0; i + 5 <= buf.length;) { const n = buf.readUInt32BE(i + 1); msgs.push(buf.subarray(i + 5, i + 5 + n)); i += 5 + n; }
      done({ status: Number(head['grpc-status'] ?? -1), message: decodeURIComponent(head['grpc-message'] ?? ''), headers: head, msgs });
    });
    s.end(frame(msg));
  });
}
const cmd = (type, value = Buffer.alloc(0)) => pbEncode([[1, 'v', 2], [2, 'b', any(type, value)]]); // FlightDescriptor { type: CMD, cmd }
const EOS = Buffer.from([255, 255, 255, 255, 0, 0, 0, 0]);
// FlightData messages -> Arrow IPC stream bytes
function ipc(msgs) {
  const parts = [];
  for (const m of msgs) {
    const d = pbDecode(m), header = d.get(2)?.[0] ?? Buffer.alloc(0), body = d.get(1000)?.[0] ?? Buffer.alloc(0);
    const pad = (8 - (header.length % 8)) % 8, pre = Buffer.alloc(8);
    pre.writeUInt32LE(0xffffffff, 0); pre.writeInt32LE(header.length + pad, 4);
    parts.push(pre, header, Buffer.alloc(pad), body);
  }
  return new Uint8Array(Buffer.concat([...parts, EOS]));
}
// GetFlightInfo, then DoGet on its ticket: { info, rows (as a table), status }
async function query(session, descriptor, engine, headers = {}, columns = undefined) {
  const info = await call(session, 'GetFlightInfo', descriptor, headers);
  if (info.status !== 0) return { status: info.status, message: info.message };
  const fi = pbDecode(info.msgs[0]);
  const ticket = pbDecode(pbDecode(fi.get(3)[0]).get(1)[0]).get(1)[0];
  const got = await call(session, 'DoGet', pbEncode([[1, 'b', ticket]]), headers);
  if (got.status !== 0) return { status: got.status, message: got.message };
  const t = engine.fromArrow(ipc(got.msgs), columns ? { columns } : {});
  const rows = t.toRows();
  t.dispose();
  return { status: 0, rows, schemaBytes: fi.get(1)[0], batches: got.msgs.length - 1 };
}

// ---- data and the server ------------------------------------------------------------------
let seed = 9;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const N = 70_000; // (two record batches)
const engine = new Engine();
const orders = engine.table({
  id: Array.from({ length: N }, (_, i) => i + 1),
  region: Array.from({ length: N }, () => ['Europe', 'Asia', '', 'Zürich'][Math.floor(rnd() * 4)]),
  price: Array.from({ length: N }, () => (rnd() < 0.05 ? NaN : Math.round(rnd() * 10000) / 100)),
  ts: Array.from({ length: N }, () => new Date(Date.UTC(2024, 0, 1) + Math.floor(rnd() * 500 * 86400) * 1000)),
});
const tables = { orders };
const server = await serveFlight({ tables, port: 0, engine, user: 'me', password: 'pw' });
const session = connect(`http://127.0.0.1:${server.port}`);
// rows as the Arrow reader gives them back (text '' and missing alike)
const norm = (rows) => rows.map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, v instanceof Date ? v.getTime() : typeof v === 'number' && v !== v ? null : v === '' ? null : v])));

// sign in: Handshake with basic credentials gives a bearer token
const unauth = await call(session, 'GetFlightInfo', cmd('CommandGetCatalogs'));
check(unauth.status === 16, `no sign-in: UNAUTHENTICATED, got ${unauth.status}`);
const bad = await call(session, 'Handshake', pbEncode([[1, 'v', 0]]), { authorization: `Basic ${Buffer.from('me:nope').toString('base64')}` });
check(bad.status === 16, 'wrong password refused');
const hs = await call(session, 'Handshake', pbEncode([[1, 'v', 0]]), { authorization: `Basic ${Buffer.from('me:pw').toString('base64')}` });
const bearer = hs.headers.authorization;
check(hs.status === 0 && /^Bearer /.test(bearer ?? ''), `Handshake: ${hs.status} ${bearer}`);
const H = { authorization: bearer };

// statements: the same rows as SQL, typed
const QUERIES = [
  'SELECT * FROM orders ORDER BY id',
  "SELECT region, COUNT(*) AS n, SUM(price) AS s, MIN(ts) AS first FROM orders WHERE region <> '' GROUP BY region ORDER BY region",
  'SELECT id, price > 50 AS big, CAST(ts AS DATE) AS d FROM orders WHERE id <= 5 ORDER BY id',
];
for (const text of QUERIES) {
  const r = await query(session, cmd('CommandStatementQuery', pbEncode([[1, 'b', text]])), engine, H);
  const want = norm(sql(text, tables).toRows());
  check(r.status === 0 && same(norm(r.rows), want), `${text}\n  got ${r.status} ${r.message ?? ''} ${JSON.stringify(norm(r.rows ?? []).slice(0, 2))}\n  want ${JSON.stringify(want.slice(0, 2))}`);
}
{
  const r = await query(session, cmd('CommandStatementQuery', pbEncode([[1, 'b', 'SELECT * FROM orders']])), engine, H);
  check(r.batches === 2, `70,000 rows in two record batches: ${r.batches}`);
  const schema = Buffer.from(r.schemaBytes).toString('latin1');
  check(schema.includes('region') && schema.includes('UTC'), 'FlightInfo carries the schema');
}
// prepared statements: DoAction CreatePreparedStatement, then the handle as a command
{
  const text = 'SELECT region, COUNT(*) AS n FROM orders GROUP BY 1 ORDER BY 1';
  const act = await call(session, 'DoAction', pbEncode([[1, 'b', 'CreatePreparedStatement'], [2, 'b', any('ActionCreatePreparedStatementRequest', pbEncode([[1, 'b', text]]))]]), H);
  const result = pbDecode(pbDecode(pbDecode(act.msgs[0]).get(1)[0]).get(2)[0]);
  const handle = result.get(1)[0];
  check(act.status === 0 && result.get(2)?.[0]?.length > 0, 'CreatePreparedStatement: a handle and the schema');
  const r = await query(session, cmd('CommandPreparedStatementQuery', pbEncode([[1, 'b', handle]])), engine, H);
  check(same(norm(r.rows), norm(sql(text, tables).toRows())), 'a prepared statement runs');
  const close = await call(session, 'DoAction', pbEncode([[1, 'b', 'ClosePreparedStatement'], [2, 'b', any('ActionClosePreparedStatementRequest', pbEncode([[1, 'b', handle]]))]]), H);
  check(close.status === 0, 'ClosePreparedStatement');
}
// the catalog
{
  const cats = await query(session, cmd('CommandGetCatalogs'), engine, H);
  check(same(cats.rows, [{ catalog_name: 'vectro' }]), `catalogs: ${JSON.stringify(cats)}`);
  const schemas = await query(session, cmd('CommandGetDbSchemas'), engine, H);
  check(same(schemas.rows, [{ catalog_name: 'vectro', db_schema_name: 'public' }]), `schemas: ${JSON.stringify(schemas.rows)}`);
  const tbls = await query(session, cmd('CommandGetTables', pbEncode([[3, 'b', 'ord%'], [5, 'v', 1]])), engine, H, ['catalog_name', 'db_schema_name', 'table_name', 'table_type']);
  check(same(tbls.rows, [{ catalog_name: 'vectro', db_schema_name: 'public', table_name: 'orders', table_type: 'TABLE' }]), `tables: ${JSON.stringify(tbls)}`);
  const none = await query(session, cmd('CommandGetTables', pbEncode([[3, 'b', 'zzz%']])), engine, H);
  check(none.status === 0 && none.rows.length === 0, 'no matching tables');
  const types = await query(session, cmd('CommandGetTableTypes'), engine, H);
  check(same(types.rows, [{ table_type: 'TABLE' }]), 'table types');
  const pk = await query(session, cmd('CommandGetPrimaryKeys', pbEncode([[3, 'b', 'orders']])), engine, H);
  check(pk.status === 0 && pk.rows.length === 0, 'no primary keys');
}
// errors and refusals
{
  const e1 = await query(session, cmd('CommandStatementQuery', pbEncode([[1, 'b', 'SELECT nope FROM orders']])), engine, H);
  check(e1.status === 3 && /no column "nope"/.test(e1.message), `bad SQL: INVALID_ARGUMENT: ${e1.status} ${e1.message}`);
  const e2 = await call(session, 'GetFlightInfo', cmd('CommandStatementUpdate', pbEncode([[1, 'b', 'DELETE FROM orders']])), H);
  check(e2.status === 3 && /read only/.test(e2.message), 'updates refused');
  const e3 = await call(session, 'DoPut', Buffer.alloc(0), H);
  check(e3.status === 12, 'DoPut: UNIMPLEMENTED');
  const actions = await call(session, 'ListActions', Buffer.alloc(0), H);
  check(actions.msgs.length === 2, 'ListActions');
}
session.close();
await server.close();
let refused = '';
try { await serveFlight({ tables, port: 0, host: '0.0.0.0', engine }); } catch (err) { refused = err.message; }
check(/password is needed/.test(refused), 'no password for other machines: refused');
await engine.close();
console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} Flight SQL checks passed`);
process.exit(failures ? 1 : 0);
