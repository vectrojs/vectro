// Arrow Flight SQL over Vectro tables: pandas, Polars, DuckDB and anything
// with an ADBC or Flight SQL driver ask in SQL and get Arrow back, column by
// column, with no conversion to rows on either side.
//
//   import { serveFlight } from 'vectrojs/serve';
//   await serveFlight({ tables: { orders }, port: 31337 });     // grpc://localhost:31337
//
//   # Python: pip install adbc-driver-flightsql
//   import adbc_driver_flightsql.dbapi as flight
//   with flight.connect('grpc://localhost:31337') as c: c.cursor().execute('SELECT ...').fetch_arrow_table()
//
// gRPC over HTTP/2 (node:http2; TLS with a certificate) and protobuf are
// written here; no dependencies. Served: statements and prepared statements
// (the SQL travels in the ticket), catalogs, schemas, tables (with their
// Arrow schemas), table types; keys are empty. Read only. With a password,
// clients sign in with Handshake (basic) and get a bearer token.
import { createServer, createSecureServer, constants } from 'node:http2';
import { randomBytes, timingSafeEqual } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { once } from 'node:events';
import { gunzipSync } from 'node:zlib';
import { sql, columnType, displayName } from '../sql.mjs';
import { typedSchema, typedBatch, typedDictionary } from '../arrow.mjs';
import { openTables } from './catalog.mjs';

const BATCH = 65_536;
const LOOPBACK = new Set(['127.0.0.1', '::1', 'localhost']);
const SQL_PREFIX = 'type.googleapis.com/arrow.flight.protocol.sql.';
const STATUS = { ok: 0, invalid: 3, notFound: 5, unimplemented: 12, internal: 13, unauthenticated: 16 };
class FlightError extends Error { constructor(code, message) { super(message); this.code = code; } }
const fail = (code, m) => { throw new FlightError(code, m); };

// ---- protobuf ----------------------------------------------------------------------------
// decode: Map field number -> [values] (varints as numbers / BigInt-safe Numbers,
// length-delimited as Buffers); encode: [[field, kind, value]], kind 'v' (varint),
// 'b' (bytes / string / encoded message), 'q' (fixed64)
export function pbDecode(buf) {
  const out = new Map();
  let i = 0;
  const varint = () => { let x = 0n, s = 0n; for (;;) { const b = buf[i++]; x |= BigInt(b & 0x7f) << s; if (!(b & 0x80)) return x; s += 7n; } };
  while (i < buf.length) {
    const key = Number(varint()), field = key >>> 3, wire = key & 7;
    let v;
    if (wire === 0) v = Number(BigInt.asIntN(64, varint()));
    else if (wire === 2) { const n = Number(varint()); v = buf.subarray(i, i + n); i += n; }
    else if (wire === 1) { v = buf.readBigInt64LE(i); i += 8; }
    else if (wire === 5) { v = buf.readInt32LE(i); i += 4; }
    else throw new FlightError(STATUS.invalid, `bad protobuf wire type ${wire}`);
    if (!out.has(field)) out.set(field, []);
    out.get(field).push(v);
  }
  return out;
}
const pbVarint = (n) => { const out = []; let x = BigInt.asUintN(64, BigInt(n)); do { let b = Number(x & 0x7fn); x >>= 7n; if (x) b |= 0x80; out.push(b); } while (x); return Buffer.from(out); };
export function pbEncode(fields) {
  const parts = [];
  for (const [field, kind, value] of fields) {
    if (value === undefined || value === null) continue;
    if (kind === 'v') { parts.push(pbVarint(field << 3), pbVarint(value)); continue; }
    if (kind === 'q') { const b = Buffer.alloc(8); b.writeBigInt64LE(BigInt(value)); parts.push(pbVarint((field << 3) | 1), b); continue; }
    const b = Buffer.isBuffer(value) ? value : value instanceof Uint8Array ? Buffer.from(value.buffer, value.byteOffset, value.byteLength) : Buffer.from(String(value), 'utf8');
    parts.push(pbVarint((field << 3) | 2), pbVarint(b.length), b);
  }
  return Buffer.concat(parts);
}
const str = (m, f) => (m.get(f)?.[0] ? Buffer.from(m.get(f)[0]).toString('utf8') : null);
const bytesOf = (m, f) => m.get(f)?.[0] ?? null;
const any = (type, value) => pbEncode([[1, 'b', SQL_PREFIX + type], [2, 'b', value]]);
function unpackAny(buf) {
  const m = pbDecode(buf);
  const url = str(m, 1) ?? '';
  return { type: url.startsWith(SQL_PREFIX) ? url.slice(SQL_PREFIX.length) : url, value: pbDecode(bytesOf(m, 2) ?? Buffer.alloc(0)), raw: bytesOf(m, 2) ?? Buffer.alloc(0) };
}

// ---- results: Arrow fields and columns --------------------------------------------------------
const ARROW = { int: 'int64', float: 'float64', bool: 'bool', text: 'utf8', date: 'timestamp', day: 'date32' };
const TABLE_TYPES = ['TABLE'];
// a Flight data message: header (flatbuffer Message) and body, from an IPC message
const flightData = (m) => pbEncode([[2, 'b', m.bytes.subarray(8, m.metaLength)], [1000, 'b', m.bytes.subarray(m.metaLength)]]);

export async function serveFlight({ tables = {}, root = null, engine = null, catalog = null, port = 31337, host = '127.0.0.1', user = null, password = null, tls = null, database = 'vectro', log = () => {}, logQueries = false } = {}) {
  if (!password && !LOOPBACK.has(host)) throw new Error('vectro: a password is needed to accept Flight connections from other machines (--password, or --host 127.0.0.1)');
  const shared = !!catalog;
  catalog ??= await openTables({ tables, root, engine, log });
  const tokens = new Set();
  const named = () => Object.fromEntries([...catalog.entries].map(([n, e]) => [n, e.table]));

  const checkBasic = (header) => {
    const m = /^Basic\s+(.+)$/i.exec(header ?? '');
    if (!m) return false;
    const [u, ...rest] = Buffer.from(m[1], 'base64').toString().split(':');
    const given = Buffer.from(rest.join(':')), want = Buffer.from(String(password));
    return (!user || u === user) && given.length === want.length && timingSafeEqual(given, want);
  };
  const authorized = (headers) => {
    if (!password) return true;
    const h = headers.authorization ?? '';
    const bearer = /^Bearer\s+(.+)$/i.exec(h);
    return (bearer && tokens.has(bearer[1])) || checkBasic(h);
  };

  // a query's Arrow fields (from its column types, before it runs)
  const compile = (text) => {
    let q;
    try { q = sql(text, named(), { types: true, foldCase: true, database }); } catch (err) { fail(STATUS.invalid, err.message.replace(/^vectro( SQL)?:\s*/, '')); }
    return { q, fields: q.columns.map((c) => ({ name: displayName(c.name), col: c.name, type: ARROW[c.type] ?? 'float64' })) };
  };
  const tableFields = (t) => t.columnNames.map((c) => ({ name: displayName(c), type: ARROW[columnType(t, c)] ?? 'float64' }));

  // catalog answers: { fields, columns (arrays), n }
  const like = (pattern) => (pattern == null ? () => true : ((re) => (s) => re.test(s))(new RegExp(`^${pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/%/g, '.*').replace(/_/g, '.')}$`, 's')));
  function catalogAnswer(type, m) {
    const fieldsOf = (spec) => spec.map(([name, t, nullable = true]) => ({ name, type: t, nullable }));
    switch (type) {
      case 'CommandGetCatalogs': return { fields: fieldsOf([['catalog_name', 'utf8', false]]), columns: [[database]], n: 1 };
      case 'CommandGetDbSchemas': {
        const ok = (str(m, 1) ?? database) === database && like(str(m, 2))('public');
        return { fields: fieldsOf([['catalog_name', 'utf8'], ['db_schema_name', 'utf8', false]]), columns: ok ? [[database], ['public']] : [[], []], n: ok ? 1 : 0 };
      }
      case 'CommandGetTableTypes': return { fields: fieldsOf([['table_type', 'utf8', false]]), columns: [TABLE_TYPES], n: TABLE_TYPES.length };
      case 'CommandGetTables': {
        const wantTypes = (m.get(4) ?? []).map((b) => Buffer.from(b).toString());
        const inCatalog = (str(m, 1) ?? database) === database && like(str(m, 2))('public') && (!wantTypes.length || wantTypes.includes('TABLE'));
        const names = inCatalog ? Object.keys(named()).filter((n) => !n.includes('.')).filter(like(str(m, 3))).sort() : [];
        const withSchema = !!m.get(5)?.[0];
        const spec = [['catalog_name', 'utf8'], ['db_schema_name', 'utf8'], ['table_name', 'utf8', false], ['table_type', 'utf8', false]];
        if (withSchema) spec.push(['table_schema', 'binary', false]);
        const columns = [names.map(() => database), names.map(() => 'public'), names, names.map(() => 'TABLE')];
        if (withSchema) columns.push(names.map((n) => typedSchema(tableFields(named()[n])).bytes));
        return { fields: fieldsOf(spec), columns, n: names.length };
      }
      case 'CommandGetPrimaryKeys': return { fields: fieldsOf([['catalog_name', 'utf8'], ['db_schema_name', 'utf8'], ['table_name', 'utf8', false], ['column_name', 'utf8', false], ['key_name', 'utf8'], ['key_sequence', 'int32', false]]), columns: [[], [], [], [], [], []], n: 0 };
      case 'CommandGetImportedKeys': case 'CommandGetExportedKeys': case 'CommandGetCrossReference':
        return { fields: fieldsOf([['pk_catalog_name', 'utf8'], ['pk_db_schema_name', 'utf8'], ['pk_table_name', 'utf8', false], ['pk_column_name', 'utf8', false], ['fk_catalog_name', 'utf8'], ['fk_db_schema_name', 'utf8'], ['fk_table_name', 'utf8', false], ['fk_column_name', 'utf8', false], ['key_sequence', 'int32', false], ['fk_key_name', 'utf8'], ['pk_key_name', 'utf8'], ['update_rule', 'int8', false], ['delete_rule', 'int8', false]]), columns: Array.from({ length: 13 }, () => []), n: 0 };
      default: return null;
    }
  }
  const CATALOG = new Set(['CommandGetCatalogs', 'CommandGetDbSchemas', 'CommandGetTableTypes', 'CommandGetTables', 'CommandGetPrimaryKeys', 'CommandGetImportedKeys', 'CommandGetExportedKeys', 'CommandGetCrossReference']);

  // FlightDescriptor -> { fields, ticket }
  function describe(desc) {
    const d = pbDecode(desc);
    if (d.get(1)?.[0] !== 2) fail(STATUS.invalid, 'only command descriptors (Flight SQL) are served');
    const cmd = unpackAny(bytesOf(d, 2) ?? Buffer.alloc(0));
    if (cmd.type === 'CommandStatementQuery' || cmd.type === 'CommandPreparedStatementQuery') {
      const text = cmd.type === 'CommandStatementQuery' ? str(cmd.value, 1) : Buffer.from(bytesOf(cmd.value, 1) ?? []).toString();
      if (!text) fail(STATUS.invalid, 'no SQL given');
      if (logQueries) log(`[flight] ${text.replace(/\s+/g, ' ').slice(0, 2000)}`);
      const { fields } = compile(text);
      return { fields, ticket: any('TicketStatementQuery', pbEncode([[1, 'b', text]])), desc };
    }
    if (CATALOG.has(cmd.type)) return { fields: catalogAnswer(cmd.type, cmd.value).fields, ticket: bytesOf(d, 2), desc };
    if (cmd.type === 'CommandStatementUpdate' || cmd.type === 'CommandPreparedStatementUpdate') fail(STATUS.invalid, 'Vectro serves its tables read only');
    fail(STATUS.unimplemented, `${cmd.type || 'this command'} is not supported`);
  }
  const flightInfo = ({ fields, ticket, desc }) => pbEncode([
    [1, 'b', typedSchema(fields).bytes], [2, 'b', desc],
    [3, 'b', pbEncode([[1, 'b', pbEncode([[1, 'b', ticket]])]])],
    [4, 'v', -1], [5, 'v', -1],
  ]);

  // DoGet: the rows as FlightData messages
  async function* rowsOf(ticket) {
    const t = unpackAny(ticket);
    let fields, columns, n, table = null;
    if (t.type === 'TicketStatementQuery') {
      const text = Buffer.from(bytesOf(t.value, 1) ?? []).toString();
      const done = catalog.hold(); // (a reload meanwhile frees the tables this query reads only after it)
      try {
        const c = compile(text);
        fields = c.fields;
        table = await c.q.collectAsync();
        const data = await table.toColumnsAsync();
        columns = fields.map((f) => data[f.col]);
        n = table.length;
        table.dispose();
      } finally { done(); }
    } else if (CATALOG.has(t.type)) ({ fields, columns, n } = catalogAnswer(t.type, t.value));
    else fail(STATUS.invalid, 'unknown ticket');
    yield flightData(typedSchema(fields));
    for (let a = 0; a < n || a === 0; a += BATCH) {
      yield flightData(typedBatch(fields, columns, a, Math.min(n, a + BATCH)));
      if (n === 0) break;
    }
  }

  // ---- gRPC ----
  const frame = (msg) => { const h = Buffer.alloc(5); h.writeUInt32BE(msg.length, 1); return Buffer.concat([h, msg]); };
  function messagesOf(buf) {
    const out = [];
    for (let i = 0; i + 5 <= buf.length;) {
      const compressed = buf[i], len = buf.readUInt32BE(i + 1);
      const body = buf.subarray(i + 5, i + 5 + len);
      out.push(compressed ? gunzipSync(body) : body);
      i += 5 + len;
    }
    return out;
  }
  const respondError = (stream, code, message) => {
    if (stream.destroyed) return;
    const trailers = { 'grpc-status': String(code), 'grpc-message': encodeURIComponent(message).slice(0, 4000) };
    if (stream.headersSent) { stream.sendTrailers?.(trailers); return; }
    stream.respond({ ':status': 200, 'content-type': 'application/grpc', ...trailers }, { endStream: true });
  };
  async function reply(stream, headers, gen) {
    stream.respond({ ':status': 200, 'content-type': 'application/grpc', ...headers }, { waitForTrailers: true });
    let status = 0, message = '';
    try {
      for await (const msg of gen) if (!stream.write(frame(msg))) await once(stream, 'drain');
    } catch (err) { status = err.code ?? STATUS.internal; message = err.message; if (!(err instanceof FlightError)) log(`[flight] error: ${err.stack}`); }
    stream.once('wantTrailers', () => stream.sendTrailers({ 'grpc-status': String(status), ...(message ? { 'grpc-message': encodeURIComponent(message).slice(0, 4000) } : {}) }));
    stream.end();
  }

  const ACTIONS = [['CreatePreparedStatement', 'a prepared statement: its handle and schema'], ['ClosePreparedStatement', 'forget a prepared statement']];
  async function handle(stream, headers) {
    const method = String(headers[':path'] ?? '').replace('/arrow.flight.protocol.FlightService/', '');
    const chunks = [];
    for await (const c of stream) chunks.push(c);
    const msgs = messagesOf(Buffer.concat(chunks));
    if (method === 'Handshake') { // basic sign-in: a bearer token in the answer's headers
      if (password && !checkBasic(headers.authorization)) return respondError(stream, STATUS.unauthenticated, 'wrong user name or password');
      const token = randomBytes(24).toString('base64url');
      tokens.add(token);
      if (tokens.size > 10_000) tokens.delete(tokens.values().next().value);
      return reply(stream, password ? { authorization: `Bearer ${token}` } : {}, (async function* () { yield pbEncode([[1, 'v', 0], [2, 'b', Buffer.alloc(0)]]); })());
    }
    if (!authorized(headers)) return respondError(stream, STATUS.unauthenticated, 'sign in first (Handshake with a user name and password)');
    const one = msgs[0] ?? Buffer.alloc(0);
    switch (method) {
      case 'GetFlightInfo': return reply(stream, {}, (async function* () { yield flightInfo(describe(one)); })());
      case 'GetSchema': return reply(stream, {}, (async function* () { yield pbEncode([[1, 'b', typedSchema(describe(one).fields).bytes]]); })());
      case 'DoGet': return reply(stream, {}, rowsOf(bytesOf(pbDecode(one), 1) ?? Buffer.alloc(0)));
      case 'ListActions': return reply(stream, {}, (async function* () { for (const [type, description] of ACTIONS) yield pbEncode([[1, 'b', type], [2, 'b', description]]); })());
      case 'DoAction': {
        const a = pbDecode(one), type = str(a, 1), body = unpackAny(bytesOf(a, 2) ?? Buffer.alloc(0));
        if (type === 'CreatePreparedStatement') {
          const text = str(body.value, 1) ?? '';
          const { fields } = compile(text);
          const result = pbEncode([[1, 'b', text], [2, 'b', typedSchema(fields).bytes]]);
          return reply(stream, {}, (async function* () { yield pbEncode([[1, 'b', any('ActionCreatePreparedStatementResult', result)]]); })());
        }
        if (type === 'ClosePreparedStatement') return reply(stream, {}, (async function* () {})());
        return respondError(stream, STATUS.unimplemented, `action ${type} is not supported (Vectro serves its tables read only)`);
      }
      case 'ListFlights': return reply(stream, {}, (async function* () {})());
      case 'DoPut': return respondError(stream, STATUS.unimplemented, 'Vectro serves its tables read only (no DoPut, no parameters for prepared statements yet)');
      default: return respondError(stream, STATUS.unimplemented, `${method} is not supported`);
    }
  }

  const server = tls ? createSecureServer({ cert: tls.cert ?? readFileSync(tls.certFile), key: tls.key ?? readFileSync(tls.keyFile) }) : createServer();
  server.on('stream', (stream, headers) => {
    stream.on('error', () => {});
    handle(stream, headers).catch((err) => { if (!(err instanceof FlightError)) log(`[flight] error: ${err.stack}`); respondError(stream, err.code ?? STATUS.internal, err.message); });
  });
  server.on('sessionError', () => {});
  server.listen(port, host);
  await once(server, 'listening');
  const actual = server.address().port;
  const sessions = new Set();
  server.on('session', (s) => { sessions.add(s); s.on('close', () => sessions.delete(s)); });
  return {
    port: actual, host, server, catalog,
    url: `${tls ? 'grpc+tls' : 'grpc'}://${host.includes(':') ? `[${host}]` : host}:${actual}`,
    async close() {
      for (const s of sessions) s.destroy();
      await new Promise((r) => server.close(r));
      if (!shared) await catalog.close();
    },
  };
}
export { constants as http2Constants };
