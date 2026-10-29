// The PostgreSQL wire protocol (version 3) over Vectro tables: Power BI,
// Tableau, Metabase, Grafana, DBeaver, psql and every Postgres driver can
// connect as if Vectro were a PostgreSQL 16 database, read only.
//
//   import { servePg } from 'vectrojs/serve';
//   await servePg({ tables: { orders }, port: 5433, user: 'me', password: 'secret' });
//
// Speaks: SSL / TLS (with a certificate given), SCRAM-SHA-256, md5 or plain
// passwords (none: anyone, which is only allowed on this machine); the simple
// and the extended query protocol (prepared statements, $1 parameters, text
// and binary values, portals fetched a part at a time); BEGIN / COMMIT, SET /
// SHOW, DECLARE / FETCH cursors; the catalog clients read (see pgcatalog.mjs).
import { createServer } from 'node:net';
import { TLSSocket, createSecureContext } from 'node:tls';
import { createHash, createHmac, pbkdf2Sync, randomBytes, timingSafeEqual } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { once } from 'node:events';
import { sql, displayName } from '../sql.mjs';
import { openTables } from './catalog.mjs';
import { OID, TYPE_OF, typeLength, typeName, BINARY, paramLiteral, bindParams, dataRows, PgError } from './pgtypes.mjs';
import { describeTables, catalogTables, driverAnswer, readsCatalog, selectNames } from './pgcatalog.mjs';

const VERSION = '16.0';
const LOOPBACK = new Set(['127.0.0.1', '::1', 'localhost']);
const BATCH = 16_384; // rows per write

// ---- messages ------------------------------------------------------------------------
const int32 = (v) => { const b = Buffer.alloc(4); b.writeInt32BE(v); return b; };
const int16 = (v) => { const b = Buffer.alloc(2); b.writeInt16BE(v); return b; };
const cstr = (s) => Buffer.from(`${s}\0`, 'utf8');
function message(type, ...parts) {
  const body = Buffer.concat(parts);
  const head = Buffer.alloc(5);
  head.write(type, 0, 'latin1');
  head.writeInt32BE(body.length + 4, 1);
  return Buffer.concat([head, body]);
}
const ready = (status) => message('Z', Buffer.from(status, 'latin1'));
const complete = (tag) => message('C', cstr(tag));
function errorMessage(err, type = 'E') {
  const code = err.code && /^[0-9A-Z]{5}$/.test(err.code) ? err.code : codeOf(err);
  const msg = String(err.message ?? err).replace(/^vectro( SQL)?:\s*/, '');
  return message(type, Buffer.from('S'), cstr(type === 'N' ? 'NOTICE' : 'ERROR'), Buffer.from('V'), cstr(type === 'N' ? 'NOTICE' : 'ERROR'), Buffer.from('C'), cstr(code), Buffer.from('M'), cstr(msg), Buffer.from([0]));
}
function codeOf(err) {
  const m = String(err.message ?? '');
  if (/no table/.test(m)) return '42P01';
  if (/no column/.test(m)) return '42703';
  if (/not supported|isn't supported|is not supported/.test(m)) return '0A000';
  if (/^vectro SQL:/.test(m) || err instanceof SyntaxError) return '42601';
  return 'XX000';
}
function rowDescription(columns, formats) {
  return message('T', int16(columns.length), ...columns.flatMap((c, k) => [
    cstr(c.name), int32(0), int16(0), int32(OID[c.type] ?? OID.text), int16(typeLength(c.type)), int32(-1), int16(formats[k] ?? 0),
  ]));
}

// ---- answers ---------------------------------------------------------------------------
// A statement's answer: { columns: [{ name, type }], n, cell(i, k), tag } or { tag } (no rows) or { empty }
function rowsOf(columns, rows, tag = 'SELECT') {
  return { columns, n: rows.length, cell: (i, k) => rows[i][k], tag, keepEmpty: true };
}
// a Vectro answer table as rows: its data copied out (toColumns), then read cell by cell
async function tableRows(columns, table) {
  const data = await table.toColumnsAsync();
  const readers = columns.map(({ name, col }) => {
    const d = data[col ?? name];
    if (d && d.labels) { const { codes, labels } = d; return (i) => { const s = labels[codes[i]]; return s === undefined ? null : s; }; }
    return (i) => { const v = d[i]; return typeof v === 'number' && v !== v ? null : v; };
  });
  return { columns, n: table.length, cell: (i, k) => readers[k](i), tag: 'SELECT' };
}

// ---- the server ------------------------------------------------------------------------
export async function servePg({ tables = {}, root = null, engine = null, catalog = null, port = 5433, host = '127.0.0.1', user = null, password = null, auth = 'scram', tls = null, database = 'vectro', log = () => {}, logQueries = false } = {}) {
  if (!password && !LOOPBACK.has(host)) throw new Error('vectro: a password is needed to accept connections from other machines (--password, or --host 127.0.0.1)');
  const shared = !!catalog;
  catalog ??= await openTables({ tables, root, engine, log });
  const secure = tls ? createSecureContext({ cert: tls.cert ?? readFileSync(tls.certFile), key: tls.key ?? readFileSync(tls.keyFile) }) : null;
  const sessions = new Map(); // pid -> session (for cancel requests)
  let nextPid = 1000;

  // catalog tables per database name and user, made again when the served
  // tables change (the old ones are kept: a query may still be reading them)
  const builtFor = new Map();
  let builtVersion = null, old = [];
  const catalogFor = (db, who) => {
    const version = `${catalog.version}|${[...catalog.entries.keys()].join(',')}`;
    if (version !== builtVersion) { for (const b of builtFor.values()) old.push(b); builtFor.clear(); builtVersion = version; }
    const key = `${db}|${who}`;
    let b = builtFor.get(key);
    if (!b) {
      const described = describeTables(catalog.named());
      b = { described, tables: catalogTables(catalog.engine, described, { database: db, user: who, settings: { ...SETTINGS } }) };
      builtFor.set(key, b);
    }
    return b;
  };
  const disposeCatalogs = () => { for (const b of [...builtFor.values(), ...old]) for (const t of Object.values(b.tables)) t.dispose(); builtFor.clear(); old = []; };

  const server = createServer((socket) => {
    const s = new Session(socket);
    socket.on('error', () => {});
  });

  class Session {
    constructor(socket) {
      this.socket = socket;
      this.buf = Buffer.alloc(0);
      this.started = false; // (after the startup message)
      this.authed = false;
      this.pid = nextPid++;
      this.secret = randomBytes(4).readInt32BE();
      this.params = {};
      this.settings = { ...SETTINGS };
      this.tx = 'I';
      this.statements = new Map();
      this.portals = new Map();
      this.cursors = new Map();
      this.skipToSync = false;
      this.queue = Promise.resolve();
      this.cancelled = false;
      this.listen(socket);
    }
    listen(socket) {
      this.onData = (chunk) => { this.buf = Buffer.concat([this.buf, chunk]); this.queue = this.queue.then(() => this.drain()).catch((err) => this.fatal(err)); };
      socket.on('data', this.onData);
      socket.on('close', () => { sessions.delete(this.pid); this.closed = true; });
    }
    write(b) { if (!this.closed) return this.socket.write(b); return true; }
    async writeAll(parts) { if (!this.write(Buffer.concat(parts)) && !this.closed) await once(this.socket, 'drain').catch(() => {}); }
    fatal(err) { log(`[pg] ${err.stack ?? err}`); try { this.write(errorMessage(err)); } catch {} this.socket.end(); }

    async drain() {
      for (;;) {
        if (!this.started) {
          if (this.buf.length < 8) return;
          const len = this.buf.readInt32BE(0);
          if (this.buf.length < len) return;
          const body = this.buf.subarray(4, len);
          this.buf = this.buf.subarray(len);
          await this.startup(body);
          continue;
        }
        if (this.buf.length < 5) return;
        const type = String.fromCharCode(this.buf[0]), len = this.buf.readInt32BE(1);
        if (this.buf.length < len + 1) return;
        const body = this.buf.subarray(5, len + 1);
        this.buf = this.buf.subarray(len + 1);
        await this.handle(type, body);
        if (this.closed) return;
      }
    }

    // ---- startup and authentication ----
    async startup(body) {
      const code = body.readInt32BE(0);
      if (code === 80877103) { // SSLRequest
        if (!secure) { this.write(Buffer.from('N')); return; }
        this.write(Buffer.from('S'));
        this.socket.removeListener('data', this.onData);
        const plain = this.socket;
        const t = new TLSSocket(plain, { isServer: true, secureContext: secure });
        t.on('error', () => {});
        this.socket = t;
        this.listen(t);
        return;
      }
      if (code === 80877104) { this.write(Buffer.from('N')); return; } // GSSENCRequest: no
      if (code === 80877102) { // CancelRequest: on its own connection
        const target = sessions.get(body.readInt32BE(4));
        if (target && target.secret === body.readInt32BE(8)) target.cancelled = true;
        this.socket.end();
        this.closed = true;
        return;
      }
      if (code >> 16 !== 3) { this.write(errorMessage(new PgError('0A000', `protocol ${code >> 16}.${code & 0xffff} is not supported (3.0 is)`))); this.socket.end(); this.closed = true; return; }
      const parts = body.subarray(4).toString('utf8').split('\0');
      for (let i = 0; i + 1 < parts.length; i += 2) if (parts[i]) this.params[parts[i]] = parts[i + 1];
      this.user = this.params.user ?? 'vectro';
      this.database = this.params.database || this.user;
      this.started = true;
      if (user && this.user !== user) return this.reject(`password authentication failed for user "${this.user}"`);
      if (!password) return this.welcome();
      if (auth === 'password') this.write(message('R', int32(3)));
      else if (auth === 'md5') { this.salt = randomBytes(4); this.write(message('R', int32(5), this.salt)); }
      else this.write(message('R', int32(10), cstr('SCRAM-SHA-256'), Buffer.from([0])));
      this.authState = auth;
    }
    reject(msg) { this.write(errorMessage(new PgError('28P01', msg))); this.socket.end(); this.closed = true; }
    password(body) {
      if (this.authState === 'password') {
        const given = body.toString('utf8').replace(/\0$/, '');
        return safeEqual(given, password) ? this.welcome() : this.reject(`password authentication failed for user "${this.user}"`);
      }
      if (this.authState === 'md5') {
        const given = body.toString('utf8').replace(/\0$/, '');
        const inner = createHash('md5').update(password + this.user).digest('hex');
        const want = `md5${createHash('md5').update(Buffer.concat([Buffer.from(inner), this.salt])).digest('hex')}`;
        return safeEqual(given, want) ? this.welcome() : this.reject(`password authentication failed for user "${this.user}"`);
      }
      // SCRAM-SHA-256 (RFC 5802 / 7677)
      if (!this.scram) {
        const mech = body.subarray(0, body.indexOf(0)).toString();
        if (mech !== 'SCRAM-SHA-256') return this.reject(`unsupported authentication mechanism ${mech}`);
        const at = body.indexOf(0) + 1, n = body.readInt32BE(at);
        const first = body.subarray(at + 4, at + 4 + n).toString('utf8');
        const m = /^([ny]),[^,]*,(.*)$/.exec(first);
        if (!m) return this.reject('channel binding is not supported');
        const bare = m[2], cnonce = /(?:^|,)r=([^,]*)/.exec(bare)?.[1];
        if (!cnonce) return this.reject('bad SCRAM message');
        const salt = randomBytes(16), iterations = 4096, nonce = cnonce + randomBytes(18).toString('base64');
        const serverFirst = `r=${nonce},s=${salt.toString('base64')},i=${iterations}`;
        this.scram = { bare, serverFirst, nonce, salted: pbkdf2Sync(password, salt, iterations, 32, 'sha256'), gs2: `${m[1]},,` };
        return this.write(message('R', int32(11), Buffer.from(serverFirst)));
      }
      const final = body.toString('utf8');
      const proof = /,p=([^,]*)$/.exec(final)?.[1], withoutProof = final.replace(/,p=[^,]*$/, '');
      if (!proof || /(?:^|,)r=([^,]*)/.exec(final)?.[1] !== this.scram.nonce) return this.reject('bad SCRAM message');
      const { salted, bare, serverFirst } = this.scram;
      const authMessage = `${bare},${serverFirst},${withoutProof}`;
      const clientKey = createHmac('sha256', salted).update('Client Key').digest();
      const storedKey = createHash('sha256').update(clientKey).digest();
      const signature = createHmac('sha256', storedKey).update(authMessage).digest();
      const given = Buffer.from(proof, 'base64');
      if (given.length !== 32) return this.reject(`password authentication failed for user "${this.user}"`);
      const recovered = Buffer.from(given.map((b, i) => b ^ signature[i]));
      if (!timingSafeEqual(createHash('sha256').update(recovered).digest(), storedKey)) return this.reject(`password authentication failed for user "${this.user}"`);
      const serverKey = createHmac('sha256', salted).update('Server Key').digest();
      this.write(message('R', int32(12), Buffer.from(`v=${createHmac('sha256', serverKey).update(authMessage).digest('base64')}`)));
      return this.welcome();
    }
    welcome() {
      this.authed = true;
      sessions.set(this.pid, this);
      const status = {
        server_version: VERSION, server_encoding: 'UTF8', client_encoding: 'UTF8', DateStyle: 'ISO, MDY', IntervalStyle: 'postgres',
        integer_datetimes: 'on', is_superuser: 'off', session_authorization: this.user, standard_conforming_strings: 'on', TimeZone: 'UTC',
        application_name: this.params.application_name ?? '', default_transaction_read_only: 'on', in_hot_standby: 'off',
      };
      this.settings.application_name = status.application_name;
      const out = [message('R', int32(0)), ...Object.entries(status).map(([k, v]) => message('S', cstr(k), cstr(v))), message('K', int32(this.pid), int32(this.secret)), ready('I')];
      this.write(Buffer.concat(out));
      log(`[pg] ${this.user}@${this.database} connected (${this.params.application_name || 'no application name'})`);
    }

    // ---- messages after startup ----
    async handle(type, body) {
      if (!this.authed) {
        if (type === 'p') return this.password(body);
        return this.reject('expected a password message');
      }
      if (type === 'X') { this.socket.end(); this.closed = true; return; }
      if (this.skipToSync && type !== 'S') return; // (after an error in the extended protocol)
      try {
        switch (type) {
          case 'Q': return await this.simpleQuery(body.toString('utf8').replace(/\0$/, ''));
          case 'P': return this.parse(body);
          case 'B': return this.bind(body);
          case 'D': return await this.describe(body);
          case 'E': return await this.execute(body);
          case 'C': { const kind = String.fromCharCode(body[0]), name = body.subarray(1, body.indexOf(0, 1)).toString(); (kind === 'S' ? this.statements : this.portals).delete(name); return this.write(message('3')); }
          case 'S': this.skipToSync = false; if (this.tx === 'I') this.portals.clear(); return this.write(ready(this.tx));
          case 'H': return undefined;
          case 'F': throw new PgError('0A000', 'function calls are not supported');
          case 'd': case 'c': case 'f': throw new PgError('0A000', 'COPY is not supported');
          default: throw new PgError('08P01', `unknown message type ${type}`);
        }
      } catch (err) {
        if (!(err instanceof PgError) && !/^vectro/.test(err.message)) log(`[pg] error: ${err.stack}`);
        this.write(errorMessage(err));
        if (this.tx === 'T') this.tx = 'E';
        if (type !== 'Q') this.skipToSync = true;
        else this.write(ready(this.tx));
      }
    }

    async simpleQuery(text) {
      const statements = splitStatements(text);
      if (!statements.length) { this.write(Buffer.concat([message('I'), ready(this.tx)])); return; }
      for (const st of statements) {
        if (this.tx === 'E' && !/^\s*(rollback|abort|commit|end)\b/i.test(st)) throw new PgError('25P02', 'current transaction is aborted, commands ignored until end of transaction block');
        const res = await this.run(st);
        if (res.empty) { this.write(message('I')); continue; }
        if (res.columns) {
          const formats = res.columns.map(() => (res.binary ? 1 : 0));
          this.write(rowDescription(res.columns, formats));
          await this.sendRows(res, 0, res.n, formats);
          this.write(complete(res.tag === 'SELECT' ? `SELECT ${res.n}` : res.tag === 'FETCH' ? `FETCH ${res.n}` : res.tag));
        } else this.write(complete(res.tag));
      }
      this.write(ready(this.tx));
    }

    parse(body) {
      let at = body.indexOf(0);
      const name = body.subarray(0, at).toString();
      const end = body.indexOf(0, at + 1);
      const text = body.subarray(at + 1, end).toString('utf8');
      at = end + 1;
      const n = body.readInt16BE(at);
      const oids = [];
      for (let i = 0; i < n; i++) oids.push(body.readInt32BE(at + 2 + 4 * i));
      if (splitStatements(text).length > 1) throw new PgError('42601', 'cannot insert multiple commands into a prepared statement');
      this.statements.set(name, { text, oids });
      this.write(message('1'));
    }
    bind(body) {
      let at = body.indexOf(0);
      const portal = body.subarray(0, at).toString();
      const end = body.indexOf(0, at + 1);
      const name = body.subarray(at + 1, end).toString();
      at = end + 1;
      const st = this.statements.get(name);
      if (!st) throw new PgError('26000', `prepared statement "${name}" does not exist`);
      const nf = body.readInt16BE(at); at += 2;
      const fmts = [];
      for (let i = 0; i < nf; i++) { fmts.push(body.readInt16BE(at)); at += 2; }
      const np = body.readInt16BE(at); at += 2;
      const literals = [];
      for (let i = 0; i < np; i++) {
        const len = body.readInt32BE(at); at += 4;
        const bytes = len < 0 ? null : body.subarray(at, at + len);
        if (len > 0) at += len;
        literals.push(paramLiteral(bytes, nf === 1 ? fmts[0] : fmts[i] ?? 0, st.oids[i] ?? 0));
      }
      const nr = body.readInt16BE(at); at += 2;
      const results = [];
      for (let i = 0; i < nr; i++) { results.push(body.readInt16BE(at)); at += 2; }
      this.portals.set(portal, { text: bindParams(st.text, literals), results, res: null, offset: 0 });
      this.write(message('2'));
    }
    formatsFor(portal, columns) {
      const r = portal.results;
      return columns.map((c, k) => { const f = r.length === 1 ? r[0] : r[k] ?? 0; return f === 1 && BINARY.has(c.type) ? 1 : 0; });
    }
    async describe(body) {
      const kind = String.fromCharCode(body[0]), name = body.subarray(1, body.indexOf(0, 1)).toString();
      if (kind === 'S') {
        const st = this.statements.get(name);
        if (!st) throw new PgError('26000', `prepared statement "${name}" does not exist`);
        const count = countParams(st.text);
        const oids = Array.from({ length: count }, (_, i) => (st.oids[i] || OID.text));
        this.write(message('t', int16(count), ...oids.map(int32)));
        const columns = await this.columnsOnly(bindParams(st.text, Array.from({ length: count }, () => 'NULL')));
        this.write(columns ? rowDescription(columns, columns.map(() => 0)) : message('n'));
        return;
      }
      const portal = this.portals.get(name);
      if (!portal) throw new PgError('34000', `portal "${name}" does not exist`);
      portal.res ??= await this.run(portal.text);
      const res = portal.res;
      this.write(res.columns ? rowDescription(res.columns, this.formatsFor(portal, res.columns)) : message('n'));
    }
    async execute(body) {
      const at = body.indexOf(0), name = body.subarray(0, at).toString(), max = body.readInt32BE(at + 1);
      const portal = this.portals.get(name);
      if (!portal) throw new PgError('34000', `portal "${name}" does not exist`);
      if (this.tx === 'E' && !/^\s*(rollback|abort|commit|end)\b/i.test(portal.text)) throw new PgError('25P02', 'current transaction is aborted, commands ignored until end of transaction block');
      portal.res ??= await this.run(portal.text);
      const res = portal.res;
      if (res.empty) return this.write(message('I'));
      if (!res.columns) { this.write(complete(res.tag)); return; }
      const end = max > 0 ? Math.min(res.n, portal.offset + max) : res.n;
      await this.sendRows(res, portal.offset, end, this.formatsFor(portal, res.columns));
      const sent = end - portal.offset;
      portal.offset = end;
      if (end < res.n) this.write(message('s')); // PortalSuspended: Execute again for more
      else this.write(complete(res.tag === 'SELECT' ? `SELECT ${portal.offset}` : res.tag === 'FETCH' ? `FETCH ${sent}` : res.tag));
    }
    async sendRows(res, from, to, formats) {
      for (let a = from; a < to; a += BATCH) {
        if (this.cancelled) { this.cancelled = false; throw new PgError('57014', 'canceling statement due to user request'); }
        await this.writeAll([dataRows(res.columns, formats, res.cell, a, Math.min(to, a + BATCH), !!res.keepEmpty)]);
        if (this.closed) return;
      }
    }

    // ---- statements ----
    sqlOpts() {
      return { types: true, foldCase: true, database: this.database, session: {
        version: `PostgreSQL ${VERSION} (Vectro) on JavaScript`, current_user: this.user, session_user: this.user, user: this.user, current_role: this.user,
        current_database: this.database, current_catalog: this.database, current_schema: 'public', pg_backend_pid: this.pid,
        pg_is_in_recovery: false, inet_server_port: port, txid_current: 1, pg_postmaster_start_time: STARTED,
      }, functions: this.functions ??= catalogFunctions(this) };
    }
    tablesFor() {
      const c = catalogFor(this.database, this.user);
      return { tables: { ...catalog.named(), ...c.tables }, described: c.described };
    }
    // a query's columns without running it (Describe of a prepared statement)
    async columnsOnly(text) {
      const t = stripComments(text);
      const word = firstWord(t);
      if (!['SELECT', 'WITH', 'VALUES', 'TABLE', '(', 'SHOW', 'FETCH', 'EXPLAIN'].includes(word)) return null;
      if (word !== 'SELECT' && word !== 'WITH' && word !== '(') { const r = await this.run(text); return r.columns ?? null; }
      const { tables, described } = this.tablesFor();
      const known = driverAnswer(text, described);
      if (known) return known.columns.map((c) => ({ name: c.name, type: c.type }));
      try {
        const q = sql(t, tables, this.sqlOpts());
        return q.columns.map((c) => ({ name: displayName(c.name), type: TYPE_OF[c.type] ?? 'float8' }));
      } catch (err) {
        if (readsCatalog(t)) return selectNames(t).map((name) => ({ name, type: 'text' }));
        throw err;
      }
    }
    async run(text) {
      const t = stripComments(text).replace(/;\s*$/, '');
      if (!t.trim()) return { empty: true };
      if (logQueries) log(`[pg] ${this.user}: ${text.replace(/\s+/g, ' ').trim().slice(0, 500)}`);
      const word = firstWord(t);
      switch (word) {
        case 'SELECT': case 'WITH': case '(': case 'VALUES': case 'TABLE': return this.query(word === 'TABLE' ? `SELECT * FROM ${t.trim().slice(5)}` : text.replace(/;\s*$/, '')); // (with its comments: known driver queries are told by them)
        case 'SET': this.set(t); return { tag: 'SET' };
        case 'RESET': return { tag: 'RESET' };
        case 'SHOW': return this.show(t);
        case 'BEGIN': case 'START': this.tx = 'T'; return { tag: 'BEGIN' };
        case 'COMMIT': case 'END': { const was = this.tx; this.tx = 'I'; this.closeCursors(); return { tag: was === 'E' ? 'ROLLBACK' : 'COMMIT' }; }
        case 'ROLLBACK': case 'ABORT':
          if (/^\s*rollback\s+to\b/i.test(t)) { if (this.tx === 'E') this.tx = 'T'; return { tag: 'ROLLBACK' }; }
          this.tx = 'I'; this.closeCursors(); return { tag: 'ROLLBACK' };
        case 'SAVEPOINT': return { tag: 'SAVEPOINT' };
        case 'RELEASE': return { tag: 'RELEASE' };
        case 'DISCARD': this.statements.clear(); this.portals.clear(); this.cursors.clear(); this.settings = { ...SETTINGS }; return { tag: 'DISCARD ALL' };
        case 'DEALLOCATE': { const m = /^\s*deallocate\s+(?:prepare\s+)?("?)([^"\s;]+)\1/i.exec(t); if (m?.[2].toLowerCase() === 'all') this.statements.clear(); else if (m) this.statements.delete(m[2]); return { tag: 'DEALLOCATE' }; }
        case 'DECLARE': return this.declare(t);
        case 'FETCH': case 'MOVE': return this.fetch(t, word);
        case 'CLOSE': { const m = /^\s*close\s+("?)([^"\s;]+)\1/i.exec(t); if (m?.[2].toLowerCase() === 'all') this.cursors.clear(); else if (m) this.cursors.delete(m[2]); return { tag: 'CLOSE CURSOR' }; }
        case 'EXPLAIN': {
          const inner = t.replace(/^\s*explain\s+(\([^)]*\)\s*|analyze\s+|verbose\s+)*/i, '');
          const { tables } = this.tablesFor();
          const plan = sql(inner, tables, this.sqlOpts()).explain().split('\n');
          return rowsOf([{ name: 'QUERY PLAN', type: 'text' }], plan.map((l) => [l]), 'EXPLAIN');
        }
        case 'LISTEN': case 'UNLISTEN': case 'NOTIFY': return { tag: word };
        case 'ANALYZE': case 'VACUUM': case 'CHECKPOINT': return { tag: word };
        case 'CREATE': case 'INSERT': case 'UPDATE': case 'DELETE': case 'DROP': case 'ALTER': case 'TRUNCATE': case 'COPY': case 'GRANT': case 'REVOKE': case 'MERGE': case 'COMMENT': case 'REFRESH': case 'LOCK': case 'CALL': case 'DO':
          throw new PgError('25006', `Vectro serves its tables read only: ${word} is not possible here`);
        case 'PREPARE': case 'EXECUTE': throw new PgError('0A000', `${word} statements are not supported (use the extended query protocol)`);
        default: throw new PgError('42601', `syntax error at or near "${t.trim().split(/\s+/)[0]}"`);
      }
    }
    async query(t) {
      const { tables, described } = this.tablesFor();
      const known = driverAnswer(t, described) ?? specialAnswer(t, this);
      if (known) return rowsOf(known.columns, known.rows);
      let q;
      try { q = sql(t, tables, this.sqlOpts()); } catch (err) {
        if (readsCatalog(t)) { // a catalog query beyond Vectro's SQL: nothing to report (no keys, indexes, comments...)
          if (logQueries) log(`[pg] answered empty: ${err.message}`);
          return rowsOf(selectNames(t).map((name) => ({ name, type: 'text' })), []);
        }
        throw err;
      }
      const columns = q.columns.map((c) => ({ name: displayName(c.name), col: c.name, type: TYPE_OF[c.type] ?? 'float8' }));
      const table = await q.collectAsync();
      try { return await tableRows(columns, table); } finally { table.dispose(); }
    }
    set(t) {
      const m = /^\s*set\s+(?:session\s+|local\s+)?("?)([\w.]+)\1\s*(?:=|\s+to\s+)\s*(.*?)\s*$/is.exec(t);
      if (!m) return; // (SET SESSION CHARACTERISTICS..., SET NAMES...: accepted)
      const name = m[2].toLowerCase(), value = m[3].replace(/^'(.*)'$/s, '$1').replace(/^"(.*)"$/s, '$1');
      this.settings[name] = value;
      if (name === 'application_name') this.write(message('S', cstr('application_name'), cstr(value)));
    }
    show(t) {
      const name = t.trim().replace(/^show\s+/i, '').replace(/;$/, '').trim().toLowerCase().replace(/^"(.*)"$/, '$1');
      const key = name === 'transaction isolation level' ? 'transaction_isolation' : name;
      if (key === 'all') return rowsOf([{ name: 'name', type: 'text' }, { name: 'setting', type: 'text' }, { name: 'description', type: 'text' }], Object.entries(this.settings).map(([k, v]) => [k, v, '']), 'SHOW');
      const value = this.settings[key] ?? this.settings[Object.keys(this.settings).find((k) => k.toLowerCase() === key)];
      if (value === undefined) throw new PgError('42704', `unrecognized configuration parameter "${name}"`);
      return rowsOf([{ name: key, type: 'text' }], [[value]], 'SHOW');
    }
    async declare(t) {
      const m = /^\s*declare\s+("?)([^"\s]+)\1\s+(binary\s+)?(?:(?:insensitive|asensitive|no\s+scroll|scroll)\s+)*cursor\s+(?:(?:with|without)\s+hold\s+)?for\s+([\s\S]+)$/i.exec(t);
      if (!m) throw new PgError('42601', 'DECLARE name CURSOR FOR query');
      const res = await this.run(m[4]);
      if (!res.columns) throw new PgError('42601', 'a cursor needs a query');
      this.cursors.set(m[2], { res, offset: 0, binary: !!m[3] });
      return { tag: 'DECLARE CURSOR' };
    }
    fetch(t, word) {
      const m = /^\s*(?:fetch|move)\s+(?:(next|prior|first|last|all|forward\s+all|forward\s+\d+|forward|absolute\s+-?\d+|relative\s+-?\d+|-?\d+)\s+)?(?:from\s+|in\s+)?("?)([^"\s;]+)\2\s*$/i.exec(t);
      if (!m) throw new PgError('42601', 'FETCH [n | ALL | NEXT] FROM cursor');
      const c = this.cursors.get(m[3]);
      if (!c) throw new PgError('34000', `cursor "${m[3]}" does not exist`);
      const how = (m[1] ?? 'next').toLowerCase().replace(/\s+/g, ' ');
      const left = c.res.n - c.offset;
      const count = how === 'next' || how === 'forward' ? 1 : /all/.test(how) ? left : /^\d+$/.test(how) ? Number(how) : /^forward \d+$/.test(how) ? Number(how.split(' ')[1]) : null;
      if (count === null) throw new PgError('0A000', `FETCH ${how.toUpperCase()} is not supported (cursors here move forward)`);
      const take = Math.max(0, Math.min(left, count)), start = c.offset;
      c.offset += take;
      if (word === 'MOVE') return { tag: `MOVE ${take}` };
      const res = c.res;
      return { columns: res.columns, n: take, cell: (i, k) => res.cell(start + i, k), tag: 'FETCH', binary: c.binary, keepEmpty: res.keepEmpty };
    }
    closeCursors() { this.cursors.clear(); }
  }

  server.listen(port, host);
  await once(server, 'listening');
  const actual = server.address().port;
  return {
    port: actual,
    host,
    server,
    catalog,
    url: `postgresql://${user ?? 'vectro'}@${host.includes(':') ? `[${host}]` : host}:${actual}/${database}`,
    async close() {
      for (const s of sessions.values()) s.socket.destroy();
      await new Promise((r) => server.close(r));
      disposeCatalogs();
      if (!shared) await catalog.close();
    },
  };
}

const STARTED = new Date().toISOString();
const SETTINGS = {
  server_version: VERSION, server_version_num: '160000', server_encoding: 'UTF8', client_encoding: 'UTF8', DateStyle: 'ISO, MDY', TimeZone: 'UTC',
  integer_datetimes: 'on', standard_conforming_strings: 'on', transaction_isolation: 'read committed', default_transaction_isolation: 'read committed',
  transaction_read_only: 'on', default_transaction_read_only: 'on', search_path: '"$user", public', max_identifier_length: '63', max_index_keys: '32',
  block_size: '8192', lc_collate: 'C', lc_ctype: 'C', max_connections: '100', extra_float_digits: '1', IntervalStyle: 'postgres', application_name: '',
  statement_timeout: '0', lock_timeout: '0', bytea_output: 'hex', is_superuser: 'off',
};

// functions catalog queries call (psql's \\dt, DBeaver, Metabase...)
function catalogFunctions(session) {
  const yes = { out: 'bool', f: () => 1 }, none = { out: 'text', f: () => null }, zero = { out: 'number', f: () => 0 };
  return {
    pg_get_userbyid: { out: 'text', f: () => session.user },
    pg_table_is_visible: yes, pg_type_is_visible: yes, pg_function_is_visible: yes,
    has_table_privilege: yes, has_schema_privilege: yes, has_database_privilege: yes, has_column_privilege: yes, has_any_column_privilege: yes, has_function_privilege: yes, has_sequence_privilege: yes,
    format_type: { out: 'text', f: (oid) => typeName(Number(oid)) ?? 'unknown' },
    pg_get_expr: none, obj_description: none, col_description: none, shobj_description: none,
    pg_get_constraintdef: none, pg_get_indexdef: none, pg_get_viewdef: none, pg_get_triggerdef: none, pg_get_functiondef: none, pg_tablespace_location: none,
    pg_encoding_to_char: { out: 'text', f: () => 'UTF8' },
    array_to_string: { out: 'text', f: (x) => x },
    quote_ident: { out: 'text', f: (x) => (x === null ? null : /^[a-z_][a-z0-9_$]*$/.test(x) ? x : `"${String(x).replaceAll('"', '""')}"`) },
    pg_size_pretty: { out: 'text', f: (n) => (n === null ? null : `${n} bytes`) },
    pg_total_relation_size: zero, pg_relation_size: zero, pg_table_size: zero, pg_indexes_size: zero, pg_database_size: zero,
    current_setting: { out: 'text', f: (name) => session.settings[name] ?? null },
  };
}

// a few calls with arguments that answer from the session
function specialAnswer(t, session) {
  const m = /^\s*select\s+(?:pg_catalog\.)?current_setting\(\s*'([^']+)'\s*\)\s*(?:as\s+("?)(\w+)\2)?\s*$/i.exec(t);
  if (m) {
    const v = session.settings[m[1]] ?? session.settings[Object.keys(session.settings).find((k) => k.toLowerCase() === m[1].toLowerCase())];
    if (v === undefined) throw new PgError('42704', `unrecognized configuration parameter "${m[1]}"`);
    return { columns: [{ name: m[3] ?? 'current_setting', type: 'text' }], rows: [[v]] };
  }
  const sc = /^\s*select\s+(?:pg_catalog\.)?set_config\(\s*'([^']+)'\s*,\s*'([^']*)'\s*,\s*(?:true|false)\s*\)\s*;?\s*$/i.exec(t);
  if (sc) { session.settings[sc[1]] = sc[2]; return { columns: [{ name: 'set_config', type: 'text' }], rows: [[sc[2]]] }; }
  return null;
}

const safeEqual = (a, b) => { const x = Buffer.from(String(a)), y = Buffer.from(String(b)); return x.length === y.length && timingSafeEqual(x, y); };
const firstWord = (t) => { const s = t.trimStart(); if (s[0] === '(') return '('; return (/^[A-Za-z_]+/.exec(s)?.[0] ?? '').toUpperCase(); };
function stripComments(text) { // leading comments only
  let s = text;
  for (;;) {
    const t = s.trimStart();
    if (t.startsWith('--')) { const j = t.indexOf('\n'); s = j < 0 ? '' : t.slice(j + 1); continue; }
    if (t.startsWith('/*')) { const j = t.indexOf('*/', 2); s = j < 0 ? '' : t.slice(j + 2); continue; }
    return s;
  }
}
const countParams = (text) => { let max = 0; text.replace(/\$(\d+)/g, (m, d) => { max = Math.max(max, Number(d)); return m; }); return max; };
// statements of a simple query, split at semicolons outside quotes and comments
function splitStatements(text) {
  const out = [];
  let cur = '', i = 0;
  while (i < text.length) {
    const c = text[i];
    if (c === "'" || c === '"') { let j = i + 1; for (; j < text.length; j++) if (text[j] === c) { if (text[j + 1] === c) j++; else break; } cur += text.slice(i, j + 1); i = j + 1; continue; }
    if (c === '-' && text[i + 1] === '-') { const j = text.indexOf('\n', i); const e = j < 0 ? text.length : j; cur += text.slice(i, e); i = e; continue; }
    if (c === '/' && text[i + 1] === '*') { const j = text.indexOf('*/', i + 2); const e = j < 0 ? text.length : j + 2; cur += text.slice(i, e); i = e; continue; }
    if (c === ';') { if (cur.trim()) out.push(cur); cur = ''; i++; continue; }
    cur += c; i++;
  }
  if (stripComments(cur).trim()) out.push(cur);
  return out;
}
