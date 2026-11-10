// Live sync: the server's side. Pages watch SQL over a WebSocket; when a table
// changes, the queries that read it run again and each watcher gets only the
// difference from the answer it has (diff.mjs). See docs/live-sync.md.
//
// One LiveHub per server (serve({ live }) makes it). Work is paced: the first
// change after a quiet time is handled at the next tick; changes that come
// while queries run wait for the next round; rounds are at least minInterval
// ms apart. Watchers of the same SQL share one run and one difference.
import { sql } from '../sql.mjs';
import { columnsOf } from '../serve/formats.mjs';
import { diffAnswers } from './diff.mjs';
import { LiveTable } from './table.mjs';

export const now = () => performance.timeOrigin + performance.now();

const iso = (ms) => (ms === ms && ms > -8.64e15 && ms < 8.64e15 ? new Date(ms).toISOString() : null);
// each cell as a JSON value: NaN and blank text as null, dates as ISO text, infinities as text
function cellsOf(col, d) {
  if (d && d.labels) { const { codes, labels } = d; return (i) => labels[codes[i]] || null; }
  if (col.type === 'text') return (i) => d[i] || null;
  if (col.type === 'date') return (i) => iso(d[i]);
  return (i) => { const v = d[i]; return v !== v ? null : v === Infinity ? 'Infinity' : v === -Infinity ? '-Infinity' : v; };
}

const escape = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

export class LiveHub {
  constructor(catalog, { tables = {}, minInterval = 16, maxRows = 50_000, maxWatches = 64, maxBuffered = 1 << 20, log = () => {} } = {}) {
    Object.assign(this, { catalog, minInterval, maxRows, maxWatches, maxBuffered, log });
    this.live = new Map();      // name -> LiveTable
    this.built = new Map();     // name -> the live table's version its engine table has
    this.groups = new Map();    // sql + key -> group (watchers of one query)
    this.clients = new Set();
    this.dirty = new Set();     // table names changed since the last round
    this.since = null;          // time of the oldest change not handled yet
    this.fresh = new Set();     // groups waiting for their first answer
    this.timer = null; this.busy = null; this.last = -Infinity; this.closed = false;
    this.stats = { rounds: 0, runs: 0, patches: 0, snapshots: 0, bytes: 0, buildMs: 0 };
    this.off = [];
    for (const [name, t] of Object.entries(tables)) this.add(name, t);
  }

  // A live table served under `name` (also to /sql and the PostgreSQL server, as it is now).
  add(name, table) {
    if (!(table instanceof LiveTable)) throw new TypeError(`vectro live: "${name}" is not a liveTable (live: { name: liveTable({ key, columns }) })`);
    if (this.catalog.entries.has(name)) throw new Error(`vectro live: there is already a table "${name}"`);
    this.live.set(name, table);
    this.catalog.entries.set(name, { table: table.tableFor(this.catalog.engine), source: 'given', owned: false });
    this.built.set(name, table.version);
    this.off.push(table.onChange(() => this.changed(name)));
  }

  // The table `name` changed (a normal table or dataset you replaced or appended to): its watchers run again.
  changed(name, at = now()) {
    if (this.closed) return;
    this.dirty.add(name);
    this.since = Math.min(this.since ?? at, at);
    this.#kick();
  }

  // Serve `table` as `name` in place of the table there now (freed once queries on it end, if `free`).
  replace(name, table, { free = false } = {}) {
    const old = this.catalog.entries.get(name);
    this.catalog.entries.set(name, { table, source: 'given', owned: false });
    if (old && free && old.table !== table) this.catalog.whenFree(() => old.table.dispose?.());
    this.changed(name);
  }

  #kick() {
    if (this.timer || this.busy || this.closed) return;
    const wait = this.last + this.minInterval - performance.now();
    this.timer = wait > 0 ? setTimeout(() => this.#round(), wait) : setImmediate(() => this.#round());
  }

  // One round: live tables that changed are copied into engine tables, then
  // the queries that read a changed table run, and their watchers get the difference.
  async #round() {
    this.timer = null;
    if (this.closed) return;
    const dirty = this.dirty, at = this.since, fresh = this.fresh;
    this.dirty = new Set(); this.since = null; this.fresh = new Set();
    this.last = performance.now();
    this.stats.rounds++;
    this.busy = (async () => {
      const b0 = performance.now();
      for (const name of dirty) {
        const lt = this.live.get(name);
        if (!lt || this.built.get(name) === lt.version) continue;
        this.built.set(name, lt.version);
        const old = this.catalog.entries.get(name);
        this.catalog.entries.set(name, { table: lt.tableFor(this.catalog.engine), source: 'given', owned: false });
        if (old) this.catalog.whenFree(() => old.table.dispose());
      }
      this.stats.buildMs = performance.now() - b0; // (copying changed live tables into engine tables)
      const done = this.catalog.hold(), tables = this.catalog.named();
      try {
        for (const g of this.groups.values()) {
          if (!g.watchers.size) continue;
          const first = fresh.has(g);
          if (!first && ![...g.reads].some((n) => dirty.has(n))) continue;
          await this.#run(g, tables, first ? null : at);
        }
      } finally { done(); }
    })();
    try { await this.busy; } catch (err) { this.log(`live: ${err.stack}`); } finally {
      this.busy = null;
      if (this.dirty.size || this.fresh.size) this.#kick();
    }
  }

  // which tables the SQL may read: every table whose name appears in it as a
  // word (more than it reads at worst: an extra run, never a missed change)
  #reads(text) {
    const out = new Set();
    for (const name of this.catalog.entries.keys()) if (new RegExp(`(^|[^\\p{L}\\p{N}_.])${escape(name)}($|[^\\p{L}\\p{N}_])`, 'iu').test(text)) out.add(name);
    return out;
  }

  // Run a group's query; send the first answer or the difference to its watchers.
  async #run(g, tables, at) {
    const t0 = performance.now();
    let answer;
    try { answer = await this.#answer(g, tables); } catch (err) {
      const message = err.message.replace(/^vectro( SQL| live)?:\s*/, '');
      for (const w of g.watchers) { w.client.send({ t: 'error', id: w.id, message }); w.client.watches.delete(w.id); }
      g.watchers.clear();
      this.groups.delete(g.gkey);
      return;
    }
    this.stats.runs++;
    const before = g.answer;
    g.answer = answer;
    if (!before) { for (const w of g.watchers) this.#snapshot(w); return; }
    const d = diffAnswers(before, answer);
    if (!d && answer.columns.join('\0') === before.columns.join('\0')) return;
    const from = g.ver;
    g.ver++;
    if (answer.columns.join('\0') !== before.columns.join('\0')) { for (const w of g.watchers) this.#snapshot(w); return; } // (the table now has other columns)
    const ms = Math.round((performance.now() - t0) * 1000) / 1000;
    const body = JSON.stringify({ ver: g.ver, from, ...d, ms, at: at ?? now() }).slice(1); // (each watcher's id goes in front)
    for (const w of g.watchers) {
      if (w.stale) continue;
      if (w.client.ws.buffered > this.maxBuffered) { w.stale = true; w.client.waitDrain(); continue; } // (a fresh answer once it has caught up)
      w.client.raw(`{"t":"patch","id":${JSON.stringify(w.id)},${body}`);
      w.ver = g.ver;
      this.stats.patches++;
    }
  }

  async #answer(g, tables) {
    const q = sql(g.sql, tables);
    const t = q.collectAsync ? await q.collectAsync() : q;
    try {
      if (t.length > this.maxRows) throw new Error(`the answer has ${t.length} rows, more than ${this.maxRows} a live query may have: add a LIMIT or GROUP BY`);
      const cols = columnsOf(t), data = await t.toColumnsAsync(), n = t.length;
      const columns = cols.map((c) => c.name);
      let key = g.key;
      if (key) { for (const k of key) if (!columns.includes(k)) throw new Error(`the key column "${k}" is not in the answer (columns: ${columns.join(', ')})`); }
      else { // (the table's key, when the answer has it; else the whole row)
        const lk = [...g.reads].map((n) => this.live.get(n)?.key).find((k) => k && columns.includes(k));
        key = lk ? [lk] : null;
      }
      const cells = cols.map((c) => cellsOf(c, data[c.col])), at = key?.map((k) => columns.indexOf(k));
      const rows = new Array(n), text = new Array(n), keys = new Array(n), seen = new Map();
      for (let i = 0; i < n; i++) {
        const row = cells.map((f) => f(i));
        rows[i] = row;
        text[i] = JSON.stringify(row);
        let k = at ? (at.length === 1 ? JSON.stringify(row[at[0]]) : JSON.stringify(at.map((j) => row[j]))) : text[i];
        const dup = seen.get(k); // (a key twice: the second one is key#2, and so on)
        if (dup !== undefined) { seen.set(k, dup + 1); k = `${k}#${dup + 1}`; } else seen.set(k, 1);
        keys[i] = k;
      }
      return { columns, key: key ?? null, keys, rows, text };
    } finally { if (t !== q) t.dispose?.(); }
  }

  #snapshot(w) {
    const a = w.group.answer;
    w.client.send({ t: 'snap', id: w.id, ver: w.group.ver, columns: a.columns, key: a.key, keys: a.keys, rows: a.rows });
    w.ver = w.group.ver; w.stale = false;
    this.stats.snapshots++;
  }

  // A WebSocket client (from acceptWebSocket).
  accept(ws) {
    const hub = this;
    const client = {
      ws, watches: new Map(), draining: false,
      raw(text) { if (ws.open) { ws.send(text); hub.stats.bytes += text.length; } },
      send(o) { this.raw(JSON.stringify(o)); },
      waitDrain() {
        if (this.draining) return;
        this.draining = true;
        ws.socket.once('drain', () => {
          this.draining = false;
          for (const w of this.watches.values()) if (w.stale && w.group.answer) hub.#snapshot(w);
        });
      },
    };
    this.clients.add(client);
    ws.onclose = () => { for (const w of client.watches.values()) this.#leave(w); client.watches.clear(); this.clients.delete(client); };
    ws.onmessage = (text) => {
      let m;
      try { m = JSON.parse(text); } catch { return client.send({ t: 'error', message: 'messages must be JSON' }); }
      try { this.#message(client, m); } catch (err) { client.send({ t: 'error', id: m?.id, message: err.message }); }
    };
    client.send({ t: 'hello', v: 1, tables: [...this.catalog.entries.keys()], s: now() });
    return client;
  }

  #message(client, m) {
    const { op, id } = m ?? {};
    if (op === 'ping') return client.send({ t: 'pong', c: m.c, s: now() });
    if (!((typeof id === 'string' && id.length <= 64) || Number.isInteger(id))) throw new Error('a watch needs an id: a number or text of up to 64 characters');
    if (op === 'unwatch') { const w = client.watches.get(id); if (w) { this.#leave(w); client.watches.delete(id); } return; }
    if (op === 'resync') { const w = client.watches.get(id); if (w?.group.answer) this.#snapshot(w); return; }
    if (op !== 'watch') throw new Error(`unknown op "${op}" (watch, unwatch, resync, ping)`);
    if (client.watches.has(id)) throw new Error(`watch ${JSON.stringify(id)} is already running`);
    if (client.watches.size >= this.maxWatches) throw new Error(`at most ${this.maxWatches} watches per connection`);
    if (typeof m.sql !== 'string' || !m.sql.trim() || m.sql.length > 100_000) throw new Error('sql must be the text of a query');
    const key = m.key == null ? null : Array.isArray(m.key) ? m.key.map(String) : [String(m.key)];
    if (key && !key.length) throw new Error('key must name at least one column');
    const gkey = `${m.sql}\0${key ? JSON.stringify(key) : ''}`;
    let g = this.groups.get(gkey);
    if (!g) { g = { gkey, sql: m.sql, key, reads: this.#reads(m.sql), watchers: new Set(), answer: null, ver: 0 }; this.groups.set(gkey, g); }
    const w = { id, client, group: g, ver: -1, stale: false };
    g.watchers.add(w);
    client.watches.set(id, w);
    if (g.answer) this.#snapshot(w);
    else { this.fresh.add(g); this.#kick(); }
  }

  #leave(w) {
    const g = w.group;
    g.watchers.delete(w);
    if (!g.watchers.size) { this.groups.delete(g.gkey); this.fresh.delete(g); }
  }

  async close() {
    this.closed = true;
    if (this.timer) { clearTimeout(this.timer); clearImmediate(this.timer); }
    for (const off of this.off) off();
    for (const c of this.clients) c.ws.close(1001);
    await this.busy?.catch(() => {});
    for (const name of this.live.keys()) {
      const e = this.catalog.entries.get(name);
      if (e) { this.catalog.entries.delete(name); this.catalog.whenFree(() => e.table.dispose()); }
    }
  }
}
