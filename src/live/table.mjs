// Live sync: a table whose rows change by key (insert, upsert, delete), for
// serve({ live }). Rows are kept in plain column arrays (numbers and dates in
// Float64Arrays, text as codes into labels); a delete moves the last row
// into the gap, so the arrays stay dense. tableFor(engine) makes an engine
// Table of the rows now (one copy per batch of changes, not per change).
//
//   const orders = liveTable({ key: 'id', columns: { id: 'number', region: 'text', price: 'number', ts: 'date' } });
//   orders.insert([{ id: 1, region: 'EU', price: 3, ts: Date.now() }]);
//   orders.upsert([{ id: 1, price: 4 }]);   // only the columns given change (a new key: the rest missing)
//   orders.delete([1]);
const KINDS = new Set(['number', 'text', 'category', 'date']);

const toMs = (v) => (v == null || v === '' ? NaN : v instanceof Date ? v.getTime() : typeof v === 'number' ? v : Date.parse(v));
const toNum = (v) => (v == null || v === '' ? NaN : typeof v === 'number' ? v : typeof v === 'boolean' ? +v : Number(v));

class Column {
  constructor(kind) {
    this.kind = kind === 'category' ? 'text' : kind;
    this.data = this.kind === 'text' ? new Uint32Array(64) : new Float64Array(64);
    if (this.kind === 'text') { this.labels = ['']; this.codes = new Map([['', 0]]); this.uses = [0]; this.free = []; }
  }
  grow(n) {
    if (n <= this.data.length) return;
    const d = new this.data.constructor(Math.max(n, this.data.length * 2));
    d.set(this.data);
    this.data = d;
  }
  code(v) {
    const s = v == null ? '' : String(v);
    let c = this.codes.get(s);
    if (c === undefined) {
      c = this.free.length ? this.free.pop() : this.labels.length;
      this.labels[c] = s; this.uses[c] = 0; this.codes.set(s, c);
    }
    return c;
  }
  set(i, v, fresh) {
    if (this.kind !== 'text') { this.data[i] = this.kind === 'date' ? toMs(v) : toNum(v); return; }
    const c = this.code(v);
    if (!fresh) this.drop(i);
    this.data[i] = c; this.uses[c]++;
  }
  drop(i) { // (text: the old value's label is freed when no row uses it)
    if (this.kind !== 'text') return;
    const c = this.data[i];
    if (--this.uses[c] === 0 && c !== 0) { this.codes.delete(this.labels[c]); this.labels[c] = ''; this.free.push(c); }
  }
  get(i) { return this.kind === 'text' ? this.labels[this.data[i]] : this.data[i]; }
}

export class LiveTable {
  constructor({ key, columns, rows = null } = {}) {
    if (!columns || typeof columns !== 'object' || !Object.keys(columns).length) throw new TypeError('vectro live: liveTable needs columns, { name: "number" | "text" | "date" }');
    for (const [n, k] of Object.entries(columns)) if (!KINDS.has(k)) throw new TypeError(`vectro live: column "${n}" has kind "${k}" (number, text or date)`);
    if (typeof key !== 'string' || !(key in columns)) throw new TypeError(`vectro live: key must name one of the columns (${Object.keys(columns).join(', ')})`);
    if (columns[key] === 'date') throw new TypeError('vectro live: the key column must be a number or text');
    this.key = key;
    this.kinds = { ...columns };
    this.cols = new Map(Object.entries(columns).map(([n, k]) => [n, new Column(k)]));
    this.slots = new Map(); // key value -> row
    this.n = 0;
    this.version = 0;     // grows with every change
    this.listeners = new Set();
    if (rows) this.insert(rows);
  }

  get length() { return this.n; }

  #keyOf(row) {
    const v = row?.[this.key];
    if (this.kinds[this.key] === 'number') { const x = toNum(v); if (x !== x) throw new TypeError(`vectro live: row without a number key "${this.key}": ${JSON.stringify(row)}`); return x; }
    if (v == null || v === '') throw new TypeError(`vectro live: row without a key "${this.key}": ${JSON.stringify(row)}`);
    return String(v);
  }
  #normKey(v) { return this.kinds[this.key] === 'number' ? toNum(v) : v == null ? '' : String(v); }

  #changed(count) {
    if (!count) return;
    this.version++;
    for (const fn of this.listeners) fn(this);
  }

  #add(row) {
    const i = this.n++;
    for (const c of this.cols.values()) c.grow(this.n);
    for (const [n, c] of this.cols) c.set(i, row[n], true);
    this.slots.set(this.#keyOf(row), i);
  }

  // New rows; a key already here is an error (nothing is changed then).
  insert(rows) {
    rows = [...rows];
    const keys = new Set();
    for (const r of rows) {
      const k = this.#keyOf(r);
      if (this.slots.has(k) || keys.has(k)) throw new Error(`vectro live: key ${JSON.stringify(k)} is already in the table (use upsert to change it)`);
      keys.add(k);
    }
    for (const r of rows) this.#add(r);
    this.#changed(rows.length);
    return rows.length;
  }

  // Rows changed by key, only in the columns each row gives; a new key is inserted.
  upsert(rows) {
    let count = 0;
    for (const r of rows) {
      const i = this.slots.get(this.#keyOf(r));
      if (i === undefined) { this.#add(r); count++; continue; }
      for (const n of Object.keys(r)) if (n !== this.key && this.cols.has(n)) { this.cols.get(n).set(i, r[n], false); count++; }
    }
    this.#changed(count);
    return count;
  }

  // Rows removed by key (keys not here are skipped); returns how many went.
  delete(keys) {
    let count = 0;
    for (const v of keys) {
      const k = this.#normKey(v), i = this.slots.get(k);
      if (i === undefined) continue;
      const last = --this.n;
      for (const c of this.cols.values()) { c.drop(i); if (i !== last) c.data[i] = c.data[last]; }
      this.slots.delete(k);
      if (i !== last) this.slots.set(this.#normKey(this.cols.get(this.key).get(i)), i);
      count++;
    }
    this.#changed(count);
    return count;
  }

  clear() { return this.delete([...this.slots.keys()]); }

  has(key) { return this.slots.has(this.#normKey(key)); }
  get(key) {
    const i = this.slots.get(this.#normKey(key));
    return i === undefined ? undefined : Object.fromEntries([...this.cols].map(([n, c]) => [n, c.get(i)]));
  }
  *rows() { for (let i = 0; i < this.n; i++) yield Object.fromEntries([...this.cols].map(([n, c]) => [n, c.get(i)])); }

  // fn(table) after each change (the hub's way to hear of them); returns a function that stops it.
  onChange(fn) { this.listeners.add(fn); return () => this.listeners.delete(fn); }

  // An engine Table of the rows now (the caller disposes it).
  tableFor(engine) {
    const cols = {}, dates = [];
    for (const [n, c] of this.cols) {
      if (c.kind === 'text') cols[n] = { codes: c.data.subarray(0, this.n), labels: c.labels };
      else { cols[n] = c.data.subarray(0, this.n); if (c.kind === 'date') dates.push(n); }
    }
    return engine.table(cols, { dates });
  }
}

export const liveTable = (opts) => new LiveTable(opts);
