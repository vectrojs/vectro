// Live sync, the page's side (browsers, Node 22+, Deno, workers): watch SQL on
// a Vectro server and keep its answer up to date from the differences the
// server sends. No wasm, no engine, no dependencies.
//
//   import { connectLive } from 'vectrojs/live';
//   const live = connectLive('ws://127.0.0.1:8787/live', { token });
//   const w = live.watch('SELECT region, SUM(price) AS revenue FROM orders GROUP BY region ORDER BY revenue DESC',
//     { key: 'region' }, (rows, change) => render(rows, change));
//   w.close(); live.close();
//
// rows: the whole answer in order, as objects. Rows that did not change stay
// the same objects from one call to the next. change: { snapshot, added,
// updated, removed, moved, ms, latency } (removed and moved as keys; ms: the
// server's time to run the query and diff it; latency: ms from the change on
// the server to now, with the clocks matched at connect).
import { applyPatch } from './diff.mjs';

const clock = () => performance.timeOrigin + performance.now();

export function connectLive(url, { token = null, WebSocket: WS = globalThis.WebSocket, retry = true, onStatus = null } = {}) {
  if (typeof WS !== 'function') throw new Error('vectro live: this runtime has no WebSocket (browsers and Node 22+ have one; or pass { WebSocket })');
  if (token) { const u = new URL(url, globalThis.location?.href); u.searchParams.set('token', token); url = u.href; }
  const watches = new Map();
  let ws = null, closed = false, delay = 250, next = 1, offset = 0, rtt = Infinity, ready, setReady;
  const fresh = () => { ready = new Promise((r) => { setReady = r; }); };
  fresh();
  const status = (s, extra) => onStatus?.(s, extra);
  const send = (o) => { if (ws?.readyState === 1) ws.send(JSON.stringify(o)); };

  function open() {
    ws = new WS(url);
    ws.onopen = () => {
      delay = 250;
      send({ op: 'ping', c: clock() });
      for (const w of watches.values()) send({ op: 'watch', id: w.id, sql: w.sql, key: w.key ?? undefined });
      status('open');
    };
    ws.onmessage = (ev) => {
      const m = JSON.parse(typeof ev.data === 'string' ? ev.data : new TextDecoder().decode(ev.data));
      if (m.t === 'hello') { setReady(); return; }
      if (m.t === 'pong') { // (the server's clock minus ours, from the round trip with the shortest time)
        const t = clock(), r = t - m.c;
        if (r < rtt) { rtt = r; offset = m.s - (m.c + t) / 2; }
        return;
      }
      const w = watches.get(m.id);
      if (m.t === 'error') { if (w) { watches.delete(m.id); w.error = new Error(`vectro live: ${m.message}`); w.onError?.(w.error); w.reject(w.error); } else status('error', m.message); return; }
      if (!w) return;
      if (m.t === 'snap') w.snap(m);
      else if (m.t === 'patch') { if (m.from !== w.version) send({ op: 'resync', id: w.id }); else w.patch(m); }
    };
    ws.onclose = () => {
      status('closed');
      if (closed || !retry) return;
      fresh();
      setTimeout(() => { if (!closed) open(); }, delay);
      delay = Math.min(delay * 2, 5000);
    };
    ws.onerror = () => {}; // (onclose follows)
  }
  open();

  return {
    get ready() { return ready; },
    get offset() { return offset; }, // server clock minus this one, ms
    watch(sqlText, opts = {}, onChange = null) {
      if (typeof opts === 'function') { onChange = opts; opts = {}; }
      const id = next++;
      let resolve, reject;
      const first = new Promise((a, b) => { resolve = a; reject = b; });
      first.catch(() => {});
      const state = { keys: [], rows: [], index: new Map() };
      let toRow = null;
      const w = {
        id, sql: String(sqlText), key: opts.key ?? null, version: -1, columns: [], error: null, onError: opts.onError ?? null, resolve, reject,
        get rows() { return state.rows; },
        get keys() { return state.keys; },
        get first() { return first; }, // resolves with the rows once the first answer is here
        snap(m) {
          w.version = m.ver; w.columns = m.columns; w.answerKey = m.key;
          const cols = m.columns;
          toRow = (values) => { const o = {}; for (let i = 0; i < cols.length; i++) o[cols[i]] = values[i]; return o; };
          state.index = new Map(); state.keys = m.keys;
          state.rows = m.rows.map((values, i) => { const r = toRow(values); state.index.set(m.keys[i], r); return r; });
          onChange?.(state.rows, { snapshot: true, added: state.rows, updated: [], removed: [], moved: [], ms: 0, latency: 0 });
          resolve(state.rows);
        },
        patch(m) {
          const change = applyPatch(state, m, toRow);
          w.version = m.ver;
          change.snapshot = false; change.ms = m.ms; change.latency = clock() - (m.at - offset);
          onChange?.(state.rows, change);
        },
        close() { watches.delete(id); send({ op: 'unwatch', id }); },
      };
      watches.set(id, w);
      send({ op: 'watch', id, sql: w.sql, key: w.key ?? undefined });
      return w;
    },
    close() { closed = true; for (const w of watches.values()) w.reject(new Error('vectro live: closed')); watches.clear(); ws?.close(); },
  };
}
