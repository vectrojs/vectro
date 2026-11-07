// An HTTP SQL server over Vectro tables: anything that reads a URL (Excel,
// Power BI's Web connector, Google Sheets, pandas, curl) can query them.
//
//   GET|POST /sql?q=SELECT ...&format=json|csv|ndjson|arrow[&limit=n]
//            (or the SQL as the POST body, or JSON { "sql": "..." })
//   GET  /tables                   names, rows, columns
//   GET  /tables/<name>            one table's columns
//   GET  /tables/<name>/rows?format=csv[&limit=n]
//   POST /reload                   read the data folder again
//
//   import { serve } from 'vectrojs/serve';
//   const s = await serve({ tables: { orders }, port: 8787 });   // or { root: '~/data' }
//
// Read only (SQL here only reads). Listens on 127.0.0.1 unless host is
// given; with a token, requests need "Authorization: Bearer <token>" (or
// ?token=, for tools that can't send headers).
import { createServer } from 'node:http';
import { relative } from 'node:path';
import { once } from 'node:events';
import { timingSafeEqual } from 'node:crypto';
import { sql } from '../node.mjs';
import { openTables, LoadError } from './catalog.mjs';
import { odata, ODataError } from './odata.mjs';
import { FORMATS, columnsOf, encode } from './formats.mjs';

const MAX_BODY = 1 << 20;

class HttpError extends Error { constructor(status, message) { super(message); this.status = status; } }

// res.write, waiting for 'drain' when the buffer is full; false once the client has gone
// (a hung-up client never drains: waiting only for 'drain' kept the request, its answer
// and the tables it holds for ever)
async function send(res, chunk) {
  if (res.destroyed) return false;
  if (res.write(chunk)) return true;
  const ac = new AbortController();
  try { await Promise.race([once(res, 'drain', { signal: ac.signal }), once(res, 'close', { signal: ac.signal })]); } catch {} finally { ac.abort(); }
  return !res.destroyed;
}

// catalog: tables opened with openTables (catalog.mjs), to share them with
// the PostgreSQL server; else tables / root / engine open them here.
export async function serve({ tables = {}, root = null, engine = null, catalog = null, port = 8787, host = '127.0.0.1', token = null, cors = false, odataPage = 50_000, log = () => {} } = {}) {
  const shared = !!catalog;
  catalog ??= await openTables({ tables, root, engine, log });
  const entries = catalog.entries, rootPath = catalog.root;
  const reload = () => catalog.reload().catch((err) => { throw new HttpError(400, err.message); });

  const allowed = (req, url) => {
    if (!token) return true;
    const auth = req.headers.authorization ?? '';
    const basic = /^Basic\s+(.+)$/i.exec(auth); // (Basic: any user name, the token as the password: Power BI's and Excel's OData credentials)
    const given = basic ? Buffer.from(basic[1], 'base64').toString().replace(/^[^:]*:/, '') : auth.replace(/^Bearer\s+/i, '') || url.searchParams.get('token') || '';
    const a = Buffer.from(given), b = Buffer.from(token);
    return a.length === b.length && timingSafeEqual(a, b);
  };
  const describe = (name, { table, source }) => ({ name, rows: table.length, columns: columnsOf(table), ...(table.isDataset ? { on_disk: true } : {}), ...(source === 'given' ? {} : { file: relative(rootPath, source) }) });

  async function body(req) {
    const parts = [];
    let size = 0;
    for await (const chunk of req) { size += chunk.length; if (size > MAX_BODY) throw new HttpError(413, 'the request body is larger than 1 MB'); parts.push(chunk); }
    return Buffer.concat(parts).toString('utf8');
  }

  async function sqlOf(req, url) {
    let text = url.searchParams.get('q') ?? url.searchParams.get('sql');
    if (text === null && req.method === 'POST') {
      const b = await body(req);
      if (/json/.test(req.headers['content-type'] ?? '')) {
        let o;
        try { o = JSON.parse(b); } catch { throw new HttpError(400, 'the body is not valid JSON'); }
        text = o?.sql ?? o?.q ?? null;
        if (o?.format && !url.searchParams.has('format')) url.searchParams.set('format', o.format);
        if (o?.limit !== undefined && !url.searchParams.has('limit')) url.searchParams.set('limit', String(o.limit));
      } else text = b;
    }
    if (!text || !String(text).trim()) throw new HttpError(400, 'no SQL given: use /sql?q=SELECT ... or POST the SQL');
    return String(text);
  }

  const formatOf = (url) => {
    const f = (url.searchParams.get('format') ?? 'json').toLowerCase();
    if (!FORMATS[f]) throw new HttpError(400, `unknown format "${f}" (json, csv, ndjson or arrow)`);
    return f;
  };
  const limitOf = (url) => {
    const l = url.searchParams.get('limit');
    if (l === null || l === '') return null;
    const n = Number(l);
    if (!(n >= 0) || !Number.isInteger(n)) throw new HttpError(400, 'limit must be a whole number');
    return n;
  };

  // run a query (Query or Table) and stream its rows in the format asked
  async function answer(res, query, format, extra, filename) {
    const t0 = performance.now();
    const t = query.collectAsync ? await query.collectAsync() : query.isDataset ? await query.lazy().collectAsync() : query;
    try {
      const columns = columnsOf(t), data = await t.toColumnsAsync(), n = t.length;
      res.writeHead(200, { 'content-type': FORMATS[format], ...(format === 'csv' ? { 'content-disposition': `inline; filename="${filename}.csv"` } : {}) });
      for (const chunk of encode(format, columns, data, n, { ...extra, row_count: n, ms: Math.round((performance.now() - t0) * 100) / 100 })) {
        if (!(await send(res, chunk))) return;
      }
      res.end();
    } finally { if (t !== query) t.dispose(); }
  }

  const json = (res, status, o) => { res.writeHead(status, { 'content-type': FORMATS.json }); res.end(JSON.stringify(o)); };

  async function route(tables, req, res) {
    const url = new URL(req.url, 'http://localhost');
    if (cors) {
      res.setHeader('access-control-allow-origin', '*');
      res.setHeader('access-control-allow-headers', 'authorization, content-type');
      if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
    }
    if (!allowed(req, url)) { res.setHeader('WWW-Authenticate', 'Basic realm="vectro"'); throw new HttpError(401, 'this server needs a token: send "Authorization: Bearer <token>", Basic with the token as the password, or add ?token='); }
    const path = url.pathname.replace(/\/+$/, '') || '/';
    if (path === '/odata' || path.startsWith('/odata/')) {
      if (req.method !== 'GET') throw new HttpError(405, 'OData here is read only: use GET');
      const params = new URLSearchParams(url.search);
      params.delete('token');
      const host = req.headers['x-forwarded-host'] ?? req.headers.host ?? 'localhost';
      let r;
      try { r = await odata({ path, params, named: tables, base: `http://${host}/odata`, headers: req.headers, pageSize: odataPage }); } catch (err) {
        if (err instanceof ODataError || /^vectro/.test(err.message)) { res.writeHead(err.status ?? 400, { 'content-type': 'application/json', 'OData-Version': '4.0' }); return res.end(JSON.stringify({ error: { code: String(err.status ?? 400), message: err.message.replace(/^vectro( SQL)?:\s*/, '') } })); }
        throw err;
      }
      log(`odata ${decodeURIComponent(url.pathname + url.search).slice(0, 200)}`);
      res.writeHead(r.status, r.headers);
      for (const chunk of r.body) if (!(await send(res, chunk))) return;
      return res.end();
    }
    const parts = path.split('/').filter(Boolean).map((p) => { try { return decodeURIComponent(p); } catch { throw new HttpError(400, `the path has a malformed %-escape: ${p}`); } });
    if (path === '/sql') {
      if (req.method !== 'GET' && req.method !== 'POST') throw new HttpError(405, 'use GET or POST');
      // (the tables held when the request came in: a reload while its body arrives doesn't change them)
      const text = await sqlOf(req, url), format = formatOf(url), limit = limitOf(url);
      let q;
      try { q = sql(text, tables); } catch (err) { throw new HttpError(400, err.message); }
      if (limit !== null) q = q.limit(limit);
      log(`sql ${text.replace(/\s+/g, ' ').slice(0, 200)}`);
      return answer(res, q, format, {}, 'result');
    }
    if (parts[0] === 'tables' && req.method === 'GET') {
      if (parts.length === 1) return json(res, 200, { tables: [...entries].map(([n, e]) => describe(n, e)) });
      const e = entries.get(parts[1]);
      if (!e) throw new HttpError(404, `no table "${parts[1]}" (tables: ${[...entries.keys()].join(', ') || 'none'})`);
      if (parts.length === 2) return json(res, 200, describe(parts[1], e));
      if (parts.length === 3 && parts[2] === 'rows') {
        const format = formatOf(url), limit = limitOf(url);
        const q = limit !== null ? e.table.lazy().limit(limit) : e.table;
        return answer(res, q, format, {}, parts[1]);
      }
    }
    if (path === '/reload' && req.method === 'POST') return json(res, 200, await reload());
    if (path === '/' && req.method === 'GET') {
      return json(res, 200, {
        server: 'vectro',
        endpoints: { sql: '/sql?q=SELECT ...&format=json|csv|ndjson|arrow', tables: '/tables', table: '/tables/<name>', rows: '/tables/<name>/rows?format=csv', odata: '/odata (an OData v4 feed: Power BI, Excel, Tableau)' },
        tables: [...entries.keys()],
      });
    }
    throw new HttpError(404, `no such endpoint: ${req.method} ${path} (see /)`);
  }

  const server = createServer((req, res) => {
    const done = catalog.hold(), tables = catalog.named(); // (a reload meanwhile frees these tables only after this request)
    route(tables, req, res).finally(done).catch((err) => {
      const status = err.status ?? (err instanceof LoadError || /^vectro/.test(err.message) ? 400 : 500);
      if (status === 500) log(`error: ${err.stack}`);
      if (res.headersSent) { res.destroy(err); return; } // (mid-stream: the client sees a cut answer)
      json(res, status, { error: err.message });
    });
  });
  server.listen(port, host);
  await once(server, 'listening');
  const actual = server.address().port;
  return {
    url: `http://${host.includes(':') ? `[${host}]` : host}:${actual}`,
    port: actual,
    server,
    engine: catalog.engine,
    tables: entries,
    catalog,
    reload,
    async close() {
      server.closeAllConnections?.();
      await new Promise((r) => server.close(r));
      if (!shared) await catalog.close();
    },
  };
}
