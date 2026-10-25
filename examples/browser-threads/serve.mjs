// Static server for the 10-browser-threads folder that makes pages
// cross-origin isolated (needed for SharedArrayBuffer, so for threads):
//   Cross-Origin-Opener-Policy: same-origin
//   Cross-Origin-Embedder-Policy: require-corp
// Usage: node examples/browser-threads/serve.mjs [port] [--no-isolation] [--lan]
// (--lan listens on the local network too, e.g. for testing on a phone;
// without it only this machine can connect)
// then open http://localhost:<port>/examples/browser-threads/ (threads),
// /examples/split/ (split tables, meant for --no-isolation) or
// /examples/check/ (correctness on every engine the browser has).
// Pages opened with ?report=<name> POST their results to /report, which
// saves them as results/<name>.json, so any browser can be tested unattended.
import { createServer } from 'node:http';
import { mkdir, readFile, stat, writeFile } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = fileURLToPath(new URL('../../', import.meta.url));
const PORT = Number(process.argv[2] ?? 8766);
const ISOLATE = !process.argv.includes('--no-isolation');
const HOST = process.argv.includes('--lan') ? '0.0.0.0' : '127.0.0.1';
const TYPES = { '.html': 'text/html', '.mjs': 'text/javascript', '.js': 'text/javascript', '.wasm': 'application/wasm', '.csv': 'text/csv', '.json': 'application/json' };

createServer(async (req, res) => {
  if (req.method === 'POST' && req.url.startsWith('/report?')) {
    const name = new URL(req.url, 'http://x').searchParams.get('name')?.replace(/[^\w.-]/g, '_') || 'report';
    const chunks = [];
    for await (const c of req) chunks.push(c);
    await mkdir(join(ROOT, 'results'), { recursive: true });
    await writeFile(join(ROOT, 'results', `${name}.json`), Buffer.concat(chunks));
    console.log(`report saved: results/${name}.json`);
    return res.writeHead(204).end();
  }
  let path = normalize(decodeURIComponent(new URL(req.url, 'http://x').pathname)).replace(/^(\.\.[/\\])+/, '');
  let file = join(ROOT, path);
  try {
    if ((await stat(file)).isDirectory()) file = join(file, 'index.html');
    const body = await readFile(file);
    res.writeHead(200, {
      'Content-Type': TYPES[extname(file)] ?? 'application/octet-stream',
      ...(ISOLATE ? { 'Cross-Origin-Opener-Policy': 'same-origin', 'Cross-Origin-Embedder-Policy': 'require-corp' } : {}),
      'Cache-Control': 'no-store',
    });
    res.end(body);
  } catch {
    res.writeHead(404).end('not found');
  }
}).listen(PORT, HOST, () => console.log(`serving ${ROOT} ${ISOLATE ? 'with' : 'without'} cross-origin isolation on http://${HOST === '0.0.0.0' ? 'all interfaces' : 'localhost'}:${PORT}`));
