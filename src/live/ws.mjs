// Live sync: the server side of a WebSocket (RFC 6455), with no dependencies:
// the handshake, text messages both ways (client frames are masked, and may
// come in pieces), ping / pong and close.
//   server.on('upgrade', (req, socket, head) => { const ws = acceptWebSocket(req, socket, head); ws.onmessage = (text) => ...; ws.send('…'); });
import { createHash } from 'node:crypto';

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// A refused upgrade: a plain HTTP answer, then the socket closed.
export function refuseUpgrade(socket, status, message) {
  const body = JSON.stringify({ error: message });
  socket.end(`HTTP/1.1 ${status} ${status === 401 ? 'Unauthorized' : status === 404 ? 'Not Found' : 'Bad Request'}\r\ncontent-type: application/json\r\ncontent-length: ${Buffer.byteLength(body)}\r\nconnection: close\r\n\r\n${body}`);
}

export function acceptWebSocket(req, socket, head, { maxMessage = 1 << 20 } = {}) {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade).toLowerCase() !== 'websocket') { refuseUpgrade(socket, 400, 'expected a WebSocket upgrade'); return null; }
  const accept = createHash('sha1').update(key + GUID).digest('base64');
  socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
  socket.setNoDelay(true);
  let open = true, buf = head?.length ? Buffer.from(head) : Buffer.alloc(0), parts = [], size = 0;
  const ws = {
    onmessage: null, onclose: null,
    get open() { return open; },
    get buffered() { return socket.writableLength; }, // bytes waiting to go out
    send: (text) => open && socket.write(frame(Buffer.from(text), 0x81)),
    close(code = 1000) {
      if (!open) return;
      open = false;
      const b = Buffer.alloc(2); b.writeUInt16BE(code);
      socket.end(frame(b, 0x88));
      setTimeout(() => socket.destroy(), 1000).unref();
    },
    socket,
  };
  const ended = () => { open = false; if (ws.ended) return; ws.ended = true; ws.onclose?.(); };
  socket.on('close', ended);
  socket.on('error', ended);
  const fail = (code) => { ws.close(code); ended(); };
  const read = () => {
    while (buf.length >= 2) {
      const fin = buf[0] & 0x80, op = buf[0] & 0x0f, masked = buf[1] & 0x80;
      let n = buf[1] & 0x7f, at = 2;
      if (n === 126) { if (buf.length < 4) return; n = buf.readUInt16BE(2); at = 4; }
      else if (n === 127) { if (buf.length < 10) return; const big = buf.readBigUInt64BE(2); if (big > BigInt(maxMessage)) return fail(1009); n = Number(big); at = 10; }
      if (!masked) return fail(1002); // (clients must mask)
      if (n > maxMessage) return fail(1009);
      if (buf.length < at + 4 + n) return;
      const mask = buf.subarray(at, at + 4), payload = Buffer.from(buf.subarray(at + 4, at + 4 + n));
      for (let i = 0; i < n; i++) payload[i] ^= mask[i & 3];
      buf = buf.subarray(at + 4 + n);
      if (op === 8) { ws.close(); ended(); return; }
      if (op === 9) { if (open) socket.write(frame(payload, 0x8a)); continue; }
      if (op === 10) continue;
      if (op === 1 || op === 2 || op === 0) {
        size += n;
        if (size > maxMessage) return fail(1009);
        parts.push(payload);
        if (fin) { const text = Buffer.concat(parts).toString('utf8'); parts = []; size = 0; ws.onmessage?.(text); }
        continue;
      }
      return fail(1002);
    }
  };
  socket.on('data', (d) => { buf = buf.length ? Buffer.concat([buf, d]) : d; read(); });
  if (buf.length) queueMicrotask(read);
  return ws;
}

function frame(payload, first) { // 0x81: text, 0x88: close, 0x8a: pong (final frames, not masked)
  const n = payload.length;
  let header;
  if (n < 126) header = Buffer.from([first, n]);
  else if (n < 65536) { header = Buffer.alloc(4); header[0] = first; header[1] = 126; header.writeUInt16BE(n, 2); }
  else { header = Buffer.alloc(10); header[0] = first; header[1] = 127; header.writeBigUInt64BE(BigInt(n), 2); }
  return Buffer.concat([header, payload]);
}
