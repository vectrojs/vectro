// A minimal WebSocket server side (RFC 6455) for the stream demo and tests:
// the handshake, then text frames from server to client. No dependencies.
//   server.on('upgrade', (req, socket) => { const ws = acceptWebSocket(req, socket); ws.send('…'); });
import { createHash } from 'node:crypto';

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

export function acceptWebSocket(req, socket) {
  const accept = createHash('sha1').update(req.headers['sec-websocket-key'] + GUID).digest('base64');
  socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
  let open = true;
  const done = () => { open = false; };
  socket.on('close', done);
  socket.on('error', done);
  socket.on('data', (d) => { if ((d[0] & 0x0f) === 8) { open = false; socket.end(); } }); // client sent close
  return {
    get open() { return open; },
    // returns false when the socket's buffer is full (wait for 'drain')
    send: (data) => open && socket.write(frame(Buffer.from(data))),
    close: () => { if (open) { open = false; socket.end(Buffer.from([0x88, 0])); } },
    socket,
  };
}

function frame(payload) {
  const n = payload.length;
  let header;
  if (n < 126) header = Buffer.from([0x81, n]);
  else if (n < 65536) { header = Buffer.alloc(4); header[0] = 0x81; header[1] = 126; header.writeUInt16BE(n, 2); }
  else { header = Buffer.alloc(10); header[0] = 0x81; header[1] = 127; header.writeBigUInt64BE(BigInt(n), 2); }
  return Buffer.concat([header, payload]);
}
