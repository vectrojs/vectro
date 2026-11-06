// Node files for datasets (see dataset.mjs): reading, writing and temporary
// folders. dataset.mjs itself uses no Node APIs, so datasets also open in
// browsers (from a File or a URL; see blobFile and urlFile there).
import { openSync, readSync, read, writeSync, closeSync, fstatSync, ftruncateSync, existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

function readFully(fd, u8, pos) {
  for (let done = 0; done < u8.length; ) {
    const got = readSync(fd, u8, done, u8.length - done, pos + done);
    if (got === 0) throw new Error('vectro: dataset file ended early');
    done += got;
  }
}

// A readable file: sync and async reads, and the descriptor (worker threads
// read chunks with it straight into engine memory). Async reads go through the
// same descriptor: a FileHandle left open when its dataset is dropped makes Node
// 25 stop the process at garbage collection. A file dropped without close() has
// its descriptor closed when it is collected.
const readAt = (fd, u8, off, len, pos) => new Promise((resolve, reject) => read(fd, u8, off, len, pos, (err, n) => (err ? reject(err) : resolve(n))));
const shut = (s) => { if (s.closed) return; s.closed = true; closeSync(s.fd); };
const dropped = new FinalizationRegistry((s) => { if (s.busy) s.closing = true; else shut(s); });
function reader(fd, path) {
  const s = { fd, busy: 0, closing: false, closed: false }; // (no reference back to the file: it can be collected)
  const file = {
    fd,
    name: path,
    size: fstatSync(fd).size,
    readSync: (u8, pos) => readFully(fd, u8, pos),
    async read(u8, pos) {
      s.busy++;
      try {
        for (let done = 0; done < u8.length; ) {
          const got = await readAt(fd, u8, done, u8.length - done, pos + done);
          if (got === 0) throw new Error('vectro: dataset file ended early');
          done += got;
        }
      } finally {
        if (--s.busy === 0 && s.closing) shut(s); // (close() during a read: the descriptor stays until it ends)
      }
    },
    close() {
      dropped.unregister(file);
      if (s.busy) s.closing = true; else shut(s);
    },
  };
  dropped.register(file, s, file);
  return file;
}

export const nodeFiles = {
  openRead: (path) => reader(openSync(path, 'r'), path),
  // A writable file; with append, an existing file is opened as it is
  // (existed: true, and readable, for its footer).
  openWrite(path, { append = false } = {}) {
    const existed = append && existsSync(path);
    const fd = openSync(path, existed ? 'r+' : 'w');
    return Object.assign(reader(fd, path), { // (the same object: a copy would let the reader be collected, closing the descriptor)
      existed,
      write(u8, pos) { for (let done = 0; done < u8.length; ) done += writeSync(fd, u8, done, u8.length - done, pos + done); },
      truncate: (size) => ftruncateSync(fd, size),
    });
  },
  tempDir: (prefix) => mkdtempSync(join(tmpdir(), prefix)),
  join,
  remove: (path) => rmSync(path, { recursive: true, force: true }),
};
