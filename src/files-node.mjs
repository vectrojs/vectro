// Node files for datasets (see dataset.mjs): reading, writing and temporary
// folders. dataset.mjs itself uses no Node APIs, so datasets also open in
// browsers (from a File or a URL; see blobFile and urlFile there).
import { openSync, readSync, writeSync, closeSync, fstatSync, ftruncateSync, existsSync, mkdtempSync, rmSync } from 'node:fs';
import { open } from 'node:fs/promises';
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
// read chunks with it straight into engine memory).
function reader(fd, path) {
  let fh = null;
  return {
    fd,
    name: path,
    size: fstatSync(fd).size,
    readSync: (u8, pos) => readFully(fd, u8, pos),
    async read(u8, pos) {
      fh ??= open(path, 'r');
      const h = await fh;
      for (let done = 0; done < u8.length; ) {
        const { bytesRead } = await h.read(u8, done, u8.length - done, pos + done);
        if (bytesRead === 0) throw new Error('vectro: dataset file ended early');
        done += bytesRead;
      }
    },
    close() {
      closeSync(fd);
      if (fh) { const h = fh; fh = null; h.then((x) => x.close(), () => {}); }
    },
  };
}

export const nodeFiles = {
  openRead: (path) => reader(openSync(path, 'r'), path),
  // A writable file; with append, an existing file is opened as it is
  // (existed: true, and readable, for its footer).
  openWrite(path, { append = false } = {}) {
    const existed = append && existsSync(path);
    const fd = openSync(path, existed ? 'r+' : 'w');
    return {
      ...reader(fd, path),
      existed,
      write(u8, pos) { for (let done = 0; done < u8.length; ) done += writeSync(fd, u8, done, u8.length - done, pos + done); },
      truncate: (size) => ftruncateSync(fd, size),
    };
  },
  tempDir: (prefix) => mkdtempSync(join(tmpdir(), prefix)),
  join,
  remove: (path) => rmSync(path, { recursive: true, force: true }),
};
