// Browser files for datasets (see dataset.mjs): writing to the origin's
// private file system (OPFS: navigator.storage.getDirectory()), or to a
// file the user chose (a FileSystemFileHandle from showSaveFilePicker), and
// reading those back. Writes go to the file's writable stream in order, in
// the background; flush() / done() wait for them (the dataset writer's
// appendAsync / closeAsync do). Nothing is committed until the file closes,
// and an abort discards it.

export const storageAvailable = () => typeof navigator !== 'undefined' && typeof navigator.storage?.getDirectory === 'function';

async function storageDir() {
  if (!storageAvailable()) throw new Error("vectro: this browser has no file storage for datasets (navigator.storage.getDirectory); use memoryFile() and download the result");
  return navigator.storage.getDirectory();
}

// A writable file: a name in browser storage, or a FileSystemFileHandle.
export async function writableFile(target, { append = false } = {}) {
  const handle = typeof target === 'string' ? await (await storageDir()).getFileHandle(target, { create: true }) : target;
  if (typeof handle.createWritable !== 'function') throw new Error('vectro: this browser cannot write files here (no createWritable); use memoryFile() and download the result');
  const before = await handle.getFile();
  const existed = append && before.size > 0;
  const stream = await handle.createWritable({ keepExistingData: existed });
  let chain = Promise.resolve(), failed = null;
  const then = (fn) => { chain = chain.then(() => (failed ? undefined : fn())).catch((err) => { failed ??= err; }); };
  const check = async () => { await chain; if (failed) throw failed; };
  return {
    name: handle.name,
    existed,
    size: before.size,
    // (the footer of the file as it was, when appending)
    async read(u8, pos) { u8.set(new Uint8Array(await before.slice(pos, pos + u8.length).arrayBuffer())); },
    write(u8, pos) {
      if (failed) throw failed;
      const data = u8.slice(); // (engine memory changes; the stream writes later)
      then(() => stream.write({ type: 'write', position: pos, data }));
    },
    truncate(size) { then(() => stream.truncate(size)); },
    close() { then(() => stream.close()); },
    abort() { chain = chain.then(() => stream.abort()).catch(() => {}); },
    flush: check,
    done: check,
  };
}

// A dataset kept in memory instead of a file: blob() gives it, e.g. to
// download or upload. For browsers without file storage.
export function memoryFile(name = 'dataset.vectro') {
  const parts = []; // [position, bytes]
  let size = 0;
  return {
    name,
    existed: false,
    size: 0,
    write(u8, pos) { parts.push([pos, u8.slice()]); size = Math.max(size, pos + u8.length); },
    truncate(n) { size = n; },
    close() {},
    blob() {
      const out = new Uint8Array(size);
      for (const [pos, data] of parts) out.set(data.subarray(0, Math.max(0, Math.min(data.length, size - pos))), pos);
      return new Blob([out], { type: 'application/octet-stream' });
    },
  };
}

// Datasets in browser storage: [{ name, size }].
export async function storedFiles() {
  if (!storageAvailable()) return [];
  const out = [];
  for await (const [name, handle] of (await storageDir()).entries()) {
    if (handle.kind === 'file') out.push({ name, size: (await handle.getFile()).size });
  }
  return out;
}

// A stored dataset as a File (to open, or to download).
export async function storedFile(name) { return (await (await storageDir()).getFileHandle(name)).getFile(); }

export async function removeStored(name) { await (await storageDir()).removeEntry(name); }

// Temporary files for spilling (see dataset.mjs setTempStorage): a folder
// per spill in browser storage, removed when the query ends.
export const opfsTemp = {
  async dir(prefix) {
    const name = `${prefix}${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    await (await storageDir()).getDirectoryHandle(name, { create: true });
    return name;
  },
  join: (dir, name) => `${dir}/${name}`,
  async openWrite(path) { return writableFile(await fileHandle(path, true)); },
  async openRead(path) {
    const file = await (await fileHandle(path, false)).getFile();
    return { name: path, size: file.size, async read(u8, pos) { u8.set(new Uint8Array(await file.slice(pos, pos + u8.length).arrayBuffer())); }, close() {} };
  },
  async remove(path) {
    const [dir, name] = path.split('/');
    const root = await storageDir();
    if (name) await (await root.getDirectoryHandle(dir)).removeEntry(name); else await root.removeEntry(dir, { recursive: true });
  },
};

async function fileHandle(path, create) {
  const [dir, name] = path.split('/');
  return (await (await storageDir()).getDirectoryHandle(dir, { create })).getFileHandle(name, { create });
}
