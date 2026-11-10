// Live sync: the difference between two answers of one query, by row key,
// and applying it. Shared by the server (hub.mjs) and the client (client.mjs);
// no imports, so it runs anywhere.
//
// An answer: { keys: [string], rows: [[value]], text: [string] } (text: each
// row as JSON, to compare rows cheaply). The difference, as the wire sends it:
//   del: [key]                 rows that left the answer
//   set: [[key, row]]          rows changed in place (they keep their place)
//   put: [[key, index, row?]]  rows added, or moved to `index` of the new answer
//                              (row given when added or also changed)
// Rows present in both answers keep their place unless they must move: the
// longest run of them still in their old order stays (longest increasing
// subsequence), so one row rising in a sorted list moves one row, not all.

// The indexes in `seq` (numbers) of one longest strictly increasing subsequence, as a Set.
export function longestIncreasing(seq) {
  const tails = [], prev = new Int32Array(seq.length); // tails[l]: index ending the best run of length l + 1
  for (let i = 0; i < seq.length; i++) {
    let lo = 0, hi = tails.length;
    while (lo < hi) { const mid = (lo + hi) >> 1; if (seq[tails[mid]] < seq[i]) lo = mid + 1; else hi = mid; }
    prev[i] = lo > 0 ? tails[lo - 1] : -1;
    tails[lo] = i;
  }
  const keep = new Set();
  for (let i = tails.length ? tails[tails.length - 1] : -1; i !== -1; i = prev[i]) keep.add(i);
  return keep;
}

// The difference from answer a to answer b, or null when they are the same.
export function diffAnswers(a, b) {
  const before = new Map();
  for (let i = 0; i < a.keys.length; i++) before.set(a.keys[i], i);
  const after = new Set(b.keys);
  const del = a.keys.filter((k) => !after.has(k));
  const kept = [], old = []; // (indexes in b of rows also in a, and their index in a)
  for (let j = 0; j < b.keys.length; j++) { const i = before.get(b.keys[j]); if (i !== undefined) { kept.push(j); old.push(i); } }
  const stay = longestIncreasing(old);
  const set = [], put = [];
  for (let j = 0, m = 0; j < b.keys.length; j++) {
    const k = b.keys[j];
    if (kept[m] !== j) { put.push([k, j, b.rows[j]]); continue; }
    const same = a.text[old[m]] === b.text[j];
    if (stay.has(m)) { if (!same) set.push([k, b.rows[j]]); } else put.push(same ? [k, j] : [k, j, b.rows[j]]);
    m++;
  }
  return del.length || set.length || put.length ? { del, set, put } : null;
}

// Apply a difference to the rows a client holds.
//   state: { keys: [string], rows: [object], index: Map key -> row object }
//   toRow(values) -> the row object for an array of values
// Returns { added, updated, removed, moved } (rows objects; removed and moved
// as keys). Rows that did not change stay the same objects, so a UI can skip them.
export function applyPatch(state, { del = [], set = [], put = [] }, toRow) {
  const { index } = state;
  const removed = [], updated = [], added = [], moved = [];
  const out = new Set(del);
  for (const k of del) { if (index.has(k)) removed.push(k); index.delete(k); }
  for (const [k, values] of set) { const r = toRow(values); index.set(k, r); updated.push(r); }
  const puts = put.map(([k, at, values]) => {
    out.add(k);
    const had = index.has(k);
    let r = index.get(k);
    if (values !== undefined) { r = toRow(values); index.set(k, r); (had ? updated : added).push(r); } else moved.push(k);
    return { k, at, r };
  }).sort((x, y) => x.at - y.at);
  // the rows that stay, in order, merged with the put rows at their places
  const keys = [], rows = [];
  let p = 0;
  const place = () => { while (p < puts.length && puts[p].at <= keys.length) { keys.push(puts[p].k); rows.push(puts[p].r); p++; } };
  for (const k of state.keys) {
    if (out.has(k)) continue;
    place();
    keys.push(k); rows.push(index.get(k));
  }
  place();
  for (; p < puts.length; p++) { keys.push(puts[p].k); rows.push(puts[p].r); } // (an index past the end: a bad patch; kept, not lost)
  state.keys = keys; state.rows = rows;
  return { added, updated, removed, moved };
}
