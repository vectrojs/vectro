// Window functions over rows already sorted by (partition keys, order keys):
// each function gives one value per row, from the rows of its partition in
// its frame. Plain loops, one pass each (running and moving sums from prefix
// sums, moving min / max with a monotonic queue). Missing values (NaN) follow
// SQL: sum / avg / min / max skip them, count(x) counts values present.
//
// spec items: { fn, arg (Float64Array | null), frame, offset, def, k, p }
//   fn: row_number rank dense_rank percent_rank cume_dist ntile
//       lag lead first_value last_value nth_value
//       sum avg min max count (count with arg null: rows)
//       percentile_cont percentile_disc (p; arg sorted within the partition)
//   frame: { unit: 'rows' | 'range', start, end } with start / end each
//       { kind: 'unbounded' | 'preceding' | 'following' | 'current', n }
//       (null: SQL's default: with ORDER BY, RANGE from the partition start to
//       the current row and its ties; without, the whole partition)

// Partition starts: part[i] = first row of row i's partition; peers likewise
// for rows with equal order keys inside a partition (peers: false skips them,
// for functions that don't look at ties).
export function bounds(n, partKeys, orderKeys, peers = true) {
  const newPart = new Uint8Array(n), partStart = new Int32Array(n), partEnd = new Int32Array(n);
  if (n) newPart[0] = 1;
  for (const k of partKeys) markChanges(k, newPart, n);
  spans(newPart, partStart, partEnd, n);
  if (!peers) return { partStart, partEnd, peerStart: partStart, peerEnd: partEnd };
  const newPeer = newPart.slice(), peerStart = new Int32Array(n), peerEnd = new Int32Array(n);
  for (const k of orderKeys) markChanges(k, newPeer, n);
  spans(newPeer, peerStart, peerEnd, n);
  return { partStart, partEnd, peerStart, peerEnd };
}

// mark[i] = 1 where k[i] differs from k[i - 1] (NaN equals NaN here)
function markChanges(k, mark, n) {
  for (let i = 1; i < n; i++) { const a = k[i], b = k[i - 1]; if (a !== b && (a === a || b === b)) mark[i] = 1; }
}

// Each row's run [start, end) from the marks of where runs begin.
function spans(mark, start, end, n) {
  let s = 0;
  for (let i = 1; i <= n; i++) {
    if (i === n || mark[i]) {
      for (let j = s; j < i; j++) { start[j] = s; end[j] = i; }
      s = i;
    }
  }
}

// Whether some item reads peers (rows with equal order keys): ranks, and
// frames by RANGE (the default with ORDER BY counts the current row's ties).
const NO_PEERS = new Set(['row_number', 'ntile', 'lag', 'lead', 'percentile_cont', 'percentile_disc']);
export const readsPeers = (items) => items.some((it) => !NO_PEERS.has(it.fn) && it.frame?.unit !== 'rows');

// Each row's frame [lo[i], hi[i]) (row indexes, within its partition).
// orderKey: the single ORDER BY key, for RANGE with distances (desc: it falls).
function frames(n, b, frame, hasOrder, orderKey, desc) {
  const lo = new Int32Array(n), hi = new Int32Array(n);
  const f = frame ?? (hasOrder ? { unit: 'range', start: { kind: 'unbounded' }, end: { kind: 'current' } } : { unit: 'rows', start: { kind: 'unbounded' }, end: { kind: 'unbounded' } });
  const { partStart: ps, partEnd: pe, peerStart: qs, peerEnd: qe } = b;
  const byValue = f.unit === 'range' && (f.start.n !== undefined || f.end.n !== undefined);
  if (byValue && !orderKey) throw new Error('vectro: RANGE with a distance needs exactly one ORDER BY key, a number or date');
  for (let i = 0; i < n; i++) {
    for (const [e, isEnd] of [[f.start, false], [f.end, true]]) {
      let at;
      if (e.kind === 'unbounded') at = isEnd ? pe[i] : ps[i];
      else if (f.unit === 'rows') {
        const j = e.kind === 'current' ? i : e.kind === 'preceding' ? i - e.n : i + e.n;
        at = Math.min(pe[i], Math.max(ps[i], isEnd ? j + 1 : j));
      } else if (e.kind === 'current') at = isEnd ? qe[i] : qs[i]; // (RANGE: the current row and its ties)
      else continue; // (RANGE n PRECEDING / FOLLOWING: by value, below)
      if (isEnd) hi[i] = at; else lo[i] = at;
    }
  }
  if (byValue) { // two pointers per partition over the keys, in their order
    const sign = desc ? -1 : 1, off = (e) => (e.kind === 'preceding' ? -e.n : e.kind === 'following' ? e.n : 0);
    for (let s = 0; s < n; s = pe[s]) {
      const end = pe[s];
      let a = s, z = s;
      for (let i = s; i < end; i++) {
        const v = sign * orderKey[i];
        if (v !== v) { if (f.start.n !== undefined) lo[i] = qs[i]; if (f.end.n !== undefined) hi[i] = qe[i]; continue; } // (missing keys: their ties, on the sides with a distance)
        if (f.start.n !== undefined) { const t = v + off(f.start); while (a < end && sign * orderKey[a] < t) a++; lo[i] = a; }
        if (f.end.n !== undefined) { const t = v + off(f.end); while (z < end && sign * orderKey[z] <= t) z++; hi[i] = z; }
      }
    }
  }
  for (let i = 0; i < n; i++) if (hi[i] < lo[i]) hi[i] = lo[i]; // (an empty frame)
  return { lo, hi };
}

// The values of one window function, for every row.
export function windowValues(n, b, item, hasOrder, orderKey, desc) {
  const out = new Float64Array(n);
  const { partStart: ps, partEnd: pe, peerStart: qs, peerEnd: qe } = b;
  const x = item.arg;
  switch (item.fn) {
    case 'row_number': for (let i = 0; i < n; i++) out[i] = i - ps[i] + 1; return out;
    case 'rank': for (let i = 0; i < n; i++) out[i] = qs[i] - ps[i] + 1; return out;
    case 'dense_rank': for (let i = 0, r = 0; i < n; i++) { if (i === ps[i]) r = 1; else if (qs[i] === i) r++; out[i] = r; } return out;
    case 'percent_rank': for (let i = 0; i < n; i++) { const m = pe[i] - ps[i]; out[i] = m > 1 ? (qs[i] - ps[i]) / (m - 1) : 0; } return out;
    case 'cume_dist': for (let i = 0; i < n; i++) out[i] = (qe[i] - ps[i]) / (pe[i] - ps[i]); return out;
    case 'ntile': for (let i = 0; i < n; i++) { // the first (m mod k) tiles get one row more
      const m = pe[i] - ps[i], k = item.k, j = i - ps[i], small = Math.floor(m / k), big = m % k;
      out[i] = j < big * (small + 1) ? Math.floor(j / (small + 1)) + 1 : big + Math.floor((j - big * (small + 1)) / Math.max(1, small)) + 1;
    } return out;
    case 'lag': case 'lead': {
      const k = (item.fn === 'lag' ? -1 : 1) * (item.offset ?? 1);
      for (let i = 0; i < n; i++) { const j = i + k; out[i] = j >= ps[i] && j < pe[i] ? x[j] : (item.def ?? NaN); }
      return out;
    }
    case 'percentile_cont': case 'percentile_disc': { // over the partition's values present (sorted: the window orders by the value)
      for (let s = 0; s < n; ) {
        const e = pe[s];
        let a = s, z = e; // present values: NaN sort last
        while (z > a && x[z - 1] !== x[z - 1]) z--;
        let xs = x, m = z - a;
        if (item.distinct && m) { // (each value once; sorted, so equal values sit together, -0 with 0)
          xs = [x[a]];
          for (let i = a + 1; i < z; i++) if (x[i] !== x[i - 1]) xs.push(x[i]);
          a = 0; m = xs.length;
        }
        let v = NaN;
        if (m) {
          if (item.fn === 'percentile_disc') v = xs[a + discRank(item.p, m, item.desc)];
          else { const r = item.p * (m - 1), lo = Math.floor(r); v = between(xs[a + lo], xs[a + Math.min(m - 1, lo + 1)], r - lo); }
        }
        out.fill(v, s, e);
        s = e;
      }
      return out;
    }
  }
  // functions of the frame
  const { lo, hi } = frames(n, b, item.frame, hasOrder, orderKey, desc);
  switch (item.fn) {
    case 'first_value': for (let i = 0; i < n; i++) out[i] = hi[i] > lo[i] ? x[lo[i]] : NaN; return out;
    case 'last_value': for (let i = 0; i < n; i++) out[i] = hi[i] > lo[i] ? x[hi[i] - 1] : NaN; return out;
    case 'nth_value': for (let i = 0; i < n; i++) { const j = lo[i] + item.k - 1; out[i] = j < hi[i] ? x[j] : NaN; } return out;
    case 'sum': case 'avg': case 'count': {
      // prefix sums of the values present, and of how many are present; the sums start
      // again at each partition (s at a partition's first row reads as 0), so a huge
      // value in one partition takes no digits from the next ones
      const s = new Float64Array(n + 1), c = new Float64Array(n + 1);
      for (let i = 0; i < n; i++) { const v = x ? x[i] : 1; const ok = v === v; s[i + 1] = (i === ps[i] ? 0 : s[i]) + (ok ? v : 0); c[i + 1] = c[i] + (ok ? 1 : 0); }
      for (let i = 0; i < n; i++) {
        const cnt = c[hi[i]] - c[lo[i]], sum = (hi[i] === ps[i] ? 0 : s[hi[i]]) - (lo[i] === ps[i] ? 0 : s[lo[i]]);
        out[i] = item.fn === 'count' ? cnt : item.fn === 'avg' ? (cnt ? sum / cnt : NaN) : cnt ? sum : NaN; // (sum of nothing: NULL)
      }
      if (item.fn !== 'count' && item.exactSmall !== false) { // short frames: add directly (prefix differences lose digits)
        for (let i = 0; i < n; i++) if (hi[i] - lo[i] <= 64) { let t = 0, m = 0; for (let j = lo[i]; j < hi[i]; j++) { const v = x[j]; if (v === v) { t += v; m++; } } out[i] = m ? (item.fn === 'avg' ? t / m : t) : NaN; }
      }
      return out;
    }
    case 'min': case 'max': { // frames only move forward: a monotonic queue of candidates
      const better = item.fn === 'min' ? (a, c) => a <= c : (a, c) => a >= c;
      const q = new Int32Array(n);
      let head = 0, tail = 0, added = 0, part = -1;
      for (let i = 0; i < n; i++) {
        if (ps[i] !== part) { part = ps[i]; head = tail = 0; added = ps[i]; }
        while (added < hi[i]) { const v = x[added]; if (v === v) { while (tail > head && better(v, x[q[tail - 1]])) tail--; q[tail++] = added; } added++; }
        while (tail > head && q[head] < lo[i]) head++;
        out[i] = tail > head ? x[q[head]] : NaN;
      }
      return out;
    }
  }
  throw new Error(`vectro: unknown window function ${item.fn}`);
}

// PERCENTILE_DISC(p): the rank (0 = the smallest of m values present) of the
// first value, in the ORDER BY direction, whose share of the values reaches p
export const discRank = (p, m, desc = false) => (desc ? m - Math.max(1, Math.ceil(p * m)) : Math.max(0, Math.ceil(p * m) - 1));
// PERCENTILE_CONT between two neighbours, f of the way from x to y: as x * (1 - f) + y * f, which can't
// overflow (±1e308) and keeps an infinite neighbour (x + (y - x) * f gives NaN there, -Infinity + Infinity)
export const between = (x, y, f) => (f === 0 || x === y ? x : x * (1 - f) + y * f);
