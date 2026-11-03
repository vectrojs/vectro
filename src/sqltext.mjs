// SQL's text functions as plain JavaScript on one set of values (sql.mjs
// runs them through Table._textmap: once per distinct value, not per row).
// Positions count characters (code points) from 1, as in DuckDB; text
// missing is '' (as everywhere in Vectro), numbers missing are NaN.
import { parseDateText } from './dates.mjs';

// a value as text: numbers shortest (whole ones without ".0"), dates as
// 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS[.mmm]'
export function toText(v, kind) {
  if (kind === 'text') return v;
  if (v !== v || v === null || v === undefined) return '';
  if (kind === 'date') {
    const iso = new Date(v).toISOString();
    if (iso.endsWith('T00:00:00.000Z')) return iso.slice(0, 10);
    return iso.slice(0, iso.endsWith('.000Z') ? 19 : 23).replace('T', ' ');
  }
  if (Number.isInteger(v) && Math.abs(v) < 1e21) return v === 0 ? '0' : String(v);
  return String(Number(v.toPrecision(15)));
}
export const isNull = (v, kind) => (kind === 'text' ? v === '' : v !== v);
export const textToNumber = (s) => (s.trim() === '' ? NaN : Number(s.trim()));
export const textToDate = (s) => (s.trim() === '' ? NaN : parseDateText(s.trim()));

const chars = (s) => Array.from(s);
function trimSet(s, set, start, end) {
  const cs = chars(s), drop = new Set(chars(set));
  let a = 0, b = cs.length;
  if (start) while (a < b && drop.has(cs[a])) a++;
  if (end) while (b > a && drop.has(cs[b - 1])) b--;
  return cs.slice(a, b).join('');
}
// DuckDB's substring: start 1 is the first character, 0 the place before
// it, negative counts from the end; a negative length takes the characters before
function substring(s, start, len) {
  const cs = chars(s), n = cs.length;
  const at = start > 0 ? start - 1 : start === 0 ? -1 : n + start;
  let a = at, b = len === undefined ? Infinity : at + len;
  if (len !== undefined && len < 0) { a = at + len; b = at; }
  return cs.slice(Math.max(0, a), Math.max(0, Math.min(n, b))).join('');
}
const regexes = new Map();
function regex(pattern, flags = '') {
  const key = `${flags}/${pattern}`;
  let re = regexes.get(key);
  if (!re) {
    if (regexes.size > 1000) regexes.clear();
    try { re = new RegExp(pattern, flags.replace(/[^gimsu]/g, '') + 'u'); } catch (err) { throw new SyntaxError(`vectro SQL: bad regular expression /${pattern}/: ${err.message}`); }
    regexes.set(key, re);
  }
  re.lastIndex = 0;
  return re;
}
const DAYS = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
const MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
function strftime(t, fmt) {
  const d = new Date(t), p2 = (x) => String(x).padStart(2, '0');
  const Y = d.getUTCFullYear(), M = d.getUTCMonth(), D = d.getUTCDate(), h = d.getUTCHours(), w = d.getUTCDay();
  const yday = Math.floor((Date.UTC(Y, M, D) - Date.UTC(Y, 0, 1)) / 86_400_000) + 1;
  const iso = () => { // ISO 8601 week and its year: the week's Thursday says which year it is in
    const th = new Date(Date.UTC(Y, M, D + 3 - ((w + 6) % 7))), y = th.getUTCFullYear();
    return { y, week: Math.floor((th - Date.UTC(y, 0, 1)) / 604_800_000) + 1 };
  };
  return fmt.replace(/%(-?)(.?)/g, (m, bare, c) => {
    const n = (x, width) => (bare ? String(x) : String(x).padStart(width, '0')); // (%-d: no leading zeros)
    switch (c) {
      case 'Y': return String(Y);
      case 'y': return n(Y % 100, 2);
      case 'm': return n(M + 1, 2);
      case 'd': return n(D, 2);
      case 'H': return n(h, 2);
      case 'I': return n(h % 12 || 12, 2);
      case 'M': return n(d.getUTCMinutes(), 2);
      case 'S': return n(d.getUTCSeconds(), 2);
      case 'j': return n(yday, 3);
      case 'U': return n(Math.floor((yday - 1 + 7 - w) / 7), 2); // week of the year, from its first Sunday
      case 'W': return n(Math.floor((yday - 1 + 7 - ((w + 6) % 7)) / 7), 2); // from its first Monday
      case 'V': return n(iso().week, 2);
      case 'G': return bare ? fail(m) : String(iso().y);
    }
    if (bare) fail(m);
    switch (c) {
      case 'e': return String(D);
      case 'p': return h < 12 ? 'AM' : 'PM';
      case 'f': return String(d.getUTCMilliseconds() * 1000).padStart(6, '0');
      case 'g': return String(d.getUTCMilliseconds()).padStart(3, '0'); // (milliseconds, as DuckDB has it)
      case 'a': return DAYS[w].slice(0, 3);
      case 'A': return DAYS[w];
      case 'w': return String(w);
      case 'u': return String(w || 7);
      case 'b': case 'h': return MONTHS[M].slice(0, 3);
      case 'B': return MONTHS[M];
      case 'F': case 'x': return `${Y}-${p2(M + 1)}-${p2(D)}`;
      case 'T': case 'X': return `${p2(h)}:${p2(d.getUTCMinutes())}:${p2(d.getUTCSeconds())}`;
      case 'c': return `${Y}-${p2(M + 1)}-${p2(D)} ${p2(h)}:${p2(d.getUTCMinutes())}:${p2(d.getUTCSeconds())}`;
      case 'z': return '+00'; // (times are UTC)
      case 'Z': return '';
      case '%': return '%';
      default: return fail(m);
    }
  });
  function fail(spec) { throw new SyntaxError(`vectro SQL: STRFTIME doesn't know ${spec} (it knows %Y %y %m %d %e %H %I %p %M %S %f %g %j %a %A %w %u %b %h %B %F %T %c %x %X %z %Z %U %W %V %G %%, and %-d %-m %-H %-I %-M %-S %-j %-y %-U %-W %-V without leading zeros)`); }
}

// name -> { args: types ('s' text, 'n' number, 't' time), min (args
// needed), rest (type of any further ones), out: 'text' | 'number', skip
// (missing values are left out instead of making the answer missing), f }
export const TEXT_FUNCTIONS = {
  UPPER: { args: 's', out: 'text', f: (s) => s.toUpperCase() },
  LOWER: { args: 's', out: 'text', f: (s) => s.toLowerCase() },
  LENGTH: { args: 's', out: 'number', f: (s) => chars(s).length },
  OCTET_LENGTH: { args: 's', out: 'number', f: (s) => new TextEncoder().encode(s).length },
  TRIM: { args: 'ss', min: 1, out: 'text', f: (s, set = ' ') => trimSet(s, set, true, true) },
  LTRIM: { args: 'ss', min: 1, out: 'text', f: (s, set = ' ') => trimSet(s, set, true, false) },
  RTRIM: { args: 'ss', min: 1, out: 'text', f: (s, set = ' ') => trimSet(s, set, false, true) },
  SUBSTRING: { args: 'snn', min: 2, out: 'text', f: substring },
  LEFT: { args: 'sn', out: 'text', f: (s, k) => { const cs = chars(s); return cs.slice(0, k < 0 ? Math.max(0, cs.length + k) : k).join(''); } },
  RIGHT: { args: 'sn', out: 'text', f: (s, k) => { const cs = chars(s); return k < 0 ? cs.slice(Math.min(cs.length, -k)).join('') : k === 0 ? '' : cs.slice(-k).join(''); } },
  REPLACE: { args: 'sss', out: 'text', f: (s, a, b) => (a === '' ? s : s.split(a).join(b)) },
  REVERSE: { args: 's', out: 'text', f: (s) => chars(s).reverse().join('') },
  REPEAT: { args: 'sn', out: 'text', f: (s, k) => (k > 0 ? s.repeat(Math.floor(k)) : '') },
  LPAD: { args: 'sns', min: 2, out: 'text', f: (s, k, fill = ' ') => { const cs = chars(s); if (cs.length >= k) return cs.slice(0, Math.max(0, k)).join(''); const f = chars(fill); if (!f.length) return s; const pad = []; while (pad.length < k - cs.length) pad.push(f[pad.length % f.length]); return pad.join('') + s; } },
  RPAD: { args: 'sns', min: 2, out: 'text', f: (s, k, fill = ' ') => { const cs = chars(s); if (cs.length >= k) return cs.slice(0, Math.max(0, k)).join(''); const f = chars(fill); if (!f.length) return s; const pad = []; while (pad.length < k - cs.length) pad.push(f[pad.length % f.length]); return s + pad.join(''); } },
  SPLIT_PART: { args: 'ssn', out: 'text', f: (s, d, k) => { const parts = d === '' ? [s] : s.split(d); const i = k > 0 ? k - 1 : k < 0 ? parts.length + k : -1; return i >= 0 && i < parts.length ? parts[i] : ''; } },
  STRPOS: { args: 'ss', out: 'number', f: (s, sub) => { const i = s.indexOf(sub); return i < 0 ? 0 : chars(s.slice(0, i)).length + 1; } },
  STARTS_WITH: { args: 'ss', out: 'number', f: (s, p) => (s.startsWith(p) ? 1 : 0) },
  ENDS_WITH: { args: 'ss', out: 'number', f: (s, p) => (s.endsWith(p) ? 1 : 0) },
  CONTAINS: { args: 'ss', out: 'number', f: (s, p) => (s.includes(p) ? 1 : 0) },
  ASCII: { args: 's', out: 'number', f: (s) => s.codePointAt(0) ?? NaN },
  CHR: { args: 'n', out: 'text', f: (k) => (k >= 0 && k <= 0x10ffff ? String.fromCodePoint(Math.floor(k)) : '') },
  REGEXP_MATCHES: { args: 'sss', min: 2, out: 'number', f: (s, p, fl) => (regex(p, fl).test(s) ? 1 : 0) },
  REGEXP_REPLACE: { args: 'ssss', min: 3, out: 'text', f: (s, p, rep, fl = '') => s.replace(regex(p, fl), rep.replace(/\$/g, '$$$$').replace(/\\(\d)/g, '$$$1')) },
  REGEXP_EXTRACT: { args: 'ssn', min: 2, out: 'text', f: (s, p, g = 0) => regex(p).exec(s)?.[g] ?? '' },
  STRFTIME: { args: 'ts', out: 'text', f: strftime },
  CONCAT: { args: '', rest: 's', out: 'text', skip: true, f: (...xs) => xs.filter((x) => x !== null).join('') },
  CONCAT_WS: { args: 's', rest: 's', out: 'text', skip: true, f: (sep, ...xs) => (sep === null ? '' : xs.filter((x) => x !== null).join(sep)) },
  '||': { args: '', rest: 's', out: 'text', f: (...xs) => xs.join('') },
  FORMAT: { args: 's', rest: 's', out: 'text', skip: true, f: (fmt, ...xs) => { // PostgreSQL's format(): %s, %I (a name, quoted when needed), %L (a literal)
    if (fmt === null) return '';
    let k = 0;
    return fmt.replace(/%([%sIL])/g, (m, c) => {
      if (c === '%') return '%';
      const x = xs[k++];
      if (c === 's') return x ?? '';
      if (c === 'I') return /^[a-z_][a-z0-9_$]*$/.test(x ?? '') ? x : `"${String(x ?? '').replaceAll('"', '""')}"`;
      return x === null || x === undefined ? 'NULL' : `'${String(x).replaceAll("'", "''")}'`;
    });
  } },
};
for (const [alias, name] of Object.entries({ UCASE: 'UPPER', LCASE: 'LOWER', LEN: 'LENGTH', CHAR_LENGTH: 'LENGTH', CHARACTER_LENGTH: 'LENGTH', STRLEN: 'OCTET_LENGTH', SUBSTR: 'SUBSTRING', INSTR: 'STRPOS', POSITION: 'STRPOS', PREFIX: 'STARTS_WITH', SUFFIX: 'ENDS_WITH', REGEXP_LIKE: 'REGEXP_MATCHES', STR_SPLIT_PART: 'SPLIT_PART' })) TEXT_FUNCTIONS[alias] = TEXT_FUNCTIONS[name];
