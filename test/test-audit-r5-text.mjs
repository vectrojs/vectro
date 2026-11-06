// Audit round 5 (text functions): LENGTH, UPPER / LOWER, SUBSTRING (all start /
// length forms), LEFT, RIGHT, TRIM, REPLACE, REVERSE, LPAD / RPAD, SPLIT_PART,
// STRPOS, STARTS_WITH, CONTAINS, ||, CONCAT, LIKE / ILIKE and text comparison on
// edge text ('', emoji with skin tones and ZWJ, combining accents, Turkish İ / ı,
// ß, right-to-left text, tabs and newlines, 100 KB and near-512 KB strings),
// checked against plain JS on tables and datasets, sync and async; the answers
// were checked against DuckDB in vectro-qa-2026-11-05/text. Also: REVERSE keeps
// emoji and accented letters whole; || keeps SQL's rule (an empty piece, which
// is missing text, makes it missing) while CONCAT skips it; LIKE ... ESCAPE; LIKE and ILIKE with several % take linear time
// (they were a backtracking regex: 10 s on 4 KB of English).
import { Engine, MODE } from './test-mode.mjs';
import { sql } from '../src/sql.mjs';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

let failures = 0, checks = 0;
const norm = (v) => (v === '' || v === null || v === undefined || (typeof v === 'number' && Number.isNaN(v)) ? null : typeof v === 'boolean' ? +v : v);
const same = (got, want, msg) => {
  checks++;
  const g = got.map(norm), w = want.map(norm), bad = g.length !== w.length ? -1 : g.findIndex((v, i) => v !== w[i]);
  if (g.length === w.length && bad < 0) return;
  failures++;
  if (failures <= 40) console.log(`FAIL ${msg}${bad >= 0 ? ` (first difference at ${JSON.stringify(KIT[bad] ?? '')})` : ''}\n  got  ${JSON.stringify(bad >= 0 ? g.slice(bad, bad + 3) : g.slice(0, 5)).slice(0, 300)}\n  want ${JSON.stringify(bad >= 0 ? w.slice(bad, bad + 3) : w.slice(0, 5)).slice(0, 300)}`);
};

const KIT = ['', 'a', 'abc', 'ABC', 'b', 'Hello World', ' padded ', '  ', 'é', 'é', '😀', '😀x😀', '👍🏽', '👨‍👩‍👧', 'İstanbul', 'ıi', 'Σίσυφος',
  'مرحبا', 'a%b', 'a_b', 'a\\b', "it's", 'x,y,z', 'a--b--c', '日本語', 'tab\there', 'line\nbreak', 'ÅNGSTRÖM', 'abcabc', 'aaa', 'a b c', '～', 'z',
  'ab😀é'.repeat(25000), 'x'.repeat(500 * 1024 - 3) + 'end'];
const S2 = KIT.map((_, i) => KIT[(i * 7 + 3) % 33]);
// (patterns with several % that don't match stay off the two big strings here: they backtrack; see the timed checks at the end)
const cs = (s) => Array.from(s);
const graphemes = (s) => Array.from(new Intl.Segmenter('en', { granularity: 'grapheme' }).segment(s), (x) => x.segment);
const substr = (s, start, len) => { const c = cs(s), n = c.length, at = start > 0 ? start - 1 : start === 0 ? -1 : n + start; let a = at, b = len === undefined ? Infinity : at + len; if (len !== undefined && len < 0) { a = at + len; b = at; } return c.slice(Math.max(0, a), Math.max(0, Math.min(n, b))).join(''); };
const trim = (s, set, l, r) => { const c = cs(s), d = new Set(cs(set)); let a = 0, b = c.length; if (l) while (a < b && d.has(c[a])) a++; if (r) while (b > a && d.has(c[b - 1])) b--; return c.slice(a, b).join(''); };
const pad = (s, k, f, left) => { const c = cs(s); if (c.length >= k) return c.slice(0, k).join(''); const fc = cs(f), p = []; while (p.length < k - c.length) p.push(fc[p.length % fc.length]); return left ? p.join('') + s : s + p.join(''); };
// SQL LIKE: % any run, _ one character (code point); no escape character unless ESCAPE is given
const likeRef = (s, pat, ci = false, esc = null) => {
  const lower = (x) => (ci ? x.replace(/[İΣ]/g, (c) => (c === 'İ' ? 'i' : 'σ')).toLowerCase() : x);
  const P = cs(lower(pat)), toks = [];
  for (let i = 0; i < P.length; i++) toks.push(P[i] === esc ? { lit: P[++i] } : P[i] === '%' ? '%' : P[i] === '_' ? '_' : { lit: P[i] });
  const T = cs(lower(s));
  let ti = 0, pi = 0, star = -1, mark = 0; // greedy matching with one backtrack point (linear-ish)
  while (ti < T.length) {
    if (pi < toks.length && (toks[pi] === '_' || (toks[pi] !== '%' && toks[pi].lit === T[ti]))) { ti++; pi++; } else if (pi < toks.length && toks[pi] === '%') { star = pi++; mark = ti; } else if (star >= 0) { pi = star + 1; ti = ++mark; } else return false;
  }
  while (pi < toks.length && toks[pi] === '%') pi++;
  return pi === toks.length;
};
const F = { // SQL -> JS on a present value (missing in gives missing out)
  'LENGTH(s)': (s) => cs(s).length, 'UPPER(s)': (s) => s.toUpperCase(), 'LOWER(s)': (s) => s.replace(/[İΣ]/g, (c) => (c === 'İ' ? 'i' : 'σ')).toLowerCase(),
  'REVERSE(s)': (s) => graphemes(s).reverse().join(''), 'TRIM(s)': (s) => trim(s, ' ', 1, 1), 'LTRIM(s)': (s) => trim(s, ' ', 1, 0), 'RTRIM(s)': (s) => trim(s, ' ', 0, 1), "TRIM(s, 'a ')": (s) => trim(s, 'a ', 1, 1),
  'SUBSTRING(s, 2)': (s) => substr(s, 2), 'SUBSTRING(s, 0, 2)': (s) => substr(s, 0, 2), 'SUBSTRING(s, -2)': (s) => substr(s, -2), 'SUBSTRING(s, -3, 2)': (s) => substr(s, -3, 2),
  'SUBSTRING(s, 3, -2)': (s) => substr(s, 3, -2), 'SUBSTRING(s FROM 2 FOR 3)': (s) => substr(s, 2, 3), 'SUBSTRING(s, 99990, 12)': (s) => substr(s, 99990, 12),
  'LEFT(s, 2)': (s) => cs(s).slice(0, 2).join(''), 'LEFT(s, -1)': (s) => cs(s).slice(0, -1).join(''), 'RIGHT(s, 2)': (s) => cs(s).slice(-2).join(''), 'RIGHT(s, -1)': (s) => cs(s).slice(1).join(''),
  "REPLACE(s, 'a', 'XY')": (s) => s.split('a').join('XY'), "REPLACE(s, 'é', 'E')": (s) => s.split('é').join('E'), 'REPEAT(s, 2)': (s) => s.repeat(2),
  "LPAD(s, 6, '*')": (s) => pad(s, 6, '*', 1), "RPAD(s, 6, 'ab')": (s) => pad(s, 6, 'ab', 0), "LPAD(s, 5, '😀')": (s) => pad(s, 5, '😀', 1),
  "SPLIT_PART(s, ',', 2)": (s) => s.split(',')[1] ?? '', "SPLIT_PART(s, '--', -1)": (s) => s.split('--').at(-1),
  "STRPOS(s, 'b')": (s) => { const i = s.indexOf('b'); return i < 0 ? 0 : cs(s.slice(0, i)).length + 1; }, "STRPOS(s, 'end')": (s) => { const i = s.indexOf('end'); return i < 0 ? 0 : cs(s.slice(0, i)).length + 1; },
  "STARTS_WITH(s, 'a')": (s) => +s.startsWith('a'), "CONTAINS(s, 'b')": (s) => +s.includes('b'), "s || '!'": (s) => s + '!', "CONCAT(s, NULL, 'x')": (s) => s + 'x',
  "s LIKE 'a%'": (s) => +likeRef(s, 'a%'), "s LIKE '%b%'": (s) => +likeRef(s, '%b%'), "s LIKE '_'": (s) => +likeRef(s, '_'), "s LIKE 'a_b'": (s) => +likeRef(s, 'a_b'), "s LIKE '%😀'": (s) => +likeRef(s, '%😀'),
  "s ILIKE 'ABC%'": (s) => +likeRef(s, 'ABC%', true), "s ILIKE 'İ%'": (s) => +likeRef(s, 'İ%', true), "s ILIKE 'σ%'": (s) => +likeRef(s, 'σ%', true),
  "s LIKE 'a$%b' ESCAPE '$'": (s) => +likeRef(s, 'a$%b', false, '$'), "s LIKE 'a!_%' ESCAPE '!'": (s) => +likeRef(s, 'a!_%', false, '!'),
  "s < 'b'": (s) => +(s.codePointAt(0) < 98), // text is compared by code point
  // || keeps SQL's rule (a missing piece makes it missing, and '' is missing text); CONCAT skips missing pieces
  'UPPER(SUBSTRING(s, 1, 1)) || LOWER(SUBSTRING(s, 2))': (s) => (cs(s).length > 1 ? cs(s)[0].toUpperCase() + F['LOWER(s)'](substr(s, 2)) : NaN), 's || SUBSTRING(s, 999999)': () => NaN,
  'CONCAT(UPPER(SUBSTRING(s, 1, 1)), LOWER(SUBSTRING(s, 2)))': (s) => cs(s)[0].toUpperCase() + F['LOWER(s)'](substr(s, 2)), 'CONCAT(s, SUBSTRING(s, 999999))': (s) => s,
};

const dir = mkdtempSync(join(tmpdir(), 'vectro-r5-text-'));
for (const [opts, source] of [[{}, 'table'], [{ threads: 1 }, 'table'], [{}, 'dataset'], [{}, 'async']]) {
  const e = new Engine(opts), tag = `${JSON.stringify(opts)} ${source}`;
  if (source === 'dataset' && !e.writeDataset) { await e.close(); continue; } // the portable engine has no file datasets
  let t = e.table({ id: Float64Array.from(KIT, (_, i) => i), s: KIT, s2: S2 });
  if (source === 'dataset') { e.writeDataset(join(dir, 't.vectro'), t, { rowsPerChunk: 5 }); t = e.openDataset(join(dir, 't.vectro')); }
  const run = async (q) => (source === 'async' ? sql(q, { t }).toRowsAsync() : sql(q, { t }).toRows());
  for (const [x, f] of Object.entries(F)) {
    let got; try { got = (await run(`SELECT id, ${x} AS x FROM t`)).sort((a, b) => a.id - b.id).map((r) => r.x); } catch (err) { got = [err.message]; }
    same(got, KIT.map((s) => (s === '' ? (x === "CONCAT(s, NULL, 'x')" ? 'x' : null) : f(s))), `${tag} ${x}`); // (CONCAT skips missing pieces)
  }
  for (const w of ["s LIKE 'a%'", "s ILIKE '%B%'", "s LIKE '%b%_'", "CONTAINS(s, 'é')", "LENGTH(s) = 1"]) {
    let got; try { got = (await run(`SELECT id FROM t WHERE ${w}`)).map((r) => r.id).sort((a, b) => a - b); } catch (err) { got = [err.message]; }
    const f = w === "s LIKE '%b%_'" ? (s) => likeRef(s, '%b%_') : F[w] ?? (w === "s ILIKE '%B%'" ? (s) => likeRef(s, '%B%', true) : w === "CONTAINS(s, 'é')" ? (s) => s.includes('é') : (s) => cs(s).length === 1);
    same(got, KIT.map((s, i) => (s !== '' && f(s) ? i : -1)).filter((i) => i >= 0), `${tag} WHERE ${w}`);
  }
  // the dataframe helpers
  for (const [x, f] of [["like(s, 'a%')", (s) => likeRef(s, 'a%')], ["ilike(s, 'İ%')", (s) => likeRef(s, 'İ%', true)], ["startsWith(s, 'a')", (s) => s.startsWith('a')], ["endsWith(s, 'c')", (s) => s.endsWith('c')], ["contains(s, 'b')", (s) => s.includes('b')]])
    same([source === 'async' ? (await (await t.filter(x).collectAsync()).toRowsAsync()).length : t.filter(x).count()], [KIT.filter((s) => s !== '' && f(s)).length], `${tag} filter(${x}).count()`);
  if (t.isDataset) t.close(); // (Windows: open files can't be deleted below)
  await e.close();
}
rmSync(dir, { recursive: true, force: true });

// LIKE / ILIKE with several % must not backtrack: 4 KB of English, 20 KB of 'a' (each case in its own process, 5 s cap)
if (MODE === 'node') {
  const words = 'the quick brown fox jumps over the lazy dog and then a cat sat on a mat in the hall'.split(' ');
  const eng = Array.from({ length: 900 }, (_, i) => words[(i * 7) % words.length]).join(' ').slice(0, 4000);
  for (const [label, s, q] of [
    ['LIKE %a%e%o%zzz on 4 KB of English', eng, "SELECT COUNT(*) AS n FROM t WHERE s LIKE '%a%e%o%zzz'"],
    ['ILIKE %A%E%O%ZZZ on 4 KB of English', eng, "SELECT COUNT(*) AS n FROM t WHERE s ILIKE '%A%E%O%ZZZ'"],
    ['LIKE %a%a%a%a%b on 20 KB of a', 'a'.repeat(20000), "SELECT COUNT(*) AS n FROM t WHERE s LIKE '%a%a%a%a%b'"],
    ['filter like(s, %a%a%a%b) on 20 KB of a', 'a'.repeat(20000), ''],
  ]) {
    const r = spawnSync(process.execPath, ['--input-type=module', '-e', `
      import { Engine } from ${JSON.stringify(new URL('../src/node.mjs', import.meta.url).href)}; import { sql } from ${JSON.stringify(new URL('../src/sql.mjs', import.meta.url).href)};
      const e = new Engine({ threads: 1 }), t = e.table({ s: [process.env.S, 'x'] });
      console.log(process.env.Q ? sql(process.env.Q, { t }).toRows()[0].n : t.filter("like(s, '%a%a%a%b')").count()); await e.close();`],
      { encoding: 'utf8', timeout: 5000, env: { ...process.env, Q: q, S: s } });
    same([r.stdout.trim()], ['0'], `${label} answers within 5 s (${r.signal ? 'killed after 5 s' : 'ok'})`);
  }
}

// T7: a regular expression whose repeated group repeats itself or has | is refused (JavaScript's
// regular expressions backtrack: '(a+)+$' on 30 a's and a b didn't finish); others still work
{
  const e = new Engine({ threads: 1 }), t = e.table({ s: ['a'.repeat(30) + 'b', 'ab@x.org'] });
  const ask = (q) => { try { return [JSON.stringify(sql(q, { t }).toRows())]; } catch (err) { return [/could take very long/.test(err.message) ? 'refused' : 'error: ' + err.message]; } };
  for (const q of ["SELECT COUNT(*) AS n FROM t WHERE REGEXP_MATCHES(s, '(a+)+$')", "SELECT REGEXP_REPLACE(s, '(a|aa)*c', 'x') AS r FROM t", "SELECT COUNT(*) AS n FROM t WHERE s ~ '((ab)+)+c'", "SELECT REGEXP_EXTRACT(s, '(?:a*){2,}c') AS r FROM t"]) same(ask(q), ['refused'], `T7 refused: ${q}`);
  same(ask("SELECT REGEXP_EXTRACT(s, '([a-z]+)@', 1) AS r FROM t"), [JSON.stringify([{ r: '' }, { r: 'ab' }])], 'T7: a plain group still works');
  same(ask("SELECT COUNT(*) AS n FROM t WHERE REGEXP_MATCHES(s, '^(?:a){2}[(a+)]+')"), [JSON.stringify([{ n: 1 }])], 'T7: a bounded group and a class with ( + ) still work');
  await e.close();
}

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} round-5 text checks passed`);
process.exit(failures ? 1 : 0);
