// SQL answers the QA audit of 3 Nov 2026 found different from DuckDB's, each
// checked against DuckDB's answer (written out here, so DuckDB isn't needed):
// set-operation precedence and NULL columns, NULLS FIRST after set operations,
// in windows and with DISTINCT, window functions over text, windowed AVG next
// to huge values, RANGE frames with a NULL order value, regular expressions on
// missing text, date arithmetic, QUANTILE, STRFTIME, literal IS NULL and
// MIN / MAX of dates. Missing values print as null ('' for text, as Vectro
// keeps it); booleans are 1 / 0; dates as ISO text.
import { Engine, sql } from '../src/node.mjs';

let failures = 0, checks = 0;
const check = (ok, msg) => { checks++; if (!ok) { failures++; if (failures <= 20) console.log('FAIL', msg); } };
const same = (a, b, msg) => { const x = JSON.stringify(a), y = JSON.stringify(b); check(x === y, `${msg}\n  got  ${x.slice(0, 400)}\n  want ${y.slice(0, 400)}`); };
const throwsLike = (fn, re, msg) => { try { fn(); check(false, `${msg}: no error`); } catch (err) { check(re.test(err.message), `${msg}: ${err.message}`); } };
const plain = (v) => (v instanceof Date ? v.toISOString().slice(0, v.getUTCHours() || v.getUTCMinutes() || v.getUTCSeconds() ? 19 : 10) : typeof v === 'number' && v !== v ? null : v);
const rows = (q, tables) => sql(q, tables).toRows().map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, plain(v)])));
const col = (q, tables, name) => rows(q, tables).map((r) => r[name]);

for (const threads of [undefined, 1]) {
  const e = new Engine(threads ? { threads } : {});
  const when = threads ? ' (threads: 1)' : '';

  // b01: INTERSECT binds tighter than UNION and EXCEPT
  {
    const T = { a: e.table({ x: [1, 2, 3] }), b: e.table({ x: [3, 4] }), c: e.table({ x: [4, 5] }) };
    same(col('SELECT x FROM a UNION ALL SELECT x FROM b INTERSECT SELECT x FROM c ORDER BY x', T, 'x'), [1, 2, 3, 4], `UNION ALL ... INTERSECT${when}`);
    same(col('SELECT x FROM a EXCEPT SELECT x FROM b INTERSECT SELECT x FROM c ORDER BY x', T, 'x'), [1, 2, 3], `EXCEPT ... INTERSECT${when}`);
    same(col('SELECT x FROM a UNION SELECT x FROM b INTERSECT SELECT x FROM c ORDER BY x', T, 'x'), [1, 2, 3, 4], `UNION ... INTERSECT${when}`);
    same(col('SELECT x FROM a INTERSECT SELECT x FROM b UNION SELECT x FROM c ORDER BY x', T, 'x'), [3, 4, 5], `INTERSECT ... UNION${when}`);
    same(col('SELECT x FROM a EXCEPT SELECT x FROM b EXCEPT SELECT x FROM a ORDER BY x', T, 'x'), [], `EXCEPT, EXCEPT: left to right${when}`);
  }

  // b10, b17: NULL literals in a text column of a set operation take the other side's type
  {
    const t = e.table({ id: [1, 2], s: ['a', 'b'], n: [1, 2] });
    same(rows('SELECT s FROM t EXCEPT SELECT NULL ORDER BY s', { t }), [{ s: 'a' }, { s: 'b' }], `EXCEPT SELECT NULL${when}`);
    same(rows('SELECT s FROM t INTERSECT SELECT NULL', { t }), [], `INTERSECT SELECT NULL${when}`);
    same(rows('SELECT s FROM t UNION SELECT s FROM t EXCEPT SELECT NULL ORDER BY s', { t }), [{ s: 'a' }, { s: 'b' }], `UNION then EXCEPT SELECT NULL${when}`);
    same(rows('SELECT NULL AS s UNION ALL SELECT s FROM t', { t }), [{ s: '' }, { s: 'a' }, { s: 'b' }], `NULL first in a UNION ALL${when}`);
    same(rows('SELECT id, NULL AS a, NULL AS b FROM t UNION ALL SELECT id, s, UPPER(s) FROM t', { t }),
      [{ id: 1, a: '', b: '' }, { id: 2, a: '', b: '' }, { id: 1, a: 'a', b: 'A' }, { id: 2, a: 'b', b: 'B' }], `two NULL columns first in a UNION ALL${when}`);
    same(rows('SELECT id, NULL AS a, NULL AS b FROM t UNION SELECT id, s, s FROM t ORDER BY id, a NULLS FIRST', { t }),
      [{ id: 1, a: '', b: '' }, { id: 1, a: 'a', b: 'a' }, { id: 2, a: '', b: '' }, { id: 2, a: 'b', b: 'b' }], `two NULL columns first in a UNION${when}`);
    same(rows('SELECT s, NULL AS x, n FROM t EXCEPT SELECT s, NULL, n FROM t WHERE n = 1', { t }), [{ s: 'b', x: null, n: 2 }], `a NULL number column in EXCEPT${when}`);
  }

  // b03: NULLS FIRST on text after a set operation
  {
    const t = e.table({ s: ['b', '', 'a'] });
    same(col("SELECT s FROM t UNION SELECT 'z' ORDER BY 1 NULLS FIRST", { t }, 's'), ['', 'a', 'b', 'z'], `UNION ORDER BY 1 NULLS FIRST${when}`);
    same(col("SELECT s FROM t UNION ALL SELECT 'z' ORDER BY s DESC NULLS FIRST LIMIT 2", { t }, 's'), ['', 'z'], `UNION ALL ORDER BY s DESC NULLS FIRST${when}`);
    same(col("SELECT s FROM t UNION SELECT 'z' ORDER BY 1", { t }, 's'), ['a', 'b', 'z', ''], `UNION ORDER BY 1 (NULLS LAST)${when}`);
  }

  // b02: NULLS FIRST on text inside OVER (ORDER BY ...)
  {
    const t = e.table({ id: [1, 2, 3, 4], s: ['b', '', 'a', ''] });
    same(col('SELECT id, ROW_NUMBER() OVER (ORDER BY s NULLS FIRST, id) AS rn FROM t ORDER BY id', { t }, 'rn'), [4, 1, 3, 2], `window ORDER BY s NULLS FIRST${when}`);
    same(col('SELECT id, ROW_NUMBER() OVER (ORDER BY s DESC NULLS FIRST, id) AS rn FROM t ORDER BY id', { t }, 'rn'), [3, 1, 4, 2], `window ORDER BY s DESC NULLS FIRST${when}`);
    same(col('SELECT id, LAG(id) OVER (ORDER BY s NULLS FIRST, id) AS prev FROM t ORDER BY id', { t }, 'prev'), [3, null, 4, 2], `LAG over ORDER BY s NULLS FIRST${when}`);
    same(col('SELECT id, ROW_NUMBER() OVER (ORDER BY s, id) AS rn FROM t ORDER BY id', { t }, 'rn'), [2, 3, 1, 4], `window ORDER BY s (NULLS LAST)${when}`);
  }

  // b09: SELECT DISTINCT ... ORDER BY ... NULLS FIRST
  {
    const t = e.table({ p: [2, NaN, 1, 2], q: [1, 1, 1, 1] });
    same(col('SELECT DISTINCT p FROM t ORDER BY p DESC NULLS FIRST', { t }, 'p'), [null, 2, 1], `DISTINCT ORDER BY p DESC NULLS FIRST${when}`);
    same(col('SELECT DISTINCT p FROM t ORDER BY p NULLS FIRST', { t }, 'p'), [null, 1, 2], `DISTINCT ORDER BY p NULLS FIRST${when}`);
    same(col('SELECT DISTINCT p FROM t ORDER BY p DESC', { t }, 'p'), [2, 1, null], `DISTINCT ORDER BY p DESC${when}`);
    same(col('SELECT DISTINCT p FROM t ORDER BY -p', { t }, 'p'), [2, 1, null], `DISTINCT ORDER BY an expression of p${when}`);
    const s = e.table({ s: ['b', '', 'a', 'b'] });
    same(col('SELECT DISTINCT s FROM s ORDER BY s NULLS FIRST', { s }, 's'), ['', 'a', 'b'], `DISTINCT text ORDER BY NULLS FIRST${when}`);
    throwsLike(() => sql('SELECT DISTINCT p FROM t ORDER BY q', { t }).toRows(), /SELECT DISTINCT \.\.\. ORDER BY: order by the selected columns/, `DISTINCT ORDER BY a column not selected${when}`);
  }

  // b04, b05, b06: window functions over text
  {
    const t = e.table({ id: [1, 2, 3, 4], p: [1, 1, 2, 2], s: ['a', '', 'b', ''] });
    same(rows('SELECT id, COUNT(s) OVER (PARTITION BY p) AS n, COUNT(s) OVER (ORDER BY id) AS running FROM t ORDER BY id', { t }),
      [{ id: 1, n: 1, running: 1 }, { id: 2, n: 1, running: 1 }, { id: 3, n: 1, running: 2 }, { id: 4, n: 1, running: 2 }], `COUNT(text) OVER${when}`);
    same(col('SELECT COUNT(UPPER(s)) OVER () AS n FROM t', { t }, 'n'), [2, 2, 2, 2], `COUNT(text expression) OVER${when}`);
    const u = e.table({ id: [1, 2, 3], s: ['a', '', 'c'] });
    same(rows('SELECT id, LAG(s) OVER (ORDER BY id) AS prev, LEAD(s, 2) OVER (ORDER BY id) AS next2, NTH_VALUE(s, 5) OVER () AS nth, FIRST_VALUE(s) OVER (ORDER BY id ROWS BETWEEN 5 FOLLOWING AND 6 FOLLOWING) AS f FROM u ORDER BY id', { u }),
      [{ id: 1, prev: '', next2: 'c', nth: '', f: '' }, { id: 2, prev: 'a', next2: '', nth: '', f: '' }, { id: 3, prev: '', next2: '', nth: '', f: '' }], `LAG / LEAD / NTH_VALUE of text past the edge${when}`);
    same(rows('SELECT COUNT(prev) AS n, COUNT(*) AS "rows" FROM (SELECT LAG(s) OVER (ORDER BY id) AS prev FROM u) z', { u }), [{ n: 1, rows: 3 }], `COUNT of a text LAG${when}`);
    same(rows('SELECT COUNT(*) AS n FROM (SELECT LAG(s) OVER (ORDER BY id) AS prev FROM u) z WHERE prev IS NULL', { u }), [{ n: 2 }], `IS NULL on a text LAG${when}`);
    same(rows("SELECT COUNT(*) AS n FROM (SELECT LAG(s) OVER (ORDER BY id) AS prev FROM u) z WHERE prev = 'a'", { u }), [{ n: 1 }], `= on a text LAG${when}`);
    const w = e.table({ id: [1, 2, 3, 4], p: [1, 1, 2, 2], s: ['b', 'a', 'd', 'c'] });
    same(rows('SELECT id, MIN(s) OVER (PARTITION BY p) AS mn, MAX(s) OVER (PARTITION BY p) AS mx FROM w ORDER BY id', { w }),
      [{ id: 1, mn: 'a', mx: 'b' }, { id: 2, mn: 'a', mx: 'b' }, { id: 3, mn: 'c', mx: 'd' }, { id: 4, mn: 'c', mx: 'd' }], `MIN / MAX(text) OVER (PARTITION BY)${when}`);
    same(rows('SELECT id, MIN(s) OVER (ORDER BY id) AS mn, MAX(s) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS mx FROM t ORDER BY id', { t }),
      [{ id: 1, mn: 'a', mx: 'a' }, { id: 2, mn: 'a', mx: 'a' }, { id: 3, mn: 'a', mx: 'b' }, { id: 4, mn: 'a', mx: 'b' }], `MIN / MAX(text) OVER, missing text skipped${when}`);
    same(rows('SELECT MAX(s) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS mx FROM t WHERE id IN (2, 4)', { t }), [{ mx: '' }, { mx: '' }], `MAX(text) OVER of missing text only${when}`);
    for (const f of ['SUM', 'AVG']) throwsLike(() => sql(`SELECT ${f}(s) OVER (PARTITION BY p) AS m FROM w`, { w }).toRows(), new RegExp(`${f}\\(\\) of text is not supported`), `${f}(text) OVER: a clear error${when}`);
  }

  // b07: windowed AVG / SUM: a huge value in one partition takes no digits from the next ones
  {
    const t = e.table({ id: [1, 2, 3, 4, 5, 6], p: [1, 1, 1, 2, 2, 2], v: [1e17, 1.25, 2.5, 1.25, 2.5, 3.75] });
    same(rows('SELECT id, AVG(v) OVER (PARTITION BY p) AS avg_p, AVG(v) OVER (PARTITION BY p ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg2, SUM(v) OVER (PARTITION BY p) AS sum_p FROM t ORDER BY id', { t }), [
      { id: 1, avg_p: 33333333333333332, avg2: 100000000000000000, sum_p: 100000000000000000 },
      { id: 2, avg_p: 33333333333333332, avg2: 50000000000000000, sum_p: 100000000000000000 },
      { id: 3, avg_p: 33333333333333332, avg2: 1.875, sum_p: 100000000000000000 },
      { id: 4, avg_p: 2.5, avg2: 1.25, sum_p: 7.5 }, { id: 5, avg_p: 2.5, avg2: 1.875, sum_p: 7.5 }, { id: 6, avg_p: 2.5, avg2: 3.125, sum_p: 7.5 }], `AVG OVER next to 1e17${when}`);
    // long frames (prefix sums, not added row by row): 300 rows a partition
    const n = 900, p = Float64Array.from({ length: n }, (_, i) => Math.floor(i / 300)), v = Float64Array.from({ length: n }, (_, i) => (i === 0 ? 1e17 : 0.25 * (i % 7)));
    const big = e.table({ id: Float64Array.from({ length: n }, (_, i) => i), p, v });
    const want = [1, 2].map((k) => { let s = 0; for (let i = 300 * k; i < 300 * (k + 1); i++) s += v[i]; return s / 300; });
    same(rows('SELECT p, MIN(a) AS lo, MAX(a) AS hi FROM (SELECT p, AVG(v) OVER (PARTITION BY p) AS a FROM big) z WHERE p > 0 GROUP BY p ORDER BY p', { big }),
      [{ p: 1, lo: want[0], hi: want[0] }, { p: 2, lo: want[1], hi: want[1] }], `AVG OVER long partitions after 1e17${when}`);
    let run = 0;
    for (let i = 600; i < 900; i++) run += v[i];
    same(rows('SELECT s FROM (SELECT id, SUM(v) OVER (PARTITION BY p ORDER BY id) AS s FROM big) z WHERE id = 899', { big }), [{ s: run }], `running SUM in a later partition after 1e17${when}`);
  }

  // b13: RANGE frames with a NULL order value keep their unbounded side
  {
    const t = e.table({ id: [1, 2, 3, 4], o: [1, 2, 3, NaN], v: [10, 20, 30, 40] });
    same(rows('SELECT id, SUM(v) OVER (ORDER BY o RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS s, COUNT(*) OVER (ORDER BY o DESC RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS c FROM t ORDER BY id', { t }),
      [{ id: 1, s: null, c: 1 }, { id: 2, s: 10, c: 2 }, { id: 3, s: 30, c: 3 }, { id: 4, s: 100, c: 1 }], `RANGE frame, NULL order value${when}`);
    same(col('SELECT id, SUM(v) OVER (ORDER BY o RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM t ORDER BY id', { t }, 's'), [30, 60, 50, 40], `RANGE frame both sides bounded, NULL order value${when}`);
  }

  // b11: regular expressions on missing text are unknown, so NOT drops those rows
  {
    const t = e.table({ s: ['abc', '', 'xyz', ''] });
    same(rows("SELECT COUNT(*) AS n FROM t WHERE NOT (s ~ 'a')", { t }), [{ n: 1 }], `NOT (s ~ 'a')${when}`);
    same(rows("SELECT COUNT(*) AS n FROM t WHERE NOT (s ~* 'A')", { t }), [{ n: 1 }], `NOT (s ~* 'A')${when}`);
    same(rows("SELECT COUNT(*) AS n FROM t WHERE NOT (s !~ 'a')", { t }), [{ n: 1 }], `NOT (s !~ 'a')${when}`);
    same(col("SELECT s ~ 'a' AS m FROM t", { t }, 'm'), [1, null, 0, null], `s ~ 'a' as a value${when}`);
    same(col("SELECT NOT (s ~ 'a') AS m FROM t", { t }, 'm'), [0, null, 1, null], `NOT (s ~ 'a') as a value${when}`);
  }

  // b18: date + n is n days later; DATE - DATE a number of days
  {
    const t = e.table({ d: [new Date('2024-02-29T00:00:00Z'), new Date('2024-03-31T00:00:00Z')] });
    same(rows('SELECT d, d + 1 AS next_day, d - 7 AS week_before, 2 + d AS two FROM t', { t }),
      [{ d: '2024-02-29', next_day: '2024-03-01', week_before: '2024-02-22', two: '2024-03-02' }, { d: '2024-03-31', next_day: '2024-04-01', week_before: '2024-03-24', two: '2024-04-02' }], `date + integer${when}`);
    same(rows("SELECT DATE '2024-03-31' - DATE '2024-02-29' AS days", { t }), [{ days: 31 }], `DATE - DATE${when}`);
    same(rows('SELECT MAX(d) - MIN(d) AS span FROM t', { t }), [{ span: 31 }], `MAX(d) - MIN(d)${when}`);
    same(rows("SELECT d + INTERVAL '1 day' AS x, d - INTERVAL '12 hours' AS y FROM t LIMIT 1", { t }), [{ x: '2024-03-01', y: '2024-02-28T12:00:00' }], `date + INTERVAL${when}`);
    same(rows("SELECT COUNT(*) AS n FROM t WHERE d + 1 > DATE '2024-03-01'", { t }), [{ n: 1 }], `date + integer in WHERE${when}`);
    same(sql('SELECT d + 1 AS x, d - d AS y FROM t', { t }, { types: true }).columns, [{ name: 'x', type: 'day' }, { name: 'y', type: 'int' }], `types of date + integer, DATE - DATE${when}`);
    const ts = e.table({ a: [new Date('2024-01-01T00:00:00Z')], b: [new Date('2024-01-01T06:30:00Z')] });
    same(rows("SELECT b - a AS ms, b - a > INTERVAL '6 hours' AS longer FROM ts", { ts }), [{ ms: 23_400_000, longer: 1 }], `TIMESTAMP - TIMESTAMP stays an interval${when}`);
    throwsLike(() => sql('SELECT d + d AS x FROM t', { t }).toRows(), /a date plus a date has no meaning/, `date + date: an error${when}`);
    throwsLike(() => sql('SELECT 1 - d AS x FROM t', { t }).toRows(), /a number minus a date has no meaning/, `number - date: an error${when}`);
  }

  // MIN / MAX of a date column are dates
  {
    const t = e.table({ day: [new Date('2024-03-01'), new Date('2024-03-05'), new Date('2024-03-05')], g: [1, 1, 2] });
    const q = sql('SELECT g, MIN(day) AS first, MAX(day) AS last FROM t GROUP BY g ORDER BY g', { t }, { types: true });
    const r = q.toRows();
    check(r[0].first instanceof Date && r[0].last instanceof Date, `MIN / MAX of dates are Date objects${when}`);
    same(rows('SELECT g, MIN(day) AS first, MAX(day) AS last FROM t GROUP BY g ORDER BY g', { t }), [{ g: 1, first: '2024-03-01', last: '2024-03-05' }, { g: 2, first: '2024-03-05', last: '2024-03-05' }], `MIN / MAX of dates by group${when}`);
    same(q.columns.map((c) => c.type), ['int', 'day', 'day'], `MIN / MAX of dates: types${when}`);
    same(rows('SELECT MIN(day) AS first FROM t', { t }), [{ first: '2024-03-01' }], `MIN of dates, no GROUP BY${when}`);
    same(rows("SELECT g FROM t GROUP BY g HAVING MAX(day) > DATE '2024-03-02' AND MIN(day) < DATE '2024-03-02'", { t }), [{ g: 1 }], `MIN / MAX of dates in HAVING${when}`);
    same(rows('SELECT MEDIAN(day) AS m FROM t', { t }), [{ m: '2024-03-05' }], `MEDIAN of dates${when}`);
  }

  // b19: QUANTILE is QUANTILE_DISC (DuckDB)
  {
    const t = e.table({ x: [1, 2, 3, 4] });
    same(rows('SELECT QUANTILE(x, 0.5) AS q, QUANTILE_DISC(x, 0.5) AS qd, QUANTILE_CONT(x, 0.5) AS qc FROM t', { t }), [{ q: 2, qd: 2, qc: 2.5 }], `QUANTILE${when}`);
  }

  // b15: STRFTIME week numbers and no-padding forms; others refused
  {
    const t = e.table({ ts: ['2024-03-04T09:05:06Z', '2024-12-30T00:00:00Z', '2021-01-01T13:00:07Z', '2027-01-03T00:00:00Z', '2023-01-01T00:00:00Z', '2005-01-02T00:00:00Z'].map((s) => new Date(s)) });
    same(col("SELECT STRFTIME(ts, '%V|%G|%g|%U|%W|%-d|%-m|%-H|%-I|%-M|%-S|%-j|%-y') AS f FROM t", { t }, 'f'),
      ['10|2024|000|09|10|4|3|9|9|5|6|64|24', '01|2025|000|52|53|30|12|0|12|0|0|365|24', '53|2020|000|00|00|1|1|13|1|0|7|1|21', '53|2026|000|01|00|3|1|0|12|0|0|3|27', '52|2022|000|01|00|1|1|0|12|0|0|1|23', '53|2004|000|01|00|2|1|0|12|0|0|2|5'], `STRFTIME week numbers${when}`);
    same(col("SELECT STRFTIME(ts, 'week %V of %G, %U, %W') AS w FROM t LIMIT 2", { t }, 'w'), ['week 10 of 2024, 09, 10', 'week 01 of 2025, 52, 53'], `STRFTIME week text${when}`);
    same(col("SELECT STRFTIME(ts, '%-d/%-m %-H:%M') AS s FROM t LIMIT 2", { t }, 's'), ['4/3 9:05', '30/12 0:00'], `STRFTIME %-d %-m %-H${when}`);
    same(col("SELECT STRFTIME(ts, '%c|%x|%X|%z|%h') AS s FROM t LIMIT 1", { t }, 's'), ['2024-03-04 09:05:06|2024-03-04|09:05:06|+00|Mar'], `STRFTIME %c %x %X %z %h${when}`);
    throwsLike(() => sql("SELECT STRFTIME(ts, '%q') AS s FROM t", { t }).toRows(), /STRFTIME doesn't know %q/, `STRFTIME %q: an error${when}`);
    throwsLike(() => sql("SELECT STRFTIME(ts, '%-B') AS s FROM t", { t }).toRows(), /STRFTIME doesn't know %-B/, `STRFTIME %-B: an error${when}`);
  }

  // '' IS NULL and 'x' IS NULL on literals
  {
    const t = e.table({ name: ['a', ''] });
    same(rows("SELECT '' IS NULL AS a, 'x' IS NULL AS b, '' IS NOT NULL AS c, 'x' IS NOT NULL AS d FROM t LIMIT 1", { t }), [{ a: 1, b: 0, c: 0, d: 1 }], `literal IS NULL${when}`);
    same(rows("SELECT COUNT(*) AS n FROM t WHERE 'x' IS NOT NULL", { t }), [{ n: 2 }], `literal IS NOT NULL in WHERE${when}`);
  }

  await e.close();
}

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} SQL audit checks passed`);
process.exit(failures ? 1 : 0);
