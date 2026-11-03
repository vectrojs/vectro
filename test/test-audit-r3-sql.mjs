// SQL answers round 3 of the QA audit (3 Nov 2026) found different from DuckDB's, each
// checked against DuckDB's answer (written out here, so DuckDB isn't needed): values
// used as conditions (WHERE f, CASE WHEN f, NOT NULL), (condition) IS NULL, unknown
// conditions used as values (COALESCE, CAST, windows, ORDER BY), text comparisons in
// code point order, RANGE frames over dates and with NULLS FIRST, ILIKE and 'İ'.
// Missing values print as null; booleans are 1 / 0.
import { Engine, sql } from '../src/node.mjs';

let failures = 0, checks = 0;
const check = (ok, msg) => { checks++; if (!ok) { failures++; if (failures <= 20) console.log('FAIL', msg); } };
const same = (a, b, msg) => { const x = JSON.stringify(a), y = JSON.stringify(b); check(x === y, `${msg}\n  got  ${x.slice(0, 400)}\n  want ${y.slice(0, 400)}`); };
const plain = (v) => (typeof v === 'number' && v !== v ? null : v);
const rows = (q, tables) => sql(q, tables).toRows().map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, plain(v)])));
const col = (q, tables, name) => rows(q, tables).map((r) => r[name]);

for (const threads of [undefined, 1]) {
  const e = new Engine(threads ? { threads } : {});
  const when = threads ? ' (threads: 1)' : '';
  const t = e.table({ id: [1, 2, 3, 4], a: [5, -3, NaN, 7], f: Float64Array.from([1, 0, NaN, NaN]), g: [1, 1, 2, 2] });

  // r01: a value used as a condition is true only where it is there and not 0
  {
    same(col('WITH c AS (SELECT id, a > 0 AS pos FROM t) SELECT id FROM c WHERE pos ORDER BY id', { t }, 'id'), [1, 4], `WHERE a condition's value${when}`);
    same(col('WITH c AS (SELECT id, a > 0 AS pos FROM t) SELECT id FROM c WHERE pos OR id = 1 ORDER BY id', { t }, 'id'), [1, 4], `OR on a condition's value${when}`);
    same(col('WITH c AS (SELECT id, a > 0 AS pos FROM t) SELECT id, CASE WHEN pos THEN 1 ELSE 0 END AS k FROM c ORDER BY id', { t }, 'k'), [1, 0, 0, 1], `CASE WHEN a condition's value${when}`);
    same(col('SELECT id FROM t WHERE f ORDER BY id', { t }, 'id'), [1], `WHERE a number column${when}`);
    same(col('SELECT id FROM t WHERE NOT f ORDER BY id', { t }, 'id'), [2], `WHERE NOT a number column${when}`);
    same(rows('SELECT CASE WHEN NULL THEN 1 ELSE 0 END AS c1, NOT NULL AS c2 FROM t LIMIT 1', { t }), [{ c1: 0, c2: null }], `CASE WHEN NULL, NOT NULL${when}`);
    same(rows('SELECT id, NOT f AS nf, f AND TRUE AS ft, f OR FALSE AS fo FROM t ORDER BY id', { t }),
      [{ id: 1, nf: 0, ft: 1, fo: 1 }, { id: 2, nf: 1, ft: 0, fo: 0 }, { id: 3, nf: null, ft: null, fo: null }, { id: 4, nf: null, ft: null, fo: null }], `NOT, AND, OR of a number column${when}`);
    same(col("SELECT id, CASE WHEN f THEN 'y' ELSE 'n' END AS k FROM t ORDER BY id", { t }, 'k'), ['y', 'n', 'n', 'n'], `CASE WHEN f, text${when}`);
    same(rows('SELECT g, COUNT(*) AS n FROM t GROUP BY g HAVING MAX(f) ORDER BY g', { t }), [{ g: 1, n: 2 }], `HAVING a number${when}`);
    same(rows('SELECT a > 0 AS p, COUNT(*) AS n FROM t GROUP BY a > 0 HAVING a > 0', { t }), [{ p: 1, n: 2 }], `HAVING a GROUP BY condition${when}`);
    const u = e.table({ k: [1, 2, 3, 4], b: [1, NaN, 0, 2] });
    same(rows('SELECT t.id, u.k FROM t JOIN u ON t.id = u.k AND u.b ORDER BY t.id', { t, u }), [{ id: 1, k: 1 }, { id: 4, k: 4 }], `JOIN ON a number column${when}`);
  }

  // r02: (condition) IS NULL is true where the condition is unknown
  {
    same(col('SELECT id, (a > 0) IS NULL AS u FROM t ORDER BY id', { t }, 'u'), [0, 0, 1, 0], `(a > 0) IS NULL${when}`);
    same(col('SELECT COUNT(*) AS n FROM t WHERE (a > 0) IS NULL', { t }, 'n'), [1], `WHERE (a > 0) IS NULL${when}`);
    same(col('SELECT COUNT(*) AS n FROM t WHERE (a = 5 AND id > 0) IS NOT NULL', { t }, 'n'), [3], `WHERE (... AND ...) IS NOT NULL${when}`);
    same(col('SELECT COUNT(*) AS n FROM t WHERE (a IN (5, 7)) IS NULL', { t }, 'n'), [1], `WHERE (a IN (...)) IS NULL${when}`);
    same(rows('SELECT id, (f = 1) IS NULL AS x, (a > 0 OR f = 1) IS NOT NULL AS y FROM t ORDER BY id', { t }),
      [{ id: 1, x: 0, y: 1 }, { id: 2, x: 0, y: 1 }, { id: 3, x: 1, y: 0 }, { id: 4, x: 1, y: 1 }], `IS NULL / IS NOT NULL selected${when}`);
  }

  // r03: an unknown condition used as a value is NULL, not FALSE
  {
    same(rows('SELECT id, COALESCE(a > 0, TRUE) AS c, NULLIF(a > 0, TRUE) AS n, (a > 0) IS DISTINCT FROM FALSE AS dist FROM t ORDER BY id', { t }),
      [{ id: 1, c: 1, n: null, dist: 1 }, { id: 2, c: 0, n: 0, dist: 0 }, { id: 3, c: 1, n: null, dist: 1 }, { id: 4, c: 1, n: null, dist: 1 }], `COALESCE, NULLIF, IS DISTINCT FROM${when}`);
    same(rows('SELECT id, IFNULL(a > 0, FALSE) AS i, GREATEST(a > 0, FALSE) AS g, CASE WHEN id > 10 THEN TRUE ELSE a > 0 END AS c FROM t ORDER BY id', { t }),
      [{ id: 1, i: 1, g: 1, c: 1 }, { id: 2, i: 0, g: 0, c: 0 }, { id: 3, i: 0, g: 0, c: null }, { id: 4, i: 1, g: 1, c: 1 }], `IFNULL, GREATEST, CASE ... ELSE condition${when}`);
    same(rows('SELECT SUM(CAST(a > 0 AS INT)) AS s, COUNT(CAST(a > 0 AS INT)) AS c, AVG(CAST(a > 0 AS INT)) AS av FROM t', { t }), [{ s: 2, c: 3, av: 2 / 3 }], `CAST(condition AS INT)${when}`);
    same(rows('SELECT id, COUNT(*) OVER (PARTITION BY a > 0) AS n, RANK() OVER (ORDER BY a > 0) AS r FROM t ORDER BY id', { t }),
      [{ id: 1, n: 2, r: 2 }, { id: 2, n: 1, r: 1 }, { id: 3, n: 1, r: 4 }, { id: 4, n: 2, r: 2 }], `PARTITION BY / OVER (ORDER BY) a condition${when}`);
    same(rows('SELECT id, COUNT(a > 0) OVER () AS c, SUM(CAST(a > 0 AS INT)) OVER (ORDER BY id) AS s FROM t ORDER BY id', { t }),
      [{ id: 1, c: 3, s: 1 }, { id: 2, c: 3, s: 1 }, { id: 3, c: 3, s: 1 }, { id: 4, c: 3, s: 2 }], `windows over a condition${when}`);
    same(col('SELECT id FROM t ORDER BY a > 0, id', { t }, 'id'), [2, 1, 4, 3], `ORDER BY a condition${when}`);
    same(col('SELECT id FROM t ORDER BY a > 0 DESC, id', { t }, 'id'), [1, 4, 2, 3], `ORDER BY a condition DESC${when}`);
  }

  // r04: text comparisons in code point order, as ORDER BY sorts
  {
    const s = e.table({ s: ['ﬁ', '😀', 'ｚ', 'z'] });
    same(col("SELECT s FROM s WHERE s > 'ﬁ' ORDER BY s", { s }, 's'), ['ｚ', '😀'], `text >${when}`);
    same(col("SELECT s, s < '😀' AS lt FROM s ORDER BY s", { s }, 'lt'), [1, 1, 1, 0], `text < selected${when}`);
    same(col("SELECT s FROM s WHERE s BETWEEN 'a' AND 'ｚ' ORDER BY s", { s }, 's'), ['z', 'ﬁ', 'ｚ'], `text BETWEEN${when}`);
    same(rows('SELECT a.s AS lo, b.s AS hi FROM s a CROSS JOIN s b WHERE a.s < b.s ORDER BY a.s, b.s', { s }),
      [{ lo: 'z', hi: 'ﬁ' }, { lo: 'z', hi: 'ｚ' }, { lo: 'z', hi: '😀' }, { lo: 'ﬁ', hi: 'ｚ' }, { lo: 'ﬁ', hi: '😀' }, { lo: 'ｚ', hi: '😀' }], `text < across a join${when}`);
  }

  // r05: RANGE with a distance and NULLS FIRST
  {
    const w = e.table({ id: [1, 2, 3, 4], o: [1, 2, NaN, 3], v: [10, 20, 30, 40] });
    same(col('SELECT id, SUM(v) OVER (ORDER BY o NULLS FIRST RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM w ORDER BY id', { w }, 's'), [10, 30, 30, 60], `RANGE, NULLS FIRST${when}`);
    same(col('SELECT id, SUM(v) OVER (ORDER BY o DESC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM w ORDER BY id', { w }, 's'), [30, 70, 30, 60], `RANGE, DESC NULLS FIRST${when}`);
    same(col('SELECT id, SUM(v) OVER (ORDER BY o NULLS LAST RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM w ORDER BY id', { w }, 's'), [10, 30, 30, 60], `RANGE, NULLS LAST${when}`);
  }

  // r06: RANGE n PRECEDING over dates: n days
  {
    const d = e.table({ id: [1, 2, 3, 4], dt: [new Date(Date.UTC(2024, 1, 27)), new Date(Date.UTC(2024, 1, 28)), new Date(Date.UTC(2024, 1, 29)), new Date(Date.UTC(2024, 2, 5))], v: [1, 10, 100, 1000] });
    same(col('SELECT id, SUM(v) OVER (ORDER BY dt RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM d ORDER BY id', { d }, 's'), [1, 11, 110, 1000], `RANGE 1 PRECEDING over dates${when}`);
    same(col('SELECT id, COUNT(*) OVER (ORDER BY dt RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS n FROM d ORDER BY id', { d }, 'n'), [3, 3, 3, 1], `RANGE 2 PRECEDING / FOLLOWING over dates${when}`);
    same(col("SELECT id, SUM(v) OVER (ORDER BY dt RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) AS s FROM d ORDER BY id", { d }, 's'), [1, 11, 110, 1000], `RANGE INTERVAL over dates${when}`);
  }

  // r07: ILIKE lowercases both sides as DuckDB does ('İ' is 'i')
  {
    const s = e.table({ s: ['İ', 'i', 'I', 'ı'] });
    same(rows("SELECT s, s ILIKE 'i%' AS a, s ILIKE 'İ%' AS b, s ILIKE 'I' AS c FROM s", { s }),
      [{ s: 'İ', a: 1, b: 1, c: 1 }, { s: 'i', a: 1, b: 1, c: 1 }, { s: 'I', a: 1, b: 1, c: 1 }, { s: 'ı', a: 0, b: 0, c: 0 }], `ILIKE and dotted capital I${when}`);
  }

  await e.close();
}

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} SQL audit round-3 checks passed`);
process.exit(failures ? 1 : 0);
