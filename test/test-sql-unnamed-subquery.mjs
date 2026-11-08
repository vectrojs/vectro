// A subquery in FROM without a name, as DuckDB and PostgreSQL 16 allow: it is called
// unnamed_subquery (then unnamed_subquery2, ...). Answers checked against DuckDB 2.0 by hand.
import { Engine, sql } from '../src/node.mjs';

let failures = 0, checks = 0;
const same = (a, b, msg) => { checks++; const x = JSON.stringify(a), y = JSON.stringify(b); if (x !== y) { failures++; console.log(`FAIL ${msg}\n  got  ${x.slice(0, 300)}\n  want ${y.slice(0, 300)}`); } };
const throws = (f, text, msg) => { checks++; let m = ''; try { f(); } catch (err) { m = err.message; } if (!m.includes(text)) { failures++; console.log(`FAIL ${msg}: ${m || 'no error'}`); } };

for (const opts of [{}, { threads: 1 }]) {
  const e = new Engine(opts), tag = JSON.stringify(opts);
  const t = e.table({ id: [1, 2, 3, 4], g: ['a', 'b', 'a', 'c'], v: [10, 20, 30, 40] });
  const u = e.table({ g: ['a', 'b', 'd'], name: ['Ann', 'Bo', 'Di'] });
  const q = (text) => sql(text, { t, u }).toRows();

  same(q('SELECT * FROM (SELECT id FROM t WHERE v > 15) ORDER BY id'), [{ id: 2 }, { id: 3 }, { id: 4 }], `${tag} plain`);
  same(q('SELECT id FROM (SELECT id, v FROM t) WHERE v < 25 ORDER BY id'), [{ id: 1 }, { id: 2 }], `${tag} WHERE right after`);
  same(q('SELECT g, SUM(v) AS s FROM (SELECT g, v * 2 AS v FROM t) GROUP BY g ORDER BY g LIMIT 2'), [{ g: 'a', s: 80 }, { g: 'b', s: 40 }], `${tag} GROUP BY, ORDER BY, LIMIT after`);
  same(q('SELECT unnamed_subquery.id FROM (SELECT id FROM t) ORDER BY id DESC LIMIT 1'), [{ id: 4 }], `${tag} referenced as unnamed_subquery`);
  same(q('SELECT unnamed_subquery.id, unnamed_subquery2.name FROM (SELECT id, g FROM t) JOIN (SELECT g, name FROM u) ON unnamed_subquery.g = unnamed_subquery2.g ORDER BY id'),
    [{ id: 1, name: 'Ann' }, { id: 2, name: 'Bo' }, { id: 3, name: 'Ann' }], `${tag} two unnamed, joined`);
  same(q('SELECT n FROM (SELECT COUNT(*) AS n FROM (SELECT DISTINCT g FROM t))'), [{ n: 3 }], `${tag} nested`);
  throws(() => q('SELECT COUNT(*) AS n FROM (SELECT g FROM t), u'), 'JOIN ... ON', `${tag} comma join: the same message as for tables`);
  same(q("SELECT x FROM (SELECT g AS x FROM t UNION SELECT g FROM u) WHERE x <> 'a' ORDER BY x"), [{ x: 'b' }, { x: 'c' }, { x: 'd' }], `${tag} UNION inside`);
  same(q('SELECT s.id FROM (SELECT id FROM t) s WHERE s.id = 2'), [{ id: 2 }], `${tag} a name without AS still works`);
  same(q('SELECT s.id FROM (SELECT id FROM t) AS s WHERE s.id = 3'), [{ id: 3 }], `${tag} AS name still works`);
  same(q('WITH c AS (SELECT * FROM (SELECT g FROM u)) SELECT COUNT(*) AS n FROM c'), [{ n: 3 }], `${tag} inside a CTE`);
  throws(() => q('SELECT * FROM (SELECT id FROM t) AS'), 'expected a name', `${tag} AS with no name`);
  throws(() => q('SELECT * FROM (SELECT id FROM t'), '")"', `${tag} missing bracket`);
  await e.close();
}

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} unnamed-subquery checks passed`);
process.exit(failures ? 1 : 0);
