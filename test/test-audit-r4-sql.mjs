// Audit round 4 (SQL), against DuckDB's answers: text compared with a scalar
// subquery; a condition on the left of IN / BETWEEN stays unknown where it is;
// ILIKE and LOWER on computed text and literals lowercase as DuckDB does.
import { Engine, sql } from '../src/node.mjs';

let failures = 0, checks = 0;
const same = (a, b, msg) => { checks++; const x = JSON.stringify(a), y = JSON.stringify(b); if (x !== y) { failures++; if (failures <= 20) console.log(`FAIL ${msg}\n  got  ${x.slice(0, 300)}\n  want ${y.slice(0, 300)}`); } };

for (const opts of [{}, { threads: 1 }]) {
  const e = new Engine(opts), tag = JSON.stringify(opts);
  const q = (text, tables) => sql(text, tables).toRows();
  // text against (SELECT ...)
  const t = e.table({ id: [0, 1, 2, 3, 4], s: ['b', 'm', '', 'z', 'a'] }), r = e.table({ i: [1, 2], s: ['m', 'q'] });
  same(q("SELECT id FROM t WHERE s = (SELECT 'm') ORDER BY id", { t }), [{ id: 1 }], `${tag} text = (SELECT 'm')`);
  same(q('SELECT id FROM t WHERE s <> (SELECT s FROM r WHERE i = 1) ORDER BY id', { t, r }), [{ id: 0 }, { id: 3 }, { id: 4 }], `${tag} text <> subquery`);
  same(q('SELECT id FROM t WHERE s < (SELECT s FROM r WHERE i = 1) ORDER BY id', { t, r }), [{ id: 0 }, { id: 4 }], `${tag} text < subquery`);
  same(q("SELECT id FROM t WHERE s BETWEEN 'a' AND (SELECT s FROM r WHERE i = 1) ORDER BY id", { t, r }), [{ id: 0 }, { id: 1 }, { id: 4 }], `${tag} BETWEEN with a subquery`);
  same(q("SELECT id, CONCAT(s, (SELECT 'x')) AS c FROM t ORDER BY id", { t }).map((x) => x.c), ['bx', 'mx', 'x', 'zx', 'ax'], `${tag} CONCAT with a text subquery`);
  // a condition on the left of IN / BETWEEN (a is NULL in row 3)
  const c = e.table({ id: [1, 2, 3], a: [5, -1, NaN] });
  same(q('SELECT id FROM c WHERE (a > 0) IN (FALSE, NULL) ORDER BY id', { c }), [{ id: 2 }], `${tag} condition IN (FALSE, NULL)`);
  same(q('SELECT id FROM c WHERE (a > 0) BETWEEN FALSE AND TRUE ORDER BY id', { c }), [{ id: 1 }, { id: 2 }], `${tag} condition BETWEEN`);
  same(q('SELECT id, (a > 0) IN (TRUE, FALSE) AS v FROM c ORDER BY id', { c }).map((x) => x.v), [1, 1, NaN], `${tag} condition IN as a value`);
  same(q('SELECT id FROM c WHERE (a > 0) IN (SELECT a < 0 FROM c WHERE id = 2) ORDER BY id', { c }), [{ id: 1 }], `${tag} condition IN (SELECT ...)`);
  // ILIKE / LOWER with İ and Σ
  const w = e.table({ id: [1, 2, 3], s: ['İ', 'ΣΑΣ', 'i'] });
  same(q("SELECT id FROM w WHERE s || '' ILIKE 'i' ORDER BY id", { w }), [{ id: 1 }, { id: 3 }], `${tag} ILIKE on computed text`);
  same(q("SELECT 'İ' ILIKE 'i' AS a, 'ΣΑΣ' ILIKE 'σασ' AS b, 'ΣΑΣ' ILIKE 'σας' AS c", { w }), [{ a: 1, b: 1, c: 0 }], `${tag} ILIKE on literals`);
  same(q('SELECT LOWER(s) AS l FROM w ORDER BY id', { w }).map((x) => x.l), ['i', 'σασ', 'i'], `${tag} LOWER as DuckDB`);
  await e.close();
}

console.log(failures ? `${failures} failures out of ${checks} checks` : `all ${checks} SQL audit round-4 checks passed`);
process.exit(failures ? 1 : 0);
