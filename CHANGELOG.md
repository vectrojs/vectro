# Changelog

## Unreleased

Fixes from audit round 5.

- LIKE and ILIKE (in SQL and in `like()` / `ilike()`) match in linear time.
  They were a backtracking regular expression: `LIKE '%a%e%o%zzz'` on 4 KB of
  ordinary text took 10 seconds.
- On a dataset, `date - date` (and `date - DATE '...'`) gives days, as on a
  table; it gave milliseconds. Each chunk now records whether a date column
  holds whole days; files written before are read from their encoding. The
  PostgreSQL, Flight and OData servers report those columns as `date` too.
- SQL: a subquery used as a value that returns more than one row (`WHERE
  price < (SELECT v FROM u)`) is an error, as in DuckDB and PostgreSQL. It
  silently joined every row to every value, so a SUM came out several times
  too big.
- SQL: GREATEST and LEAST of many values, and COALESCE / IFNULL nested in
  each other, plan in milliseconds. Each value used to be written out two to
  four times per level: GREATEST of 12 columns took more than 10 seconds and
  then ran out of memory. Other functions nested many levels deep (ROUND,
  NULLIF, %) stop at once with "the expression is too large" instead of
  hanging.
- Datasets read asynchronously through the file descriptor they already hold.
  On Node 25, a dataset queried with an async call and then dropped without
  `close()` stopped the whole process at garbage collection ("A FileHandle
  object was closed during garbage collection"). A dataset dropped without
  `close()` now has its file closed when it is collected.
- Writing a dataset over an existing file no longer changes it under a dataset
  that has it open. `writeDataset`, `csvToDataset` and `createDataset` write a
  new file beside the path and move it into place when it is complete, so an
  open dataset keeps reading the old file. Before, it read the new bytes with
  the old layout: rows mixed old and new values. A writer that fails or is
  aborted leaves no file behind, and leaves the file it would have replaced
  as it was.
- SQL: a missing date compared with a date written as text (`d <> '2024-02-29'`,
  `NOT (d = '…')`, `d NOT IN ('…')`) is unknown, so the row is left out, as with
  `DATE '…'`. Those rows were kept.
- Dates stay dates through COALESCE, CASE, GREATEST, LEAST, FULL JOIN ...
  USING a date column, and a UNION with NULL on one side; they came back as
  milliseconds. In the dataframe API, `derive('x', 'coalesce(d1, d2)')` and
  `? :` between dates make date columns too.
- SQL: CAST to INTEGER, BIGINT, SMALLINT and the other whole-number types
  rounds half away from zero, as DuckDB and PostgreSQL do (2.5 is 3, -2.5 is
  -3, 3.7 is 4); it cut the fraction off. A value the type can't hold, NaN or
  ±Infinity becomes missing, as text that isn't a number already did.
- SQL `%` is the exact remainder, as JavaScript's `%` and DuckDB give it; it
  was computed as `a - trunc(a / b) * b`, which is wrong whenever `a / b`
  isn't exact (`7 % 0.1` gave 0, `9007199254740994 % 3` gave 2, `5 % Infinity`
  gave NULL). Expressions can use it too, as `mod(a, b)`. The kernels compute
  it in WebAssembly (no remainder instruction there), so `dist/vectro.wasm`
  is rebuilt.
- Percentiles: `PERCENTILE_DISC(p) WITHIN GROUP (ORDER BY x DESC)` picks the
  right value (it took the ascending 1 - p, one value off whenever p × n is
  whole); `MEDIAN(DISTINCT x)` and the other percentiles with DISTINCT count
  each value once (DISTINCT was ignored); percentiles of text skip missing
  text and give a label (MEDIAN of text is the lower middle label, as in
  DuckDB; it gave a number), and PERCENTILE_CONT of text is an error.
- PERCENTILE_CONT, MEDIAN and QUANTILE_CONT between an infinite value and
  another give the infinite one (the median of -Infinity and 5 is -Infinity;
  it was missing), and between huge values (±1e308) don't overflow.
- Text holding both `'` and `"` works everywhere: as a PostgreSQL parameter
  (`O'Brien "Bob"` was refused), in SQL, and in expressions, where `''`
  inside `'...'` is one `'` (`filter("name == 'O''Brien'")`).
- (Unchanged, now tested) `||` keeps SQL's rule: a missing piece makes the
  whole text missing, and empty text is missing text in Vectro, so
  `'a' || SUBSTRING('a', 2)` is missing. CONCAT skips missing pieces:
  `CONCAT(UPPER(SUBSTRING(s, 1, 1)), LOWER(SUBSTRING(s, 2)))` capitalizes a
  one-letter word too.
- A script run with `node --input-type=module -e '…'` or piped into node can
  use worker threads: they inherited `--input-type`, which Node refuses for
  workers, so the process stopped as soon as the first one started.
- An async dataset query is answered only once the chunks it read ahead are
  done. When it failed or stopped early (a limit), that reading was still on
  the worker threads, so the next sync call was sometimes refused with "an
  async query is running on this engine", although the query was awaited.
- Datasets: a derive or select whose column nothing uses, followed by a limit
  or top, no longer fails (`derive('x', 'a + b').limit(2).agg({ s: 'sum(a)' })`
  said "unknown column b"; `select('id', 'a').top(1, '-a').agg(...)` said "no
  column id"). Each chunk's plan now keeps only the columns used above it.
- Datasets: a group whose values in one chunk hold both +Infinity and
  -Infinity sums to NaN, as on a table; SUM, AVG and group sums left that
  chunk out (a SUM of 3 instead of NaN, a group sum of 0).
- Datasets have the documented async versions: `sortAsync`, `topAsync`,
  `limitAsync`, `joinAsync` (the rows, in a table, as on a table), `sumAsync`,
  `meanAsync`, `minAsync`, `maxAsync`, `explainAsync`, and
  `groupBy(...).aggAsync` (also on lazy queries). They were missing
  (TypeError). `count` / `countAsync` take params.
- Streams: `keepMs` drops every row older than the newest time minus
  `keepMs`, also rows that arrived late with an old time. Only the rows at the
  front were dropped, so a late row from long ago stayed in "the last second"
  and went into its totals.
- Tables dropped without `dispose()` have their engine memory freed once
  they are garbage collected. A table made inside a chain
  (`t.sort(...).head(20)`) can't be disposed, and was kept until
  `engine.close()`: 150 such calls held 460 MB, and a long-running app ran out
  of memory. `dispose()` still frees at once and is still the way to free
  memory soon. `column()` now returns a new array view each call (over the
  same memory).

## 0.1.4

Fixes from four more audit rounds: streams, SQL against DuckDB, the
dataframe API against plain JavaScript, and the servers, plus a fuzzer
(`npm run test:fuzz`) that now runs before every publish. No API changes.

**Streams**
- `keepRows` keeps the newest rows of the whole stream, and `keepMs`
  measures from the newest time over all workers (they were per worker).
- A CSV push or TCP connection cut inside a quoted field no longer loses rows.
- Text filters over many labels (`contains`, `like`, `tag == bill`) on a live
  stream free their 0 / 1 per row after each query. Before, they piled up
  until the worker's memory grew and the query failed.

**SQL**
- Three-valued logic everywhere: a value used as a condition is true only
  where it is present and not 0; a condition used as a value (COALESCE, CASE,
  arithmetic, IN, BETWEEN, aggregates, windows) is 1 / 0 / NULL;
  `(condition) IS NULL` is true where it is unknown.
- Set operations: INTERSECT binds tighter than UNION and EXCEPT; NULL columns
  take the other side's type; NULLS FIRST works after them.
- Text: `<`, `>`, BETWEEN and joins compare by code point, as ORDER BY does;
  a scalar subquery that returns text compares as text; ILIKE and LOWER
  lowercase as DuckDB does (`İ` is `i`).
- Windows over text (COUNT, LAG / LEAD, MIN / MAX); windowed SUM / AVG start
  again at each partition; RANGE frames over dates count days.
- Dates: `date + n` adds days, `DATE - DATE` gives days, MIN / MAX of dates
  are dates. QUANTILE is QUANTILE_DISC, as in DuckDB. STRFTIME knows more
  specifiers and refuses unknown ones.

**Dataframe**
- Expressions that read no column (`sum(1)`, `filter('on == 1', { on: 1 })`)
  work; `-0` sorts as 0; text sorts by code point.
- A text column used as a number (`city > 0`, `sum(city)`) is a clear error
  instead of using its dictionary codes. Two text columns compare by label.
- `compress()`, joins and `filter().select()` never show the hidden columns
  text filters use.
- `table()` checks every column before copying and frees what it copied if
  one fails. Clearer messages for bad arguments and very deep expressions.

**Loading data**
- csvToDataset: blank lines at the end of a piece stay rows only when more
  rows follow (as readCSV reads them); types are guessed from the first
  1,000 complete rows, as readCSV guesses them.
- JSON lines: text after the first 1,000 lines loads the column as text;
  keys that first appear later become columns.
- Impossible dates (2024-02-31) are missing, not rolled into March.
- Free text in a Float32Array table loads as a category.

**Engine and servers**
- `close()` rejects queued and running queries (they stayed pending forever),
  and every later call says the engine is closed. This covers every async
  method, UNION queries, vector searches, live windows and `connect()`.
- `POST /reload` loads first, then swaps the tables in; the old ones are
  freed once the requests that started before have ended. The PostgreSQL
  server frees old catalogs the same way.
- Bad %-escapes in URLs are a 400; psql's `\d name` describes a table.

## 0.1.3

Fixes from a second, wider audit: thousands of random cases for the
dataframe API, CSV and JSON-lines loading and SQL, each checked against plain
JavaScript or DuckDB. No API changes.

**Loading data**
- A quote inside an unquoted CSV field (`Monitor 27" wide`, `O"Brien`) is a
  plain character, as the parser always read it. Before, the row counter took
  it for the start of a quoted field: rows went missing, and with a file read
  in chunks some rows got another row's values. The row counter, the cuts
  between threads, split tables and csvToDataset now follow the parser's rules.
- A CSV column guessed as numbers (or dates) from the first 1,000 rows that
  has other text later is loaded again as text, instead of turning that text
  into missing values. Split tables and csvToDataset, which read pieces apart,
  stop with an error naming the column and the `types` to pass.
- CSV files with more than about 250 columns load on every thread (they were
  parsed in one task with too many arrays).
- Repeated header names never collide: `a,a,a_2` loads as `a, a_3, a_2`.
- JSON lines: a numeric array in some lines no longer drops the lines without
  it (every line is a row; those with the vector go in the vector index), and
  keys like `@timestamp` or `user-agent` are cleaned as CSV headers are
  (`_timestamp`) instead of the file being skipped.

**Queries**
- Text filters (`contains`, `endsWith`, `like`, `ilike`, `in`, SQL `LIKE`)
  whose matching labels are spread over many codes (a column of thousands of
  SKUs) work, through a 0 / 1 per row made once from the labels, instead of
  failing with "spread over too many runs".
- `filter(...).select(...)` returns only the columns selected.
- A lazy query counting a derived condition (`derive('big', 'price > 100')`,
  then `count(big)`) gives the same answer as the eager one.
- `isMissing(x)` on a text column is true for missing text (`''`).
- SQL `GROUP BY a > 0` puts rows where the condition is NULL in a group of
  their own.

## 0.1.2

A correctness release, from comparing about 1,700 new SQL queries with DuckDB
on the same data. No API changes. Every case below now gives DuckDB's answer,
or a clear error.

**Engine**
- A query too big for the 4GB memory (a join with billions of output rows)
  now fails with `out of memory (4GB limit)`. Before, the size wrapped around
  in the allocator, and later queries on the same engine could return wrong
  numbers.
- Missing text (`''`) sorts last, up or down, as missing numbers do, in
  `sort`, `top`, window ordering and SQL `ORDER BY`.
- `null` and `undefined` in a text column load as missing text, also when
  the column starts with one (`engine.table`, `tableFromRows`).
- A missing text key never matches in a join, as the docs say.
- A filter with no column in it (`'1'`, `'1 == 0'`) keeps every row or none,
  instead of failing.
- Comparing a text column with `<` or `>` and then grouping gives a clear
  error instead of overflowing the stack. On datasets, the unknown-column
  error lists every column.

**SQL**
- Conditions follow SQL's three values everywhere: a condition on a missing
  value is NULL when selected, `IS TRUE` / `IS FALSE` treat it as neither,
  comparing two conditions is unknown if either is, and a `NULL` literal in
  `AND` / `OR` is unknown rather than true.
- `WHERE TRUE`, `WHERE 1 = 1`, `WHERE NULL` and other constant conditions work.
- `ORDER BY text NULLS FIRST` works.
- `x NOT IN (SELECT ...)` keeps every row when the subquery is empty and no
  row when it contains a NULL.
- `LEAST` and `GREATEST` skip missing values (NULL only if all are).
- The same aggregate twice (`SUM(x) AS a, SUM(x) AS b`) gives NULL both times
  over no values.
- Subqueries in the SELECT list or HAVING of a GROUP BY query work.

## 0.1.1

A performance release. No API changes.

**Faster group-by**
- SQL `SUM` no longer computes a hidden count of non-missing values when the
  column has none missing, and sum, mean, min and max skip the per-row
  missing-value check on such columns.
- On many threads, a group-by remembers its key range and merges the
  threads' partial results with a WebAssembly kernel instead of a JavaScript
  loop. This also removes the 31-thread limit for queries with many groups.
- Measured on 2M orders (`SELECT product_id, sum(amount) ... GROUP BY 1`,
  `compare/bench-2m-groupby.mjs`, numbers in `compare/results.json`): with
  1,000 products, 3.0 ms on 1 thread (DuckDB 2.0: 6.8 ms) and 2.3 ms on 8
  threads (DuckDB 2.0: 2.4 ms). Before this release Vectro was 1.9x slower
  than DuckDB on 8 threads in this case; it is now level or faster at every
  size from 100 to 100,000 products.

## 0.1.0 (first release)

The first public release. Everything below is new.

**Engine**
- Columnar tables in engine memory; expressions compiled into fused SIMD
  WebAssembly loops and run on every CPU core (Node worker threads, Web
  Workers on cross-origin-isolated pages), one thread elsewhere, and a
  vectorized interpreter where code generation isn't allowed (Cloudflare Workers).
- Filters, computed columns, aggregates, group by any keys (millions of
  groups: partitioned by key range on every thread, sorted by key), exact and
  approximate distinct counts, joins (inner, left, right, full), sort and
  top-k, zone maps, compressed columns. Top-k over a file reads only the
  blocks that can still hold a winning row.
- A query planner: `table.lazy()` chains are optimized as a whole.
- CSV (parallel parser), JSON lines, Apache Arrow (read and write).
- Datasets bigger than memory (`.vectro` files): chunked, bit-packed, read in
  parallel; group-by, joins and sorts spill to disk; in browsers too.
- Streams (CSV, JSON lines, WebSocket feeds) with running windows.
- Vector search (cosine / dot / L2, exact top-k).

**SQL**
- `sql(text, tables)`: joins, subqueries (IN, EXISTS, correlated), CTEs,
  UNION / INTERSECT / EXCEPT, window functions, percentiles, text functions,
  dates and intervals. 40 queries checked against DuckDB give the same answers.
- Shortcuts for common shapes: only the rows that can rank in the first k for
  `ROW_NUMBER() / RANK() ... <= k`; distinct counts of IDs with a byte map;
  medians from bucket counts; CASE with quoted labels as label codes.

**Serving** (beta)
- `vectro serve`: SQL over HTTP (JSON, CSV, Arrow) and an OData v4 feed;
  the PostgreSQL wire protocol (Power BI, Tableau, Metabase, Grafana, psql);
  Arrow Flight SQL (pandas, Polars, DuckDB through ADBC). Checked with
  pgJDBC, Npgsql, Microsoft's OData libraries and the ADBC driver.
- An MCP server for AI assistants (`vectro-mcp`).

**Connectors** (experimental)
- `connectors/dist/Vectro.mez` for Power BI and `vectro.taco` for Tableau.
  Not signed, not yet tried in the applications.

**Missing values**
- SQL uses three-valued logic for missing values: `NOT (x > 5)`, `x <> 5`,
  `NOT IN`, `NOT BETWEEN`, `NOT LIKE` and `LIKE` drop rows where the value
  is missing, as other databases do. Table expressions keep JavaScript's rules
  (`x != 5` is true where x is missing).
- Blank text (`''`, the missing value of a text column) is not counted by
  `count(x)`, `countDistinct(x)`, `approxDistinct(x)` or SQL's `COUNT`, on
  tables, datasets and split tables.
- `null` in arrays and rows loads as missing (it used to load as 0); a `Date`
  in `tableFromRows` makes a date column.
- Clear errors for a text param, for date columns in a `Float32Array` table
  (times don't fit in float32) and for SQL on a split table.
- `readCSV(path, { compress: true })` packs the columns, as `parseCSV` does.

**Testing**
- `VECTRO_POISON=1` overwrites memory as it is freed, so code that reads an
  array after freeing it gives wrong answers in tests (part of `test:all`).

**Known gaps**
- Slower than DuckDB on one shape measured: a join over a file on disk with
  a GROUP BY on the joined table's columns (1.2-1.3x at 100M and 1B rows).
  Faster on all 20 SQL queries measured, on every group-by, join and
  distinct count measured in memory, and on 7 of 8 queries over a billion
  rows on disk (see the README's numbers).
- Free-text columns (text that rarely repeats) can be shown but not grouped
  or filtered, except when served (`vectro serve` loads them as categories).
- Window functions and percentiles keep the rows they read in memory.
- An engine holds at most 4 GB in memory; tables are freed with `dispose()`.
- Integers are exact up to 2^53; times are UTC.
