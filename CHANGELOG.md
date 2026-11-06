# Changelog

## 0.2.0

Fixes from audit round 5: 59 bugs found by checking SQL, the dataframe API,
datasets, streams, loading and the servers against DuckDB and plain
JavaScript; every fix has a test in `npm test`. Some answers change on
purpose, to match DuckDB and standard SQL:

**Changed answers**
- `CAST(x AS INTEGER)` (and the other whole-number types) rounds instead of
  cutting the fraction off, and is missing when the value doesn't fit.
- `%` is the exact remainder.
- `CAST(number AS VARCHAR)` keeps every digit; true / false cast to text are
  `'true'` / `'false'`.
- `SELECT *` after a join names columns as SQL does (`id`, then `id_2`; it
  was `id_right`) and keeps the right key of `ON`.
- Column names in any script stay as written (`Größe` was `Gr__e`).
- HTTP and MCP answers write ±Infinity as `"Infinity"` / `"-Infinity"` (it
  was null).
- A regular expression whose repeated group repeats itself or has `|` is
  refused.
- `column()` returns a new view (over the same memory) each call.
- `||` keeps SQL's rule (a missing piece makes it missing); CONCAT skips
  missing pieces.

**SQL**
- New aggregates: `STDDEV` / `STDDEV_SAMP`, `STDDEV_POP`, `VARIANCE` /
  `VAR_SAMP` and `VAR_POP`, with or without GROUP BY and with `FILTER`, on
  tables and datasets. Computed in two passes (each group's mean, then the
  squares around it), so values far from 0 keep their digits (`1e9 + 1 ...
  1e9 + 4` gives exactly what DuckDB gives).
- LIKE and ILIKE (in SQL and in `like()` / `ilike()`) match in linear time.
  They were a backtracking regular expression: `LIKE '%a%e%o%zzz'` on 4 KB of
  ordinary text took 10 seconds.
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
- SQL: a missing date compared with a date written as text (`d <> '2024-02-29'`,
  `NOT (d = '…')`, `d NOT IN ('…')`) is unknown, so the row is left out, as with
  `DATE '…'`. Those rows were kept.
- Dates stay dates through COALESCE, CASE, GREATEST, LEAST, FULL JOIN ...
  USING a date column, and a UNION with NULL on one side; they came back as
  milliseconds. In the dataframe API, `derive('x', 'coalesce(d1, d2)')` and
  `? :` between dates make date columns too.
- SQL: CAST to INTEGER, BIGINT, SMALLINT and the other whole-number types
  rounds, as DuckDB and PostgreSQL do; it cut the fraction off. Text and
  constants round half away from zero (`CAST('2.5' AS INT)` and
  `CAST(2.5 AS INT)` are 3, -2.5 is -3); a column or computed number, a
  DOUBLE there, rounds half to even (2.5 is 2, 3.5 is 4). A value the type can't hold, NaN or
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
- SQL regular expressions (`REGEXP_MATCHES`, `REGEXP_REPLACE`,
  `REGEXP_EXTRACT`, `~`) refuse a pattern with a repeated group that itself
  repeats or has `|`, such as `(a+)+$` or `(a|aa)*`. JavaScript's regular
  expressions backtrack, and `(a+)+$` on 31 characters never finished, so one
  query could freeze a server. Rewrite such a pattern, for example with a
  character class (`[ab]+`).
- SQL: `CAST(x AS DECIMAL(p, s))` rounds to s digits (half away from zero)
  and is missing when the whole part needs more than p - s digits; plain
  `DECIMAL` is `DECIMAL(18, 3)`, as in DuckDB. It kept every digit. `CAST(x AS
  REAL)` (and `FLOAT`, `FLOAT4`) is a 32-bit float; it stayed 64-bit.
  Expressions can use it as `fround(x)`.
- SQL: `ROUND(x, -1)` rounds to tens (it was refused), the digits can be a
  constant expression, `ROUND(1e308, 2)` is 1e308 (it was Infinity), and
  `ROUND(0.49999999999999994)` is 0 (it was 1).
- SQL: `CAST(number AS VARCHAR)` keeps every digit (`0.1 + 0.2` is
  `0.30000000000000004`, as DuckDB writes it; it was rounded to 15 digits), and
  a true / false value is `'true'` / `'false'` (it was `'1'` / `'0'`).
- SQL: `CAST('inf' AS DOUBLE)`, `'-inf'`, `'infinity'` and `'nan'` (any case)
  are read, as DuckDB reads them; they were missing.
- SQL text: `REVERSE` keeps each visible character whole (an emoji with a skin
  tone, a letter with an accent); `REGEXP_REPLACE(s, p, r, 'g')` doesn't
  replace an empty match right after a match (`'aaa'`, `'a*'` gives `'b'`, as
  DuckDB; it was `'bb'`); `SPLIT_PART(s, '', n)` is the n-th character.
- SQL: `STRFTIME` writes years below 1000 with 4 digits (`0001-01-01`), and
  week numbers and days of the year are right for years 1 to 99 (they were
  worked out as 1901 to 1999).
- SQL: `LIKE ... ESCAPE '!'` (and `ILIKE`) is read: `'a!%'` matches `a%`
  exactly. It was refused. In expressions: `like(s, 'a!%', '!')`. LIKE on
  text in quotes (`'abc' LIKE 'a%'`) is worked out; it failed with an error
  that showed generated code.
- SQL: aggregates take `FILTER (WHERE ...)` (`COUNT(*) FILTER (WHERE v > 1)`,
  `SUM(x) FILTER (...)`); it was a syntax error. An unknown function, also an
  aggregate such as `STDDEV(v)` in a GROUP BY query, says it is unknown and
  lists the aggregates; it said "v must be in GROUP BY".
- SQL: `GROUP BY q, q` (or `q, 1`) groups by q once; it failed with "groupBy
  got the same key twice".
- SQL: a text IN list can hold NULL (`s IN ('a', NULL)`), as BI tools write it;
  it was refused. `s NOT IN ('a', NULL)` keeps nothing, as in SQL.
- SQL: `LIMIT` and `OFFSET` take a constant expression (`LIMIT 2 + 3`) and
  round as DuckDB does (`LIMIT 2.5` is 3 rows; it was 2), and `OFFSET` works
  without `LIMIT` (it was refused).
- SQL: a query nested more than 400 levels deep (brackets or function calls)
  fails with that number in the message. Before, about 860 brackets ran out
  of stack, and the message said "thousands of levels".
- SQL windows: a RANGE frame with a distance (`RANGE BETWEEN 1 PRECEDING AND
  CURRENT ROW`) with `NULLS FIRST` keeps missing order keys out of the frames
  of ±Infinity keys (they were counted together). `LAG` / `LEAD` of text take a
  text default (`LAG(s, 1, 'none')`); it was refused ("must be a number"), and
  a number default on text gave a wrong label.
- SQL joins: `SELECT *` and `b.*` name every column as its table has it, as
  SQL does: `b.*` is `id, k, s` (it was `id_right, ...`), and a name that two
  tables have is written once and then `id_2`, as `SELECT a.id, b.id` already
  did (`SELECT *` gave `id_right`). `SELECT *` after `JOIN ... ON a.k = b.k`
  keeps b's key (missing where an outer join found no match); it was dropped.
  A USING key is still once. This changes column names of `SELECT *` joins.
- SQL joins: `ON a.k + 1 = b.k` (an expression on each side, also `CAST`,
  `UPPER(...)`) joins on those values; it was refused. A text key joined to a
  number key names the columns as written (`"b.s" and "c.s"`), not a hidden
  `__k0`, and `FULL JOIN ... USING (s)` with an empty table's column keeps the
  other side's text (it gave numbers).

**Dataframe and datasets**
- On a dataset, `date - date` (and `date - DATE '...'`) gives days, as on a
  table; it gave milliseconds. Each chunk now records whether a date column
  holds whole days; files written before are read from their encoding. The
  PostgreSQL, Flight and OData servers report those columns as `date` too.
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
- Lazy queries and datasets: a join keeps the left table's column order, as
  an eager join does (the key came first). On a dataset,
  `join(...).top(k, 's_right')` (a renamed text column of the right table)
  works; it failed with "s_right is a text column". `groupBy('s').agg({ s:
  ... })` (an aggregation named like a key) is refused, as on a table; on a
  dataset it put the sums where the keys should be.
- Split tables: `topAsync` keeps table order among tied rows, as `top` on a
  table does (row 13000 came before row 5000). The values were right.

**Loading data**
- JSON lines: a numeric string (`{"x":"2"}`, `" 1e3 "`) goes into a number
  column, as the docs say; one such value made the whole column text. Blank
  text doesn't change a column's kind (it is missing in a number column), and
  an escaped `"\u0030"` is 0 (it was missing).
- A UTF-8 BOM before the first JSON lines record no longer loses that record,
  and a BOM before the first row pushed or sent to a CSV or JSON lines stream
  is dropped (it went into the first value, or lost the first row).
- `writeArrow`: a text column whose first value is null is written as text
  (it was taken for numbers, and every value was lost), and null text is
  missing (it was written as the text "null").
- CSV: a time with a `+hh` offset (`2024-01-02 03:04:05+00`, as PostgreSQL
  exports a timestamptz) makes a date column; it stayed text.
- CSV: a file whose lines end in CR alone (old Mac files) loads its rows, in
  `parseCSV`, `readCSV`, `csvToDataset` and split tables; it loaded as one
  header line and no rows, with no error.
- `tableFromRows`: the columns are every key of any row (missing where a row
  lacks one); keys first seen after the first row were dropped. A number in a
  text column is its text (`'5'`); it stayed a number.
- JSON lines files (servers, MCP): every key is a column, as the docs say. A
  key whose value is nested in some rows keeps its plain values (the whole key
  was dropped, and with a list of numbers in one row the file was taken for
  vectors), and a nested value in a text column is its JSON text
  (`'{"nested":true}'`); a key with only nested values is such a column (it
  was dropped). A vector field is one that holds a list of numbers of one
  length in every row. Streams still leave nested values missing.
- Column names can use letters of any script (`Größe`, `名前`), in
  expressions, SQL, sorting, the servers and OData; CSV headers and JSON keys
  keep them (`Größe` became `Gr__e` and `Grüße` `Gr__e_2`). Other signs and
  spaces still become `_` (`Order Date` is `Order_Date`). Data files named so
  keep those letters in their table names. This changes the names such files
  load with.

**Streams**
- Streams: `keepMs` drops every row older than the newest time minus
  `keepMs`, also rows that arrived late with an old time. Only the rows at the
  front were dropped, so a late row from long ago stayed in "the last second"
  and went into its totals.
- Streams: a blank line in a CSV stream (a heartbeat newline) is not a row,
  as JSON lines streams already skipped them; it was a row of missing values,
  so `count()` grew. `keepRows` counts rows, not blank lines.

**Engine and servers**
- A script run with `node --input-type=module -e '…'` or piped into node can
  use worker threads: they inherited `--input-type`, which Node refuses for
  workers, so the process stopped as soon as the first one started.
- Tables dropped without `dispose()` have their engine memory freed once
  they are garbage collected. A table made inside a chain
  (`t.sort(...).head(20)`) can't be disposed, and was kept until
  `engine.close()`: 150 such calls held 460 MB, and a long-running app ran out
  of memory. `dispose()` still frees at once and is still the way to free
  memory soon. `column()` now returns a new array view each call (over the
  same memory).
- HTTP answers (JSON, JSON lines, CSV) and the MCP tools write ±Infinity as
  the text `"Infinity"` / `"-Infinity"` (`Infinity` / `-Infinity` in CSV).
  They were null, the same as a missing value, so `MIN(x) = -Infinity` reached
  Excel or an LLM as "no value". NaN is still missing.
- Node: a script that leaves a `SplitEngine` open ends by itself, as with a
  plain `Engine`; its worker threads kept it running forever. The workers keep
  the process alive only while an answer is awaited, or while a stream reads
  from a connection (`connect()`), until `close()`.

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
