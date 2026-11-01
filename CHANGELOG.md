# Changelog

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
