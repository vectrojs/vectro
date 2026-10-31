# Changelog

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
  top-k, zone maps, compressed columns.
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

**Known gaps**
- Slower than DuckDB on: FULL OUTER JOIN counts, distinct counts of a million
  values spread over a wide range, top rows and joins over billion-row
  datasets (see the README's numbers). Faster on the other SQL queries
  measured (19 of 20) and on every group-by measured (2.2-2.7x with millions
  of groups).
- Free-text columns (text that rarely repeats) can be shown but not grouped
  or filtered, except when served (`vectro serve` loads them as categories).
- Window functions and percentiles keep the rows they read in memory.
- An engine holds at most 4 GB in memory; tables are freed with `dispose()`.
- Integers are exact up to 2^53; times are UTC.
