# Contributing to Vectro

Thanks for helping. Issues and pull requests are welcome.

## Running the tests

```sh
npm install          # (no dependencies; this only sets up the scripts)
npm test             # the main suite (Node 20+)
npm run test:all     # every mode: threads, one thread, interpreter, edge, browser simulation, compressed
```

Each test compares Vectro's answers with plain JavaScript (or DuckDB, in
`compare/`). A change that makes something faster must keep every answer
the same.

- `compare/`: benchmarks and cross-checks against DuckDB, SQLite and Arquero
  (`cd compare && npm install` first). `compare/check-drivers.mjs` runs the
  real database drivers (needs Java and .NET; Python for Flight SQL).
- `examples/`: browser pages and the BI tools setup.

## Benchmarks

Numbers in the README must be reproducible: each system in its own process,
best of several runs, answers checked against each other, and the slower
cases reported too.

## Style

- Plain JavaScript modules, no dependencies in `src/`.
- Comments say what and why in plain words; match the code around you.
- New behaviour comes with a test that checks answers, not just that it runs.

## Reporting a problem

Open an issue with your Vectro, Node (or browser) version and the smallest
code or SQL that shows it. For a BI tool that can't connect, run
`vectro serve` with `--log-queries` and include the statement it printed.

Questions that aren't bugs: **hello@vectrojs.dev**.
