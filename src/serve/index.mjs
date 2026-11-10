// vectrojs/serve: Vectro tables served to other programs.
//   serve()   SQL over HTTP (JSON, CSV, Arrow): Excel, Power BI's Web connector, pandas
//   servePg() the PostgreSQL protocol: Power BI, Tableau, Metabase, Grafana, psql
//   serveFlight() Arrow Flight SQL: pandas, Polars, DuckDB through ADBC, as Arrow
//   openTables() tables from code or a data folder, to share between them
//   liveTable() a table that changes by key, for serve({ live }): pages watch SQL on it
export { serve } from './http.mjs';
export { servePg } from './pg.mjs';
export { openTables } from './catalog.mjs';
export { serveFlight } from './flight.mjs';
export { liveTable, LiveTable } from '../live/table.mjs';
