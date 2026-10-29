// vectrojs/serve: Vectro tables served to other programs.
//   serve()   SQL over HTTP (JSON, CSV, Arrow): Excel, Power BI's Web connector, pandas
//   servePg() the PostgreSQL protocol: Power BI, Tableau, Metabase, Grafana, psql
//   openTables() tables from code or a data folder, to share between both
export { serve } from './http.mjs';
export { servePg } from './pg.mjs';
export { openTables } from './catalog.mjs';
