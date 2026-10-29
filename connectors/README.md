# Vectro connectors for Power BI and Tableau

Ready-made connectors, so **Vectro** appears by name in Power BI's Get Data
list and Tableau's Connect list. Built files are in `dist/`; rebuild them with
`node connectors/build.mjs` (no dependencies).

Without them, both tools still connect through their built-in PostgreSQL
connectors (see [examples/bi](../examples/bi/README.md)); the connectors add
Vectro's name and icon, the right default port and, in Power BI, SQL of your own.

## Power BI: `dist/Vectro.mez`

Reads the tables through Vectro's OData feed (Power Query folds filters,
chosen columns and sorting into Vectro), or runs a SQL query of your own.

1. Start Vectro's HTTP server: `npx vectro serve --root ~/data` (port 8787;
   add `--token secret` to require a token).
2. Copy `Vectro.mez` to `Documents\Power BI Desktop\Custom Connectors`
   (create the folder; older versions: `Documents\Microsoft Power BI Desktop\Custom Connectors`).
3. File > Options and settings > Options > Security > Data Extensions: allow
   extensions to load without validation (the connector isn't signed), then
   restart Power BI Desktop.
4. Get Data > search **Vectro** (Beta). Server `http://localhost:8787`; leave
   the SQL box empty to choose tables, or write a SELECT.
5. Sign in with **Anonymous**, or **Key** with the token.

Import mode. For DirectQuery on large tables, use Power BI's PostgreSQL
connector with `vectro serve --pg`. For refresh in the Power BI Service, put
`Vectro.mez` in the on-premises data gateway's Custom Connectors folder.

## Tableau: `dist/vectro.taco`

A connector plugin (Tableau Connector SDK) for Vectro's PostgreSQL protocol,
using Tableau's PostgreSQL SQL and the PostgreSQL JDBC driver.

1. Start Vectro with the PostgreSQL protocol: `npx vectro serve --root ~/data --pg 5433 --user me --password secret`.
2. Put the PostgreSQL JDBC driver (`postgresql-42.x.jar`, from
   https://jdbc.postgresql.org/download/) in Tableau's drivers folder:
   `C:\Program Files\Tableau\Drivers` (Windows) or `~/Library/Tableau/Drivers` (Mac).
3. Copy `vectro.taco` to `Documents/My Tableau Repository/Connectors`.
4. The connector isn't signed yet, so start Tableau Desktop with signature
   checks off:
   - Windows: `"C:\Program Files\Tableau\Tableau <version>\bin\tableau.exe" -DDisableVerifyConnectorPluginSignature=true`
   - Mac: `open -n "/Applications/Tableau Desktop <version>.app" --args -DDisableVerifyConnectorPluginSignature=true`
5. Connect > To a Server > **Vectro**: server `localhost`, port `5433`, user
   and password; then choose the database and the tables.

## How they were checked

- `Vectro.pq` parses with Microsoft's Power Query parser (`@microsoft/powerquery-parser`);
  it follows the pattern of Microsoft's TripPin sample (OData.Feed with
  Implementation 2.0, a navigation table, TestConnection for the gateway).
  The OData feed and SQL endpoint it calls are checked with Microsoft's OData
  libraries (`compare/check-drivers.mjs`).
- The Tableau files validate against the Connector SDK's XML schemas
  (`xmllint --schema`), and are Tableau's PostgreSQL JDBC sample with Vectro's
  name, port and read-only settings; the PostgreSQL protocol is checked with
  pgJDBC, the driver Tableau uses.
- Neither has been loaded in Power BI Desktop or Tableau Desktop yet.

Signing (so no security settings need changing) needs a code-signing
certificate: Power BI uses a .pqx signed with one, Tableau a .taco signed with
`jarsigner`.
