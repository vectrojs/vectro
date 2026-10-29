// Try the connector (Power Query SDK): the tables, then a query
let
    tables = Vectro.Contents("http://localhost:8787"),
    regions = Vectro.Contents("http://localhost:8787", "SELECT region, COUNT(*) AS orders FROM orders GROUP BY 1 ORDER BY 2 DESC")
in
    regions
