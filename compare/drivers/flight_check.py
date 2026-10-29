"""Vectro's Arrow Flight SQL server from Python: the ADBC Flight SQL driver
(pandas, Polars, DuckDB on the Arrow it returns) and pyarrow's Flight client
with a user name and password. Prints lines compare/check-drivers.mjs checks.
   python flight_check.py grpc://127.0.0.1:31337 user password"""
import sys, warnings
warnings.filterwarnings('ignore')
import adbc_driver_flightsql.dbapi as flightsql
from adbc_driver_flightsql import DatabaseOptions
import pyarrow.flight as fl

uri, user, password = sys.argv[1], sys.argv[2], sys.argv[3]
kw = {'db_kwargs': {'username': user, 'password': password}} if password else {}
with flightsql.connect(uri, **kw) as conn:
    cur = conn.cursor()
    cur.execute("SELECT region, COUNT(*) AS n FROM orders GROUP BY 1 ORDER BY 1")
    t = cur.fetch_arrow_table()
    print('adbc arrow:', [str(f.type) for f in t.schema], t.to_pylist())
    df = cur.execute("SELECT cust, price, region, ts FROM orders ORDER BY cust").fetch_df()
    print('pandas:', list(df.dtypes.astype(str)), df.iloc[0].tolist())
    try:
        import polars as pl
        p = pl.read_database("SELECT region, SUM(price) AS s FROM orders GROUP BY 1 ORDER BY 1", conn)
        print('polars:', p.schema, p.rows())
    except Exception as e:
        print('polars: error', e)
    try:
        import duckdb
        arrow = cur.execute("SELECT * FROM orders").fetch_arrow_table()
        print('duckdb:', duckdb.sql("SELECT region, COUNT(*) AS n FROM arrow GROUP BY 1 ORDER BY 1").fetchall())
    except Exception as e:
        print('duckdb: error', e)
    objs = conn.adbc_get_objects(depth='columns').read_all().to_pylist()
    tables = objs[0]['catalog_db_schemas'][0]['db_schema_tables']
    print('objects:', objs[0]['catalog_name'], sorted(x['table_name'] for x in tables), [c['column_name'] for c in tables[0]['table_columns']])

client = fl.FlightClient(uri)
opts = fl.FlightCallOptions()
if password:
    token = client.authenticate_basic_token(user, password)
    opts = fl.FlightCallOptions(headers=[token])
print('pyarrow flight actions:', sorted(a.type for a in client.list_actions(opts)))
