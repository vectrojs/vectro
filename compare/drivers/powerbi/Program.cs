using System;
using Npgsql;

// What Power BI's PostgreSQL connector does through Npgsql 4.0.17: connect,
// list tables, read rows (binary), fold a group-by.
var cs = args.Length > 0 ? args[0] : "Host=127.0.0.1;Port=55432;Username=postgres;Password=secret;Database=postgres;Application Name=capture-npgsql";
using var c = new NpgsqlConnection(cs);
c.Open();
Console.WriteLine($"server {c.PostgreSqlVersion}");
void Run(string sql) {
  using var cmd = new NpgsqlCommand(sql, c);
  using var r = cmd.ExecuteReader();
  var cols = new string[r.FieldCount];
  for (int i = 0; i < r.FieldCount; i++) cols[i] = $"{r.GetName(i)}:{r.GetDataTypeName(i)}:{r.GetFieldType(i).Name}";
  Console.WriteLine($"> {sql.Replace("\n", " ").Substring(0, Math.Min(90, sql.Length))}\n  [{string.Join(", ", cols)}]");
  int n = 0;
  while (r.Read()) {
    if (n++ >= 5) continue;
    var vals = new string[r.FieldCount];
    for (int i = 0; i < r.FieldCount; i++) vals[i] = r.IsDBNull(i) ? "null" : r.GetValue(i) is DateTime d ? d.ToString("o") : Convert.ToString(r.GetValue(i), System.Globalization.CultureInfo.InvariantCulture);
    Console.WriteLine("  " + string.Join(" | ", vals));
  }
  Console.WriteLine($"  ({n} rows)");
}
Run("select character_set_name from INFORMATION_SCHEMA.character_sets");
Run("select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE\nfrom INFORMATION_SCHEMA.tables\nwhere TABLE_SCHEMA not in ('information_schema', 'pg_catalog')\norder by TABLE_SCHEMA, TABLE_NAME");
Run("select \"$Table\".\"cust\", \"$Table\".\"price\", \"$Table\".\"qty\", \"$Table\".\"region\", \"$Table\".\"ts\", \"$Table\".\"d\", \"$Table\".\"ok\" from \"public\".\"orders\" \"$Table\" limit 1000");
Run("select \"_\".\"region\", \"_\".\"a0\" from (select \"rows\".\"region\" as \"region\", sum(\"rows\".\"price\") as \"a0\" from (select \"_\".\"region\", \"_\".\"price\" from \"public\".\"orders\" \"_\" where \"_\".\"qty\" >= 1) \"rows\" group by \"region\") \"_\" where not \"_\".\"a0\" is null limit 1000001");
using (var cmd = new NpgsqlCommand("select count(*) from orders where region = @r", c)) { cmd.Parameters.AddWithValue("r", "EU"); Console.WriteLine($"> param query: {cmd.ExecuteScalar()}"); }
var t = c.GetSchema("Tables"); Console.WriteLine($"GetSchema Tables: {t.Rows.Count} rows");
var cl = c.GetSchema("Columns", new string[] { null, null, "orders" }); Console.WriteLine($"GetSchema Columns: {cl.Rows.Count} rows");
