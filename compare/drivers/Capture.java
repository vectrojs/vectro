import java.sql.*;
import java.util.*;

// What a BI tool asks through pgJDBC (Tableau's driver): metadata, then queries.
public class Capture {
  static void show(ResultSet rs, int max) throws SQLException {
    ResultSetMetaData md = rs.getMetaData();
    int n = md.getColumnCount(), k = 0;
    StringBuilder h = new StringBuilder();
    for (int i = 1; i <= n; i++) h.append(md.getColumnName(i)).append(':').append(md.getColumnTypeName(i)).append(i < n ? ", " : "");
    System.out.println("  [" + h + "]");
    while (rs.next()) {
      if (k++ >= max) continue;
      StringBuilder r = new StringBuilder();
      for (int i = 1; i <= n; i++) r.append(rs.getString(i)).append(i < n ? " | " : "");
      System.out.println("  " + r);
    }
    System.out.println("  (" + k + " rows)");
  }
  public static void main(String[] a) throws Exception {
    String url = a.length > 0 ? a[0] : "jdbc:postgresql://127.0.0.1:55432/postgres";
    Properties p = new Properties();
    p.setProperty("user", a.length > 1 ? a[1] : "postgres");
    p.setProperty("password", a.length > 2 ? a[2] : "secret");
    p.setProperty("ApplicationName", "capture-jdbc");
    try (Connection c = DriverManager.getConnection(url, p)) {
      DatabaseMetaData m = c.getMetaData();
      System.out.println("product " + m.getDatabaseProductName() + " " + m.getDatabaseProductVersion());
      System.out.println("schemas"); show(m.getSchemas(), 20);
      System.out.println("catalogs"); show(m.getCatalogs(), 20);
      System.out.println("tables"); show(m.getTables(null, "public", "%", new String[] { "TABLE", "VIEW" }), 20);
      System.out.println("columns"); show(m.getColumns(null, "public", "orders", "%"), 20);
      System.out.println("primary keys"); show(m.getPrimaryKeys(null, "public", "orders"), 20);
      System.out.println("imported keys"); show(m.getImportedKeys(null, "public", "orders"), 20);
      System.out.println("type info"); show(m.getTypeInfo(), 3);
      try (Statement s = c.createStatement()) { System.out.println("limit 0"); show(s.executeQuery("SELECT * FROM \"public\".\"orders\" \"orders\" LIMIT 0"), 5); }
      try (PreparedStatement ps = c.prepareStatement("SELECT \"orders\".\"region\" AS \"region\", SUM(\"orders\".\"price\") AS \"sum:price:ok\", COUNT(1) AS \"cnt\" FROM \"public\".\"orders\" \"orders\" WHERE \"orders\".\"qty\" >= ? GROUP BY 1")) {
        ps.setInt(1, 1);
        for (int i = 0; i < 6; i++) { ResultSet rs = ps.executeQuery(); if (i == 5) { System.out.println("prepared x6"); show(rs, 5); } rs.close(); }
      }
      c.setAutoCommit(false);
      try (Statement s = c.createStatement()) {
        s.setFetchSize(1);
        System.out.println("fetch size 1"); show(s.executeQuery("SELECT cust, price, qty, region, ts, d, ok FROM orders ORDER BY cust"), 5);
      }
      c.commit();
    }
  }
}
