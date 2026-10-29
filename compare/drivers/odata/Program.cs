using System.Xml;
using Microsoft.OData.Edm;
using Microsoft.OData.Edm.Csdl;
using Microsoft.OData.Edm.Validation;
using Simple.OData.Client;

// Vectro's OData feed through Microsoft's OData libraries: the $metadata
// document parsed and validated, then queries the way client tools send them.
var root = args.Length > 0 ? args[0] : "http://127.0.0.1:8787/odata/";
using var http = new HttpClient();
var xml = await http.GetStringAsync(root + "$metadata");
var model = CsdlReader.Parse(XmlReader.Create(new StringReader(xml)));
model.Validate(out var errors);
Console.WriteLine($"metadata valid: {!errors.Any()} {string.Join("; ", errors.Select(e => e.ErrorMessage))}");
foreach (var t in model.SchemaElements.OfType<IEdmEntityType>())
  Console.WriteLine($"  {t.Name}: key {string.Join(",", t.Key().Select(k => k.Name))}; {string.Join(", ", t.StructuralProperties().Select(p => $"{p.Name} {p.Type.FullName()}"))}");

var client = new ODataClient(new ODataClientSettings(new Uri(root)) { IgnoreUnmappedProperties = true });
var annotations = new ODataFeedAnnotations();
var rows = (await client.For("orders").Filter("region eq 'EU' and qty gt 1").OrderByDescending("ts").Top(3).Select("cust", "ts", "region", "qty").FindEntriesAsync(annotations)).ToList();
Console.WriteLine($"filtered: {rows.Count} rows; first {rows[0]["cust"]} {rows[0]["ts"]:o} {rows[0]["region"]} {rows[0]["qty"]}");
var count = await client.For("orders").Filter("price gt 5").Count().FindScalarAsync<int>();
Console.WriteLine($"count: {count}");
// paging: follow @odata.nextLink to the end
int pages = 0, total = 0;
var pa = new ODataFeedAnnotations();
var all = await client.For("customers").Count().FindScalarAsync<int>();
var page = await client.For("customers").FindEntriesAsync(pa);
for (;;) {
  pages++; total += page.Count();
  if (pa.NextPageLink == null) break;
  page = await client.For("customers").FindEntriesAsync(pa.NextPageLink, pa);
}
Console.WriteLine($"paged: {total} of {all} rows in {pages} pages");
var dates = (await client.For("customers").Filter("signup_date ge 2024-06-01 and startswith(name, 'c1')").Top(2).FindEntriesAsync()).ToList();
Console.WriteLine($"dates: {dates.Count} rows; first signup {dates[0]["signup_date"]}");
