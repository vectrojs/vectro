// Vectro for Power BI: the tables Vectro serves (vectro serve --port), read
// through its OData feed (Power Query folds filters, chosen columns and
// sorting into the requests), or a SQL query of your own through its HTTP
// endpoint. For DirectQuery, use Power BI's PostgreSQL connector with
// vectro serve --pg instead.
[Version = "0.1.0"]
section Vectro;

[DataSource.Kind = "Vectro", Publish = "Vectro.Publish"]
shared Vectro.Contents = Value.ReplaceType(VectroImpl, VectroType);

VectroType = type function (
    server as (type text meta [
        Documentation.FieldCaption = "Server",
        Documentation.FieldDescription = "Vectro's HTTP address (vectro serve --port), for example http://localhost:8787",
        Documentation.SampleValues = {"http://localhost:8787"}
    ]),
    optional query as (type text meta [
        Documentation.FieldCaption = "SQL query (optional)",
        Documentation.FieldDescription = "A SELECT to run in Vectro; leave it empty to choose tables",
        Documentation.SampleValues = {"SELECT region, SUM(qty) AS units FROM orders GROUP BY 1"},
        Formatting.IsMultiLine = true,
        Formatting.IsCode = true
    ]))
    as table meta [
        Documentation.Name = "Vectro",
        Documentation.LongDescription = "Tables served by Vectro, an analytics engine in JavaScript and WebAssembly."
    ];

VectroImpl = (server as text, optional query as text) as table =>
    let
        base = Text.TrimEnd(server, "/") & "/",
        credential = Extension.CurrentCredential(),
        headers = if credential <> null and credential[AuthenticationKind] = "Key" then [Authorization = "Bearer " & credential[Key]] else [],
        result = if query = null or Text.Trim(query) = "" then Tables(base, headers) else Sql(base, query, headers)
    in
        result;

// every table, through OData: a navigation table Power BI's Navigator shows
Tables = (base as text, headers as record) as table =>
    OData.Feed(base & "odata/", null, [Implementation = "2.0", Headers = headers]);

// a query of your own: the HTTP endpoint's JSON, with its column types
Sql = (base as text, query as text, headers as record) as table =>
    let
        response = Web.Contents(base, [RelativePath = "sql", Query = [q = query, format = "json"], Headers = headers]),
        json = Json.Document(response),
        columns = json[columns],
        names = List.Transform(columns, each [name]),
        rows = Table.FromRows(json[rows], names),
        typeOf = (kind as text) as type => if kind = "number" then type number else if kind = "date" then type datetimezone else type text,
        typed = Table.TransformColumnTypes(rows, List.Transform(columns, each {[name], typeOf([type])}))
    in
        typed;

// the data source: anonymous, or a token (vectro serve --token)
Vectro = [
    TestConnection = (dataSourcePath) => { "Vectro.Contents", dataSourcePath },
    Authentication = [
        Key = [
            KeyLabel = Extension.LoadString("KeyLabel"),
            Label = Extension.LoadString("KeyAuthLabel")
        ],
        Implicit = []
    ],
    Label = Extension.LoadString("DataSourceLabel")
];

Vectro.Publish = [
    Beta = true,
    Category = "Database",
    ButtonText = { Extension.LoadString("ButtonTitle"), Extension.LoadString("ButtonHelp") },
    LearnMoreUrl = "https://github.com/vectrojs/vectro",
    SourceImage = Vectro.Icons,
    SourceTypeImage = Vectro.Icons
];

Vectro.Icons = [
    Icon16 = { Extension.Contents("Vectro16.png"), Extension.Contents("Vectro20.png"), Extension.Contents("Vectro24.png"), Extension.Contents("Vectro32.png") },
    Icon32 = { Extension.Contents("Vectro32.png"), Extension.Contents("Vectro40.png"), Extension.Contents("Vectro48.png"), Extension.Contents("Vectro64.png") }
];
