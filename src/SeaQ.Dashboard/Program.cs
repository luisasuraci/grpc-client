using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using SeaQ.Common;

var parsed = CliArguments.Parse(args);
DbConfig dbConfig;
try
{
    dbConfig = new DbConfig(
        parsed.GetRequired("db-backend"),
        parsed.GetRequired("db-host"),
        parsed.GetRequiredInt("db-port"),
        parsed.GetRequired("db-name"),
        parsed.GetRequired("db-user"),
        parsed.GetRequired("db-password"));
}
catch (ArgumentException exception)
{
    Console.Error.WriteLine(exception.Message);
    Console.Error.WriteLine();
    Console.Error.WriteLine("Usage:");
    Console.Error.WriteLine("  dotnet run --project src/SeaQ.Dashboard -- --db-backend postgresql|mariadb --db-host 127.0.0.1 --db-port 5432 --db-name thea --db-user user --db-password pass");
    return;
}

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls(parsed.GetOptional("urls", "http://0.0.0.0:5000"));
builder.Services.AddSingleton(new DatabaseService(dbConfig));

var app = builder.Build();

app.MapGet("/", async (HttpContext context, DatabaseService database, CancellationToken cancellationToken) =>
{
    try
    {
        var tagFilter = context.Request.Query["tagFilter"].ToString();
        var startTs = context.Request.Query["startTs"].ToString();
        var endTs = context.Request.Query["endTs"].ToString();
        var page = ParseInt(context.Request.Query["page"], 1);
        var pageSize = ParseInt(context.Request.Query["pageSize"], 50, new[] { 50, 100, 250, 500, 1000 });

        var query = await database.BuildDashboardQueryAsync(tagFilter, startTs, endTs, page, pageSize, cancellationToken);
        var metrics = await database.GetMetricsAsync(query, cancellationToken);
        var tableRows = await database.GetTableRowsAsync(query, metrics.Page, metrics.PageSize, cancellationToken);
        var chartRows = string.IsNullOrWhiteSpace(metrics.TagFilter)
            ? Array.Empty<ChartSignalRow>()
            : await database.GetChartRowsAsync(query, cancellationToken);

        var html = RenderDashboard(metrics, tableRows, chartRows);
        context.Response.ContentType = "text/html; charset=utf-8";
        await context.Response.WriteAsync(html, cancellationToken);
    }
    catch (Exception exception)
    {
        context.Response.StatusCode = StatusCodes.Status500InternalServerError;
        context.Response.ContentType = "text/html; charset=utf-8";
        await context.Response.WriteAsync(RenderError(exception), cancellationToken);
    }
});

app.Run();

static string RenderDashboard(DashboardMetrics metrics, IReadOnlyList<TableSignalRow> rows, IReadOnlyList<ChartSignalRow> chartRows)
{
    double rpm = 0;
    double throughput = 0;
    if (metrics.CountSignals > 0 && metrics.MinTimestamp is not null && metrics.MaxTimestamp is not null && metrics.MaxTimestamp != metrics.MinTimestamp)
    {
        var spanSeconds = (metrics.MaxTimestamp.Value - metrics.MinTimestamp.Value) / (double)(metrics.TimestampsAreSeconds ? 1 : 1000);
        if (spanSeconds > 0)
        {
            rpm = metrics.CountSignals / (spanSeconds / 60d);
            throughput = metrics.TotalBytes / spanSeconds;
        }
    }

    var pageOptionsHtml = string.Join(Environment.NewLine, new[] { 50, 100, 250, 500, 1000 }
        .Select(option => $"<option value=\"{option}\"{(metrics.PageSize == option ? " selected" : string.Empty)}>{option}</option>"));

    var tableHtml = BuildTableHtml(rows);
    var queryBase = $"tagFilter={WebUtility.UrlEncode(metrics.TagFilter)}&startTs={WebUtility.UrlEncode(metrics.StartInput)}&endTs={WebUtility.UrlEncode(metrics.EndInput)}&pageSize={metrics.PageSize}";
    var previousPage = Math.Max(metrics.Page - 1, 1);
    var nextPage = Math.Min(metrics.Page + 1, metrics.TotalPages);
    var bucketLabel = metrics.TimestampsAreSeconds ? "60 secondi" : "60000 ms";
    var chartHtml = BuildChartHtml(metrics, chartRows, bucketLabel);
    var chartJson = JsonSerializer.Serialize(chartRows.Select(row => new
    {
        tag = row.Tag,
        timestampMs = SignalValueHelpers.NormalizeEpochMs(row.TimestampRaw),
        count = row.Count,
    }));

    var sb = new StringBuilder();
    sb.AppendLine("<!DOCTYPE html>");
    sb.AppendLine("<html lang=\"it\">");
    sb.AppendLine("<head>");
    sb.AppendLine("  <meta charset=\"utf-8\" />");
    sb.AppendLine("  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />");
    sb.AppendLine("  <title>SeaQ - Statistiche segnali</title>");
    sb.AppendLine("  <script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\"></script>");
    sb.AppendLine("  <style>");
    sb.AppendLine("    body { font-family: Arial, sans-serif; margin: 0; background: #0b1220; color: #e5e7eb; }");
    sb.AppendLine("    .container { max-width: 1400px; margin: 0 auto; padding: 24px; }");
    sb.AppendLine("    h1, h2 { margin-bottom: 12px; }");
    sb.AppendLine("    form, .metrics, .pager, .card { background: #111827; border-radius: 12px; padding: 16px; margin-bottom: 20px; }");
    sb.AppendLine("    form { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 12px; align-items: end; }");
    sb.AppendLine("    label { display: flex; flex-direction: column; gap: 6px; font-size: 14px; }");
    sb.AppendLine("    input, select, button, a.button { border-radius: 8px; border: 1px solid #374151; background: #0f172a; color: #e5e7eb; padding: 10px; text-decoration: none; }");
    sb.AppendLine("    button, a.button { cursor: pointer; text-align: center; }");
    sb.AppendLine("    .metrics { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }");
    sb.AppendLine("    .metric { background: #0f172a; border-radius: 10px; padding: 16px; }");
    sb.AppendLine("    .metric .label { color: #9ca3af; font-size: 13px; }");
    sb.AppendLine("    .metric .value { font-size: 28px; margin-top: 8px; }");
    sb.AppendLine("    .notice { padding: 12px 16px; border-radius: 10px; margin-bottom: 16px; }");
    sb.AppendLine("    .notice.info { background: #172554; }");
    sb.AppendLine("    .notice.warning { background: #78350f; }");
    sb.AppendLine("    table { width: 100%; border-collapse: collapse; overflow: hidden; border-radius: 12px; }");
    sb.AppendLine("    th, td { padding: 10px; border-bottom: 1px solid #1f2937; text-align: left; }");
    sb.AppendLine("    th { background: #0f172a; position: sticky; top: 0; }");
    sb.AppendLine("    .pager { display: flex; justify-content: space-between; align-items: center; gap: 12px; }");
    sb.AppendLine("    .pager .links { display: flex; gap: 8px; }");
    sb.AppendLine("    .caption { color: #9ca3af; font-size: 13px; margin-top: 10px; }");
    sb.AppendLine("    #chart { width: 100%; height: 500px; }");
    sb.AppendLine("    @media (max-width: 960px) { form, .metrics { grid-template-columns: 1fr; } .pager { flex-direction: column; align-items: stretch; } }");
    sb.AppendLine("  </style>");
    sb.AppendLine("</head>");
    sb.AppendLine("<body>");
    sb.AppendLine("  <div class=\"container\">");
    sb.AppendLine("    <h1>SeaQ - Statistiche segnali</h1>");
    sb.AppendLine("    <form method=\"get\">");
    sb.AppendLine($"      <label>Cerca tag (contains)<input type=\"text\" name=\"tagFilter\" value=\"{HtmlEncoder.Default.Encode(metrics.TagFilter)}\" /></label>");
    sb.AppendLine($"      <label>Timestamp start (ms)<input type=\"text\" name=\"startTs\" value=\"{HtmlEncoder.Default.Encode(metrics.StartInput)}\" /></label>");
    sb.AppendLine($"      <label>Timestamp end (ms)<input type=\"text\" name=\"endTs\" value=\"{HtmlEncoder.Default.Encode(metrics.EndInput)}\" /></label>");
    sb.AppendLine($"      <label>Righe per pagina<select name=\"pageSize\">{pageOptionsHtml}</select></label>");
    sb.AppendLine("      <div style=\"display:flex; gap:12px;\"><button type=\"submit\">Applica filtri</button><a class=\"button\" href=\"/\">Pulisci filtri</a></div>");
    sb.AppendLine("      <input type=\"hidden\" name=\"page\" value=\"1\" />");
    sb.AppendLine("    </form>");
    sb.AppendLine("    <div class=\"metrics\">");
    sb.AppendLine($"      <div class=\"metric\"><div class=\"label\">Totale segnali</div><div class=\"value\">{metrics.TotalSignals}</div></div>");
    sb.AppendLine($"      <div class=\"metric\"><div class=\"label\">Segnali filtrati</div><div class=\"value\">{metrics.FilteredSignals}</div></div>");
    sb.AppendLine($"      <div class=\"metric\"><div class=\"label\">Rate</div><div class=\"value\">{rpm.ToString("F2", CultureInfo.InvariantCulture)} segnali/min</div></div>");
    sb.AppendLine($"      <div class=\"metric\"><div class=\"label\">Throughput</div><div class=\"value\">{throughput.ToString("F2", CultureInfo.InvariantCulture)} B/s</div></div>");
    sb.AppendLine("    </div>");
    if (metrics.UsingDefaultWindow)
    {
        sb.AppendLine("    <div class=\"notice info\">Filtro temporale di default attivo: ultimi 15 minuti.</div>");
    }
    sb.AppendLine("    <div class=\"pager\">");
    sb.AppendLine($"      <div>Pagina {metrics.Page} di {metrics.TotalPages}</div>");
    sb.AppendLine($"      <div class=\"links\"><a class=\"button\" href=\"/?{queryBase}&page={previousPage}\">Pagina precedente</a><a class=\"button\" href=\"/?{queryBase}&page={nextPage}\">Pagina successiva</a></div>");
    sb.AppendLine("    </div>");
    sb.AppendLine("    <div class=\"card\"><h2>Segnali</h2>");
    sb.AppendLine(tableHtml);
    sb.AppendLine("    </div>");
    sb.AppendLine("    <div class=\"card\"><h2>Rate segnali per tag</h2>");
    sb.AppendLine(chartHtml);
    sb.AppendLine("    </div>");
    sb.AppendLine("  </div>");
    sb.AppendLine("  <script>");
    sb.AppendLine($"    const rows = {chartJson};");
    sb.AppendLine("    if (rows.length > 0 && document.getElementById('chart')) {");
    sb.AppendLine("      const grouped = new Map();");
    sb.AppendLine("      for (const row of rows) {");
    sb.AppendLine("        const key = row.tag;");
    sb.AppendLine("        if (!grouped.has(key)) grouped.set(key, { x: [], y: [], mode: rows.length > 5000 ? 'lines' : 'lines+markers', type: 'scattergl', name: key });");
    sb.AppendLine("        grouped.get(key).x.push(new Date(row.timestampMs));");
    sb.AppendLine("        grouped.get(key).y.push(row.count);");
    sb.AppendLine("      }");
    sb.AppendLine("      Plotly.newPlot('chart', Array.from(grouped.values()), { paper_bgcolor: '#111827', plot_bgcolor: '#111827', font: { color: '#e5e7eb' }, xaxis: { title: 'timestamp' }, yaxis: { title: 'count' }, margin: { t: 20, r: 20, b: 50, l: 50 } }, { responsive: true });");
    sb.AppendLine("    }");
    sb.AppendLine("  </script>");
    sb.AppendLine("</body>");
    sb.AppendLine("</html>");
    return sb.ToString();
}

static string BuildTableHtml(IReadOnlyList<TableSignalRow> rows)
{
    var sb = new StringBuilder();
    if (rows.Count == 0)
    {
        sb.AppendLine("<div class=\"notice warning\">Nessun segnale trovato con i filtri impostati.</div>");
        return sb.ToString();
    }

    sb.AppendLine("<table>");
    sb.AppendLine("<thead><tr><th>tag</th><th>timestamp_ms</th><th>timestamp</th><th>value</th><th>value_type</th><th>quality</th><th>payload_bytes</th></tr></thead>");
    sb.AppendLine("<tbody>");
    foreach (var row in rows)
    {
        var normalized = SignalValueHelpers.NormalizeEpochMs(row.TimestampMs) ?? row.TimestampMs;
        var utcTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(normalized).ToString("u");
        sb.AppendLine($"<tr><td>{HtmlEncoder.Default.Encode(row.Tag)}</td><td>{row.TimestampMs}</td><td>{utcTimestamp}</td><td>{HtmlEncoder.Default.Encode(row.Value)}</td><td>{HtmlEncoder.Default.Encode(row.ValueType)}</td><td>{HtmlEncoder.Default.Encode(row.Quality)}</td><td>{row.PayloadBytes}</td></tr>");
    }
    sb.AppendLine("</tbody></table>");
    return sb.ToString();
}

static string BuildChartHtml(DashboardMetrics metrics, IReadOnlyList<ChartSignalRow> chartRows, string bucketLabel)
{
    if (string.IsNullOrWhiteSpace(metrics.TagFilter))
    {
        return "<div class=\"notice info\">Grafico disponibile solo con filtro tag attivo.</div>";
    }

    if (chartRows.Count == 0)
    {
        return "<div class=\"notice info\">Nessun dato disponibile per il grafico con i filtri correnti.</div>";
    }

    return $"<div id=\"chart\"></div><div class=\"caption\">Bucket grafico attuale: {bucketLabel} (tutti i tag).</div><div class=\"caption\">Se più tag condividono gli stessi istanti, il rendering mantiene le serie separate nel grafico.</div>";
}

static string RenderError(Exception exception)
{
    var encoded = HtmlEncoder.Default.Encode(exception.ToString());
    return $"<!DOCTYPE html><html lang=\"it\"><head><meta charset=\"utf-8\" /><title>Errore dashboard</title></head><body style=\"font-family:Arial,sans-serif;padding:24px;\"><h1>Connessione al database non disponibile</h1><pre>{encoded}</pre></body></html>";
}

static int ParseInt(string? rawValue, int defaultValue, int[]? allowed = null)
{
    if (!int.TryParse(rawValue, out var value))
    {
        return defaultValue;
    }

    if (allowed is not null && !allowed.Contains(value))
    {
        return defaultValue;
    }

    return value;
}
