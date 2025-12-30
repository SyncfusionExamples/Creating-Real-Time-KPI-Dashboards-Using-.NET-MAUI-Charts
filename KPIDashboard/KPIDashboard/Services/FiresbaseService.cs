using System.Text.Json;
using System.Net.Http;
using System.Linq;

namespace KPIDashboard
{
    public sealed class FirebaseService : IAsyncDisposable
    {
        private static readonly HttpClient _httpClient = new();
        private static readonly JsonSerializerOptions _jsonOptions = new() { PropertyNameCaseInsensitive = true };

        public event EventHandler<string>? KpiChanged;
        public event EventHandler<SalesRecord>? NewSalesRecord;

        public FirebaseService(string? _ = null) { }

        // --- Helpers ---
        private static double ParseSalesValue(JsonElement e)
        {
            if (e.ValueKind == JsonValueKind.Number) return e.GetDouble();
            if (e.ValueKind == JsonValueKind.String && double.TryParse(e.GetString(), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var v)) return v;
            return 0.0;
        }

        private static string EnsureJsonSuffix(string baseUrl) => baseUrl.EndsWith(".json") ? baseUrl : baseUrl;

        private sealed class FbSalesRecord { public DateTime Date { get; set; } public string? SalesChannel { get; set; } public string? Region { get; set; } public JsonElement Sales { get; set; } public int? Quantity { get; set; } }
        private sealed class FbTimePoint { public DateTime Time { get; set; } public double Value { get; set; } }
        private sealed class FbCategoryPoint { public string? Category { get; set; } public double Value { get; set; } }
        private sealed class FbSalesProgress { public double Target { get; set; } public double Actual { get; set; } public double Remaining { get; set; } }

        // --- Real-time: clear DB and write one-by-one while app runs, using SSE as pure-C# fallback listener ---
        private Task? _streamTask;
        private Task? _simTask;
        private CancellationTokenSource? _streamCts;
        private string? _recordsBasePath;
        private DateTime _simCurrentDate;
        private readonly Random _rand = new();

        public async Task StartListeningAsync(CancellationToken externalToken = default)
        {
            if (_streamTask is not null && !_streamTask.IsCompleted) return;

            _streamCts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
            var ct = _streamCts.Token;
            var baseUrl = Environment.GetEnvironmentVariable("FIREBASE_BASE") ?? "https://kpi-dashboard-demo-aa2ed-default-rtdb.asia-southeast1.firebasedatabase.app";
            var url = EnsureJsonSuffix(baseUrl.TrimEnd('/')) + "/salesrecords.json";

            // store base path for posts (without .json)
            _recordsBasePath = url.EndsWith(".json") ? url.Substring(0, url.Length - ".json".Length) : url;

            // Clear database on start
            try { await ClearSalesRecordsAsync(ct).ConfigureAwait(false); } catch { }

            // initialize simulation start date from env or default to today
            var startDateStr = Environment.GetEnvironmentVariable("FIREBASE_START_DATE"); // ISO or yyyy-MM-dd
            if (!string.IsNullOrEmpty(startDateStr) && DateTime.TryParse(startDateStr, out var parsed)) _simCurrentDate = parsed;
            else _simCurrentDate = DateTime.UtcNow;

            // Start SSE stream reader
            _streamTask = StartSseStreamAsync(url, ct);

            // Start simulator: push one record at a time while app runs
            _simTask = Task.Run(async () =>
            {
                try
                {
                    while (!ct.IsCancellationRequested)
                    {
                        await PushSimulatedRecordAsync(ct).ConfigureAwait(false);
                        // advance simulation date by one day to keep revenue trend moving forward
                        _simCurrentDate = _simCurrentDate.AddDays(1);
                        try { await Task.Delay(1000, ct).ConfigureAwait(false); } catch (OperationCanceledException) { break; }
                    }
                }
                catch (OperationCanceledException) { }
            }, ct);
        }

        private async Task StartSseStreamAsync(string url, CancellationToken ct)
        {
            var backoffMs = 1000;

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    using var req = new HttpRequestMessage(HttpMethod.Get, url);
                    req.Headers.Accept.Clear();
                    req.Headers.Accept.ParseAdd("text/event-stream");

                    using var resp = await _httpClient.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
                    resp.EnsureSuccessStatusCode();

                    using var stream = await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
                    using var reader = new StreamReader(stream);

                    // Reset backoff after successful connect
                    backoffMs = 1000;

                    string? line;
                    var dataBuilder = new System.Text.StringBuilder();

                    while (!ct.IsCancellationRequested && (line = await reader.ReadLineAsync().ConfigureAwait(false)) is not null)
                    {
                        if (line.StartsWith(":")) continue; // comment/keep-alive
                        if (line.StartsWith("data:"))
                        {
                            if (dataBuilder.Length > 0) dataBuilder.Append('\n');
                            dataBuilder.Append(line[5..].TrimStart());
                            continue;
                        }
                        if (string.IsNullOrWhiteSpace(line))
                        {
                            if (dataBuilder.Length > 0)
                            {
                                var json = dataBuilder.ToString();
                                dataBuilder.Clear();
                                try
                                {
                                    using var doc = JsonDocument.Parse(json);
                                    var root = doc.RootElement;

                                    // path may indicate child updates
                                    root.TryGetProperty("path", out var pathEl);
                                    var pathStr = pathEl.ValueKind == JsonValueKind.String ? pathEl.GetString() ?? string.Empty : string.Empty;

                                    if (root.TryGetProperty("data", out var dataEl))
                                    {
                                        // If data is null, skip
                                        if (dataEl.ValueKind == JsonValueKind.Null) { /* deleted or empty */ }
                                        else if (dataEl.ValueKind == JsonValueKind.Object)
                                        {
                                            // If path refers to a child (e.g. "/-Mx..."), parse that object as a single record
                                            if (!string.IsNullOrEmpty(pathStr) && pathStr != "/")
                                            {
                                                if (TryParseFbRecord(dataEl, out var rec))
                                                {
                                                    try { NewSalesRecord?.Invoke(this, ToSalesRecord(rec)); } catch { }
                                                }
                                            }
                                            else
                                            {
                                                // Snapshot or map of children: parse all child objects and emit each
                                                foreach (var prop in dataEl.EnumerateObject())
                                                {
                                                    var v = prop.Value;
                                                    if (v.ValueKind != JsonValueKind.Object) continue;
                                                    if (TryParseFbRecord(v, out var rec))
                                                    {
                                                        try { NewSalesRecord?.Invoke(this, ToSalesRecord(rec)); } catch { }
                                                    }
                                                }
                                            }
                                        }
                                        else if (dataEl.ValueKind == JsonValueKind.Array)
                                        {
                                            // array of records
                                            var list = ParseFbArray(dataEl);
                                            foreach (var r in list)
                                            {
                                                try { NewSalesRecord?.Invoke(this, ToSalesRecord(r)); } catch { }
                                            }
                                        }
                                    }
                                }
                                catch { }
                            }
                            continue;
                        }
                    }
                }
                catch (OperationCanceledException) { break; }
                catch
                {
                    try { await Task.Delay(backoffMs, ct).ConfigureAwait(false); } catch (OperationCanceledException) { break; }
                    backoffMs = Math.Min(backoffMs * 2, 15000);
                }
            }
        }

        private async Task PushSimulatedRecordAsync(CancellationToken ct)
        {
            var rec = new Dictionary<string, object?>
            {
                ["Date"] = _simCurrentDate.ToString("o"),
                ["SalesChannel"] = new[] { "Retail", "In-Store", "Online" }[_rand.Next(0,3)],
                ["Region"] = new[] { "East", "South", "West", "North" }[_rand.Next(0,4)],
                ["Sales"] = Math.Round(100 + _rand.NextDouble() * 1000.0, 2),
                ["Quantity"] = _rand.Next(1, 20)
            };

            try
            {
                if (string.IsNullOrEmpty(_recordsBasePath)) return;
                var url = _recordsBasePath + ".json";
                var content = new StringContent(JsonSerializer.Serialize(rec), System.Text.Encoding.UTF8, "application/json");
                using var resp = await _httpClient.PostAsync(url, content, ct).ConfigureAwait(false);
                // ignore response; SSE listener will receive the child event and invoke NewSalesRecord immediately
            }
            catch { }
        }

        private async Task ClearSalesRecordsAsync(CancellationToken ct)
        {
            try
            {
                if (string.IsNullOrEmpty(_recordsBasePath)) return;
                var url = _recordsBasePath + ".json";
                using var req = new HttpRequestMessage(HttpMethod.Delete, url);
                using var resp = await _httpClient.SendAsync(req, ct).ConfigureAwait(false);
            }
            catch { }
        }

        // --- Existing parsing helpers ---
        private static List<FbSalesRecord> ParseFbArray(JsonElement arrayEl)
        {
            var list = new List<FbSalesRecord>();
            foreach (var el in arrayEl.EnumerateArray())
            {
                try
                {
                    var rec = new FbSalesRecord
                    {
                        Date = el.TryGetProperty("Date", out var d) && d.ValueKind == JsonValueKind.String ? DateTime.Parse(d.GetString()!) : el.GetProperty("Date").GetDateTime(),
                        SalesChannel = el.TryGetProperty("SalesChannel", out var sc) && sc.ValueKind == JsonValueKind.String ? sc.GetString() : null,
                        Region = el.TryGetProperty("Region", out var r) && r.ValueKind == JsonValueKind.String ? r.GetString() : null,
                        Sales = el.TryGetProperty("Sales", out var s) ? s : default,
                        Quantity = el.TryGetProperty("Quantity", out var q) && q.ValueKind == JsonValueKind.Number ? q.GetInt32() : (int?)null
                    };
                    list.Add(rec);
                }
                catch { }
            }
            return list;
        }

        private static bool TryParseFbRecord(JsonElement el, out FbSalesRecord rec)
        {
            rec = default!;
            try
            {
                var date = DateTime.MinValue;
                if (el.TryGetProperty("Date", out var d))
                {
                    if (d.ValueKind == JsonValueKind.String) date = DateTime.Parse(d.GetString()!);
                    else if (d.ValueKind == JsonValueKind.Number) date = d.GetDateTime();
                    else date = d.GetDateTime();
                }
                else
                {
                    return false;
                }

                var salesChannel = el.TryGetProperty("SalesChannel", out var sc) && sc.ValueKind == JsonValueKind.String ? sc.GetString() : null;
                var region = el.TryGetProperty("Region", out var r) && r.ValueKind == JsonValueKind.String ? r.GetString() : null;
                var sales = el.TryGetProperty("Sales", out var s) ? s : default;
                var quantity = el.TryGetProperty("Quantity", out var q) && q.ValueKind == JsonValueKind.Number ? q.GetInt32() : (int?)null;

                rec = new FbSalesRecord { Date = date, SalesChannel = salesChannel, Region = region, Sales = sales, Quantity = quantity };
                return true;
            }
            catch
            {
                return false;
            }
        }

        private static SalesRecord ToSalesRecord(FbSalesRecord it) => new() { Date = it.Date, Channel = it.SalesChannel, Region = it.Region, UnitsSold = it.Quantity ?? 1, Revenue = ParseSalesValue(it.Sales) };

        public async ValueTask DisposeAsync()
        {
            try
            {
                _streamCts?.Cancel();
                if (_streamTask is not null) await _streamTask;
                if (_simTask is not null) await _simTask;
            }
            catch { }
            finally { _streamCts?.Dispose(); }
        }

        public Task EnsureSeedAsync(CancellationToken ct = default) => Task.CompletedTask;
    }

    public sealed class SalesNotifyPayload { public DateTime Timestamp { get; set; } public string Channel { get; set; } = ""; public SalesRecord? Record { get; set; } }
}

