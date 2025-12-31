using System.Text;
using System.Text.Json;

namespace KPIDashboard
{
    /// <summary>
    /// FirebaseService: per-session isolation, SSE listener, optional simulator.
    /// Works cross-platform (.NET MAUI / Console) as it's pure HTTP/JSON.
    /// </summary>
    public sealed class FirebaseService : IAsyncDisposable
    {
        private static readonly HttpClient _httpClient = new();
        private static readonly JsonSerializerOptions _jsonOptions = new() { PropertyNameCaseInsensitive = true };

        // Events
        public event EventHandler<SalesRecord>? NewSalesRecord;
        public event EventHandler<string>? KpiChanged; // reserved

        // Runtime state
        private Task? _streamTask;
        private Task? _simTask;
        private CancellationTokenSource? _streamCts;
        private string? _recordsBasePath; // WITHOUT .json
        private DateTime _simCurrentDate;
        private readonly Random _rand = new();

        // Per-session id (env override or auto GUID)
        private readonly string _instanceId =
            Environment.GetEnvironmentVariable("SESSION_ID") ?? Guid.NewGuid().ToString("N");

        public FirebaseService(string? _ = null) { }

        public async Task StartListeningAsync(CancellationToken externalToken = default)
        {
            // Prevent double-start
            if (_streamTask is not null && !_streamTask.IsCompleted) return;

            _streamCts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
            var ct = _streamCts.Token;

            // Base URL: never end with .json; trim trailing slash
            var baseUrl = Environment.GetEnvironmentVariable("FIREBASE_BASE")
                ?? "https://kpi-dashboard-demo-aa2ed-default-rtdb.asia-southeast1.firebasedatabase.app";
            baseUrl = baseUrl.TrimEnd('/');

            // Per-session path (append .json ONLY to data path)
            var sessionPathJson = $"/sessions/{_instanceId}/salesrecords.json";
            var fullJsonUrl = baseUrl + sessionPathJson;

            // Store base path WITHOUT .json
            _recordsBasePath = fullJsonUrl.Substring(0, fullJsonUrl.Length - ".json".Length);

            // Optional: clear only this session path
            var clearOnStart = Environment.GetEnvironmentVariable("FIREBASE_CLEAR_ON_START") == "1";
            if (clearOnStart)
            {
                try { await ClearPathAsync(_recordsBasePath + ".json", ct).ConfigureAwait(false); } catch { }
            }

            // Simulator start date
            var startDateStr = Environment.GetEnvironmentVariable("FIREBASE_START_DATE");
            _simCurrentDate = (!string.IsNullOrEmpty(startDateStr) && DateTime.TryParse(startDateStr, out var parsed))
                ? parsed
                : DateTime.UtcNow;

            // Start SSE listener (read-only) for THIS session
            _streamTask = StartSseStreamAsync(_recordsBasePath + ".json", ct);

            var delayMs = int.TryParse(Environment.GetEnvironmentVariable("FIREBASE_SIM_DELAY_MS"), out var d) ? d : 1000;

            _simTask = Task.Run(async () =>
            {
                try
                {
                    while (!ct.IsCancellationRequested)
                    {
                        await PushSimulatedRecordAsync(ct).ConfigureAwait(false);
                        _simCurrentDate = _simCurrentDate.AddDays(1);
                        try { await Task.Delay(delayMs, ct).ConfigureAwait(false); } catch (OperationCanceledException) { break; }
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

                    backoffMs = 1000; // reset on success

                    string? line;
                    var dataBuilder = new StringBuilder();

                    while (!ct.IsCancellationRequested && (line = await reader.ReadLineAsync().ConfigureAwait(false)) is not null)
                    {
                        if (line.StartsWith(":")) continue; // keep-alive

                        if (line.StartsWith("data:"))
                        {
                            if (dataBuilder.Length > 0) dataBuilder.Append(line[5..].TrimStart());
                            var payload = line.Length > 5 ? line.Substring(5).TrimStart() : string.Empty; // safe slice
                            dataBuilder.Append(payload);
                            continue;
                        }

                        // Blank line => end of SSE event
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

                                    root.TryGetProperty("path", out var pathEl);
                                    var pathStr = pathEl.ValueKind == JsonValueKind.String ? pathEl.GetString() ?? string.Empty : string.Empty;

                                    if (root.TryGetProperty("data", out var dataEl))
                                    {
                                        if (dataEl.ValueKind == JsonValueKind.Null)
                                        {
                                            // deleted or empty
                                        }
                                        else if (dataEl.ValueKind == JsonValueKind.Object)
                                        {
                                            // Child update => single record
                                            if (!string.IsNullOrEmpty(pathStr) && pathStr != "/")
                                            {
                                                if (TryParseFbRecord(dataEl, out var rec))
                                                    try { NewSalesRecord?.Invoke(this, ToSalesRecord(rec)); } catch { }
                                            }
                                            else
                                            {
                                                // Snapshot/map => iterate children
                                                foreach (var prop in dataEl.EnumerateObject())
                                                {
                                                    var v = prop.Value;
                                                    if (v.ValueKind != JsonValueKind.Object) continue;
                                                    if (TryParseFbRecord(v, out var rec))
                                                        try { NewSalesRecord?.Invoke(this, ToSalesRecord(rec)); } catch { }
                                                }
                                            }
                                        }
                                        else if (dataEl.ValueKind == JsonValueKind.Array)
                                        {
                                            var list = ParseFbArray(dataEl);
                                            foreach (var r in list)
                                                try { NewSalesRecord?.Invoke(this, ToSalesRecord(r)); } catch { }
                                        }
                                    }
                                }
                                catch { /* swallow for demo stability or log */ }
                            }
                            continue;
                        }
                    }
                }
                catch (OperationCanceledException) { break; }
                catch
                {
                    try { await Task.Delay(backoffMs, ct).ConfigureAwait(false); } catch (OperationCanceledException) { break; }
                    backoffMs = Math.Min(backoffMs * 2, 15000); // exponential backoff
                }
            }
        }

        private async Task PushSimulatedRecordAsync(CancellationToken ct)
        {
            var rec = new Dictionary<string, object?>
            {
                ["Date"] = _simCurrentDate.ToString("o"),
                ["SalesChannel"] = new[] { "Retail", "In-Store", "Online" }[_rand.Next(0, 3)],
                ["Region"] = new[] { "East", "South", "West", "North" }[_rand.Next(0, 4)],
                ["Sales"] = Math.Round(100 + _rand.NextDouble() * 1000.0, 2),
                ["Quantity"] = _rand.Next(1, 20),
                ["SourceInstanceId"] = _instanceId
            };

            try
            {
                if (string.IsNullOrEmpty(_recordsBasePath)) return;
                var postUrl = _recordsBasePath + ".json";
                var content = new StringContent(JsonSerializer.Serialize(rec), Encoding.UTF8, "application/json");
                using var resp = await _httpClient.PostAsync(postUrl, content, ct).ConfigureAwait(false);
                // SSE listener will receive child-added event
            }
            catch { }
        }

        private async Task ClearPathAsync(string jsonUrl, CancellationToken ct)
        {
            using var req = new HttpRequestMessage(HttpMethod.Delete, jsonUrl);
            using var resp = await _httpClient.SendAsync(req, ct).ConfigureAwait(false);
            resp.EnsureSuccessStatusCode();
        }

        // --- Parsing helpers ---
        private sealed class FbSalesRecord
        {
            public DateTime Date { get; set; }
            public string? SalesChannel { get; set; }
            public string? Region { get; set; }
            public JsonElement Sales { get; set; }
            public int? Quantity { get; set; }
            public string? SourceInstanceId { get; set; }
        }

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
                        Quantity = el.TryGetProperty("Quantity", out var q) && q.ValueKind == JsonValueKind.Number ? q.GetInt32() : (int?)null,
                        SourceInstanceId = el.TryGetProperty("SourceInstanceId", out var src) && src.ValueKind == JsonValueKind.String ? src.GetString() : null
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
                    else date = d.GetDateTime();
                }
                else return false;

                var salesChannel = el.TryGetProperty("SalesChannel", out var sc) && sc.ValueKind == JsonValueKind.String ? sc.GetString() : null;
                var region = el.TryGetProperty("Region", out var r) && r.ValueKind == JsonValueKind.String ? r.GetString() : null;
                var sales = el.TryGetProperty("Sales", out var s) ? s : default;
                var quantity = el.TryGetProperty("Quantity", out var q) && q.ValueKind == JsonValueKind.Number ? q.GetInt32() : (int?)null;
                var sourceInstanceId = el.TryGetProperty("SourceInstanceId", out var src) && src.ValueKind == JsonValueKind.String ? src.GetString() : null;

                rec = new FbSalesRecord { Date = date, SalesChannel = salesChannel, Region = region, Sales = sales, Quantity = quantity, SourceInstanceId = sourceInstanceId };
                return true;
            }
            catch { return false; }
        }

        private static double ParseSalesValue(JsonElement e)
        {
            if (e.ValueKind == JsonValueKind.Number) return e.GetDouble();
            if (e.ValueKind == JsonValueKind.String && double.TryParse(e.GetString(), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var v)) return v;
            return 0.0;
        }

        private static SalesRecord ToSalesRecord(FbSalesRecord it) => new()
        {
            Date = it.Date,
            Channel = it.SalesChannel,
            Region = it.Region,
            UnitsSold = it.Quantity ?? 1,
            Revenue = ParseSalesValue(it.Sales),
            SourceInstanceId = it.SourceInstanceId
        };

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
}
