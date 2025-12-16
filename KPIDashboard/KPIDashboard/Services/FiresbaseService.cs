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

        private async Task<List<T>> FetchArrayAsync<T>(string url, CancellationToken ct)
        {
            try
            {
                using var resp = await _httpClient.GetAsync(url, ct).ConfigureAwait(false);
                if (!resp.IsSuccessStatusCode) return new List<T>();
                var json = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
                return JsonSerializer.Deserialize<List<T>>(json, _jsonOptions) ?? new List<T>();
            }
            catch
            {
                return new List<T>();
            }
        }

        // --- Aggregation APIs ---
        public async Task<List<TimePoint>> GetRevenueTrendFromSalesRecordsAsync(string firebaseBaseUrl, CancellationToken ct = default)
        {
            var url = EnsureJsonSuffix(firebaseBaseUrl.TrimEnd('/')) + "/salesrecords.json";
            var records = await FetchArrayAsync<FbSalesRecord>(url, ct).ConfigureAwait(false);
            if (!records.Any()) return new List<TimePoint>();

            var maxDate = records.Max(r => r.Date.Date);
            var start = maxDate.AddDays(-59).Date;

            var grouped = records
                .Where(r => r.Date.Date >= start && r.Date.Date <= maxDate)
                .GroupBy(r => r.Date.Date)
                .ToDictionary(g => g.Key, g => g.Sum(r => ParseSalesValue(r.Sales)));

            return Enumerable.Range(0, (maxDate - start).Days + 1)
                             .Select(i => start.AddDays(i))
                             .Select(d => new TimePoint { Time = d, Value = grouped.TryGetValue(d, out var v) ? v : 0 })
                             .ToList();
        }

        public async Task<List<CategoryPoint>> GetRevenueByRegionFromSalesRecordsAsync(string firebaseBaseUrl, CancellationToken ct = default)
        {
            var url = EnsureJsonSuffix(firebaseBaseUrl.TrimEnd('/')) + "/salesrecords.json";
            var records = await FetchArrayAsync<FbSalesRecord>(url, ct).ConfigureAwait(false);
            if (!records.Any()) return new List<CategoryPoint>();

            var maxDate = records.Max(r => r.Date.Date);
            var start = maxDate.AddDays(-59).Date;
            static string Norm(string? s) => string.IsNullOrWhiteSpace(s) || string.Equals(s, "nan", StringComparison.OrdinalIgnoreCase) ? "Others" : s!;

            var groups = records.Where(r => r.Date.Date >= start && r.Date.Date <= maxDate)
                                .GroupBy(r => Norm(r.Region))
                                .Select(g => new { Region = g.Key, Sum = g.Sum(r => ParseSalesValue(r.Sales)), Count = g.Count() })
                                .OrderByDescending(x => x.Sum)
                                .ToList();

            var total = groups.Sum(g => g.Sum);
            if (total > 0) return groups.Select(g => new CategoryPoint { Category = g.Region, Value = (g.Sum / total) * 100.0 }).ToList();

            var totalCount = groups.Sum(g => g.Count);
            if (totalCount <= 0) return new List<CategoryPoint>();
            return groups.Select(g => new CategoryPoint { Category = g.Region, Value = (g.Count / (double)totalCount) * 100.0 }).ToList();
        }

        public async Task<List<CategoryPoint>> GetLeadsByChannelFromSalesRecordsAsync(string firebaseBaseUrl, CancellationToken ct = default)
        {
            var url = EnsureJsonSuffix(firebaseBaseUrl.TrimEnd('/')) + "/salesrecords.json";
            var records = await FetchArrayAsync<FbSalesRecord>(url, ct).ConfigureAwait(false);
            if (!records.Any()) return new List<CategoryPoint>();

            var maxDate = records.Max(r => r.Date.Date);
            var start = maxDate.AddDays(-59).Date;

            return records.Where(r => r.Date.Date >= start && r.Date.Date <= maxDate)
                          .GroupBy(r => string.IsNullOrWhiteSpace(r.SalesChannel) ? "Others" : r.SalesChannel!)
                          .Select(g => new CategoryPoint { Category = g.Key, Value = g.Count() })
                          .OrderByDescending(c => c.Value)
                          .ToList();
        }

        // --- Real-time streaming via Firebase REST SSE (initial 60 immediately, then one-by-one) ---
        private Task? _streamTask;
        private Task? _drainTask;
        private CancellationTokenSource? _streamCts;
        private readonly System.Collections.Concurrent.ConcurrentQueue<SalesRecord> _emitQueue = new();

        public Task StartListeningAsync(CancellationToken externalToken = default)
        {
            if (_streamTask is not null && !_streamTask.IsCompleted) return _streamTask;

            _streamCts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
            var ct = _streamCts.Token;
            var baseUrl = Environment.GetEnvironmentVariable("FIREBASE_BASE") ?? "https://kpi-dashboard-e99ce-default-rtdb.firebaseio.com";
            var url = EnsureJsonSuffix(baseUrl.TrimEnd('/')) + "/salesrecords.json";

            // Start SSE stream reader
            _streamTask = StartSseStreamAsync(url, ct);

            // Start 1-per-second drain that invokes NewSalesRecord from queued items
            _drainTask = Task.Run(async () =>
            {
                try
                {
                    while (!ct.IsCancellationRequested)
                    {
                        if (_emitQueue.TryDequeue(out var rec))
                        {
                            try { NewSalesRecord?.Invoke(this, rec); } catch { }
                            try { await Task.Delay(1000, ct).ConfigureAwait(false); } catch (OperationCanceledException) { break; }
                        }
                        else
                        {
                            // No queued items; idle briefly
                            try { await Task.Delay(100, ct).ConfigureAwait(false); } catch (OperationCanceledException) { break; }
                        }
                    }
                }
                catch (OperationCanceledException) { }
            }, ct);

            return _streamTask!;
        }

        private async Task StartSseStreamAsync(string url, CancellationToken ct)
        {
            // Keep reconnecting on drops with simple backoff
            var backoffMs = 1000;
            var items = new List<FbSalesRecord>();
            var emittedCount = 0;

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
                    var eventName = string.Empty;
                    var dataBuilder = new System.Text.StringBuilder();

                    while (!ct.IsCancellationRequested && (line = await reader.ReadLineAsync().ConfigureAwait(false)) is not null)
                    {
                        if (line.StartsWith(":"))
                        {
                            // comment/keep-alive line; ignore
                            continue;
                        }
                        if (line.StartsWith("event:"))
                        {
                            eventName = line[6..].Trim();
                            continue;
                        }
                        if (line.StartsWith("data:"))
                        {
                            if (dataBuilder.Length > 0) dataBuilder.Append('\n');
                            dataBuilder.Append(line[5..].TrimStart());
                            continue;
                        }
                        if (string.IsNullOrWhiteSpace(line))
                        {
                            // Dispatch assembled event
                            if (dataBuilder.Length > 0)
                            {
                                var json = dataBuilder.ToString();
                                dataBuilder.Clear();
                                try
                                {
                                    using var doc = JsonDocument.Parse(json);
                                    var root = doc.RootElement;

                                    // Try get path (may be present for put/patch events)
                                    root.TryGetProperty("path", out var pathEl);
                                    var pathStr = pathEl.ValueKind == JsonValueKind.String ? pathEl.GetString() ?? string.Empty : string.Empty;

                                    if (root.TryGetProperty("data", out var dataEl))
                                    {
                                        List<FbSalesRecord> latest = new();

                                        if (dataEl.ValueKind == JsonValueKind.Array)
                                        {
                                            latest = ParseFbArray(dataEl);
                                        }
                                        else if (dataEl.ValueKind == JsonValueKind.Object)
                                        {
                                            latest = ParseFbObject(dataEl);
                                        }

                                        if (latest.Count == 0)
                                        {
                                            // nothing to do
                                        }
                                        else
                                        {
                                            // Normalize ordering
                                            latest = latest.OrderBy(x => x.Date).ToList();

                                            if (items.Count == 0 && emittedCount == 0)
                                            {
                                                // First connect: emit first 60 immediately, queue the rest
                                                var first60 = latest.Take(60).ToList();
                                                foreach (var it in first60) { try { NewSalesRecord?.Invoke(this, ToSalesRecord(it)); } catch { } }
                                                emittedCount = first60.Count;

                                                foreach (var it in latest.Skip(emittedCount)) _emitQueue.Enqueue(ToSalesRecord(it));
                                                items = latest;
                                            }
                                            else
                                            {
                                                // If this event refers to a specific child path (e.g. "/-Mx..." or "/3"), treat as immediate new record(s)
                                                if (!string.IsNullOrEmpty(pathStr) && pathStr != "/")
                                                {
                                                    foreach (var it in latest)
                                                    {
                                                        try { NewSalesRecord?.Invoke(this, ToSalesRecord(it)); } catch { }
                                                    }
                                                    // merge into items keeping order
                                                    items = items.Concat(latest).OrderBy(x => x.Date).ToList();
                                                }
                                                else
                                                {
                                                    // Snapshot/append case: queue any new tail items based on count difference
                                                    if (latest.Count > items.Count)
                                                    {
                                                        foreach (var it in latest.Skip(items.Count)) _emitQueue.Enqueue(ToSalesRecord(it));
                                                    }
                                                    items = latest;
                                                }
                                            }
                                        }
                                    }
                                }
                                catch { }
                            }
                            // End of event
                            eventName = string.Empty;
                            continue;
                        }
                    }
                }
                catch (OperationCanceledException) { break; }
                catch
                {
                    // Backoff before reconnect
                    try { await Task.Delay(backoffMs, ct).ConfigureAwait(false); } catch (OperationCanceledException) { break; }
                    backoffMs = Math.Min(backoffMs * 2, 15000);
                }
            }
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
                        Quantity = el.TryGetProperty("Quantity", out var q) && q.ValueKind == JsonValueKind.Number ? q.GetInt32() : (int?)null
                    };
                    list.Add(rec);
                }
                catch { }
            }
            return list;
        }

        // Parses either a single record object or a keyed object containing multiple child records
        private static List<FbSalesRecord> ParseFbObject(JsonElement objEl)
        {
            var list = new List<FbSalesRecord>();

            // If this object itself looks like a record (has Date property), parse as single
            if (objEl.ValueKind == JsonValueKind.Object && objEl.TryGetProperty("Date", out _))
            {
                if (TryParseFbRecord(objEl, out var rec)) list.Add(rec);
                return list;
            }

            // Otherwise treat as map of children: { "-Mx...": { ... }, "-Mx...": { ... } }
            foreach (var prop in objEl.EnumerateObject())
            {
                var v = prop.Value;
                if (v.ValueKind != JsonValueKind.Object) continue;
                try
                {
                    if (TryParseFbRecord(v, out var rec)) list.Add(rec);
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
                if (_drainTask is not null) await _drainTask;
            }
            catch { }
            finally { _streamCts?.Dispose(); }
        }

        public Task EnsureSeedAsync(CancellationToken ct = default) => Task.CompletedTask;
    }

    public sealed class SalesNotifyPayload { public DateTime Timestamp { get; set; } public string Channel { get; set; } = ""; public SalesRecord? Record { get; set; } }
}

