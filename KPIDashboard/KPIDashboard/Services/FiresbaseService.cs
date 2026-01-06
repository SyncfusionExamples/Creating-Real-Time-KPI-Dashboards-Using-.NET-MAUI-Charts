using System.Text;
using System.Text.Json;

namespace KPIDashboard
{
    /// <summary>
    /// Provides real-time connectivity to Firebase Realtime Database for the KPI dashboard demo.
    /// Implements per-session isolation, an SSE (Server-Sent Events) listener for live updates,
    /// and an optional simulator that pushes demo records at a configurable interval.
    /// </summary>
    public sealed class FirebaseService : IAsyncDisposable
    {
        /// <summary>
        /// Shared HTTP client used for all network operations.
        /// </summary>
        private static readonly HttpClient HttpClientShared = new();

        // Events
        /// <summary>
        /// Raised when a new sales record is received from Firebase.
        /// </summary>
        public event EventHandler<SalesRecord>? NewSalesRecord;

        // Runtime state
        private Task? StreamTask;
        private Task? SimulationTask;
        private CancellationTokenSource? StreamCancellationSource;

        /// <summary>
        /// Base path to the records (WITHOUT the .json suffix).
        /// </summary>
        private string? RecordsBasePath;

        /// <summary>
        /// Current date used by the simulator when pushing demo records.
        /// </summary>
        private DateTime SimulationCurrentDate;

        /// <summary>
        /// Random number generator used for demo data.
        /// </summary>
        private readonly Random RandomGenerator = new();

        /// <summary>
        /// Per-session instance ID (environment override via SESSION_ID or a generated GUID).
        /// </summary>
        private readonly string InstanceId =
            Environment.GetEnvironmentVariable("SESSION_ID") ?? Guid.NewGuid().ToString("N");

        /// <summary>
        /// Starts the SSE listener and (optionally) the demo data simulator for this session.
        /// </summary>
        /// <param name="externalToken">Cancellation token provided by the caller.</param>
        public async Task StartListeningAsync(CancellationToken externalToken = default)
        {
            // Prevent double-start
            if (StreamTask is not null && !StreamTask.IsCompleted)
            {
                return;
            }

            StreamCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
            var linkedToken = StreamCancellationSource.Token;

            // Base URL: never end with .json; trim trailing slash
            var firebaseBaseUrl = Environment.GetEnvironmentVariable("FIREBASE_BASE")
                ?? "https://kpi-dashboard-demo-aa2ed-default-rtdb.asia-southeast1.firebasedatabase.app";
            firebaseBaseUrl = firebaseBaseUrl.TrimEnd('/');

            // Per-session path (append .json ONLY to data path)
            var sessionPathJson = $"/sessions/{InstanceId}/salesrecords.json";
            var fullJsonUrl = firebaseBaseUrl + sessionPathJson;

            // Store base path WITHOUT .json
            RecordsBasePath = fullJsonUrl.Substring(0, fullJsonUrl.Length - ".json".Length);

            // Optional: clear only this session path
            var clearOnStart = Environment.GetEnvironmentVariable("FIREBASE_CLEAR_ON_START") == "1";
            if (clearOnStart)
            {
                try
                {
                    await ClearPathAsync(RecordsBasePath + ".json", linkedToken).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Swallow for demo stability; in production, log the exception.
                }
            }

            // Simulator start date
            var startDateStr = Environment.GetEnvironmentVariable("FIREBASE_START_DATE");
            SimulationCurrentDate = (!string.IsNullOrEmpty(startDateStr) && DateTime.TryParse(startDateStr, out var parsedDate))
                ? parsedDate
                : DateTime.UtcNow;

            // Start SSE listener (read-only) for THIS session
            StreamTask = StartSseStreamAsync(RecordsBasePath + ".json", linkedToken);

            // Simulator delay (milliseconds), default 1000
            var simulatorDelayMs = int.TryParse(Environment.GetEnvironmentVariable("FIREBASE_SIM_DELAY_MS"), out var parsedDelay)
                ? parsedDelay
                : 1000;

            // Push demo records in the background
            SimulationTask = Task.Run(async () =>
            {
                try
                {
                    while (!linkedToken.IsCancellationRequested)
                    {
                        await PushSimulatedRecordAsync(linkedToken).ConfigureAwait(false);
                        SimulationCurrentDate = SimulationCurrentDate.AddDays(1);
                        try
                        {
                            await Task.Delay(simulatorDelayMs, linkedToken).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Expected during cancellation; no action required.
                }
            }, linkedToken);
        }

        /// <summary>
        /// Opens a long-lived SSE stream to the specified Firebase RTDB URL and dispatches incoming records.
        /// Includes exponential backoff on failures.
        /// </summary>
        /// <param name="url">The RTDB JSON URL to stream.</param>
        /// <param name="cancellationToken">Cancellation token to stop the stream.</param>
        private async Task StartSseStreamAsync(string url, CancellationToken cancellationToken)
        {
            var backoffMs = 1000;

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    using var request = new HttpRequestMessage(HttpMethod.Get, url);
                    request.Headers.Accept.Clear();
                    request.Headers.Accept.ParseAdd("text/event-stream");

                    using var response = await HttpClientShared.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
                    response.EnsureSuccessStatusCode();

                    using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
                    using var reader = new StreamReader(stream);

                    // Reset backoff on success
                    backoffMs = 1000;

                    string? currentLine;
                    var dataBuilder = new StringBuilder();

                    while (!cancellationToken.IsCancellationRequested && (currentLine = await reader.ReadLineAsync().ConfigureAwait(false)) is not null)
                    {
                        // Skip keep-alive comments
                        if (currentLine.StartsWith(":"))
                        {
                            continue;
                        }

                        // Accumulate data: lines
                        if (currentLine.StartsWith("data:"))
                        {
                            if (dataBuilder.Length > 0)
                            {
                                dataBuilder.Append(currentLine[5..].TrimStart());
                            }

                            var payload = currentLine.Length > 5 ? currentLine.Substring(5).TrimStart() : string.Empty; // safe slice
                            dataBuilder.Append(payload);
                            continue;
                        }

                        // Blank line => end of SSE event
                        if (string.IsNullOrWhiteSpace(currentLine))
                        {
                            if (dataBuilder.Length > 0)
                            {
                                var json = dataBuilder.ToString();
                                dataBuilder.Clear();

                                try
                                {
                                    using var doc = JsonDocument.Parse(json);
                                    var root = doc.RootElement;

                                    root.TryGetProperty("path", out var pathElement);
                                    var pathString = pathElement.ValueKind == JsonValueKind.String ? pathElement.GetString() ?? string.Empty : string.Empty;

                                    if (root.TryGetProperty("data", out var dataElement))
                                    {
                                        if (dataElement.ValueKind == JsonValueKind.Null)
                                        {
                                            // Deleted or empty
                                        }
                                        else if (dataElement.ValueKind == JsonValueKind.Object)
                                        {
                                            // Child update => single record
                                            if (!string.IsNullOrEmpty(pathString) && pathString != "/")
                                            {
                                                if (TryParseFbRecord(dataElement, out var fbRecord))
                                                {
                                                    try
                                                    {
                                                        NewSalesRecord?.Invoke(this, ToSalesRecord(fbRecord));
                                                    }
                                                    catch (Exception)
                                                    {
                                                        // Swallow for demo; consider logging in production.
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                // Snapshot/map => iterate children
                                                foreach (var property in dataElement.EnumerateObject())
                                                {
                                                    var value = property.Value;
                                                    if (value.ValueKind != JsonValueKind.Object)
                                                    {
                                                        continue;
                                                    }

                                                    if (TryParseFbRecord(value, out var fbRecord))
                                                    {
                                                        try
                                                        {
                                                            NewSalesRecord?.Invoke(this, ToSalesRecord(fbRecord));
                                                        }
                                                        catch (Exception)
                                                        {
                                                            // Swallow for demo; consider logging in production.
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        else if (dataElement.ValueKind == JsonValueKind.Array)
                                        {
                                            var list = ParseFbArray(dataElement);
                                            foreach (var fbRecord in list)
                                            {
                                                try
                                                {
                                                    NewSalesRecord?.Invoke(this, ToSalesRecord(fbRecord));
                                                }
                                                catch (Exception)
                                                {
                                                    // Swallow for demo; consider logging in production.
                                                }
                                            }
                                        }
                                    }
                                }
                                catch (Exception)
                                {
                                    // Swallow for demo stability; consider logging.
                                }
                            }

                            continue;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception)
                {
                    try
                    {
                        await Task.Delay(backoffMs, cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }

                    // Exponential backoff (max 15s)
                    backoffMs = Math.Min(backoffMs * 2, 15000);
                }
            }
        }

        /// <summary>
        /// Pushes a simulated sales record to the current RecordsBasePath.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for the operation.</param>
        private async Task PushSimulatedRecordAsync(CancellationToken cancellationToken)
        {
            var record = new Dictionary<string, object?>
            {
                ["Date"] = SimulationCurrentDate.ToString("o"),
                ["SalesChannel"] = new[] { "Retail", "In-Store", "Online" }[RandomGenerator.Next(0, 3)],
                ["Region"] = new[] { "East", "South", "West", "North" }[RandomGenerator.Next(0, 4)],
                ["Sales"] = Math.Round(100 + RandomGenerator.NextDouble() * 1000.0, 2),
                ["Quantity"] = RandomGenerator.Next(1, 20),
                ["SourceInstanceId"] = InstanceId
            };

            try
            {
                if (string.IsNullOrEmpty(RecordsBasePath))
                {
                    return;
                }

                var postUrl = RecordsBasePath + ".json";
                var content = new StringContent(JsonSerializer.Serialize(record), Encoding.UTF8, "application/json");

                using var response = await HttpClientShared.PostAsync(postUrl, content, cancellationToken).ConfigureAwait(false);
                // SSE listener will receive child-added event
            }
            catch (Exception)
            {
                // Swallow for demo stability; consider logging in production.
            }
        }

        /// <summary>
        /// Clears the specified RTDB JSON path using HTTP DELETE.
        /// </summary>
        /// <param name="jsonUrl">The full JSON URL to delete.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private async Task ClearPathAsync(string jsonUrl, CancellationToken cancellationToken)
        {
            using var request = new HttpRequestMessage(HttpMethod.Delete, jsonUrl);
            using var response = await HttpClientShared.SendAsync(request, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();
        }

        // --- Parsing helpers ---

        /// <summary>
        /// Internal DTO used to hold raw Firebase sales record data.
        /// </summary>
        private sealed class FbSalesRecord
        {
            public DateTime Date { get; set; }
            public string? SalesChannel { get; set; }
            public string? Region { get; set; }
            public JsonElement Sales { get; set; }
            public int? Quantity { get; set; }
            public string? SourceInstanceId { get; set; }
        }

        /// <summary>
        /// Parses a JSON array element into a list of <see cref="FbSalesRecord"/>.
        /// </summary>
        private static List<FbSalesRecord> ParseFbArray(JsonElement arrayElement)
        {
            var list = new List<FbSalesRecord>();

            foreach (var element in arrayElement.EnumerateArray())
            {
                try
                {
                    var record = new FbSalesRecord
                    {
                        Date = element.TryGetProperty("Date", out var dateEl) && dateEl.ValueKind == JsonValueKind.String ? DateTime.Parse(dateEl.GetString()!) : element.GetProperty("Date").GetDateTime(),
                        SalesChannel = element.TryGetProperty("SalesChannel", out var scEl) && scEl.ValueKind == JsonValueKind.String ? scEl.GetString() : null,
                        Region = element.TryGetProperty("Region", out var regionEl) && regionEl.ValueKind == JsonValueKind.String ? regionEl.GetString() : null,
                        Sales = element.TryGetProperty("Sales", out var salesEl) ? salesEl : default,
                        Quantity = element.TryGetProperty("Quantity", out var qtyEl) && qtyEl.ValueKind == JsonValueKind.Number ? qtyEl.GetInt32() : (int?)null,
                        SourceInstanceId = element.TryGetProperty("SourceInstanceId", out var srcEl) && srcEl.ValueKind == JsonValueKind.String ? srcEl.GetString() : null
                    };

                    list.Add(record);
                }
                catch (Exception)
                {
                    // Swallow for demo stability; consider logging.
                }
            }

            return list;
        }

        /// <summary>
        /// Attempts to parse a single Firebase record object.
        /// </summary>
        /// <param name="element">JSON element representing the record.</param>
        /// <param name="record">Parsed record output (if successful).</param>
        /// <returns>True if parsing succeeded; otherwise false.</returns>
        private static bool TryParseFbRecord(JsonElement element, out FbSalesRecord record)
        {
            record = default!;

            try
            {
                var parsedDate = DateTime.MinValue;
                if (element.TryGetProperty("Date", out var dateEl))
                {
                    if (dateEl.ValueKind == JsonValueKind.String)
                    {
                        parsedDate = DateTime.Parse(dateEl.GetString()!);
                    }
                    else
                    {
                        parsedDate = dateEl.GetDateTime();
                    }
                }
                else
                {
                    return false;
                }

                var salesChannel = element.TryGetProperty("SalesChannel", out var scEl) && scEl.ValueKind == JsonValueKind.String ? scEl.GetString() : null;
                var region = element.TryGetProperty("Region", out var regionEl) && regionEl.ValueKind == JsonValueKind.String ? regionEl.GetString() : null;
                var sales = element.TryGetProperty("Sales", out var salesEl) ? salesEl : default;
                var quantity = element.TryGetProperty("Quantity", out var qtyEl) && qtyEl.ValueKind == JsonValueKind.Number ? qtyEl.GetInt32() : (int?)null;
                var sourceInstanceId = element.TryGetProperty("SourceInstanceId", out var srcEl) && srcEl.ValueKind == JsonValueKind.String ? srcEl.GetString() : null;

                record = new FbSalesRecord
                {
                    Date = parsedDate,
                    SalesChannel = salesChannel,
                    Region = region,
                    Sales = sales,
                    Quantity = quantity,
                    SourceInstanceId = sourceInstanceId
                };

                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Parses the numeric value of the Sales field from a <see cref="JsonElement"/>.
        /// </summary>
        private static double ParseSalesValue(JsonElement element)
        {
            if (element.ValueKind == JsonValueKind.Number)
            {
                return element.GetDouble();
            }

            if (element.ValueKind == JsonValueKind.String && double.TryParse(element.GetString(), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var value))
            {
                return value;
            }

            return 0.0;
        }

        /// <summary>
        /// Converts an internal <see cref="FbSalesRecord"/> into the public <see cref="SalesRecord"/> model.
        /// </summary>
        private static SalesRecord ToSalesRecord(FbSalesRecord input) => new()
        {
            Date = input.Date,
            Channel = input.SalesChannel,
            Region = input.Region,
            UnitsSold = input.Quantity ?? 1,
            Revenue = ParseSalesValue(input.Sales),
            SourceInstanceId = input.SourceInstanceId
        };

        /// <summary>
        /// Disposes the service by cancelling background tasks and waiting for their completion.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            try
            {
                StreamCancellationSource?.Cancel();

                if (StreamTask is not null)
                {
                    await StreamTask;
                }

                if (SimulationTask is not null)
                {
                    await SimulationTask;
                }
            }
            catch (Exception)
            {
                // Swallow for demo stability; consider logging.
            }
            finally
            {
                StreamCancellationSource?.Dispose();
            }
        }

        /// <summary>
        /// No-op seed method (reserved for future use).
        /// </summary>
        public Task EnsureSeedAsync(CancellationToken ct = default) => Task.CompletedTask;
    }
}
