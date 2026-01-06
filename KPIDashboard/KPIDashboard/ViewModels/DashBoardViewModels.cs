using System.Collections.ObjectModel;
using System.ComponentModel;
using Microsoft.Extensions.Logging;

namespace KPIDashboard
{
    /// <summary>
    /// ViewModel that listens to FirebaseService for live sales records and keeps chart collections in sync.
    /// Maintains rolling buffers (capped at 60 points) for trend and region computations, and updates insights.
    /// </summary>
    public class DashboardViewModel : INotifyPropertyChanged, IAsyncDisposable
    {
        private readonly CancellationTokenSource _cts = new();
        private const int MaxPoints = 60;

        // Firebase RTDB base URL
        private readonly string _firebaseBaseUrl;
        private readonly ILogger<DashboardViewModel>? _logger;

        // Chart-bound collections
        public ObservableCollection<TimePoint> RevenueTrend { get; } = new();
        public ObservableCollection<CategoryPoint> LeadsByChannel { get; } = new();
        public ObservableCollection<CategoryPoint> RevenueByRegion { get; } = new();
        public ObservableCollection<CategoryPoint> SalesActual { get; } = new();
        public ObservableCollection<CategoryPoint> SalesRemaining { get; } = new();
        public ObservableCollection<InsightItem> Insights { get; } = new();

        // Brushes for charts (examples)
        public List<Brush> LeadsBrushes { get; set; } = new();
        public List<Brush> CustomBrushes { get; set; }

        // KPI target/actual
        private double _target = 100000; // will be loaded from DB view
        private double _currentActual = 0;

        public double Target => _target;
        public double Actual => _currentActual;

        private readonly FirebaseService _db;

        public DashboardViewModel(FirebaseService db, ILogger<DashboardViewModel>? logger)
        {
            _db = db;
            _logger = logger;
            _firebaseBaseUrl = Environment.GetEnvironmentVariable("FIREBASE_BASE") ?? "https://kpi-dashboard-e99ce-default-rtdb.firebaseio.com";

            CustomBrushes = new List<Brush>
            {
                new SolidColorBrush(Color.FromArgb("#FF6FAE")),
                new SolidColorBrush(Color.FromArgb("#00C2FF")),
                new SolidColorBrush(Color.FromArgb("#9B8CFF")),
                new SolidColorBrush(Color.FromArgb("#FFB020")),
                new SolidColorBrush(Color.FromArgb("#7C4DFF")),
            };

            LeadsBrushes = new List<Brush>
            {
                new SolidColorBrush(Color.FromArgb("#FF6F00")),
                new SolidColorBrush(Color.FromArgb("#00E5FF")),
                new SolidColorBrush(Color.FromArgb("#C51162")),
                new SolidColorBrush(Color.FromArgb("#7C4DFF")),
            };

            EnsureInsightsInitialized();
            _ = InitializeAsync();
        }

        /// <summary>
        /// Initializes realtime listening by subscribing to NewSalesRecord, seeding the database if needed,
        /// and starting the Firebase SSE stream. Emits initial records, then one per second in demo mode.
        /// </summary>
        private async Task InitializeAsync()
        {
            try
            {
                // Per-row realtime updates
                _db.NewSalesRecord += (_, rec) => OnNewSalesRecord(rec);

                // Ensure DB has seed and state row; seeds initial 60 if empty
                await _db.EnsureSeedAsync(_cts.Token);

                // Start listeners. Firebase loop will emit initial 60 records via NewSalesRecord events
                // and then emit one record per second.
                await _db.StartListeningAsync(_cts.Token);
            }
            catch (Exception ex)
            {
                await MainThread.InvokeOnMainThreadAsync(() =>
                {
                    Insights.Clear();
                    Insights.Add(new InsightItem { Name = "Init Error", Value = ex.Message });
                });
            }
        }

        private void EnsureInsightsInitialized()
        {
            if (Insights.Count > 0)
            {
                return;
            }

            Insights.Add(new InsightItem { Name = "Momentum", Value = "+0" });
            Insights.Add(new InsightItem { Name = "Top Region", Value = "-" });
            Insights.Add(new InsightItem { Name = "Top Channel", Value = "-" });
        }

        private void UpdateInsightsValues()
        {
            if (Insights.Count < 3)
            {
                return;
            }

            if (RevenueTrend.Count >= 10)
            {
                double first = RevenueTrend[^10].Value;
                double last = RevenueTrend[^1].Value;
                double delta = last - first;
                Insights[0].Value = delta >= 0 ? $"+{delta:0}" : $"{delta:0}";
            }
            else
            {
                Insights[0].Value = "+0";
            }

            var maxRegion = RevenueByRegion.OrderByDescending(r => r.Value).FirstOrDefault();
            if (maxRegion != null)
            {
                Insights[1].Value = $"{maxRegion.Category} {maxRegion.Value:0.#}%";
            }
            else
            {
                Insights[1].Value = "-";
            }

            var topChannel = LeadsByChannel.OrderByDescending(c => c.Value).FirstOrDefault();
            if (topChannel != null)
            {
                Insights[2].Value = $"{topChannel.Category} ({topChannel.Value:0} units)";
            }
            else
            {
                Insights[2].Value = "-";
            }
        }

        public event PropertyChangedEventHandler? PropertyChanged;

        /// <summary>
        /// Disposes the ViewModel by cancelling its token and disposing the FirebaseService.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            _cts.Dispose();
            await _db.DisposeAsync();
        }

        private readonly Dictionary<string, double> _unitsByChannel = new(StringComparer.OrdinalIgnoreCase);
        private readonly List<TimePoint> _trendBuffer = new(); // to cap last 60
        private readonly List<SalesRecord> _recordBuffer = new(); // rolling buffer for last 60 records
        private readonly object _bufferLock = new();

        /// <summary>
        /// Handles a newly received sales record: updates rolling buffers, chart collections, and insights.
        /// </summary>
        /// <param name="rec">The incoming sales record.</param>
        private void OnNewSalesRecord(SalesRecord rec)
        {
            TimePoint[] trendSnapshot;
            List<CategoryPoint> regionSnapshot;

            // Append to buffers under lock; take snapshots for UI thread
            lock (_bufferLock)
            {
                // Trend buffer (cap 60)
                if (_trendBuffer.Count >= MaxPoints)
                {
                    _trendBuffer.RemoveAt(0);
                }

                _trendBuffer.Add(new TimePoint { Time = rec.Date, Value = (double)rec.Revenue });
                trendSnapshot = _trendBuffer.ToArray();

                // Record buffer (cap 60)
                if (_recordBuffer.Count >= MaxPoints)
                {
                    _recordBuffer.RemoveAt(0);
                }

                _recordBuffer.Add(rec);

                // Precompute region share from buffer to avoid recomputing on UI thread
                regionSnapshot = BuildRevenueByRegionFromBuffer(_recordBuffer);
            }

            MainThread.BeginInvokeOnMainThread(() =>
            {
                // Revenue trend
                RevenueTrend.Clear();
                foreach (var point in trendSnapshot)
                {
                    RevenueTrend.Add(point);
                }

                // Units by channel (absolute sum)
                var channelKey = rec.Channel ?? "(unknown)";
                _unitsByChannel.TryGetValue(channelKey, out var units);
                _unitsByChannel[channelKey] = units + rec.UnitsSold;
                Rebind(LeadsByChannel, _unitsByChannel);

                // Sales progress (Actual)
                _currentActual += (double)rec.Revenue;
                if (SalesActual.Count == 0)
                {
                    SalesActual.Add(new CategoryPoint { Category = "Sales", Value = _currentActual });
                }
                else
                {
                    SalesActual[0].Value = _currentActual;
                }

                if (SalesRemaining.Count == 0)
                {
                    SalesRemaining.Add(new CategoryPoint { Category = "Sales", Value = Math.Max(0, _target - _currentActual) });
                }
                else
                {
                    SalesRemaining[0].Value = Math.Max(0, _target - _currentActual);
                }

                // Revenue by Region from rolling buffer
                RevenueByRegion.Clear();
                foreach (var regionPoint in regionSnapshot)
                {
                    RevenueByRegion.Add(regionPoint);
                }

                // Update insights after region rebinding so Top Region is fresh
                UpdateInsightsValues();
            });
        }

        /// <summary>
        /// Rebinds a category collection from a dictionary source, sorting by value descending.
        /// </summary>
        /// <param name="target">Target observable collection to populate.</param>
        /// <param name="source">Dictionary of category to numeric value.</param>
        private static void Rebind(ObservableCollection<CategoryPoint> target, Dictionary<string, double> source)
        {
            target.Clear();
            foreach (var kv in source.OrderByDescending(kv => kv.Value))
            {
                target.Add(new CategoryPoint { Category = kv.Key, Value = kv.Value });
            }
        }

        /// <summary>
        /// Computes regional revenue share (%) from the rolling buffer of recent records.
        /// Falls back to share by count when revenue totals are zero.
        /// </summary>
        /// <param name="records">Recent sales records to aggregate.</param>
        /// <returns>List of region category points with percentage values.</returns>
        private static List<CategoryPoint> BuildRevenueByRegionFromBuffer(IEnumerable<SalesRecord> records)
        {
            static string Normalize(string? input)
            {
                var trimmed = input?.Trim();
                if (string.IsNullOrWhiteSpace(trimmed))
                {
                    return "Unknown";
                }

                if (string.Equals(trimmed, "nan", StringComparison.OrdinalIgnoreCase))
                {
                    return "Unknown";
                }

                return trimmed!;
            }

            var groups = records
                .GroupBy(r => Normalize(r.Region))
                .Select(g => new { Region = g.Key, Sum = g.Sum(r => r.Revenue), Count = g.Count() })
                .OrderByDescending(x => x.Sum)
                .ToList();

            var totalRevenue = groups.Sum(g => g.Sum);
            if (totalRevenue > 0)
            {
                return groups
                    .Select(g => new CategoryPoint { Category = g.Region, Value = (g.Sum / totalRevenue) * 100.0 })
                    .ToList();
            }

            var totalCount = groups.Sum(g => g.Count);
            if (totalCount <= 0)
            {
                return new List<CategoryPoint>();
            }

            return groups
                .Select(g => new CategoryPoint { Category = g.Region, Value = (g.Count / (double)totalCount) * 100.0 })
                .ToList();
        }
    }
}
