using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Globalization;

namespace KPIDashboard
{
    public class DashboardViewModel : INotifyPropertyChanged, IAsyncDisposable
    {
        private readonly PeriodicTimer _timer = new(TimeSpan.FromSeconds(1));
        private readonly CancellationTokenSource _cts = new();

        public ObservableCollection<TimePoint> RevenueTrend { get; } = new();
        public ObservableCollection<CategoryPoint> LeadsByChannel { get; } = new();  
        public ObservableCollection<CategoryPoint> RevenueByRegion { get; } = new();
        public ObservableCollection<CategoryPoint> SalesActual { get; } = new();
        public ObservableCollection<CategoryPoint> SalesRemaining { get; } = new();
        public ObservableCollection<InsightItem> Insights { get; } = new();

        public List<Brush> LeadsBrushes { get; set; } = new();
        public List<Brush> CustomBrushes { get; set; }

        private const double SalesTarget = 100000;
        private double _currentActual = 0; 

        public double Target => SalesTarget;
        public double Actual => _currentActual;

        private List<SalesRecord> _streamData = new();
        private int _currentIndex = 0;

        private readonly TimeSpan _windowSpan = TimeSpan.FromDays(60);
        private readonly LinkedList<SalesRecord> _window = new(); 

        public DashboardViewModel()
        {
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

        private async Task InitializeAsync()
        {
            await LoadCsvDataFromResourcesAsync("sales_data.csv");

            if (_streamData.Count == 0)
                return;

            // Seed with the first chunk and construct initial window based on last record date within that chunk
            int seedCount = Math.Min(100, _streamData.Count);
            _currentIndex = seedCount;

            DateTime newest = _streamData[seedCount - 1].Date;
            DateTime cutoff = newest - _windowSpan;

            // Fill window with records in [cutoff, newest]
            foreach (var r in _streamData.Take(seedCount))
            {
                if (r.Date >= cutoff && r.Date <= newest)
                    _window.AddLast(r);
            }

            await MainThread.InvokeOnMainThreadAsync(RecomputeFromWindow);

            // start realtime
            _ = StartRealtimeLoopAsync();
        }

        // LOAD CSV FROM Resources/Raw
        private async Task LoadCsvDataFromResourcesAsync(string fileNameInRaw)
        {
            using var stream = await FileSystem.OpenAppPackageFileAsync(fileNameInRaw);
            using var reader = new StreamReader(stream);

            var header = await reader.ReadLineAsync();
            if (string.IsNullOrWhiteSpace(header)) return;

            var headers = header.Split(',');

            int idxRegion = Array.FindIndex(headers, h => h.Equals("Region", StringComparison.OrdinalIgnoreCase));
            int idxSales = Array.FindIndex(headers, h => h.Equals("Sales", StringComparison.OrdinalIgnoreCase));
            int idxDate = Array.FindIndex(headers, h => h.Equals("Date", StringComparison.OrdinalIgnoreCase));
            int idxQuantity = Array.FindIndex(headers, h => h.Equals("Quantity", StringComparison.OrdinalIgnoreCase));
            int idxSalesChannel = Array.FindIndex(headers, h => h.Equals("SalesChannel", StringComparison.OrdinalIgnoreCase));

            // minimal columns needed
            if (idxRegion < 0 || idxSales < 0 || idxDate < 0 || idxQuantity < 0 || idxSalesChannel < 0)
                return;

            var culture = CultureInfo.InvariantCulture;

            string? line;
            var all = new List<SalesRecord>();
            while ((line = await reader.ReadLineAsync()) != null)
            {
                var parts = line.Split(',');
                if (parts.Length < headers.Length) continue;

                if (!DateTime.TryParse(parts[idxDate], culture, DateTimeStyles.None, out var date))
                    continue;

                var region = parts[idxRegion];
                if (string.IsNullOrWhiteSpace(region) || region.Equals("nan", StringComparison.OrdinalIgnoreCase))
                    region = "Others";

                var channel = parts[idxSalesChannel];

                // parse numbers (allow blanks => 0)
                double revenue = 0;
                if (!string.IsNullOrWhiteSpace(parts[idxSales]))
                    double.TryParse(parts[idxSales], NumberStyles.Number | NumberStyles.AllowDecimalPoint, culture, out revenue);

                double quantity = 0;
                if (!string.IsNullOrWhiteSpace(parts[idxQuantity]))
                    double.TryParse(parts[idxQuantity], NumberStyles.Number, culture, out quantity);

                all.Add(new SalesRecord
                {
                    Date = date,
                    Channel = channel,
                    Region = region,
                    UnitsSold = quantity,
                    Revenue = revenue
                });
            }

            _streamData = all.OrderBy(r => r.Date).ToList();
        }

        private async Task StartRealtimeLoopAsync()
        {
            try
            {
                while (await _timer.WaitForNextTickAsync(_cts.Token))
                {
                    if (_currentIndex < _streamData.Count)
                    {
                        await MainThread.InvokeOnMainThreadAsync(UpdateRealtime);
                    }
                    else
                    {
                        _cts.Cancel();
                    }
                }
            }
            catch (OperationCanceledException) { }
        }

        // Add the next record, update the rolling window [newest-60d, newest], then recompute all visuals from the window.
        private void UpdateRealtime()
        {
            if (_currentIndex >= _streamData.Count) return;

            var record = _streamData[_currentIndex];
            _currentIndex++;

            // add new record to window
            _window.AddLast(record);

            // roll window by time
            var cutoff = record.Date - _windowSpan;
            while (_window.First != null && _window.First.Value.Date < cutoff)
                _window.RemoveFirst();

            RecomputeFromWindow();
        }

        // Recompute all collections from the current window
        private void RecomputeFromWindow()
        {
            // Revenue trend (within window) - plot raw points in chronological order
            RevenueTrend.Clear();
            foreach (var r in _window)
                RevenueTrend.Add(new TimePoint { Time = r.Date, Value = r.Revenue });

            // Region share (percent) within the same window
            RevenueByRegion.Clear();
            var byRegion = _window
                .GroupBy(r => r.Region)
                .Select(g => new { Region = g.Key, Sum = g.Sum(x => x.Revenue) })
                .Where(x => x.Sum > 0)
                .OrderByDescending(x => x.Sum)
                .ToList();

            double totalRevenue = byRegion.Sum(x => x.Sum);
            if (totalRevenue > 0)
            {
                foreach (var x in byRegion)
                {
                    var pct = (x.Sum / totalRevenue) * 100.0; // no clamping to 1%
                    RevenueByRegion.Add(new CategoryPoint { Category = x.Region, Value = pct });
                }
            }

            // Units by Channel (within window)
            LeadsByChannel.Clear();
            var byChannel = _window
                .GroupBy(r => r.Channel)
                .Select(g => new CategoryPoint { Category = g.Key, Value = g.Sum(x => x.UnitsSold) })
                .OrderByDescending(cp => cp.Value);

            foreach (var cp in byChannel)
                LeadsByChannel.Add(cp);

            // Actual vs Target (actual = revenue inside window)
            _currentActual = _window.Sum(r => r.Revenue);
            SalesActual.Clear();
            SalesActual.Add(new CategoryPoint { Category = "Sales", Value = _currentActual });
            SalesRemaining.Clear();
            SalesRemaining.Add(new CategoryPoint { Category = "Sales", Value = Math.Max(0, SalesTarget - _currentActual) });

            // Insights from the same window
            UpdateInsightsValues();
        }

        private void EnsureInsightsInitialized()
        {
            if (Insights.Count > 0) return;
            Insights.Add(new InsightItem { Name = "Momentum", Value = "+0" });
            Insights.Add(new InsightItem { Name = "Top Region", Value = "-" });
            Insights.Add(new InsightItem { Name = "Top Channel", Value = "-" });
        }

        private void UpdateInsightsValues()
        {
            if (Insights.Count < 3) return;

            // Momentum: change over last 10 points in the synchronized trend window
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

            // Top Region from synchronized window (percent)
            var maxRegion = RevenueByRegion.OrderByDescending(r => r.Value).FirstOrDefault();
            if (maxRegion != null)
                Insights[1].Value = $"{maxRegion.Category} {maxRegion.Value:0.#}%";
            else
                Insights[1].Value = "-";

            // Top Channel from synchronized window (units)
            var topChannel = LeadsByChannel.OrderByDescending(c => c.Value).FirstOrDefault();
            if (topChannel != null)
                Insights[2].Value = $"{topChannel.Category} ({topChannel.Value:0} units)";
            else
                Insights[2].Value = "-";
        }

        public event PropertyChangedEventHandler? PropertyChanged;

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            _cts.Dispose();
            await Task.CompletedTask;
        }
    }
}


