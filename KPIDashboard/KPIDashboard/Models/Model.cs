using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace KPIDashboard;

public class TimePoint : INotifyPropertyChanged
{
    private DateTime _time;
    private double _value;
    public DateTime Time { get => _time; set { _time = value; OnPropertyChanged(); } }
    public double Value { get => _value; set { _value = value; OnPropertyChanged(); } }
    public event PropertyChangedEventHandler? PropertyChanged;
    protected void OnPropertyChanged([CallerMemberName] string? name = null)
        => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}

public class CategoryPoint : INotifyPropertyChanged
{
    private string _category = string.Empty;
    private double _value;
    public string Category { get => _category; set { if (_category != value) { _category = value; OnPropertyChanged(); } } }
    public double Value { get => _value; set { if (_value != value) { _value = value; OnPropertyChanged(); } } }
    public event PropertyChangedEventHandler? PropertyChanged;
    protected void OnPropertyChanged([CallerMemberName] string? name = null) =>
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}

public class InsightItem : INotifyPropertyChanged
{
    private string _name = string.Empty;  
    private string _value = string.Empty; 

    public string Name { get => _name; set { if (_name != value) { _name = value; OnPropertyChanged(); } } }
    public string Value { get => _value; set { if (_value != value) { _value = value; OnPropertyChanged(); } } }

    public event PropertyChangedEventHandler? PropertyChanged;
    protected void OnPropertyChanged([CallerMemberName] string? name = null) =>
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}

public class SalesRecord
{
    public DateTime Date { get; set; }
    public string? Channel { get; set; }
    public string? Region { get; set; }
    public double UnitsSold { get; set; }
    public double Revenue { get; set; }
}