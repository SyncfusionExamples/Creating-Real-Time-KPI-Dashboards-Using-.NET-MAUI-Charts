namespace KPIDashboard.DashboardPages
{
    public partial class MainPageDesktop : ContentPage
    {
        public MainPageDesktop()
        {
            InitializeComponent();
            // Resolve ViewModel from DI; fail-fast if DI not configured
            BindingContext = MauiProgram.Services!
                .GetRequiredService<KPIDashboard.DashboardViewModel>();
        }
    }
}
