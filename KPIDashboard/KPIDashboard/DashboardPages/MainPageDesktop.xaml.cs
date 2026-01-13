namespace KPIDashboard.DashboardPages
{
    public partial class MainPageDesktop : ContentPage
    {
        public MainPageDesktop()
        {
            InitializeComponent();

            BindingContext = (MauiProgram.Services
                    ?? throw new InvalidOperationException("IServiceProvider is not initialized."))
                    .GetRequiredService<DashboardViewModel>();
        }
    }
}