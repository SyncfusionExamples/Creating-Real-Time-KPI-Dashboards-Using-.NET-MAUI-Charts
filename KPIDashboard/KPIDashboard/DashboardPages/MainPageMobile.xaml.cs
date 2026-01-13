namespace KPIDashboard.DashboardPages
{
    public partial class MainPageMobile : ContentPage
    {
        public MainPageMobile()
        {
            InitializeComponent();

            BindingContext = (MauiProgram.Services
                    ?? throw new InvalidOperationException("IServiceProvider is not initialized."))
                    .GetRequiredService<DashboardViewModel>();
        }
    }
}