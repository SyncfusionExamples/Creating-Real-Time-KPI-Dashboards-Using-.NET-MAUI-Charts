namespace KPIDashboard.DashboardPages
{
    public partial class MainPageDesktop : ContentPage
    {
        public MainPageDesktop()
        {
            InitializeComponent();

            BindingContext = MauiProgram.Services!
                .GetRequiredService<KPIDashboard.DashboardViewModel>();
        }
    }
}