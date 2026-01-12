namespace KPIDashboard.DashboardPages
{
    public partial class MainPageMobile : ContentPage
    {
        public MainPageMobile()
        {
            InitializeComponent();

            BindingContext = MauiProgram.Services!
              .GetRequiredService<DashboardViewModel>();
        }
    }
}