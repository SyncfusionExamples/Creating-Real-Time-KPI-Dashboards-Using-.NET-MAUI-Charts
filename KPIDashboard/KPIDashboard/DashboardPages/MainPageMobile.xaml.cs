using KPIDashboard;
using Microsoft.Extensions.Logging;

namespace KPIDashboard.DashboardPages
{
    public partial class MainPageMobile : ContentPage
    {
        public MainPageMobile()
        {
            InitializeComponent();

            BindingContext = MauiProgram.Services!
              .GetRequiredService<KPIDashboard.DashboardViewModel>();
        }
    }
}








