using Microsoft.Extensions.Logging;
using Syncfusion.Maui.Toolkit.Hosting;

namespace KPIDashboard
{
    public static class MauiProgram
    {
        public static IServiceProvider? Services { get; private set; }

        public static MauiApp CreateMauiApp()
        {
            var builder = MauiApp.CreateBuilder();

            builder
                .UseMauiApp<App>()
                .ConfigureSyncfusionToolkit()
                .ConfigureFonts(fonts =>
                {
                    fonts.AddFont("OpenSans-Regular.ttf", "OpenSansRegular");
                    fonts.AddFont("OpenSans-Semibold.ttf", "OpenSansSemibold");
                });

            // Register services
            // Use Firebase-only service
            builder.Services.AddSingleton<FirebaseService>();
            builder.Services.AddSingleton<DashboardViewModel>(sp =>
            {
                var db = sp.GetRequiredService<FirebaseService>();
                var logger = sp.GetService<ILogger<DashboardViewModel>>();
                return new DashboardViewModel(db, logger);
            });

#if DEBUG
            builder.Logging.AddDebug();
#endif
            var app = builder.Build();
            Services = app.Services;
            return app;
        }
    }


}
