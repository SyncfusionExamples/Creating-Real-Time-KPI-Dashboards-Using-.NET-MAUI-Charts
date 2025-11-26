namespace KPIDashboard
{
    public partial class AppShell : Shell
    {
        public AppShell()
        {
            InitializeComponent();

            var isDesktop = DeviceInfo.Idiom == DeviceIdiom.Desktop || DeviceInfo.Idiom == DeviceIdiom.TV;
            if (Items.FirstOrDefault() is TabBar tabBar)
            {
                // Keep only one ShellContent depending on device type
                if (isDesktop)
                {
                    // Remove mobile page
                    if (tabBar.Items.Count > 1)
                        tabBar.Items.RemoveAt(1);
                }
                else
                {
                    // Remove desktop page
                    if (tabBar.Items.Count > 1)
                        tabBar.Items.RemoveAt(0);
                }
            }
        }
    }
}
