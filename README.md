# Creating Real-Time KPI Dashboards Using .NET MAUI Toolkit Charts

## Overview
The Real-Time KPI Dashboard showcases a cross-platform (.NET MAUI) analytics experience that streams sales and marketing metrics and turns them into actionable insights using Syncfusion® .NET MAUI Toolkit Charts. It focuses on clarity, responsiveness, and continuous updates for decision-making.

## Syncfusion .NET MAUI Toolkit Charts
A high-performance [charting library](https://help.syncfusion.com/maui-toolkit/cartesian-charts/getting-started) for .NET MAUI apps with:
- Broad chart coverage: Line, column/bar, doughnut/pie, stacked variants, and more.
- Interactivity: Zooming, panning, tooltips, trackball, selection, and data labels.
- Performance: Optimized rendering with smooth real-time animations.
- Styling: Flexible APIs for axes, legends, labels, and annotations.

## Real-Time KPI Dashboard

### Layout overview
Two column, four row Grid layout:
- Title bar: "Real Time KPI Dashboard for Sales performance analysis"
- Insight strip: Momentum, Top Region, Top Channel
- Content area:
  - Revenue Trend (Line)
  - Units by Channel (Column)
  - Revenue by Region (Doughnut)

### Dashboard components

#### Revenue Trend
- Purpose: Show revenue changes over time for quick pattern recognition.
- Highlights: Time-based line visualization, tooltips for exact values, smooth real-time updates.

#### Units by Channel
- Purpose: Compare performance across acquisition channels.
- Highlights: Clear column comparison, readable data labels, custom color palettes.

#### Revenue by Region
- Purpose: Reveal regional contribution to total revenue.
- Highlights: Doughnut for share, outside data labels with legend, easy regional comparison.

#### Insight Cards
- Purpose: Provide quick-glance signals for momentum and top performers.
- Highlights: Momentum status, Top Region by revenue share, Top Channel by units.

![KPIInsightDashboardWidowsDemo](https://github.com/user-attachments/assets/56f3cb3e-2d44-4541-9568-c39ac4e8fc20)

## Troubleshooting

**Path Too Long Exception**  

If you are facing a path too long exception when building this example project, close Visual Studio and rename the repository to a shorter name before building the project.

For a step-by-step procedure, refer to the [Real-Time KPI Dashboard blog](https://www.syncfusion.com/blogs/post/real-time-kpi-dashboard-dotnet-maui)


