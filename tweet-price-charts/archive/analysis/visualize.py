"""
Generate interactive visualizations for the tweet-price correlation analysis.
Creates HTML charts using Plotly that can be opened in any browser.
"""
from typing import List, Dict
import json
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
from datetime import datetime

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"

# Ensure output directory exists
OUTPUT_DIR.mkdir(exist_ok=True)


def load_analysis_report() -> Dict:
    """Load the analysis report."""
    with open(DATA_DIR / "analysis_report.json") as f:
        return json.load(f)


def create_dual_axis_chart(report: Dict) -> go.Figure:
    """
    Create the main visualization: tweet frequency vs price over time.
    Dual-axis chart with tweet bars and price line.
    """
    merged_data = pd.DataFrame(report["merged_data"])
    merged_data["date"] = pd.to_datetime(merged_data["date"])
    
    # Create figure with secondary y-axis
    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.7, 0.3],
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=("$PUMP Price vs Alon's Tweet Activity", "Daily Tweet Count")
    )
    
    # Price line (primary y-axis, row 1)
    fig.add_trace(
        go.Scatter(
            x=merged_data["date"],
            y=merged_data["close"],
            name="$PUMP Price",
            line=dict(color="#00D4AA", width=2),
            fill="tozeroy",
            fillcolor="rgba(0, 212, 170, 0.1)",
        ),
        row=1, col=1
    )
    
    # 7-day rolling tweet average (secondary y-axis, row 1)
    fig.add_trace(
        go.Scatter(
            x=merged_data["date"],
            y=merged_data["tweets_7d_avg"],
            name="Tweet Frequency (7d avg)",
            line=dict(color="#FF6B6B", width=2, dash="dot"),
            yaxis="y2"
        ),
        row=1, col=1
    )
    
    # Tweet count bars (row 2)
    colors = ["#FF6B6B" if c > 0 else "#333333" for c in merged_data["tweet_count"]]
    fig.add_trace(
        go.Bar(
            x=merged_data["date"],
            y=merged_data["tweet_count"],
            name="Daily Tweets",
            marker_color=colors,
            opacity=0.7,
        ),
        row=2, col=1
    )
    
    # Highlight quiet periods (more than 7 days)
    quiet_periods = [qp for qp in report["quiet_periods"] if qp["gap_days"] >= 7]
    
    for qp in quiet_periods:
        start = pd.to_datetime(qp["start"].split("T")[0])
        if qp.get("is_current"):
            end = datetime.now()
        else:
            end = pd.to_datetime(qp["end"].split("T")[0])
        
        # Only add if within our date range
        if end >= merged_data["date"].min() and start <= merged_data["date"].max():
            fig.add_vrect(
                x0=start, x1=end,
                fillcolor="rgba(255, 107, 107, 0.15)",
                layer="below",
                line_width=0,
                row=1, col=1
            )
            
            # Add annotation for significant drops
            if qp.get("price_change_during") and qp["price_change_during"] < -10:
                mid_date = start + (end - start) / 2
                fig.add_annotation(
                    x=mid_date,
                    y=merged_data["close"].max() * 0.9,
                    text=f"🔇 {qp['gap_days']}d silence<br>{qp['price_change_during']:.0f}%",
                    showarrow=False,
                    font=dict(size=10, color="#FF6B6B"),
                    row=1, col=1
                )
    
    # Update layout
    fig.update_layout(
        title=dict(
            text="Does Alon's Tweeting Affect $PUMP Price?",
            font=dict(size=24, color="#FFFFFF"),
            x=0.5,
        ),
        template="plotly_dark",
        paper_bgcolor="#0D1117",
        plot_bgcolor="#0D1117",
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            bgcolor="rgba(0,0,0,0)",
        ),
        yaxis=dict(
            title=dict(text="Price (USD)", font=dict(color="#00D4AA")),
            tickfont=dict(color="#00D4AA"),
            gridcolor="rgba(255,255,255,0.1)",
        ),
        yaxis2=dict(
            title=dict(text="Tweets (7d avg)", font=dict(color="#FF6B6B")),
            tickfont=dict(color="#FF6B6B"),
            overlaying="y",
            side="right",
        ),
        yaxis3=dict(
            title=dict(text="Daily Tweets"),
            gridcolor="rgba(255,255,255,0.1)",
        ),
        xaxis=dict(gridcolor="rgba(255,255,255,0.1)"),
        xaxis2=dict(gridcolor="rgba(255,255,255,0.1)"),
        height=700,
    )
    
    return fig


def create_comparison_chart(report: Dict) -> go.Figure:
    """
    Create a comparison chart: returns on tweet days vs no-tweet days.
    """
    ti = report["tweet_impact"]
    
    categories = ["Days with Tweets", "Days without Tweets"]
    avg_returns = [
        ti["tweet_day_stats"]["avg_return"],
        ti["no_tweet_day_stats"]["avg_return"]
    ]
    win_rates = [
        ti["tweet_day_stats"]["positive_days"] / max(1, ti["tweet_day_stats"]["count"]) * 100,
        ti["no_tweet_day_stats"]["positive_days"] / max(1, ti["no_tweet_day_stats"]["count"]) * 100
    ]
    
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Average Daily Return", "Win Rate (% Positive Days)"),
        specs=[[{"type": "bar"}, {"type": "bar"}]]
    )
    
    # Average returns
    colors = ["#00D4AA" if r > 0 else "#FF6B6B" for r in avg_returns]
    fig.add_trace(
        go.Bar(
            x=categories,
            y=avg_returns,
            marker_color=colors,
            text=[f"{r:+.2f}%" for r in avg_returns],
            textposition="outside",
            textfont=dict(size=16, color="white"),
        ),
        row=1, col=1
    )
    
    # Win rates
    fig.add_trace(
        go.Bar(
            x=categories,
            y=win_rates,
            marker_color=["#00D4AA", "#666666"],
            text=[f"{r:.0f}%" for r in win_rates],
            textposition="outside",
            textfont=dict(size=16, color="white"),
        ),
        row=1, col=2
    )
    
    fig.update_layout(
        title=dict(
            text="Tweet Days vs No-Tweet Days: The Numbers Don't Lie",
            font=dict(size=20, color="#FFFFFF"),
            x=0.5,
        ),
        template="plotly_dark",
        paper_bgcolor="#0D1117",
        plot_bgcolor="#0D1117",
        showlegend=False,
        height=450,
        yaxis=dict(
            title="Return (%)",
            gridcolor="rgba(255,255,255,0.1)",
            zeroline=True,
            zerolinecolor="rgba(255,255,255,0.3)",
        ),
        yaxis2=dict(
            title="Win Rate (%)",
            gridcolor="rgba(255,255,255,0.1)",
            range=[0, 100],
        ),
    )
    
    # Add statistical significance annotation
    if "statistical_test" in ti:
        p_val = ti["statistical_test"]["p_value"]
        sig_text = f"Statistically Significant (p={p_val:.4f})" if p_val < 0.05 else f"Not Significant (p={p_val:.4f})"
        fig.add_annotation(
            x=0.5, y=-0.15,
            xref="paper", yref="paper",
            text=f"📊 {sig_text}",
            showarrow=False,
            font=dict(size=14, color="#00D4AA" if p_val < 0.05 else "#FF6B6B"),
        )
    
    return fig


def create_quiet_period_chart(report: Dict) -> go.Figure:
    """
    Visualize the impact of quiet periods on price.
    """
    # Filter quiet periods with price data
    quiet_periods = [
        qp for qp in report["quiet_periods"]
        if qp.get("price_change_during") is not None
    ]
    
    if not quiet_periods:
        return None
    
    # Sort by gap days
    quiet_periods.sort(key=lambda x: x["gap_days"], reverse=True)
    
    labels = []
    changes = []
    colors = []
    
    for qp in quiet_periods[:15]:  # Top 15
        if qp.get("is_current"):
            label = f"CURRENT ({qp['gap_days']}d)"
        else:
            label = f"{qp['last_tweet_before'][:10]} ({qp['gap_days']}d)"
        
        labels.append(label)
        changes.append(qp["price_change_during"])
        colors.append("#FF6B6B" if qp["price_change_during"] < 0 else "#00D4AA")
    
    fig = go.Figure()
    
    fig.add_trace(
        go.Bar(
            y=labels,
            x=changes,
            orientation="h",
            marker_color=colors,
            text=[f"{c:+.1f}%" for c in changes],
            textposition="outside",
            textfont=dict(color="white"),
        )
    )
    
    fig.update_layout(
        title=dict(
            text="When Alon Goes Quiet: Price Impact by Silence Duration",
            font=dict(size=20, color="#FFFFFF"),
            x=0.5,
        ),
        template="plotly_dark",
        paper_bgcolor="#0D1117",
        plot_bgcolor="#0D1117",
        height=500,
        xaxis=dict(
            title="Price Change During Silence (%)",
            gridcolor="rgba(255,255,255,0.1)",
            zeroline=True,
            zerolinecolor="rgba(255,255,255,0.3)",
        ),
        yaxis=dict(
            gridcolor="rgba(255,255,255,0.1)",
            categoryorder="total ascending",
        ),
        margin=dict(l=150),
    )
    
    return fig


def create_correlation_scatter(report: Dict) -> go.Figure:
    """
    Scatter plot showing relationship between tweet activity and price.
    """
    merged_data = pd.DataFrame(report["merged_data"])
    
    fig = go.Figure()
    
    fig.add_trace(
        go.Scatter(
            x=merged_data["tweets_7d_sum"],
            y=merged_data["close"],
            mode="markers",
            marker=dict(
                size=8,
                color=merged_data["tweet_count"],
                colorscale="RdYlGn",
                showscale=True,
                colorbar=dict(title="Daily Tweets"),
            ),
            text=merged_data["date"].astype(str),
            hovertemplate="Date: %{text}<br>7d Tweets: %{x}<br>Price: $%{y:.4f}<extra></extra>",
        )
    )
    
    # Add trend line
    x = merged_data["tweets_7d_sum"].dropna()
    y = merged_data.loc[x.index, "close"]
    
    if len(x) > 2:
        z = np.polyfit(x, y, 1)
        p = np.poly1d(z)
        x_line = np.linspace(x.min(), x.max(), 100)
        
        fig.add_trace(
            go.Scatter(
                x=x_line,
                y=p(x_line),
                mode="lines",
                name="Trend",
                line=dict(color="#FF6B6B", dash="dash", width=2),
            )
        )
    
    # Get correlation value
    corr = report["correlations"].get("tweets_7d_avg_vs_price", {})
    corr_val = corr.get("correlation", 0)
    p_val = corr.get("p_value", 1)
    
    fig.update_layout(
        title=dict(
            text=f"Tweet Activity vs Price (r={corr_val:.3f}, p={p_val:.4f})",
            font=dict(size=20, color="#FFFFFF"),
            x=0.5,
        ),
        template="plotly_dark",
        paper_bgcolor="#0D1117",
        plot_bgcolor="#0D1117",
        height=500,
        xaxis=dict(
            title="Tweets in Last 7 Days",
            gridcolor="rgba(255,255,255,0.1)",
        ),
        yaxis=dict(
            title="$PUMP Price (USD)",
            gridcolor="rgba(255,255,255,0.1)",
        ),
        showlegend=False,
    )
    
    return fig


def create_summary_dashboard(report: Dict) -> go.Figure:
    """
    Create a comprehensive dashboard with all key metrics.
    """
    # Create a dashboard layout
    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=(
            "Price & Tweet Activity Over Time",
            "Tweet Days vs No-Tweet Days",
            "Daily Tweet Count",
            "Quiet Period Impact",
            "Correlation: Tweets vs Price",
            "Key Statistics"
        ),
        row_heights=[0.4, 0.3, 0.3],
        specs=[
            [{"secondary_y": True}, {"type": "bar"}],
            [{"type": "bar"}, {"type": "bar"}],
            [{"type": "scatter"}, {"type": "table"}],
        ],
        vertical_spacing=0.12,
        horizontal_spacing=0.1,
    )
    
    merged_data = pd.DataFrame(report["merged_data"])
    merged_data["date"] = pd.to_datetime(merged_data["date"])
    
    # 1. Price line
    fig.add_trace(
        go.Scatter(
            x=merged_data["date"],
            y=merged_data["close"],
            name="Price",
            line=dict(color="#00D4AA", width=2),
            fill="tozeroy",
            fillcolor="rgba(0, 212, 170, 0.1)",
        ),
        row=1, col=1, secondary_y=False
    )
    
    # Tweet avg on secondary y
    fig.add_trace(
        go.Scatter(
            x=merged_data["date"],
            y=merged_data["tweets_7d_avg"],
            name="Tweets (7d avg)",
            line=dict(color="#FF6B6B", width=2, dash="dot"),
        ),
        row=1, col=1, secondary_y=True
    )
    
    # 2. Tweet vs No-Tweet comparison
    ti = report["tweet_impact"]
    fig.add_trace(
        go.Bar(
            x=["Tweet Days", "No-Tweet Days"],
            y=[ti["tweet_day_stats"]["avg_return"], ti["no_tweet_day_stats"]["avg_return"]],
            marker_color=["#00D4AA", "#FF6B6B"],
            text=[f"{ti['tweet_day_stats']['avg_return']:+.2f}%", f"{ti['no_tweet_day_stats']['avg_return']:+.2f}%"],
            textposition="outside",
        ),
        row=1, col=2
    )
    
    # 3. Daily tweets
    fig.add_trace(
        go.Bar(
            x=merged_data["date"],
            y=merged_data["tweet_count"],
            marker_color="#FF6B6B",
            opacity=0.7,
            name="Daily Tweets",
        ),
        row=2, col=1
    )
    
    # 4. Quiet periods impact
    quiet_periods = [qp for qp in report["quiet_periods"] if qp.get("price_change_during") is not None]
    quiet_periods.sort(key=lambda x: x["gap_days"], reverse=True)
    top_quiet = quiet_periods[:8]
    
    if top_quiet:
        labels = [f"{qp['gap_days']}d" for qp in top_quiet]
        changes = [qp["price_change_during"] for qp in top_quiet]
        colors = ["#FF6B6B" if c < 0 else "#00D4AA" for c in changes]
        
        fig.add_trace(
            go.Bar(
                x=labels,
                y=changes,
                marker_color=colors,
                name="Price Impact",
            ),
            row=2, col=2
        )
    
    # 5. Scatter plot
    fig.add_trace(
        go.Scatter(
            x=merged_data["tweets_7d_sum"],
            y=merged_data["close"],
            mode="markers",
            marker=dict(size=6, color="#00D4AA", opacity=0.6),
            name="Data Points",
        ),
        row=3, col=1
    )
    
    # 6. Stats table
    corr = report["correlations"]
    stats_data = [
        ["Metric", "Value"],
        ["Correlation (7d tweets vs price)", f"{corr.get('tweets_7d_avg_vs_price', {}).get('correlation', 0):.3f}"],
        ["Tweet day avg return", f"{ti['tweet_day_stats']['avg_return']:+.2f}%"],
        ["No-tweet day avg return", f"{ti['no_tweet_day_stats']['avg_return']:+.2f}%"],
        ["Current silence", f"{quiet_periods[0]['gap_days'] if quiet_periods and quiet_periods[0].get('is_current') else 'N/A'} days"],
        ["Current silence impact", f"{quiet_periods[0].get('price_change_during', 0):.1f}%" if quiet_periods else "N/A"],
    ]
    
    fig.add_trace(
        go.Table(
            header=dict(
                values=["Metric", "Value"],
                fill_color="#1F2937",
                font=dict(color="white", size=12),
                align="left",
            ),
            cells=dict(
                values=[[row[0] for row in stats_data[1:]], [row[1] for row in stats_data[1:]]],
                fill_color="#0D1117",
                font=dict(color="white", size=11),
                align="left",
            ),
        ),
        row=3, col=2
    )
    
    fig.update_layout(
        title=dict(
            text="$PUMP Price vs @a1lon9 Tweet Activity: Complete Analysis",
            font=dict(size=22, color="#FFFFFF"),
            x=0.5,
        ),
        template="plotly_dark",
        paper_bgcolor="#0D1117",
        plot_bgcolor="#0D1117",
        height=1000,
        showlegend=False,
    )
    
    return fig


def main():
    """Generate all visualizations."""
    print("Loading analysis report...")
    report = load_analysis_report()
    
    print("Generating visualizations...")
    
    # Main chart
    fig1 = create_dual_axis_chart(report)
    fig1.write_html(OUTPUT_DIR / "price_vs_tweets.html")
    print(f"  ✓ Saved: price_vs_tweets.html")
    
    # Comparison chart
    fig2 = create_comparison_chart(report)
    fig2.write_html(OUTPUT_DIR / "tweet_day_comparison.html")
    print(f"  ✓ Saved: tweet_day_comparison.html")
    
    # Quiet period chart
    fig3 = create_quiet_period_chart(report)
    if fig3:
        fig3.write_html(OUTPUT_DIR / "quiet_periods.html")
        print(f"  ✓ Saved: quiet_periods.html")
    
    # Scatter plot
    fig4 = create_correlation_scatter(report)
    fig4.write_html(OUTPUT_DIR / "correlation_scatter.html")
    print(f"  ✓ Saved: correlation_scatter.html")
    
    # Dashboard
    fig5 = create_summary_dashboard(report)
    fig5.write_html(OUTPUT_DIR / "dashboard.html")
    print(f"  ✓ Saved: dashboard.html")
    
    print(f"\n🎉 All visualizations saved to: {OUTPUT_DIR}")
    print("\nOpen any .html file in your browser to view the interactive charts!")


if __name__ == "__main__":
    main()

