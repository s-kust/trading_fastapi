import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from constants import RSI_PERIOD


def draw_save_candlestick_with_rsi(df: pd.DataFrame) -> None:
    df_last_30_days = df.last("30D").copy()
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.7, 0.3],  # Candlestick chart takes 70% height, indicator 30%
    )

    # Add Candlestick trace to the first row
    fig.add_trace(
        go.Candlestick(
            x=df_last_30_days.index,
            open=df_last_30_days["Open"],
            high=df_last_30_days["High"],
            low=df_last_30_days["Low"],
            close=df_last_30_days["Close"],
            name="OHLC",
            increasing_line_color="green",  # Green for increasing candles
            decreasing_line_color="red",  # Red for decreasing candles
        ),
        row=1,
        col=1,
    )

    # Add Technical Indicator (SMA_10) trace to the second row
    fig.add_trace(
        go.Scatter(
            x=df_last_30_days.index,
            y=df_last_30_days[f"RSI_{RSI_PERIOD}"],
            mode="lines",
            name=f"RSI_{RSI_PERIOD}",
            line=dict(color="blue", width=2),
        ),
        row=2,
        col=1,
    )

    fig.update_layout(
        title_text="Candlestick Chart with 10-Day SMA (Last 30 Days)",
        title_x=0.5,  # Center the title
        xaxis_rangeslider_visible=False,  # Hide the range slider on the bottom x-axis
        height=700,  # Set overall chart height
        template="plotly_white",  # Use a clean white background template
        hovermode="x unified",  # Show hover info for all traces at a given x-coordinate
    )

    # Update Y-axis titles
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Indicator Value", row=2, col=1)

    # Remove x-axis labels from the top subplot to avoid redundancy
    fig.update_xaxes(showticklabels=False, row=1, col=1)
    fig.update_xaxes(title_text="Date zzz", row=2, col=1)

    fig.write_image("fig1.png")
