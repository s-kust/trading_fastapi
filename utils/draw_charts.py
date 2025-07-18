import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from constants import RSI_PERIOD

# NOTE
# If RuntimeError: Kaleido now requires that chrome/chromium is installed separately,
# see https://stackoverflow.com/questions/79204447/kaleido-runtimeerror


def draw_save_candlestick_with_rsi(df: pd.DataFrame, ticker: str) -> None:
    # df_last_30_days = df.last("30D").copy()
    df_last = df[df.index >= (df.index.max() - pd.Timedelta(days=90))].copy()
    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.5, 0.2, 0.3],  # Adjusted heights for OHLC, Volume, Indicator
    )

    # Add Candlestick trace to the first row
    fig.add_trace(
        go.Candlestick(
            x=df_last.index,
            open=df_last["Open"],
            high=df_last["High"],
            low=df_last["Low"],
            close=df_last["Close"],
            name="OHLC",
            increasing_line_color="green",  # Green for increasing candles
            decreasing_line_color="red",  # Red for decreasing candles
        ),
        row=1,
        col=1,
    )

    # Add Volume trace to the second row
    fig.add_trace(
        go.Bar(
            x=df_last.index,
            y=df_last["Volume"],
            name="Volume",
            marker_color="rgba(0,0,255,0.5)",  # Blue with some transparency
        ),
        row=2,
        col=1,
    )

    # Add Technical Indicator (SMA_10) trace to the second row
    fig.add_trace(
        go.Scatter(
            x=df_last.index,
            y=df_last[f"RSI_{RSI_PERIOD}"],
            mode="lines",
            name=f"RSI_{RSI_PERIOD}",
            line=dict(color="blue", width=2),
        ),
        row=3,
        col=1,
    )

    fig.update_layout(
        title_text=f"{ticker}: Candlestick Chart with RSI_14",
        title_x=0.5,  # Center the title
        xaxis_rangeslider_visible=False,  # Hide the range slider on the bottom x-axis
        height=800,  # Set overall chart height
        template="plotly_white",  # Use a clean white background template
        hovermode="x unified",  # Show hover info for all traces at a given x-coordinate
    )

    # Update Y-axis titles
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    fig.update_yaxes(title_text="RSI_14", row=3, col=1)

    # Remove x-axis labels from the top subplot to avoid redundancy
    fig.update_xaxes(showticklabels=False, row=1, col=1)
    fig.update_xaxes(showticklabels=False, row=2, col=1)

    last_date = str(df.index[-1])
    last_rsi_value = df[f"RSI_{RSI_PERIOD}"].iloc[-1]
    last_rsi_value = int(last_rsi_value)
    fig.update_xaxes(title_text=f"{last_date=}, {last_rsi_value=}", row=3, col=1)

    # Add rangebreaks to remove weekends (Saturday and Sunday)
    # 6 corresponds to Saturday, 7 to Sunday.
    # This applies to both x-axes due to shared_xaxes=True
    fig.update_xaxes(
        rangebreaks=[
            dict(
                bounds=[6, 1], pattern="day of week"
            )  # Hide Saturday (6) and Sunday (7, which wraps to 1 for Monday)
        ],
        row=1,
        col=1,
    )
    fig.update_xaxes(
        rangebreaks=[
            dict(
                bounds=[6, 1], pattern="day of week"
            )  # Hide Saturday (6) and Sunday (7, which wraps to 1 for Monday)
        ],
        row=2,
        col=1,
    )
    fig.update_xaxes(
        rangebreaks=[
            dict(
                bounds=[6, 1], pattern="day of week"
            )  # Hide Saturday (6) and Sunday (7, which wraps to 1 for Monday)
        ],
        row=3,
        col=1,
    )

    fig.write_image("fig1.png")
