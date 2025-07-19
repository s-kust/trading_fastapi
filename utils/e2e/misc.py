from utils.derived_columns import update_close_rsi_for_ticker
from utils.draw_charts import draw_save_candlestick_with_rsi
from utils.import_data import add_fresh_ohlc_to_ticker_data


def update_ohlc_rsi_chart(ticker: str) -> None:
    """
    Update OHLC dataframe, RSI column, and RSI chart for ticker.
    """
    df = add_fresh_ohlc_to_ticker_data(ticker=ticker)
    df = update_close_rsi_for_ticker(ticker=ticker, initial_ohlc_df=df)
    draw_save_candlestick_with_rsi(df=df, ticker=ticker)
