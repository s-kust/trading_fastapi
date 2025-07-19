from utils.e2e import update_ohlc_rsi_chart
from utils.logging import get_app_logger


async def update_ohlc_rsi_charts_for_tickers() -> None:
    app_logger = get_app_logger()
    tickers = ["GLD", "COPX"]
    for ticker in tickers:
        log_msg_1 = f"update_ohlc_rsi_charts_for_tickers - {ticker=} - starting"
        app_logger.info(log_msg_1)
        update_ohlc_rsi_chart(ticker=ticker)
        log_msg_2 = f"update_ohlc_rsi_charts_for_tickers - {ticker=} - finished OK"
        app_logger.info(log_msg_2)
