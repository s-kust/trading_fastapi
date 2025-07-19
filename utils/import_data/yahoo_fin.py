import pandas as pd
import yfinance as yf


def get_ohlc_from_yf(
    ticker: str, period: str = "max", interval: str = "1d"
) -> pd.DataFrame:
    """
    Get OHLC DataFrame with Volume from Yahoo Finance.
    Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max.
    Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
    """
    res = yf.Ticker(ticker=ticker).history(period=period, interval=interval)

    # NOTE  If period and interval mismatch, Yahoo Finance returns empty DataFrame.
    # A mismatch is an interval too small for a long period.
    if res.shape[0] == 0:
        raise RuntimeError(
            f"get_ohlc_from_yf: YFin returned empty Df for {ticker=},{period=}, {interval=}"
        )

    res.index = pd.to_datetime(res.index)
    res.index = res.index.tz_convert(None)
    res.index = res.index.date
    res = res.sort_index()
    return res[["Open", "High", "Low", "Close", "Volume"]]


def import_yahoo_fin_daily(ticker: str) -> pd.DataFrame:
    """
    We don't want to have today's data because today's trading day may not be over yet.
    """
    res = get_ohlc_from_yf(ticker=ticker, period="max", interval="1d")
    res.index = pd.to_datetime(res.index, utc=True)
    res.index = res.index.normalize()
    res.index = res.index.date  # type: ignore
    res = res.sort_index()
    res = res[res.index < pd.to_datetime("today").date()]
    return res
