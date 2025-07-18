import logging
from logging.config import dictConfig

import yfinance as yf
from dotenv import load_dotenv
from fastapi import FastAPI

from constants import S3_BUCKET, S3_FOLDER_DAILY_DATA
from utils.derived_columns import add_rsi_column
from utils.import_data import (
    add_fresh_ohlc_to_ticker_data,
    import_alpha_vantage_daily,
    import_yahoo_fin_daily,
)
from utils.log_config import log_config
from utils.s3 import read_daily_ohlc_from_s3, read_df_from_s3_csv, write_df_to_s3_csv

dictConfig(log_config)
app_logger = logging.getLogger("app")
load_dotenv(".env")

app = FastAPI()


@app.get("/")
async def root() -> dict:
    ticker = "NFLX"

    # df = add_fresh_ohlc_to_ticker_data(ticker=ticker)
    # df = import_yahoo_fin_daily(ticker=ticker)
    # print(df)

    filename = f"{ticker.upper()}.csv"
    df = read_df_from_s3_csv(filename=filename, folder="daily_OHLC_with_RSI/")
    if df is not None and not df.empty:
        last_rsi_valid_index = df["RSI_14"].last_valid_index()
        start_index = max(0, last_rsi_valid_index - 14)
        print(f"{last_rsi_valid_index=}")
        print(f"{start_index=}")

    return {
        "message": f"Hello World RSI, {ticker=}",
        "S3_BUCKET": S3_BUCKET,
        "S3_FOLDER_DAILY_DATA": S3_FOLDER_DAILY_DATA,
        # "msg": msg,
        "df 1st": str(df.index[0]),
        "df last": str(df.index[-1]),
    }
