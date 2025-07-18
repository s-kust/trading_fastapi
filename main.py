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
from utils.s3 import read_daily_ohlc_from_s3, write_df_to_s3_csv

dictConfig(log_config)
app_logger = logging.getLogger("app")
load_dotenv(".env")

app = FastAPI()


@app.get("/")
async def root() -> dict:
    ticker = "NFLX"
    df = add_fresh_ohlc_to_ticker_data(ticker=ticker)

    return {
        "message": "Hello World RSI 2",
        "S3_BUCKET": S3_BUCKET,
        "S3_FOLDER_DAILY_DATA": S3_FOLDER_DAILY_DATA,
        # "msg": msg,
        "df 1st": str(df.index[0]),
        "df last": str(df.index[-1]),
    }
