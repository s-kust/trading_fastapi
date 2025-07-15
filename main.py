import logging
from logging.config import dictConfig

import yfinance as yf
from dotenv import load_dotenv
from fastapi import FastAPI

from constants import S3_BUCKET, S3_FOLDER_DAILY_DATA
from utils.log_config import log_config
from utils.s3 import read_daily_ohlc_from_s3

dictConfig(log_config)
app_logger = logging.getLogger("app")
load_dotenv()

app = FastAPI()


@app.get("/")
async def root() -> dict:
    ticker = "NFLX"
    try:
        df = read_daily_ohlc_from_s3(ticker=ticker)
    except Exception as e:
        print("Before calling app_logger.error")
        app_logger.error(e, exc_info=True)
        print("After calling app_logger.error")
        raise e
    # df = yf.Ticker(ticker=ticker).history(period='max', interval='1d')
    if df is None:
        print("Before calling app_logger.error 2")
        app_logger.error("Empty DF")
        print("After calling app_logger.error 2")
    msg = f"This is debug message with {ticker=}"
    app_logger.debug(msg)
    return {
        "message": "Hello World 11",
        "S3_BUCKET": S3_BUCKET,
        "S3_FOLDER_DAILY_DATA": S3_FOLDER_DAILY_DATA,
        "msg": msg,
        # "df 1st": str(df.index[0]),
        # "df last": str(df.index[-1])
    }
