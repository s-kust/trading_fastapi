import logging
from logging.config import dictConfig

import yfinance as yf
from dotenv import load_dotenv
from fastapi import FastAPI

from constants import S3_BUCKET, S3_FOLDER_DAILY_DATA
from utils.import_data import import_alpha_vantage_daily, import_yahoo_fin_daily
from utils.log_config import log_config
from utils.s3 import read_daily_ohlc_from_s3, write_df_to_s3_csv

dictConfig(log_config)
app_logger = logging.getLogger("app")
load_dotenv(".env")

app = FastAPI()


@app.get("/")
async def root() -> dict:
    ticker = "TSLA"

    try:
        df_yahoo_fin = import_yahoo_fin_daily(ticker=ticker)
        print("df_yahoo_fin")
        print(df_yahoo_fin)
        print()
    except Exception as e:
        app_logger.error(e, exc_info=True)
        raise e

    try:
        df_a_v = import_alpha_vantage_daily(ticker=ticker)
        print("df_a_v")
        print(df_a_v)
        print()
    except Exception as e:
        app_logger.error(e, exc_info=True)
        raise e

    # try:
    #     df = read_daily_ohlc_from_s3(ticker=ticker)
    # except Exception as e:
    #     print("Before calling app_logger.error")
    #     app_logger.error(e, exc_info=True)
    #     print("After calling app_logger.error")
    #     raise e

    # df = yf.Ticker(ticker=ticker).history(period="max", interval="1d")
    # if df is None:
    #     print("Before calling app_logger.error 2")
    #     app_logger.error("Empty DF")
    #     print("After calling app_logger.error 2")
    # else:
    #     s3_filename = f"{ticker}.csv"
    #     s3_write_res = write_df_to_s3_csv(df=df, filename=s3_filename)
    #     log_msg = f"write_df_to_s3_csv {s3_filename} - " + s3_write_res
    #     app_logger.info(log_msg)
    # msg = f"This is debug message with {ticker=}"
    # app_logger.debug(msg)

    return {
        "message": "Hello World 12",
        "S3_BUCKET": S3_BUCKET,
        "S3_FOLDER_DAILY_DATA": S3_FOLDER_DAILY_DATA,
        # "msg": msg,
        # "df 1st": str(df.index[0]),
        # "df last": str(df.index[-1])
    }
