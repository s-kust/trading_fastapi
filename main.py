import logging
from logging.config import dictConfig

import yfinance as yf
from dotenv import load_dotenv
from fastapi import FastAPI

from constants import RSI_PERIOD, S3_BUCKET, S3_FOLDER_DAILY_DATA
from utils.derived_columns.rsi import add_rsi_column, update_close_rsi_for_ticker
from utils.draw_charts import draw_save_candlestick_with_rsi_v2
from utils.import_data import (
    add_fresh_ohlc_to_main_data,
    add_fresh_ohlc_to_ticker_data,
    import_alpha_vantage_daily,
    import_yahoo_fin_daily,
)
from utils.logging import log_config
from utils.s3 import read_daily_ohlc_from_s3, read_df_from_s3_csv, write_df_to_s3_csv

dictConfig(log_config)
app_logger = logging.getLogger("app")
load_dotenv(".env")

app = FastAPI()


@app.get("/")
async def root() -> dict:
    ticker = "TSLA"
    # res = update_close_rsi_for_ticker(ticker=ticker)

    filename = f"{ticker.upper()}.csv"
    main_df = read_df_from_s3_csv(filename=filename, folder="daily_OHLC_with_RSI/")
    if main_df is None:
        raise RuntimeError("main_df is None")
    draw_save_candlestick_with_rsi_v2(df=main_df)

    # new_data = import_yahoo_fin_daily(ticker=ticker)
    # res = add_fresh_ohlc_to_main_data(main_df=main_df, new_data=new_data)

    # print(res)

    # df = read_daily_ohlc_from_s3(ticker=ticker)
    # df = add_rsi_column(df=df)
    # try:
    #     s3_filename = f"{ticker}.csv"
    #     s3_write_res = write_df_to_s3_csv(
    #         df=df, filename=s3_filename, folder="daily_OHLC_with_RSI/"
    #     )
    #     log_msg = f"write_df_to_s3_csv {s3_filename} - " + s3_write_res
    #     app_logger.info(log_msg)
    # except Exception as e:
    #     app_logger.error(e, exc_info=True)
    #     raise e

    # df = add_fresh_ohlc_to_ticker_data(ticker=ticker)
    # df = import_yahoo_fin_daily(ticker=ticker)
    # print(df)

    # filename = f"{ticker.upper()}.csv"
    # df = read_df_from_s3_csv(filename=filename, folder="daily_OHLC_with_RSI/")
    # if df is not None and not df.empty:
    #     first_rsi_nan_index_label = df["RSI_14"].isnull().idxmax()
    #     first_rsi_nan_position = df.index.get_loc(first_rsi_nan_index_label)
    #     start_index = max(0, first_rsi_nan_position - 14)  # type: ignore
    #     print(f"{first_rsi_nan_index_label=}")
    #     print(f"{first_rsi_nan_position=}")
    #     print(f"{start_index=}")

    #     # filtered_df = df.iloc[start_index:]
    #     # print(filtered_df)
    #     # print()
    #     # filtered_df = add_rsi_column(df=filtered_df, col_name="Close")
    #     # filtered_df = filtered_df[filtered_df[f"RSI_{RSI_PERIOD}"].notnull()]
    #     # print(filtered_df)

    return {
        "message": f"Hello World RSI, {ticker=}",
        "S3_BUCKET": S3_BUCKET,
        "S3_FOLDER_DAILY_DATA": S3_FOLDER_DAILY_DATA,
        # "msg": msg,
        # "df 1st": str(df.index[0]),
        # "df last": str(df.index[-1]),
    }
