import logging
from logging.config import dictConfig

import pandas as pd

from utils.import_data.yahoo_fin import import_yahoo_fin_daily
from utils.log_config import log_config
from utils.s3 import read_daily_ohlc_from_s3, write_df_to_s3_csv

dictConfig(log_config)
app_logger = logging.getLogger("app")


def add_fresh_ohlc_to_main_data(
    main_df: pd.DataFrame, new_data: pd.DataFrame
) -> pd.DataFrame:
    if not main_df.empty:
        res = pd.concat([main_df, new_data[new_data.index > main_df.index.max()]])
    else:
        res = pd.concat([main_df, new_data])
    res.index = pd.to_datetime(res.index)
    res = res.sort_index()
    return res


def add_fresh_ohlc_to_ticker_data(ticker: str) -> pd.DataFrame:
    new_data = import_yahoo_fin_daily(ticker=ticker)
    main_df = read_daily_ohlc_from_s3(ticker=ticker)
    if main_df is not None and not main_df.empty:
        res = add_fresh_ohlc_to_main_data(main_df=main_df, new_data=new_data)
    else:
        res = new_data
    try:
        s3_filename = f"{ticker}.csv"
        s3_write_res = write_df_to_s3_csv(df=res, filename=s3_filename)
        log_msg = f"write_df_to_s3_csv {s3_filename} - " + s3_write_res
        app_logger.info(log_msg)
    except Exception as e:
        app_logger.error(e, exc_info=True)
        raise e
    return res
