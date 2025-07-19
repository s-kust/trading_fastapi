import pandas as pd

from utils.import_data.yahoo_fin import import_yahoo_fin_daily
from utils.logging import execute_and_log
from utils.s3 import read_daily_ohlc_from_s3, write_df_to_s3_csv


def add_fresh_ohlc_to_main_data(
    main_df: pd.DataFrame, new_data: pd.DataFrame
) -> pd.DataFrame:
    print("main_df")
    print(main_df)
    print(f"{type(main_df.index)=}")
    print()
    print("new_data")
    print(new_data)
    print(f"{type(new_data.index)=}")
    print()
    if not main_df.empty:
        res = pd.concat([main_df, new_data[new_data.index > main_df.index.max()]])
    else:
        res = pd.concat([main_df, new_data])
    res.index = pd.to_datetime(res.index)
    res = res.sort_index()
    return res


def add_fresh_ohlc_to_ticker_data(ticker: str) -> pd.DataFrame:
    """
    Add fresh rows to the OHLC data for ticker and save OHLC DF in S3 bucket.
    """
    new_data = import_yahoo_fin_daily(ticker=ticker)
    main_df = read_daily_ohlc_from_s3(ticker=ticker)
    if main_df is not None and not main_df.empty:
        res = add_fresh_ohlc_to_main_data(main_df=main_df, new_data=new_data)
    else:
        res = new_data
    s3_filename = f"{ticker}.csv"
    execute_and_log(
        func=write_df_to_s3_csv, params={"df": res, "filename": s3_filename}
    )
    return res
