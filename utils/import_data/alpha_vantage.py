import os

import pandas as pd
import requests
from dotenv import load_dotenv

from constants import OHLC_REQUIRED_COLUMNS

load_dotenv(".env")
ALPHA_VANTAGE_API_KEY = os.environ.get("alpha_vantage_key")


def get_daily_raw_from_alpha_vantage(ticker: str) -> dict:
    # NOTE Currently, the last returned row is for yesterday
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={ALPHA_VANTAGE_API_KEY}&outputsize=full"
    return requests.get(url).json()


def _rename_alpha_vantage_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "1. open": "Open",
            "2. high": "High",
            "3. low": "Low",
            "4. close": "Close",
            "5. volume": "Volume",
        }
    )
    df["Open"] = df["Open"].astype(float)
    df["High"] = df["High"].astype(float)
    df["Low"] = df["Low"].astype(float)
    df["Close"] = df["Close"].astype(float)
    df["Volume"] = df["Volume"].astype(int)
    return df


def transform_a_v_raw_data_to_df(data: dict, key_name: str) -> pd.DataFrame:
    """
    Transform alpha vantage raw data to OHLC pd.DataFrame
    """
    if key_name not in data:
        raise ValueError(
            f"transform_a_v_raw_data_to_df: no key {key_name} in input data dict"
        )
    df = pd.DataFrame.from_dict(data[key_name]).transpose()
    df.index = pd.to_datetime(df.index)
    df.index = df.index.date
    df = df.sort_index()
    return _rename_alpha_vantage_df_columns(df)


def _check_imported_data(df: pd.DataFrame, ticker: str, data_type: str) -> None:
    if not isinstance(df, pd.DataFrame):
        raise ValueError(
            f"In _check_imported_data ({ticker=}, {data_type=}): not instance of pd.DataFrame"
        )
    if df.empty:
        raise ValueError(
            f"In _check_imported_data ({ticker=}, {data_type=}): empty DataFrame"
        )
    df_columns = set(df.columns.values)
    for col_name in OHLC_REQUIRED_COLUMNS:
        if col_name not in df_columns:
            error_msg = f"In _check_imported_data ({ticker=}, {data_type=}): column {col_name} absent in pd.DataFrame columns: {set(df.columns.values)}"
            raise ValueError(error_msg)
    all_columns_numeric = df.apply(
        lambda s: pd.to_numeric(s, errors="coerce").notnull().all()
    ).all()
    if not all_columns_numeric:
        raise ValueError(
            f"In _check_imported_data ({ticker=}, {data_type=}): not all pd.DataFrame columns numeric"
        )


def import_alpha_vantage_daily(ticker: str) -> pd.DataFrame:
    raw_data_daily: dict = get_daily_raw_from_alpha_vantage(ticker=ticker)
    data_daily: pd.DataFrame = transform_a_v_raw_data_to_df(
        data=raw_data_daily, key_name="Time Series (Daily)"
    )
    _check_imported_data(df=data_daily, ticker=ticker, data_type="Daily")
    for col in data_daily.columns:
        data_daily[col] = pd.to_numeric(data_daily[col])
    return data_daily[["Open", "High", "Low", "Close", "Volume"]]
