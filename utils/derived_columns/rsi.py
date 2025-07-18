from typing import Callable

import numpy as np
import pandas as pd

from constants import RSI_PERIOD, S3_FOLDER_RSI
from utils.import_data.misc import add_fresh_ohlc_to_main_data
from utils.s3 import read_daily_ohlc_from_s3, read_df_from_s3_csv, write_df_to_s3_csv


def _add_rsi_col_initial_validation(
    df: pd.DataFrame, col_name: str, ma_type: str = "simple"
) -> None:
    """Helper function to preform input validation for the add_rsi_column function"""
    if df.empty:
        raise ValueError("add_rsi_column: empty input DataFrame")
    if col_name not in df.columns:
        raise ValueError(f"add_rsi_column: no {col_name} column in input DataFrame")
    if ma_type not in ["simple", "exponential"]:
        raise ValueError(f"add_rsi_column: {ma_type=}, must be simple or exponential")
    if RSI_PERIOD < 2:
        raise ValueError(f"add_rsi_column: {RSI_PERIOD=}, must be >= 2")


def _calculate_ma(
    series: pd.Series, period: int = RSI_PERIOD, ma_type: str = "simple"
) -> pd.Series:
    """Helper function to calculate different types of moving averages"""
    if ma_type == "simple":
        return series.rolling(period).mean()
    elif ma_type == "exponential":
        return series.ewm(
            span=period, adjust=False
        ).mean()  # adjust=False for classic EMA
    else:
        raise ValueError(
            f"Unsupported moving average type: {ma_type=}, should be simple or exponential "
        )


def add_rsi_column(
    df: pd.DataFrame, col_name: str = "Close", ma_type: str = "simple"
) -> pd.DataFrame:
    """
    Adds the Relative Strength Index (RSI) column to a DataFrame.

    The function calculates RSI using either a simple moving average (SMA)
    or an exponential moving average (EMA) for the average gains and losses.

    Args:
        df (pd.DataFrame): The input DataFrame containing the price data.
        col_name (str): The name of the column in `df` that contains the price data.
        ma_type (str, optional): The type of moving average to use ('simple' or 'exponential').
                                  Defaults to 'simple'.

    Returns:
        pd.DataFrame: A new DataFrame with the RSI column added, named 'RSI_{RSI_PERIOD}'.

    Raises:
        ValueError: If the input DataFrame is empty, the specified column does not exist,
                    the `ma_type` is invalid, or `RSI_PERIOD` is less than 2.
    """
    # NOTE inspired by https://stackoverflow.com/a/29400434/3139228

    _add_rsi_col_initial_validation(df=df, col_name=col_name, ma_type=ma_type)

    internal_df = df.copy()
    # Get the difference in price
    delta = internal_df[col_name].diff()
    # Get rid of the first row, which is NaN
    # since it did not have a previous row to calculate the differences
    delta = delta[1:]
    # Make the positive gains (up) and negative gains (down) Series
    up, down = delta.clip(lower=0), delta.clip(upper=0).abs()

    roll_up = _calculate_ma(series=up, period=RSI_PERIOD, ma_type=ma_type)
    roll_down = _calculate_ma(series=down, period=RSI_PERIOD, ma_type=ma_type)
    rs = roll_up / roll_down
    rsi = 100.0 - (100.0 / (1.0 + rs))
    # Avoid division-by-zero if `roll_down` is zero
    # This prevents inf and/or nan values.
    rsi[:] = np.select([roll_down == 0, roll_up == 0, True], [100, 0, rsi])
    # check results again
    valid_rsi = rsi[RSI_PERIOD - 1 :]
    assert ((0 <= valid_rsi) & (valid_rsi <= 100)).all()
    # Note: rsi[:RSI_PERIOD - 1] is excluded from above assertion
    # because it is NaN for simple MA.
    internal_df[f"RSI_{RSI_PERIOD}"] = rsi
    return internal_df


def update_close_rsi_for_ticker(ticker: str) -> pd.DataFrame:
    ohlc_df = read_daily_ohlc_from_s3(ticker=ticker)
    if ohlc_df is None:
        raise RuntimeError(f"update_close_rsi_for_ticker: no OHLC DF for {ticker=}")
    filename = f"{ticker.upper()}.csv"
    rsi_df = read_df_from_s3_csv(filename=filename, folder=S3_FOLDER_RSI)
    if rsi_df is None:
        raise RuntimeError(f"update_close_rsi_for_ticker: no RSI DF for {ticker=}")
    rsi_df = rsi_df[rsi_df[f"RSI_{RSI_PERIOD}"].notnull()]
    res = add_fresh_ohlc_to_main_data(main_df=rsi_df, new_data=ohlc_df)
    first_rsi_nan_index_label = res["RSI_14"].isnull().idxmax()
    first_rsi_nan_position = res.index.get_loc(first_rsi_nan_index_label)
    start_index = max(0, first_rsi_nan_position - RSI_PERIOD)  # type: ignore
    filtered_df = res.iloc[start_index:]
    filtered_df = add_rsi_column(df=filtered_df, col_name="Close")
    filtered_df = filtered_df[filtered_df[f"RSI_{RSI_PERIOD}"].notnull()]
    print(filtered_df)
    print()
    res = pd.concat([res[res.index < filtered_df.index.min()], filtered_df])  # type: ignore
    return res
