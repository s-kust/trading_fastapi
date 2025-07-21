from typing import Callable, Tuple

import numpy as np
import pandas as pd


def get_min_price_for_indicator_threshold(
    df: pd.DataFrame,
    indicator_func: Callable[[pd.DataFrame, str, int], float],
    indicator_threshold: float,
    n_prices: int = 14,
    price_column: str = "Close",
) -> Tuple[float, float, str]:
    """
    Calculates the minimum price for the next day at which the indicator value
    will be slightly more than the given threshold (no more than 0.5 more).

    Args:
        df: OHLC DataFrame with an additional indicator column.
        indicator_func: A callable function that calculates the indicator value.
                        It should take a pandas DataFrame, the price column name (str),
                        and the period (int), returning the indicator value (float).
        indicator_threshold: The target threshold value for the indicator.
        n_prices: The number of last price values needed for the indicator calculation.
                  (e.g., 14 for RSI_14).
        price_column: The name of the price column used for indicator calculation (default "Close").

    Returns:
        A tuple containing:
        - The calculated next day's price to meet the threshold (float).
        - The actual indicator value calculated for that price (float).
        - A string indicating the situation ("Threshold met within tolerance",
          "Converged within price epsilon", or "Max iterations reached") (str).
        Raises ValueError if input data is invalid or insufficient.
    """
    if df.empty:
        raise ValueError("Input DataFrame is empty.")
    if price_column not in df.columns:
        raise ValueError(f"Price column '{price_column}' not found in DataFrame.")
    if n_prices <= 0:
        raise ValueError("n_prices must be a positive integer.")

    # We need at least `n_prices` historical data points to form the basis
    # for calculating the indicator for the hypothetical next day's price.
    if len(df) < n_prices:
        raise ValueError(
            f"Not enough historical data in '{price_column}' for period {n_prices}. "
            f"DataFrame has {len(df)} rows, but needs at least {n_prices} for calculation."
        )

    # Get the last `n_prices` historical rows from the DataFrame.
    # Use .copy() to avoid SettingWithCopyWarning when modifying later.
    historical_df_tail = df.tail(n_prices).copy()

    # Define the initial search range for the next day's price.
    # The range is set dynamically based on the last known closing price to
    # provide a reasonable starting point for the binary search.
    last_close = df[price_column].iloc[-1]
    low_price = last_close * 0.1  # Lower bound (allowing for significant drops)
    high_price = last_close * 5.0  # Upper bound (allowing for significant gains)

    # Ensure the lower bound for price is not zero or negative.
    low_price = max(0.01, low_price)

    # Define the convergence criteria for the binary search.
    # `epsilon_price` determines how close `low_price` and `high_price` need to be.
    epsilon_price = 0.01  # Price convergence tolerance (e.g., 1 cent)
    max_iterations = 200  # Maximum iterations to prevent infinite loops

    found_price = None
    final_indicator_value = np.nan
    situation_string = (
        "Max iterations reached"  # Default situation if loop finishes without breaking
    )

    # Perform binary search to find the target price.
    for _ in range(max_iterations):
        mid_price = (low_price + high_price) / 2.0

        # Create a temporary DataFrame for indicator calculation.
        # It includes the historical `n_prices` rows plus the hypothetical `mid_price`.

        # Create a new row for the hypothetical price.
        # Initialize with NaNs for all columns to match the original DataFrame's structure.
        new_row_data = {col: np.nan for col in df.columns}
        new_row_data[price_column] = mid_price

        # Determine the index for the new row. If the original DataFrame has a DatetimeIndex,
        # increment the last date. Otherwise, use a simple integer index.
        if isinstance(df.index, pd.DatetimeIndex) and not df.empty:
            new_index = [df.index[-1] + pd.Timedelta(days=1)]
        else:
            new_index = (
                [df.index[-1] + 1] if not df.empty else [0]
            )  # Handle empty df for index

        new_row_df = pd.DataFrame([new_row_data], index=new_index)

        # Concatenate the historical tail with the new row to form the DataFrame for calculation.
        temp_df_for_calc = pd.concat([historical_df_tail, new_row_df])

        # Calculate the indicator value using the provided `indicator_func`.
        # Pass the temporary DataFrame, price_column, and n_prices.
        current_indicator_value = indicator_func(
            temp_df_for_calc, price_column, n_prices
        )

        # Handle cases where the indicator function might return NaN (e.g., insufficient data
        # within the `temp_df_for_calc` for the specific indicator's calculation, though
        # our checks should prevent this if n_prices is correctly set).
        if np.isnan(current_indicator_value):
            # If NaN, it usually means the price is too low to generate a valid indicator,
            # so we treat it as being below the threshold and try a higher price.
            low_price = mid_price
            continue

        # Check if the calculated indicator value falls within the desired range:
        if indicator_threshold <= current_indicator_value <= indicator_threshold + 0.5:
            # If the condition is met, we've found a suitable price.
            found_price = round(mid_price, 2)
            final_indicator_value = current_indicator_value
            situation_string = "Threshold met within tolerance"
            break  # Found a suitable price, exit loop

        # Adjust the search range based on the comparison with the threshold.
        if current_indicator_value < indicator_threshold:
            # If the indicator is too low, we need a higher price.
            low_price = mid_price
        else:  # current_indicator_value > indicator_threshold + 0.5
            # If the indicator is too high, we need a lower price.
            high_price = mid_price

        # Check for convergence based on price range. If the range is very small,
        # further iterations won't significantly change the price.
        if (high_price - low_price) < epsilon_price:
            found_price = round(mid_price, 2)
            # Recalculate indicator for the found_price to ensure accuracy for the return value
            new_row_data_converged = {col: np.nan for col in df.columns}
            new_row_data_converged[price_column] = found_price
            new_row_df_converged = pd.DataFrame(
                [new_row_data_converged], index=new_index
            )  # Use the same new_index
            temp_df_for_calc_converged = pd.concat(
                [historical_df_tail, new_row_df_converged]
            )
            final_indicator_value = indicator_func(
                temp_df_for_calc_converged, price_column, n_prices
            )
            situation_string = "Converged within price epsilon"
            break  # Converged, exit loop

    # If the loop completes without breaking (max iterations reached)
    if found_price is None:
        found_price = round(mid_price, 2)  # Use the last mid_price as the best estimate
        # Recalculate indicator for the found_price
        new_row_data_max_iter = {col: np.nan for col in df.columns}
        new_row_data_max_iter[price_column] = found_price
        new_row_df_max_iter = pd.DataFrame(
            [new_row_data_max_iter], index=new_index
        )  # Use the same new_index
        temp_df_for_calc_max_iter = pd.concat([historical_df_tail, new_row_df_max_iter])
        final_indicator_value = indicator_func(
            temp_df_for_calc_max_iter, price_column, n_prices
        )
        situation_string = "Max iterations reached"

    return found_price, final_indicator_value, situation_string
