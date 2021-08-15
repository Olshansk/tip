import datetime
from typing import List, Optional, Set, Tuple

import holidays
import numpy as np
import pandas as pd

from src.backtest_helpers import *
from src.data_types import *


def get_ticker_price(df: pd.DataFrame, ticker: str) -> int:
    row = df[df.ticker == ticker]
    assert len(row) == 1, f"{len(row)} not found in dataframe."
    return row["price"].iloc[0]


def get_closest_previous_work_day(
    check_day: datetime.datetime, holidays=holidays.US()
) -> datetime.datetime:
    if check_day.weekday() <= 4 and check_day not in holidays:
        return check_day
    offset = max(1, (check_day.weekday() + 6) % 7 - 3)
    most_recent = check_day - datetime.timedelta(offset)
    if most_recent not in holidays:
        return most_recent
    else:
        return get_closest_previous_work_day(most_recent, holidays)


def get_rebalance_dates(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    period_length: datetime.timedelta,
) -> List[datetime.datetime]:
    curr_date = start_date
    dates = []
    while curr_date < end_date:
        dates.append(get_closest_previous_work_day(curr_date))
        curr_date += period_length
    return dates


def sort_df_by_metric(df: pd.DataFrame, metric: EvaluationMetric) -> pd.DataFrame:
    if metric.value == EvaluationMetric.EV_EBIT.value:
        return df.sort_values(by="evebit").reset_index()
    elif metric.value == EvaluationMetric.P_E.value:
        return df.sort_values(by="pe").reset_index()
    elif metric.value == EvaluationMetric.P_B.value:
        return df.sort_values(by="pb").reset_index()
    elif metric.value == EvaluationMetric.DIV_YIELD.value:
        raise Exception("EvaluationMetric.DIV_YIELD not yet supported.")
    else:
        raise Exception(f"Unsupported evaluation metric {metric}")


def filter_df_by_date(df: pd.DataFrame, date: datetime.datetime) -> pd.DataFrame:
    return df[df.date == date.strftime("%Y-%m-%d")]


def filter_stocks_by_universe(
    df: pd.DataFrame, stocks_universe: StockUniverse
) -> pd.DataFrame:
    if stocks_universe.value == StockUniverse.SMALL.value:
        return df[df["marketcap"] < 1]
    elif stocks_universe.value == StockUniverse.MID.value:
        return df[(df["marketcap"] >= 1) & (df["marketcap"] <= 10)]
    elif stocks_universe.value == StockUniverse.LARGE.value:
        return df[(df["marketcap"] >= 10)]
    else:
        raise Exception(f"Unsupported stock universe {stocks_universe}")


def get_top_n_stocks_by_metric(
    df: pd.DataFrame, n: int, metric: EvaluationMetric
) -> List[str]:
    df_res = None
    if metric.value == EvaluationMetric.EV_EBIT.value:
        # assert df['evebit'].dropna().is_monotonic_increasing
        df_res = df[(df["evebit"] > 0) & (df["ev"] > 0)]
    elif metric.value == EvaluationMetric.P_E.value:
        # assert df['pe'].is_monotonic_increasing
        df_res = df[df["pe"] > 0]
    elif metric.value == EvaluationMetric.P_B.value:
        # assert df['pb'].is_monotonic_increasing
        df_res = df[df["pb"] > 0]
    elif metric.value == EvaluationMetric.DIV_YIELD.value:
        raise Exception("EvaluationMetric.DIV_YIELD not yet supported.")
    else:
        raise Exception(f"Unsupported evaluation metric {metric}")

    return list(df_res[:n]["ticker"])


def get_last_available_price(df: pd.DataFrame, ticker: str) -> int:
    # assert df.index.is_monotonic_increasing
    # assert df.index.dtype == 'datetime64[ns]' and df.index.is_monotonic_increasing
    # assert_date_index_is_sorted(df)
    return df[df.ticker == ticker].iloc[-1]["price"]


def get_per_stock_change(
    df: pd.DataFrame,
    df_prev: Optional[pd.DataFrame],
    df_full: pd.DataFrame,
    tickers: List[str],
) -> List[StockRebalanceInstance]:
    """
    For a list of interested tickers, get the ticker, price at the last
    rebalance date and the current price. If the current price is not available,
    return

    Args:
        df_full: Used to get the prices for stocks that are not available (acquired or closed).
    """
    res = []
    stocks_of_interest = df.loc[df["ticker"].isin(tickers)]
    missing_stocks = set(tickers) - set(stocks_of_interest["ticker"])

    for _idx, r in stocks_of_interest.iterrows():
        ticker = r["ticker"]
        price = r["price"]
        prev_price = (
            get_ticker_price(df_prev, ticker) if df_prev is not None else np.nan
        )
        res.append(StockRebalanceInstance(ticker, prev_price, price))

    for missing_ticker in missing_stocks:
        prev_price = (
            get_ticker_price(df_prev, missing_ticker) if df_prev is not None else np.nan
        )
        price = get_last_available_price(df_full, missing_ticker)
        res.append(StockRebalanceInstance(missing_ticker, prev_price, price))

    res.sort(key=lambda t: t.ticker)
    return res


# ASSUMPTION: df is filtered by date.
def get_share_allocation(
    df: pd.DataFrame,
    tickers: List[str],  # portfolio
    investment_amount: int,
    weight_approach: StockBasketWeightApproach,
) -> List[ShareAllocation]:
    if weight_approach != StockBasketWeightApproach.EQUAL_WEIGHTING:
        raise Exception(f"{weight_approach} not supported yet.")

    amount_per_stock = investment_amount / len(tickers)
    res = []
    for ticker in tickers:
        price = get_ticker_price(df, ticker)
        num_shares = amount_per_stock / price
        res.append(ShareAllocation(ticker, num_shares))
    return res


def get_portfolio_value(
    df: pd.DataFrame, share_allocation: List[ShareAllocation]
) -> float:
    total = 0
    for allocation in share_allocation:
        price = get_ticker_price(df, allocation.ticker)
        total += price * allocation.num_shares
    return total


def get_stock_basket_price(
    df: pd.DataFrame,
    df_full: pd.DataFrame,  # Used to get the prices for stocks that closed
    share_allocation: List[ShareAllocation],
) -> Tuple[float, Set[str]]:
    """
    Args:
        df_full: Used to get the prices for stocks that are not available (acquired or closed).
    """
    tickers = [alloc.ticker for alloc in share_allocation]
    stocks_of_interest = df.loc[df["ticker"].isin(tickers)]
    missing_stocks = set(tickers) - set(stocks_of_interest["ticker"])
    basket_price = 0
    for alloc in share_allocation:
        ticker = alloc.ticker
        if ticker in missing_stocks:
            price = get_last_available_price(df_full, ticker)
        else:
            price = get_ticker_price(df, ticker)
        basket_price += price * alloc.num_shares
    return (round(basket_price, 2), missing_stocks)
