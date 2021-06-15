import pandas as pd
import holidays
import datetime

from src.serialization_lib import *
from src.data_types import *
from typing import List, Optional, Tuple, Set
from pandas.tseries.offsets import BDay

def get_closest_previous_work_day(
    check_day: datetime.datetime,
    holidays=holidays.US()
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
    period_length: datetime.timedelta
) -> List[datetime.datetime]:
    curr_date = start_date
    dates = []
    while curr_date < end_date:
        dates.append(get_closest_previous_work_day(curr_date))
        curr_date += period_length
    return dates

def sort_df_by_metric(
    df: pd.DataFrame,
    metric: EvaluationMetric
) -> pd.DataFrame:
    if metric.value == EvaluationMetric.EV_EBIT.value:
        return df.sort_values(by='evebit')
    elif metric.value == EvaluationMetric.P_E.value:
        return df.sort_values(by='pe')
    elif metric.value == EvaluationMetric.P_B.value:
        return df.sort_values(by='pb')
    elif metric.value == EvaluationMetric.DIV_YIELD.value:
        raise Exception('EvaluationMetric.DIV_YIELD not yet supported.')
    else:
        raise Exception(f'Unsupported evaluation metric {metric}')

def filter_df_by_date(
    df: pd.DataFrame,
    date: datetime.datetime
) -> pd.DataFrame:
    return df[df.date == date.strftime('%Y-%m-%d')]

# ASSUMPTION: df is already filtered by date.
def filter_stocks_by_universe(
    df: pd.DataFrame,
    stocks_universe: StockUniverse
) -> pd.DataFrame:
    if stocks_universe.value == StockUniverse.SMALL.value:
        return df[df['marketcap'] < 1]
    elif stocks_universe.value == StockUniverse.MID.value:
        return df[(df['marketcap'] >= 1) & (df['marketcap'] <= 10)]
    elif stocks_universe.value == StockUniverse.LARGE.value:
        return df[(df['marketcap'] >= 10)]
    else:
        raise Exception(f'Unsupported stock universe {stocks_universe}')

# ASSUMPTION: df is sorted by metric.
def get_top_n_stocks_by_metric(
    df: pd.DataFrame,
    n: int,
    metric: EvaluationMetric
) -> List[str]:
    df_res = None
    if metric.value == EvaluationMetric.EV_EBIT.value:
        df_res = df[(df['evebit'] > 0) & (df['ev'] > 0)]
    elif metric.value == EvaluationMetric.P_E.value:
        df_res = df[df['pe'] > 0]
    elif metric.value == EvaluationMetric.P_B.value:
        df_res = df[df['pb'] > 0]
    elif metric.value == EvaluationMetric.DIV_YIELD.value:
        raise Exception('EvaluationMetric.DIV_YIELD not yet supported.')
    else:
        raise Exception(f'Unsupported evaluation metric {metric}')

    return list(df_res[:n]['ticker'])

# ASSUMPTION: df is sorted by date.
def get_last_available_price(
    df: pd.DataFrame,
    ticker: str
) -> int:
    return df[df.ticker == ticker].iloc[-1]['price']

# ASSUMPTION: df is filtered by date.
def get_stock_price_per_stock(
    df: pd.DataFrame,
    df_prev: Optional[pd.DataFrame],
    df_full: pd.DataFrame,  # Used to get the prices for stocks that are not available (acquired or closed)
    tickers: List[str]
) -> Tuple[str, int]:
    res = []
    stocks_of_interest = df.loc[df['ticker'].isin(tickers)]
    missing_stocks = set(tickers) - set(stocks_of_interest['ticker'])

    for idx, r in stocks_of_interest.iterrows():
        ticker = r['ticker']
        price = r['price']
        prev_price = df_prev[df_prev['ticker'] == ticker]['price'].iloc[0] if df_prev is not None else 'NA'
        res.append((ticker, prev_price, price))

    for missing_ticker in missing_stocks:
        prev_price = df_prev[df_prev['ticker'] == missing_ticker]['price'].iloc[0] if df_prev is not None else 'NA'
        price = get_last_available_price(df_full, missing_ticker)
        res.append((missing_ticker, prev_price, price))

    res.sort(key = lambda t: t[0])
    return res

# ASSUMPTION: df is filtered by date.
def get_share_allocation(
    df: pd.DataFrame,
    tickers: List[str], # portfolio
    investment_amount: int,
    weight_approach: StockBasketWeightApproach
) -> List[ShareAllocation]:
    if weight_approach != StockBasketWeightApproach.EQUAL_WEIGHTING:
        raise Exception(f'{weight_approach} not supported yet.')

    amount_per_stock = investment_amount / len(tickers)
    res = []
    for ticker in tickers:
        price = df[df['ticker'] == ticker]['price'].iloc[0]
        num_shares = amount_per_stock / price
        res.append(ShareAllocation(ticker, num_shares))
    return res

# ASSUMPTION: df is filtered by date.
def get_portfolio_value(
    df: pd.DataFrame,
    share_allocation: List[ShareAllocation]
) -> float:
    total = 0
    for allocation in share_allocation:
        price = df[df.ticker == allocation.ticker]['price'].iloc[0]
        total += (price * allocation.num_shares)
    return total

# ASSUMPTION: df is filtered by date.
def get_stock_basket_price(
    df: pd.DataFrame,
    df_full: pd.DataFrame, # Used to get the prices for stocks that closed
    share_allocation: List[ShareAllocation],
    should_print = False
) -> Tuple[float, Set[str]]:
    tickers = [alloc.ticker for alloc in share_allocation]
    stocks_of_interest = df.loc[df['ticker'].isin(tickers)]
    missing_stocks = set(tickers) - set(stocks_of_interest['ticker'])
    basket_price = 0
    for alloc in share_allocation:
        ticker = alloc.ticker
        if ticker in missing_stocks:
            price = get_last_available_price(df_full, ticker)
        else:
            price = df[df.ticker == ticker]['price'].iloc[0]
        basket_price += (price * alloc.num_shares)
    return (round(basket_price, 2), missing_stocks)