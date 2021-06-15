from src.data_generation_helpers import *
from src.data_types import *
from datetime import timedelta

import datetime
import pandas as pd


def compute_dfs(
    base_metric: EvaluationMetric,
    test_metric: EvaluationMetric,
    stocks_universe: StockUniverse,
    weight_strategy: StockBasketWeightApproach,
    rebalance_days: int,
    portfolio_size: int,
    initial_portfolio_value: int,
    daily_data: pd.DataFrame,
    date_sorted_daily_data: pd.DataFrame,
    base_sorted_daily_data: pd.DataFrame,
    test_sorted_daily_data: pd.DataFrame,
):
    start_date = datetime.datetime.strptime(min(daily_data['date']), '%Y-%m-%d')
    end_date = datetime.datetime.strptime(max(daily_data['date']), '%Y-%m-%d')

    rebalance_dates = get_rebalance_dates(start_date, end_date, timedelta(days=rebalance_days))

    base_portfolio_value = initial_portfolio_value
    test_portfolio_value = initial_portfolio_value

    start_date = rebalance_dates[0]

    # Get data for start date
    daily_data_df = filter_df_by_date(date_sorted_daily_data, start_date)
    base_sorted_df = filter_df_by_date(base_sorted_daily_data, start_date)
    test_sorted_df = filter_df_by_date(test_sorted_daily_data, start_date)

    # Filter based on the universe of stocks we are interested in.
    base_sorted_df_in_universe = filter_stocks_by_universe(base_sorted_df, stocks_universe)
    test_sorted_df_in_universe = filter_stocks_by_universe(test_sorted_df, stocks_universe)

    # Get base stocks from the universe selected for each metric
    base_portfolio = get_top_n_stocks_by_metric(base_sorted_df_in_universe, portfolio_size, base_metric)
    test_portfolio = get_top_n_stocks_by_metric(test_sorted_df_in_universe, portfolio_size, test_metric)

    # Compute number of shares we can buy of each stock
    base_share_allocation = get_share_allocation(daily_data_df, base_portfolio, base_portfolio_value, weight_strategy)
    test_share_allocation = get_share_allocation(daily_data_df, test_portfolio, test_portfolio_value, weight_strategy)

    # SANITY CHECK: compute these values rather than assigning them for consistency.
    base_price, base_tickers_closed = get_stock_basket_price(base_sorted_df, date_sorted_daily_data, base_share_allocation)
    test_price, test_tickers_closed = get_stock_basket_price(test_sorted_df, date_sorted_daily_data, test_share_allocation)

    # SANITY CHECK
    assert initial_portfolio_value == base_price
    assert initial_portfolio_value == test_price
    assert base_portfolio_value == base_price
    assert test_portfolio_value == test_price

    res = {}
    debug = {}

    res[start_date] = {
        'base_price': base_price,
        'test_price': test_price,
    }

    prev_base_price = base_price
    prev_test_price = test_price
    prev_date = start_date

    # Skip the first date (start_date) because it is handeled above
    for date in rebalance_dates[1:]:
        print(date.strftime('%Y-%m-%d'))

        # Filter DFs for optimization purposes
        daily_data_df = filter_df_by_date(date_sorted_daily_data, date)
        base_sorted_df = filter_df_by_date(base_sorted_daily_data, date)
        test_sorted_df = filter_df_by_date(test_sorted_daily_data, date)

        # Compute value of previous portfolio at today's date
        base_price, base_tickers_closed = get_stock_basket_price(base_sorted_df, date_sorted_daily_data, base_share_allocation)
        test_price, test_tickers_closed = get_stock_basket_price(test_sorted_df, date_sorted_daily_data, test_share_allocation)

        # Compute teh change in the portfolio value
        base_change = base_price / prev_base_price
        test_change = test_price / prev_test_price

        # Compute new portfolio value
        base_portfolio_value = round(base_portfolio_value * base_change, 2)
        test_portfolio_value = round(test_portfolio_value * test_change, 2)

        res[date] = {
            'base_price_prev': prev_base_price,
            'base_price': base_price,
            'test_price_prev': prev_test_price,
            'test_price': test_price,
        }

        # Get data for the previous date to compute `base_portfolio_per_ticker_change` for debugging below
        prev_daily_data_df = filter_df_by_date(date_sorted_daily_data, prev_date)
        prev_base_sorted_df = filter_df_by_date(base_sorted_daily_data, prev_date)
        prev_test_sorted_df = filter_df_by_date(test_sorted_daily_data, prev_date)

        # DEBUG ONLY
        debug[date] = {
            'prev_date': prev_date,
            'curr_date': date,
            'base_portfolio_prev_price': prev_base_price,
            'base_portfolio_curr_price': base_price,
            'base_portfolio_tickers_closed': base_tickers_closed,
            'base_portfolio_per_ticker_data': get_stock_price_per_stock(base_sorted_df, prev_base_sorted_df, date_sorted_daily_data, base_portfolio),
        }

        # Filter based on the universe of stocks we are interested in.
        base_sorted_df_in_universe = filter_stocks_by_universe(base_sorted_df, stocks_universe)
        test_sorted_df_in_universe = filter_stocks_by_universe(test_sorted_df, stocks_universe)

        # Get the newley selected portfolio.
        base_portfolio = get_top_n_stocks_by_metric(base_sorted_df_in_universe, portfolio_size, base_metric)
        test_portfolio = get_top_n_stocks_by_metric(test_sorted_df_in_universe, portfolio_size, test_metric)

        base_share_allocation = get_share_allocation(daily_data_df, base_portfolio, base_portfolio_value, weight_strategy)
        test_share_allocation = get_share_allocation(daily_data_df, test_portfolio, test_portfolio_value, weight_strategy)

        # Get new portfolio price
        base_price, base_tickers_closed = get_stock_basket_price(base_sorted_df, date_sorted_daily_data, base_share_allocation)
        test_price, test_tickers_closed = get_stock_basket_price(test_sorted_df, date_sorted_daily_data, test_share_allocation)

        # SANITY CHECK: since we just got these stocks, none of them should be closed...
        assert len(base_tickers_closed) == 0
        assert len(test_tickers_closed) == 0

        # Cache the last price of the current portfolio (previous date)
        prev_base_price = base_price
        prev_test_price = test_price
        prev_date = date

        # DEBUG ONLY: Adding this to the debug DF for easier debugging...
        debug[date].update({
            'new_base_portfolio_per_ticker_data': get_stock_price_per_stock(base_sorted_df, None, date_sorted_daily_data, base_portfolio),
        })

    df_res = pd.DataFrame.from_dict(res, orient='index')
    df_debug = pd.DataFrame.from_dict(debug, orient='index')

    filename = df_feather_filename('df_debug', base_metric, test_metric, rebalance_days, portfolio_size, stocks_universe)
    write_df_debug_to_feather(df_debug, filename)

    filename = df_feather_filename('df_res', base_metric, test_metric, rebalance_days, portfolio_size, stocks_universe)
    write_df_res_to_feather(df_res, filename)

    return (df_res, df_debug)