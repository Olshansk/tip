import pandas as pd
import pprint

from src.data_generation import *
from src.data_generation_helpers import *
from src.serialization_lib import *
from src.data_types import *
from pandas.tseries.offsets import BDay
pd.set_option('display.max_colwidth', None)

pp = pprint.PrettyPrinter(indent=4)

daily_metrics = pd.read_csv('SHARADAR_DAILY_3_9ffd00fad4f19bbdec75c6e670d3df83.csv')
daily_prices = pd.read_csv('SHARADAR_SEP_2_0bd2000858d1d8d1f48d4cdea5f8c9e2.csv')

d1 = daily_metrics.copy()

d2 = daily_prices[['ticker', 'date','closeadj']]
d2.rename(columns={'closeadj': 'price'}, inplace=True)

daily_data = d1.merge(d2, on=['date', 'ticker'], how='inner')
daily_data

# Prepare Inputs for Base + Test
INITIAL_PORTFOLIO_VALUE = 10000
PORTFOLIO_SIZE = 30
REBALANCE_DAYS = 90

BASE_METRIC = EvaluationMetric.EV_EBIT
TEST_METRIC = EvaluationMetric.P_B
STOCKS_UNIVERSE = StockUniverse.LARGE
PORTFOLIO_WEIGHT_STRATEGY = StockBasketWeightApproach.EQUAL_WEIGHTING

# OPTIMIZATION ONLY: Sorting every time would take too long...
date_sorted_daily_data = daily_data.sort_values(by='date')
base_sorted_daily_data = sort_df_by_metric(daily_data, BASE_METRIC)
test_sorted_daily_data = sort_df_by_metric(daily_data, TEST_METRIC)

for rebalance_days in [365]:
    for portfolio_size in [10]:
# for rebalance_days in [30, 90, 180, 730, 1825]:
#     for portfolio_size in [5, 10, 15, 30, 60]:
        print(f"rebalance_days:{rebalance_days}", f"portfolio_size:{portfolio_size}")
        df_res, df_debug = compute_dfs(
            BASE_METRIC,
            TEST_METRIC,
            STOCKS_UNIVERSE,
            PORTFOLIO_WEIGHT_STRATEGY,
            rebalance_days,
            portfolio_size,
            INITIAL_PORTFOLIO_VALUE,
            daily_data,
            date_sorted_daily_data,
            base_sorted_daily_data,
            test_sorted_daily_data)