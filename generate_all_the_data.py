import pandas as pd
from pandas.tseries.offsets import BDay

from src.data_generation import *
from src.data_generation_helpers import *
from src.data_types import *
from src.serialization_lib import *

# Prepare Inputs for Base + Test
INITIAL_PORTFOLIO_VALUE = 10000
PORTFOLIO_SIZE = 30
REBALANCE_DAYS = 90

BASE_METRIC = EvaluationMetric.EV_EBIT
TEST_METRIC = EvaluationMetric.P_B
STOCKS_UNIVERSE = StockUniverse.LARGE
PORTFOLIO_WEIGHT_STRATEGY = StockBasketWeightApproach.EQUAL_WEIGHTING

daily_data = read_df_from_feather('daily_data.feather')

# OPTIMIZATION ONLY: Sorting every time would take too long...
date_sorted_daily_data = daily_data.sort_values(by="date")
base_sorted_daily_data = sort_df_by_metric(daily_data, BASE_METRIC)
test_sorted_daily_data = sort_df_by_metric(daily_data, TEST_METRIC)

# for rebalance_days in [1000, 2000]:
#     for portfolio_size in [10, 30]:
for rebalance_days in [30, 90, 180, 730, 1825]:
    for portfolio_size in [5, 10, 15, 30, 60]:
        print(f"rebalance_days:{rebalance_days}", f"portfolio_size:{portfolio_size}")
        try:
            # pass
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
                test_sorted_daily_data,
            )
        except Exception as e:
            print("Caught exception while processing...", e, f"rebalance_days:{rebalance_days}", f"portfolio_size:{portfolio_size}")
