# Preprocess a bunch of data sets in advance.

from src.backtest import *
from src.backtest_helpers import *
from src.data_types import *
from src.serialization_lib import *


def run():
    INITIAL_PORTFOLIO_VALUE = 10000

    PORTFOLIO_SIZE = [5, 10, 15, 30, 60]
    REBALANCE_DAYS = [30, 90, 180, 360, 730, 1825]

    # PORTFOLIO_SIZE = [10, 30]
    # REBALANCE_DAYS = [1000, 2000]

    DATA_RAW_BASE_PATH = "/Volumes/SDCard/TipBackTest/raw_data"
    DATA_PROCESSED_BASE_PATH = "/Volumes/SDCard/TipBackTest/processed_data"

    BASE_METRIC = EvaluationMetric.EV_EBIT
    TEST_METRIC = EvaluationMetric.P_B
    STOCKS_UNIVERSE = StockUniverse.LARGE
    PORTFOLIO_WEIGHT_STRATEGY = StockBasketWeightApproach.EQUAL_WEIGHTING

    daily_data = read_df_from_feather(
        os.path.join(DATA_PROCESSED_BASE_PATH, f"daily_data_prod.feather")
    )
    daily_data_base_sorted = read_df_from_feather(
        os.path.join(DATA_PROCESSED_BASE_PATH, f"daily_data_base_sorted_prod.feather")
    )
    daily_data_test_sorted = read_df_from_feather(
        os.path.join(DATA_PROCESSED_BASE_PATH, f"daily_data_test_sorted_prod.feather")
    )

    for rebalance_days in REBALANCE_DAYS:
        for portfolio_size in PORTFOLIO_SIZE:
            print(
                f"rebalance_days:{rebalance_days}", f"portfolio_size:{portfolio_size}"
            )
            try:
                _backtest_result = compute_backtest_dfs(
                    BASE_METRIC,
                    TEST_METRIC,
                    STOCKS_UNIVERSE,
                    PORTFOLIO_WEIGHT_STRATEGY,
                    rebalance_days,
                    portfolio_size,
                    INITIAL_PORTFOLIO_VALUE,
                    daily_data,
                    daily_data_base_sorted,
                    daily_data_test_sorted,
                )
            except Exception as e:
                print(
                    "Caught exception while processing...",
                    e,
                    f"rebalance_days:{rebalance_days}",
                    f"portfolio_size:{portfolio_size}",
                )
