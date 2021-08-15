from typing import List

import numpy as np
import pandas as pd

from src.data_types import *


def get_feather_filename(
    prefix: str,
    base_metric: EvaluationMetric,
    test_metric: EvaluationMetric,
    rebalance_days: int,
    portfolio_size: int,
    stocks_universe: StockUniverse,
    env: str = "prod",
) -> str:
    return (
        f"{prefix}:"
        f'{str(base_metric).replace("/", "_")}_VS'
        f'{str(test_metric).replace("/", "_")}:'
        f"rebalanced_every_{rebalance_days}:"
        f"portfolio_size_{portfolio_size}:"
        f"{str(stocks_universe)}:"
        f"{env}"
        f".feather"
    )


def serialize_portfolio(inputs: List[StockRebalanceInstance]) -> List[str]:
    """
    Helper so DataFrame column can be stored as a feather or HDF file.
    """
    return [f"{i.ticker}:{i.prev_price}:{i.curr_price}" for i in inputs]


def deserialize_portfolio(inputs: List[StockRebalanceInstance]) -> None:
    """
    Helper to deserialize portfolio from a previously saved feather or HDF file.
    """
    r = []
    for t in inputs:
        split = t.split(":")
        r.append(
            StockRebalanceInstance(
                split[0],
                float(split[1]) if split[1] != np.nan else np.nan,
                float(split[2]) if split[2] != np.nan else np.nan,
            )
        )
    return r


COLUMN_TO_FEATHER_SERIALIZATION_MAPPING = {
    "base_portfolio_tickers_closed": list,
    "base_portfolio_per_ticker_data": serialize_portfolio,
    "new_base_portfolio_per_ticker_data": serialize_portfolio,
}


def write_df_to_feather(df_orig: pd.DataFrame, filename: str):
    df = df_orig.copy()
    for key, ser_func in COLUMN_TO_FEATHER_SERIALIZATION_MAPPING.items():
        if key in df:
            df[key] = df[key].map(ser_func)
    df = df.reset_index()
    if "date" not in df.columns and df.dtypes["index"] == "datetime64[ns]":
        df.rename({"index": "date"}, inplace=True)
    df.to_feather(filename)


COLUMN_TO_FEATHER_DESERIALIZATION_MAPPING = {
    "base_portfolio_tickers_closed": set,
    "base_portfolio_per_ticker_data": deserialize_portfolio,
    "new_base_portfolio_per_ticker_data": deserialize_portfolio,
}


def read_df_from_feather(filename: str):
    df = pd.read_feather(filename)
    for key, deser_func in COLUMN_TO_FEATHER_DESERIALIZATION_MAPPING.items():
        if key in df:
            df[key] = df[key].map(deser_func)
    return df
    # return df.set_index("date")
