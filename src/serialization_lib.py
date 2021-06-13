from typing import List, Tuple

import pandas as pd

# Helpers to convert (stock, prev_price, curr_price) into a serializable manner.

def serialize_portfolio(inputs: List[Tuple[str, int, int]]):
    return [f"{t[0]}:{t[1]}:{t[2]}" for t in inputs]

def deserialize_portfolio(inputs: List[Tuple[str, int, int]]):
    r = []
    for t in inputs:
        split = t.split(":")
        r.append([split[0], float(split[1]) if split[1] != 'NA' else 'NA', float(split[2]) if split[2] != 'NA' else 'NA'])
    return r

# Debug DF helpers

def df_debug_feather_filename(rebalance_days):
    return f'df_debug_{rebalance_days}.feather'

def write_df_debug_to_feather(df, rebalance_days):
    df_temp = df.copy()
    df_temp['base_portfolio_tickers_closed'] = df_temp['base_portfolio_tickers_closed'].apply(list)
    df_temp['base_portfolio_per_ticker_data'] = df_temp['base_portfolio_per_ticker_data'].map(serialize_portfolio)
    df_temp['new_base_portfolio_per_ticker_data'] = df_temp['new_base_portfolio_per_ticker_data'].map(serialize_portfolio)
    df_temp.reset_index().rename(columns={'index': 'date'}).to_feather(df_debug_feather_filename(rebalance_days))


def read_df_debug_from_feather(rebalance_days):
    df = pd.read_feather(df_debug_feather_filename(rebalance_days))
    df['base_portfolio_tickers_closed'] = df['base_portfolio_tickers_closed'].apply(set)
    df['base_portfolio_per_ticker_data'] = df['base_portfolio_per_ticker_data'].map(deserialize_portfolio)
    df['new_base_portfolio_per_ticker_data'] = df['new_base_portfolio_per_ticker_data'].map(deserialize_portfolio)
    return df.set_index('date')

# Result DF Helpers

def df_res_feather_filename(rebalance_days):
    return f'df_res_{rebalance_days}.feather'

def write_df_res_to_feather(df, rebalance_days):
    df_temp = df.copy()
    df_temp.reset_index().rename(columns={'index': 'date'}).to_feather(df_res_feather_filename(rebalance_days))

def read_df_res_from_feather(rebalance_days):
    df = pd.read_feather(df_res_feather_filename(rebalance_days))
    return df.set_index('date')