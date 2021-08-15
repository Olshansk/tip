from src.backtest import *
from src.data_types import *


def plot_backtest(back_test_result: BackTestResult) -> None:
    df_to_plot = back_test_result.df[["base_portfolio_value", "test_portfolio_value"]]
    df_to_plot.plot(
        title=(
            f"{str(back_test_result.base_metric)} (base)"
            f" VS {str(back_test_result.test_metric)} (test)\n"
            f" Rebalance freq: {back_test_result.rebalance_days} days\n"
            f" Portfolio size: {back_test_result.portfolio_size} stocks\n"
            f" Universe of stocks: {str(back_test_result.stocks_universe)}"
        )
    )
