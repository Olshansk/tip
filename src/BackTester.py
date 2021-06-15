# Compute Results for Base + Test


@dataclasses.dataclass
class BackTestResult:
    df: pd.DataFrame
    base_metric: EvaluationMetric
    test_metric: EvaluationMetric
    rebalance_days: int
    portfolio_size: int
    stocks_universe: StockUniverse


class BackTester:
    def __init__(
        self,
        base_metric: EvaluationMetric,
        test_metric: EvaluationMetric,
        daily_data: pd.DataFrame,
        date_sorted_daily_data: Optional[pd.DataFrame] = None,
        base_sorted_daily_data: Optional[pd.DataFrame] = None,
        test_sorted_daily_data: Optional[pd.DataFrame] = None,
    ):
        self.base_metric = base_metric
        self.test_metric = test_metric
        self.daily_data = daily_data

        self.start_date = datetime.datetime.strptime(
            min(daily_data["date"]), "%Y-%m-%d"
        )
        self.end_date = datetime.datetime.strptime(max(daily_data["date"]), "%Y-%m-%d")

        self.date_sorted_daily_data = (
            date_sorted_daily_data
            if date_sorted_daily_data is not None
            else daily_data.sort_values(by="date")
        )
        self.base_sorted_daily_data = (
            base_sorted_daily_data
            if base_sorted_daily_data is not None
            else sort_df_by_metric(daily_data, base_metric)
        )
        self.test_sorted_daily_data = (
            test_sorted_daily_data
            if test_sorted_daily_data is not None
            else sort_df_by_metric(daily_data, test_metric)
        )

    @staticmethod
    def plot_backtest(back_test_result: BackTestResult):
        df_to_plot = back_test_result.df[
            ["base_portfolio_value", "test_portfolio_value"]
        ]
        df_to_plot.plot(
            title=(
                f"{str(back_test_result.base_metric)} (base)"
                f" VS {str(back_test_result.test_metric)} (test)\n"
                f" Rebalance freq: {back_test_result.rebalance_days} days\n"
                f" Portfolio size: {back_test_result.portfolio_size} stocks\n"
                f" Universe of stocks: {str(back_test_result.stocks_universe)}"
            )
        )

    def run_backtest(
        self,
        rebalance_days: int,
        portfolio_size: int,
        initial_portfolio_value: int,
        stocks_universe: StockUniverse,
    ) -> pd.DataFrame:
        rebalance_dates = get_rebalance_dates(
            self.start_date, self.end_date, timedelta(days=rebalance_days)
        )

        base_portfolio_value = initial_portfolio_value
        test_portfolio_value = initial_portfolio_value
        portfolio_size = portfolio_size

        start_date = rebalance_dates[0]

        base_sorted_df = filter_df_by_date(self.base_sorted_daily_data, start_date)
        test_sorted_df = filter_df_by_date(self.test_sorted_daily_data, start_date)

        base_sorted_df = filter_stocks_by_universe(base_sorted_df, stocks_universe)
        test_sorted_df = filter_stocks_by_universe(test_sorted_df, stocks_universe)

        base_portfolio = get_top_n_stocks_by_metric(
            base_sorted_df, portfolio_size, self.base_metric
        )
        test_portfolio = get_top_n_stocks_by_metric(
            test_sorted_df, portfolio_size, self.test_metric
        )

        base_price = get_stock_basket_price(
            base_sorted_df, self.date_sorted_daily_data, base_portfolio
        )
        test_price = get_stock_basket_price(
            test_sorted_df, self.date_sorted_daily_data, test_portfolio
        )

        res = {}
        res[start_date] = {
            "base_basket_price": base_price,
            "base_portfolio_value": base_portfolio_value,
            "test_basket_price": test_price,
            "test_portfolio_value": test_portfolio_value,
        }

        for date in rebalance_dates[0:4]:
            print(date.strftime("%Y-%m-%d"))

            print(base_portfolio)

            prev_base_price = base_price
            prev_test_price = test_price

            base_sorted_df = filter_df_by_date(self.base_sorted_daily_data, date)
            test_sorted_df = filter_df_by_date(self.test_sorted_daily_data, date)

            base_price = get_stock_basket_price(
                base_sorted_df, self.date_sorted_daily_data, base_portfolio
            )
            test_price = get_stock_basket_price(
                test_sorted_df, self.date_sorted_daily_data, test_portfolio
            )

            base_change = base_price / prev_base_price
            test_change = test_price / prev_test_price

            base_portfolio_value = round(base_portfolio_value * base_change, 2)
            test_portfolio_value = round(test_portfolio_value * test_change, 2)

            res[date] = {
                "base_basket_price": base_price,
                "base_portfolio_value": base_portfolio_value,
                "test_basket_price": test_price,
                "test_portfolio_value": test_portfolio_value,
            }

            base_sorted_df = filter_stocks_by_universe(base_sorted_df, stocks_universe)
            test_sorted_df = filter_stocks_by_universe(test_sorted_df, stocks_universe)

            base_portfolio = get_top_n_stocks_by_metric(
                base_sorted_df, portfolio_size, self.base_metric
            )
            test_portfolio = get_top_n_stocks_by_metric(
                test_sorted_df, portfolio_size, self.test_metric
            )

            base_price = get_stock_basket_price(
                base_sorted_df, self.date_sorted_daily_data, base_portfolio
            )
            test_price = get_stock_basket_price(
                test_sorted_df, self.date_sorted_daily_data, test_portfolio
            )

        df_res = pd.DataFrame.from_dict(res, orient="index")
        return BackTestResult(
            df_res,
            self.base_metric,
            self.test_metric,
            rebalance_days,
            portfolio_size,
            stocks_universe,
        )
