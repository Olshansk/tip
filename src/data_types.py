import dataclasses
from enum import Enum, auto


@dataclasses.dataclass
class ShareAllocation:
    ticker: str
    num_shares: float


@dataclasses.dataclass
class Stock:
    ticker: str
    price: float


@dataclasses.dataclass
class StockRebalanceInstance:
    ticker: str
    prev_price: float
    curr_price: float


class StockUniverse(Enum):
    SMALL = auto()  # < $1B
    MID = auto()  # $1B - $10B
    LARGE = auto()  # > $100B

    def human_readable(self):
        if self.value == StockUniverse.SMALL.value:
            return 'Small Cap'
        elif self.value == StockUniverse.MID.value:
            return 'Mid Cap'
        elif self.value == StockUniverse.LARGE.value:
            return 'Large Cap'
        else:
            raise Exception(f'Unsupported evaluation metric {self.value}')


class StockBasketWeightApproach(Enum):
    EQUAL_WEIGHTING = auto()
    MARKET_CAP_ADJUSTED = auto()  # Not supported yet


class EvaluationMetric(Enum):
    EV_EBIT = auto()
    P_E = auto()
    P_B = auto()
    DIV_YIELD = auto()

    def __str__(self):
        if self.value == EvaluationMetric.EV_EBIT.value:
            return "EV/EBIT"
        elif self.value == EvaluationMetric.P_E.value:
            return "P/E"
        elif self.value == EvaluationMetric.P_B.value:
            return "P/B"
        elif self.value == EvaluationMetric.DIV_YIELD.value:
            return "% Div Yield"
        else:
            raise Exception(f"Unsupported evaluation metric {self.value}")

    def sorted_column(self):
        if self.value == EvaluationMetric.EV_EBIT.value:
            return "evebit"
        elif self.value == EvaluationMetric.P_E.value:
            return "pe"
        elif self.value == EvaluationMetric.P_B.value:
            return "pb"
        elif self.value == EvaluationMetric.DIV_YIELD.value:
            return "divyield"
        else:
            raise Exception(f"Unsupported evaluation metric {self.value}")

    def file_friendly(self):
        if self.value == EvaluationMetric.EV_EBIT.value:
            return "EV_EBIT"
        elif self.value == EvaluationMetric.P_E.value:
            return "P_E"
        elif self.value == EvaluationMetric.P_B.value:
            return "P_V"
        elif self.value == EvaluationMetric.DIV_YIELD.value:
            return "DIV_YIELD"
        else:
            raise Exception(f"Unsupported evaluation metric {self.value}")
