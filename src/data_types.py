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

class StockUniverse(Enum):
    SMALL = auto()  # < $1B
    MID = auto()  # $1B - $10B
    LARGE = auto()  # > $100B

class StockBasketWeightApproach(Enum):
    EQUAL_WEIGHTING = auto()
    MARKET_CAP_ADJUSTED = auto() # Not supported yet

class EvaluationMetric(Enum):
    EV_EBIT = auto()
    P_E = auto()
    P_B = auto()
    DIV_YIELD = auto()

    def __str__(self):
        if self.value == EvaluationMetric.EV_EBIT.value:
            return 'EV/EBIT'
        elif self.value == EvaluationMetric.P_E.value:
            return 'P/E'
        elif self.value == EvaluationMetric.P_B.value:
            return 'P/B'
        elif self.value == EvaluationMetric.DIV_YIELD.value:
            return '% Div Yield'
        else:
            raise Exception(f'Unsupported evaluation metric {metric}')