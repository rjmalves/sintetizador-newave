from enum import Enum
from typing import Dict, List
from app.internal.constants import (
    ITERATION_COL,
    SCENARIO_COL,
    SPAN_COL,
)


class Step(Enum):
    FORWARD = "FOR"
    BACKWARD = "BKW"
    FINAL_SIMULATION = "SF"

    @classmethod
    def factory(cls, val: str) -> "Step":
        for v in cls:
            if v.value == val:
                return v
        return cls.FINAL_SIMULATION

    def __repr__(self):
        return self.value

    @property
    def short_name(self):
        SHORT_NAMES: Dict[str, str] = {
            "FOR": "FOR",
            "BKW": "BKW",
            "SF": "SF",
        }
        return SHORT_NAMES.get(self.value)

    @property
    def long_name(self):
        LONG_NAMES: Dict[str, str] = {
            "FOR": "Forward",
            "BKW": "Backward",
            "SF": "Simulação Final",
        }
        return LONG_NAMES.get(self.value)

    @property
    def entity_df_columns(self) -> List[str]:
        col_maps: Dict[Step, List[str]] = {
            Step.FORWARD: [ITERATION_COL],
            Step.BACKWARD: [ITERATION_COL, SPAN_COL],
            Step.FINAL_SIMULATION: [],
        }
        return col_maps.get(self, [])
