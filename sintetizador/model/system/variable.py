from enum import Enum
from typing import Dict


class Variable(Enum):
    EST = "EST"
    PAT = "PAT"
    SBM = "SBM"
    REE = "REE"
    UTE = "UTE"
    UHE = "UHE"
    PEE = "PEE"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.SBM

    def __repr__(self) -> str:
        return self.value

    @property
    def short_name(self):
        SHORT_NAMES: Dict[str, str] = {
            "EST": "EST",
            "PAT": "PAT",
            "SBM": "SBM",
            "REE": "REE",
            "UTE": "UTE",
            "UHE": "UHE",
            "PEE": "PEE",
        }
        return SHORT_NAMES.get(self.value)

    @property
    def long_name(self):
        LONG_NAMES: Dict[str, str] = {
            "EST": "Estágios",
            "PAT": "Patamares",
            "SBM": "Submercados",
            "REE": "Reservatórios Equivalentes",
            "UTE": "Usinas Termelétricas",
            "UHE": "Usinas Hidroelétricas",
            "PEE": "Parques Eólicos Equivalentes",
        }
        return LONG_NAMES.get(self.value)
