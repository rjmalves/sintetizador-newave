from enum import Enum
from typing import Dict


class SpatialResolution(Enum):
    SISTEMA_INTERLIGADO = "SIN"
    SUBMERCADO = "SBM"
    RESERVATORIO_EQUIVALENTE = "REE"
    USINA_HIDROELETRICA = "UHE"

    @classmethod
    def factory(cls, val: str) -> "SpatialResolution":
        for v in cls:
            if v.value == val:
                return v
        return cls.SISTEMA_INTERLIGADO

    def __repr__(self):
        return self.value

    @property
    def short_name(self):
        SHORT_NAMES: Dict[str, str] = {
            "SIN": "SIN",
            "SBM": "SBM",
            "REE": "REE",
            "UHE": "UHE",
        }
        return SHORT_NAMES.get(self.value)

    @property
    def long_name(self):
        LONG_NAMES: Dict[str, str] = {
            "SIN": "Sistema Interligado",
            "SBM": "Submercado",
            "REE": "Reservatório Equivalente",
            "UHE": "Usina Hidroelétrica",
        }
        return LONG_NAMES.get(self.value)
