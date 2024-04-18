from enum import Enum
from typing import Dict, List
from app.internal.constants import (
    HYDRO_CODE_COL,
    EER_CODE_COL,
    SUBMARKET_CODE_COL,
)


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

    @property
    def entity_df_columns(self) -> List[str]:
        col_maps: Dict[SpatialResolution, List[str]] = {
            SpatialResolution.SISTEMA_INTERLIGADO: [],
            SpatialResolution.SUBMERCADO: [SUBMARKET_CODE_COL],
            SpatialResolution.RESERVATORIO_EQUIVALENTE: [
                EER_CODE_COL,
                SUBMARKET_CODE_COL,
            ],
            SpatialResolution.USINA_HIDROELETRICA: [
                HYDRO_CODE_COL,
                EER_CODE_COL,
                SUBMARKET_CODE_COL,
            ],
        }
        return col_maps.get(self, [])
