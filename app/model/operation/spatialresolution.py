from enum import Enum
from typing import Dict, List, Optional
from app.internal.constants import (
    HYDRO_CODE_COL,
    THERMAL_CODE_COL,
    EER_CODE_COL,
    EEP_COL,
    SUBMARKET_CODE_COL,
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_TARGET_CODE_COL,
    START_DATE_COL,
    END_DATE_COL,
    BLOCK_DURATION_COL,
    VALUE_COL,
    OPERATION_SYNTHESIS_COMMON_COLUMNS as COLUMNS,
    LOWER_BOUND_COL,
    UPPER_BOUND_COL,
)


class SpatialResolution(Enum):
    SISTEMA_INTERLIGADO = "SIN"
    SUBMERCADO = "SBM"
    RESERVATORIO_EQUIVALENTE = "REE"
    USINA_HIDROELETRICA = "UHE"
    USINA_TERMELETRICA = "UTE"
    PARQUE_EOLICO_EQUIVALENTE = "PEE"
    PAR_SUBMERCADOS = "SBP"

    @classmethod
    def factory(cls, val: str) -> "SpatialResolution":
        for v in cls:
            if v.value == val:
                return v
        return cls.SISTEMA_INTERLIGADO

    def __repr__(self):
        return self.value

    @property
    def long_name(self):
        LONG_NAMES: Dict[str, str] = {
            "SIN": "Sistema Interligado",
            "SBM": "Submercado",
            "REE": "Reservatório Equivalente",
            "UHE": "Usina Hidroelétrica",
            "UTE": "Usina Termelétrica",
            "PEE": "Parque Eólico Equivalente",
            "SBP": "Par de Submercados",
        }
        return LONG_NAMES.get(self.value)

    @property
    def entity_df_columns(self) -> List[str]:
        col_maps: Dict[SpatialResolution, List[str]] = {
            SpatialResolution.SISTEMA_INTERLIGADO: [],
            SpatialResolution.SUBMERCADO: [
                SUBMARKET_CODE_COL,
            ],
            SpatialResolution.RESERVATORIO_EQUIVALENTE: [
                EER_CODE_COL,
                SUBMARKET_CODE_COL,
            ],
            SpatialResolution.PARQUE_EOLICO_EQUIVALENTE: [
                EEP_COL,
                SUBMARKET_CODE_COL,
            ],
            SpatialResolution.USINA_HIDROELETRICA: [
                HYDRO_CODE_COL,
                EER_CODE_COL,
                SUBMARKET_CODE_COL,
            ],
            SpatialResolution.USINA_TERMELETRICA: [
                THERMAL_CODE_COL,
                SUBMARKET_CODE_COL,
            ],
            SpatialResolution.PAR_SUBMERCADOS: [
                EXCHANGE_SOURCE_CODE_COL,
                EXCHANGE_TARGET_CODE_COL,
            ],
        }
        return col_maps.get(self, [])

    @property
    def main_entity_synthesis_df_column(self) -> Optional[str]:
        col_maps: Dict[SpatialResolution, Optional[str]] = {
            SpatialResolution.SISTEMA_INTERLIGADO: None,
            SpatialResolution.SUBMERCADO: SUBMARKET_CODE_COL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE: EER_CODE_COL,
            SpatialResolution.PARQUE_EOLICO_EQUIVALENTE: EEP_COL,
            SpatialResolution.USINA_HIDROELETRICA: HYDRO_CODE_COL,
            SpatialResolution.USINA_TERMELETRICA: THERMAL_CODE_COL,
            SpatialResolution.PAR_SUBMERCADOS: EXCHANGE_SOURCE_CODE_COL,
        }
        return col_maps.get(self)

    @property
    def all_synthesis_df_columns(self) -> List[str]:
        return (
            self.entity_df_columns
            + COLUMNS
            + [LOWER_BOUND_COL, UPPER_BOUND_COL]
        )

    @property
    def entity_synthesis_df_columns(self) -> List[str]:
        all_columns = self.all_synthesis_df_columns
        return [
            c for c in all_columns if c not in [BLOCK_DURATION_COL, VALUE_COL]
        ]

    @property
    def sorting_synthesis_df_columns(self) -> List[str]:
        main_column = self.main_entity_synthesis_df_column
        all_columns = [main_column] + COLUMNS if main_column else COLUMNS
        return [
            c
            for c in all_columns
            if c
            not in [
                START_DATE_COL,
                END_DATE_COL,
                BLOCK_DURATION_COL,
                VALUE_COL,
            ]
        ]

    @property
    def non_entity_sorting_synthesis_df_columns(self) -> List[str]:
        sorting_columns = self.sorting_synthesis_df_columns
        return [
            c
            for c in sorting_columns
            if c not in self.entity_synthesis_df_columns
        ]
