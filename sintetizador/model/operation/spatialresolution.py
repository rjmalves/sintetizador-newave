from enum import Enum
from typing import Dict


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
