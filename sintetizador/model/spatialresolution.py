from enum import Enum


class SpatialResolution(Enum):
    SISTEMA_INTERLIGADO = "SIN"
    SUBMERCADO = "SBM"
    RESERVATORIO_EQUIVALENTE = "REE"
    USINA_HIDROELETRICA = "UHE"
    USINA_TERMELETRICA = "UTE"
    USINA_EOLICA = "UEE"

    @classmethod
    def factory(cls, val: str) -> "SpatialResolution":
        for v in cls:
            if v.value == val:
                return v
        return cls.SISTEMA_INTERLIGADO

    def __repr__(self):
        return self.value
