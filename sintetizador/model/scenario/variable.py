from enum import Enum


class Variable(Enum):
    ENA_ABSOLUTA = "ENAA"
    ENA_MLT = "ENAM"
    VAZAO_INCREMENTAL_ABSOLUTA = "QINC"
    VAZAO_INCREMENTAL_MLT = "QINCM"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.ENA_ABSOLUTA

    def __repr__(self) -> str:
        return self.value
