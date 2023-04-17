from enum import Enum


class Variable(Enum):
    ENA_ABSOLUTA = "ENAA"
    VAZAO_INCREMENTAL_ABSOLUTA = "QINC"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.ENA_ABSOLUTA

    def __repr__(self) -> str:
        return self.value
