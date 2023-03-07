from enum import Enum


class Variable(Enum):
    CORTES = "CORTES"
    ESTADOS = "ESTADOS"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.CORTES

    def __repr__(self) -> str:
        return self.value
