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

    @property
    def short_name(self) -> str | None:
        SHORT_NAMES: dict[str, str] = {
            "CORTES": "CORTES",
            "ESTADOS": "ESTADOS",
        }
        return SHORT_NAMES.get(self.value)

    @property
    def long_name(self) -> str | None:
        LONG_NAMES: dict[str, str] = {
            "CORTES": "Cortes de Benders",
            "ESTADOS": "Estados Visitados na Construção dos Cortes",
        }
        return LONG_NAMES.get(self.value)
