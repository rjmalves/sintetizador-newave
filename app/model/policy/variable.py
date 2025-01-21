from enum import Enum


class Variable(Enum):
    CORTES = "CORTES"

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
        SHORT_NAMES: dict[str, str] = {"CORTES": "CORTES"}
        return SHORT_NAMES.get(self.value)

    @property
    def long_name(self) -> str | None:
        LONG_NAMES: dict[str, str] = {"CORTES": "Cortes de Benders"}
        return LONG_NAMES.get(self.value)
