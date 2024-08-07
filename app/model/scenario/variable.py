from enum import Enum


class Variable(Enum):
    ENA_ABSOLUTA = "ENAA"
    VAZAO_INCREMENTAL = "QINC"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.ENA_ABSOLUTA

    def __repr__(self) -> str:
        return self.value

    @property
    def short_name(self) -> str | None:
        SHORT_NAMES: dict[str, str] = {
            "ENAA": "ENA",
            "QINC": "QINC",
        }
        return SHORT_NAMES.get(self.value)

    @property
    def long_name(self) -> str | None:
        LONG_NAMES: dict[str, str] = {
            "ENAA": "Energia Natural Afluente",
            "QINC": "Vazão Natural Incremental",
        }
        return LONG_NAMES.get(self.value)
