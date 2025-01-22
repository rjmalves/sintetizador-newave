from enum import Enum


class Variable(Enum):
    CORTES_COEFICIENTES = "CORTES_COEFICIENTES"
    CORTES_VARIAVEIS = "CORTES_VARIAVEIS"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.CORTES_COEFICIENTES

    def __repr__(self) -> str:
        return self.value

    @property
    def short_name(self) -> str | None:
        SHORT_NAMES: dict[str, str] = {
            "CORTES_COEFICIENTES": "CORTES_COEFICIENTES",
            "CORTES_VARIAVEIS": "CORTES_VARIAVEIS",
        }
        return SHORT_NAMES.get(self.value)

    @property
    def long_name(self) -> str | None:
        LONG_NAMES: dict[str, str] = {
            "CORTES_COEFICIENTES": "Coeficientes dos cortes de Benders",
            "CORTES_VARIAVEIS": "Descrição das variáveis dos cortes de Benders",
        }
        return LONG_NAMES.get(self.value)
