from enum import Enum


class Variable(Enum):
    CONVERGENCIA = "CONVERGENCIA"
    TEMPO_EXECUCAO = "TEMPO"
    COMPOSICAO_CUSTOS = "COMPOSICAO_CUSTOS"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.CONVERGENCIA

    def __repr__(self) -> str:
        return self.value
