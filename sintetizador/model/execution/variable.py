from enum import Enum


class Variable(Enum):
    PROGRAMA = "PROGRAMA"
    CONVERGENCIA = "CONVERGENCIA"
    TEMPO_EXECUCAO = "TEMPO"
    COMPOSICAO_CUSTOS = "CUSTOS"
    RECURSOS_JOB = "RECURSOS_JOB"
    RECURSOS_CLUSTER = "RECURSOS_CLUSTER"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.CONVERGENCIA

    def __repr__(self) -> str:
        return self.value
