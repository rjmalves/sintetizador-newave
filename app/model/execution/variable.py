from enum import Enum
from typing import Dict


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

    @property
    def short_name(self) -> str | None:
        SHORT_NAMES: Dict[str, str] = {
            "PROGRAMA": "PROGRAMA",
            "CONVERGENCIA": "CONVERGENCIA",
            "TEMPO": "TEMPO",
            "CUSTOS": "CUSTOS",
            "RECURSOS_JOB": "RECURSOS_JOB",
            "RECURSOS_CLUSTER": "RECURSOS_CLUSTER",
        }
        return SHORT_NAMES.get(self.value)

    @property
    def long_name(self) -> str | None:
        LONG_NAMES: Dict[str, str] = {
            "PROGRAMA": "Modelo de Otimização",
            "CONVERGENCIA": "Convergência do Processo Iterativo",
            "TEMPO": "Tempo de Execução",
            "CUSTOS": "Composição de Custos da Solução",
            "RECURSOS_JOB": "Recursos Computacionais do Job",
            "RECURSOS_CLUSTER": "Recursos Computacionais do Cluster",
        }
        return LONG_NAMES.get(self.value)
