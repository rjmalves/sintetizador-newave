from dataclasses import dataclass
from typing import Optional, List
from app.model.execution.variable import Variable


@dataclass
class ExecutionSynthesis:
    variable: Variable

    def __repr__(self) -> str:
        return self.variable.value

    @classmethod
    def factory(cls, synthesis: str) -> Optional["ExecutionSynthesis"]:
        return cls(
            Variable.factory(synthesis),
        )


SUPPORTED_SYNTHESIS: List[str] = [
    "PROGRAMA",
    "CONVERGENCIA",
    "TEMPO",
    "CUSTOS",
]
