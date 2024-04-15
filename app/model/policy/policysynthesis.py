from dataclasses import dataclass
from typing import Optional, List
from app.model.policy.variable import Variable


@dataclass
class PolicySynthesis:
    variable: Variable

    def __repr__(self) -> str:
        return self.variable.value

    @classmethod
    def factory(cls, synthesis: str) -> Optional["PolicySynthesis"]:
        return cls(
            Variable.factory(synthesis),
        )


SUPPORTED_SYNTHESIS: List[str] = [
    "CORTES",
    "ESTADOS",
]
