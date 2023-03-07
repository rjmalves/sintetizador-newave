from dataclasses import dataclass
from typing import Optional
from sintetizador.model.policy.variable import Variable


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
