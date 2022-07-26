from dataclasses import dataclass
from typing import Optional
from sintetizador.model.system.variable import Variable


@dataclass
class SystemSynthesis:
    variable: Variable

    def __repr__(self) -> str:
        return self.variable.value

    @classmethod
    def factory(cls, synthesis: str) -> Optional["SystemSynthesis"]:
        return cls(
            Variable.factory(synthesis),
        )
