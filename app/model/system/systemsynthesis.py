from dataclasses import dataclass
from typing import Optional
from app.model.system.variable import Variable


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


SUPPORTED_SYNTHESIS: list[str] = [
    "EST",
    "PAT",
    "SBM",
    "REE",
    "UTE",
    "UHE",
]
