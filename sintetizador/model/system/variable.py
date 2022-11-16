from enum import Enum


class Variable(Enum):
    SUBMERCADOS = "SUBMERCADOS"
    REES = "REES"
    UTES = "UTES"
    UHES = "UHES"
    UEES = "UEES"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.SUBMERCADOS

    def __repr__(self) -> str:
        return self.value
