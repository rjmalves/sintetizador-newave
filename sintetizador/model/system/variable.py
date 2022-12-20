from enum import Enum


class Variable(Enum):
    EST = "EST"
    PAT = "PAT"
    SBM = "SBM"
    REE = "REE"
    UTE = "UTE"
    UHE = "UHE"
    PEE = "PEE"

    @classmethod
    def factory(cls, val: str) -> "Variable":
        for v in cls:
            if v.value == val:
                return v
        return cls.SBM

    def __repr__(self) -> str:
        return self.value
