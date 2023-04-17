from enum import Enum


class Step(Enum):
    FORWARD = "FOR"
    BACKWARD = "BKW"
    FINAL_SIMULATION = "SF"

    @classmethod
    def factory(cls, val: str) -> "Step":
        for v in cls:
            if v.value == val:
                return v
        return cls.FINAL_SIMULATION

    def __repr__(self):
        return self.value
