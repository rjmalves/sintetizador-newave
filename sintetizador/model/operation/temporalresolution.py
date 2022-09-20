from enum import Enum


class TemporalResolution(Enum):
    MES = "MES"
    PATAMAR = "PAT"

    @classmethod
    def factory(cls, val: str) -> "TemporalResolution":
        for v in cls:
            if v.value == val:
                return v
        return cls.MES

    def __repr__(self):
        return self.value
