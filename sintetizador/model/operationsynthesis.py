from dataclasses import dataclass
from typing import Optional
from sintetizador.model.variable import Variable
from sintetizador.model.spatialresolution import SpatialResolution
from sintetizador.model.temporalresolution import TemporalResolution


@dataclass
class OperationSynthesis:
    variable: Variable
    spatial_resolution: SpatialResolution
    temporal_resolution: TemporalResolution

    def __repr__(self) -> str:
        return "_".join(
            [self.variable, self.spatial_resolution, self.temporal_resolution]
        )

    @classmethod
    def factory(cls, synthesis: str) -> Optional["OperationSynthesis"]:
        data = synthesis.split("_")
        if len(data) != 3:
            return None
        else:
            return cls(
                Variable.factory(data[0]),
                SpatialResolution.factory(data[1]),
                TemporalResolution.factory(data[2]),
            )
