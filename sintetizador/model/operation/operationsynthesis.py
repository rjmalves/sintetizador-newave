from dataclasses import dataclass
from typing import Optional
from sintetizador.model.operation.variable import Variable
from sintetizador.model.operation.spatialresolution import SpatialResolution
from sintetizador.model.operation.temporalresolution import TemporalResolution


@dataclass
class OperationSynthesis:
    variable: Variable
    spatial_resolution: SpatialResolution
    temporal_resolution: TemporalResolution

    def __repr__(self) -> str:
        return "_".join(
            [
                self.variable.value,
                self.spatial_resolution.value,
                self.temporal_resolution.value,
            ]
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
