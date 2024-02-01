from dataclasses import dataclass
from typing import Optional
from sintetizador.model.operation.variable import Variable
from sintetizador.model.operation.spatialresolution import SpatialResolution


@dataclass
class OperationSynthesis:
    variable: Variable
    spatial_resolution: SpatialResolution

    def __repr__(self) -> str:
        return "_".join(
            [
                self.variable.value,
                self.spatial_resolution.value,
            ]
        )

    def __hash__(self) -> int:
        return hash(
            f"{self.variable.value}_" + f"{self.spatial_resolution.value}_"
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, OperationSynthesis):
            return False
        else:
            return all(
                [
                    self.variable == o.variable,
                    self.spatial_resolution == o.spatial_resolution,
                ]
            )

    @classmethod
    def factory(cls, synthesis: str) -> Optional["OperationSynthesis"]:
        data = synthesis.split("_")
        if len(data) != 2:
            return None
        else:
            return cls(
                Variable.factory(data[0]),
                SpatialResolution.factory(data[1]),
            )
