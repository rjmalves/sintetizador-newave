from dataclasses import dataclass
from typing import Optional
from sintetizador.model.scenario.variable import Variable
from sintetizador.model.scenario.spatialresolution import SpatialResolution
from sintetizador.model.scenario.step import Step


@dataclass
class ScenarioSynthesis:
    variable: Variable
    spatial_resolution: SpatialResolution
    step: Step

    def __repr__(self) -> str:
        return "_".join(
            [
                self.variable.value,
                self.spatial_resolution.value,
                self.step.value,
            ]
        )

    def __hash__(self) -> int:
        return hash(
            f"{self.variable.value}_"
            + f"{self.spatial_resolution.value}_"
            + f"{self.step.value}"
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, ScenarioSynthesis):
            return False
        else:
            return all(
                [
                    self.variable == o.variable,
                    self.spatial_resolution == o.spatial_resolution,
                    self.step == o.step,
                ]
            )

    @classmethod
    def factory(cls, synthesis: str) -> Optional["ScenarioSynthesis"]:
        data = synthesis.split("_")
        if len(data) != 3:
            return None
        else:
            return cls(
                Variable.factory(data[0]),
                SpatialResolution.factory(data[1]),
                Step.factory(data[2]),
            )
