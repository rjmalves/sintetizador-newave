from dataclasses import dataclass
from typing import Optional
from app.model.scenario.variable import Variable
from app.model.scenario.spatialresolution import SpatialResolution
from app.model.scenario.step import Step
from app.model.scenario.unit import Unit
from app.internal.constants import (
    SCENARIO_SYNTHESIS_COMMON_COLUMNS as COLUMNS,
    VALUE_COL,
    LTA_VALUE_COL,
    START_DATE_COL,
    END_DATE_COL,
)


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

    @property
    def entity_df_columns(self) -> list[str]:
        return self.spatial_resolution.entity_df_columns + self.step.entity_df_columns

    @property
    def all_synthesis_df_columns(self) -> list[str]:
        return self.entity_df_columns + COLUMNS

    @property
    def entity_synthesis_df_columns(self) -> list[str]:
        all_columns = self.all_synthesis_df_columns
        return [c for c in all_columns if c not in [LTA_VALUE_COL, VALUE_COL]]

    @property
    def sorting_synthesis_df_columns(self) -> list[str]:
        all_columns = self.all_synthesis_df_columns
        return [
            c
            for c in all_columns
            if c not in [START_DATE_COL, END_DATE_COL, VALUE_COL, LTA_VALUE_COL]
        ]

    @property
    def non_entity_sorting_synthesis_df_columns(self) -> list[str]:
        sorting_columns = self.sorting_synthesis_df_columns
        return [c for c in sorting_columns if c not in self.entity_synthesis_df_columns]


SUPPORTED_SYNTHESIS: list[str] = [
    "ENAA_REE_FOR",
    "ENAA_REE_BKW",
    "ENAA_REE_SF",
    "ENAA_SBM_FOR",
    "ENAA_SBM_BKW",
    "ENAA_SBM_SF",
    "ENAA_SIN_FOR",
    "ENAA_SIN_BKW",
    "ENAA_SIN_SF",
    "QINC_UHE_FOR",
    "QINC_UHE_BKW",
    "QINC_UHE_SF",
    "QINC_REE_FOR",
    "QINC_REE_BKW",
    "QINC_REE_SF",
    "QINC_SBM_FOR",
    "QINC_SBM_BKW",
    "QINC_SBM_SF",
    "QINC_SIN_FOR",
    "QINC_SIN_BKW",
    "QINC_SIN_SF",
]

UNITS: dict[ScenarioSynthesis, Unit] = {
    ScenarioSynthesis(
        Variable.ENA_ABSOLUTA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
        Step.FORWARD,
    ): Unit.MWmes,
    ScenarioSynthesis(
        Variable.ENA_ABSOLUTA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
        Step.BACKWARD,
    ): Unit.MWmes,
    ScenarioSynthesis(
        Variable.ENA_ABSOLUTA,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
        Step.FINAL_SIMULATION,
    ): Unit.MWmes,
    ScenarioSynthesis(
        Variable.ENA_ABSOLUTA,
        SpatialResolution.SUBMERCADO,
        Step.FORWARD,
    ): Unit.MWmes,
    ScenarioSynthesis(
        Variable.ENA_ABSOLUTA,
        SpatialResolution.SUBMERCADO,
        Step.BACKWARD,
    ): Unit.MWmes,
    ScenarioSynthesis(
        Variable.ENA_ABSOLUTA,
        SpatialResolution.SUBMERCADO,
        Step.FINAL_SIMULATION,
    ): Unit.MWmes,
    ScenarioSynthesis(
        Variable.ENA_ABSOLUTA,
        SpatialResolution.SISTEMA_INTERLIGADO,
        Step.FORWARD,
    ): Unit.MWmes,
    ScenarioSynthesis(
        Variable.ENA_ABSOLUTA,
        SpatialResolution.SISTEMA_INTERLIGADO,
        Step.BACKWARD,
    ): Unit.MWmes,
    ScenarioSynthesis(
        Variable.ENA_ABSOLUTA,
        SpatialResolution.SISTEMA_INTERLIGADO,
        Step.FINAL_SIMULATION,
    ): Unit.MWmes,
    ScenarioSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.USINA_HIDROELETRICA,
        Step.FORWARD,
    ): Unit.m3s,
    ScenarioSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.USINA_HIDROELETRICA,
        Step.BACKWARD,
    ): Unit.m3s,
    ScenarioSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.USINA_HIDROELETRICA,
        Step.FINAL_SIMULATION,
    ): Unit.m3s,
    ScenarioSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
        Step.FORWARD,
    ): Unit.m3s,
    ScenarioSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
        Step.BACKWARD,
    ): Unit.m3s,
    ScenarioSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.RESERVATORIO_EQUIVALENTE,
        Step.FINAL_SIMULATION,
    ): Unit.m3s,
    ScenarioSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.SUBMERCADO,
        Step.FORWARD,
    ): Unit.m3s,
    ScenarioSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.SUBMERCADO,
        Step.BACKWARD,
    ): Unit.m3s,
    ScenarioSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.SUBMERCADO,
        Step.FINAL_SIMULATION,
    ): Unit.m3s,
    ScenarioSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
        Step.FORWARD,
    ): Unit.m3s,
    ScenarioSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
        Step.BACKWARD,
    ): Unit.m3s,
    ScenarioSynthesis(
        Variable.VAZAO_INCREMENTAL,
        SpatialResolution.SISTEMA_INTERLIGADO,
        Step.FINAL_SIMULATION,
    ): Unit.m3s,
}
