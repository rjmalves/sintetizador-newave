import logging
import multiprocessing as _mp
import platform as _platform
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from logging import ERROR, INFO
from traceback import print_exc
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import polars as pl
from dateutil.relativedelta import relativedelta

from app.internal.constants import (
    CONFIG_COL,
    DATE_COL,
    EER_CODE_COL,
    END_DATE_COL,
    HYDRO_CODE_COL,
    ITERATION_COL,
    LOWER_BOUND_COL,
    LTA_COL,
    LTA_VALUE_COL,
    MONTH_COL,
    NULL_INFLOW_STATION,
    SCENARIO_COL,
    SCENARIO_SYNTHESIS_METADATA_OUTPUT,
    SCENARIO_SYNTHESIS_STATS_ROOT,
    SCENARIO_SYNTHESIS_SUBDIR,
    SPAN_COL,
    STAGE_COL,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
    VARIABLE_COL,
)
from app.model.scenario.scenariosynthesis import (
    SUPPORTED_SYNTHESIS,
    UNITS,
    ScenarioSynthesis,
)
from app.model.scenario.spatialresolution import SpatialResolution
from app.model.scenario.step import Step
from app.model.scenario.variable import Variable
from app.model.settings import Settings
from app.services.deck.deck import Deck
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.log import Log
from app.utils.operations import calc_statistics
from app.utils.regex import match_variables_with_wildcards
from app.utils.timing import time_and_log


class ScenarioSynthetizer:
    DEFAULT_OPERATION_SYNTHESIS_ARGS: List[str] = SUPPORTED_SYNTHESIS

    COMMON_COLUMNS: List[str] = [
        ITERATION_COL,
        STAGE_COL,
        START_DATE_COL,
        END_DATE_COL,
        SCENARIO_COL,
        SPAN_COL,
    ]

    CACHED_SYNTHESIS: Dict[Tuple[Variable, Step], pl.DataFrame] = {}

    CACHED_MLT_VALUES: Dict[
        Tuple[Variable, SpatialResolution], pl.DataFrame
    ] = {}

    logger: Optional[logging.Logger] = None

    SYNTHESIS_STATS: Dict[
        Tuple[SpatialResolution, Step], List[pl.DataFrame]
    ] = {}

    @classmethod
    def clear_cache(cls) -> None:
        cls.CACHED_SYNTHESIS.clear()
        cls.CACHED_MLT_VALUES.clear()
        cls.SYNTHESIS_STATS.clear()

    @classmethod
    def _log(cls, msg: str, level: int = INFO) -> None:
        if cls.logger is not None:
            cls.logger.log(level, msg)

    @classmethod
    def _default_args(cls) -> List[ScenarioSynthesis]:
        args = [
            ScenarioSynthesis.factory(a)
            for a in cls.DEFAULT_OPERATION_SYNTHESIS_ARGS
        ]
        return [arg for arg in args if arg is not None]

    @classmethod
    def _match_wildcards(cls, variables: List[str]) -> List[str]:
        return match_variables_with_wildcards(
            variables, cls.DEFAULT_OPERATION_SYNTHESIS_ARGS
        )

    @classmethod
    def _process_variable_arguments(
        cls,
        args: List[str],
    ) -> List[ScenarioSynthesis]:
        args_data = [ScenarioSynthesis.factory(c) for c in args]
        return [arg for arg in args_data if arg is not None]

    @classmethod
    def filter_valid_variables(
        cls, variables: List[ScenarioSynthesis], uow: AbstractUnitOfWork
    ) -> List[ScenarioSynthesis]:
        valid_variables: List[ScenarioSynthesis] = []
        has_hydro = Deck.final_simulation_aggregation(
            uow
        ) or Deck.hybrid_policy(uow)
        for v in variables:
            if v.variable == Variable.VAZAO_INCREMENTAL and not has_hydro:
                continue
            valid_variables.append(v)
        cls._log(f"Sinteses: {valid_variables}")
        return valid_variables

    @classmethod
    def _generate_hydro_incremental_inflow_dataframe(
        cls, hydro_code: int, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        hydros = Deck.hydros(uow)
        vazoes = Deck.vazoes(uow)
        inflow_station = hydros.filter(pl.col(HYDRO_CODE_COL) == hydro_code)[
            "posto"
        ].item(0)
        natural_inflow = vazoes[str(inflow_station)].to_numpy()
        null_station = inflow_station == NULL_INFLOW_STATION
        if not null_station:
            upstream_hydro_codes = [
                u
                for u in hydros.filter(
                    pl.col("codigo_usina_jusante") == hydro_code
                )[HYDRO_CODE_COL].to_list()
                if u != 0
            ]
            upstream_inflow_stations = list(
                set(
                    [
                        hydros.filter(pl.col(HYDRO_CODE_COL) == uhe_montante)[
                            "posto"
                        ].item(0)
                        for uhe_montante in upstream_hydro_codes
                    ]
                )
            )
            for upstream_station in upstream_inflow_stations:
                natural_inflow = (
                    natural_inflow - vazoes[str(upstream_station)].to_numpy()
                )
        history_starting_year = int(
            hydros.filter(pl.col(HYDRO_CODE_COL) == hydro_code)[
                "ano_inicio_historico"
            ].item(0)
        )
        history_ending_year = int(
            hydros.filter(pl.col(HYDRO_CODE_COL) == hydro_code)[
                "ano_fim_historico"
            ].item(0)
        )
        dates = pd.date_range(
            datetime(year=history_starting_year, month=1, day=1),
            datetime(year=history_ending_year, month=12, day=1),
            freq="MS",
        )
        return pl.DataFrame(
            data={
                DATE_COL: dates.to_pydatetime().tolist(),
                VALUE_COL: natural_inflow[: len(dates)],
            }
        )

    @classmethod
    def _eval_monthly_lta(cls, history: pl.DataFrame) -> pl.DataFrame:
        """Extract monthly LTA (mean flow) by month."""
        return (
            history.with_columns(pl.col(DATE_COL).dt.month().alias(MONTH_COL))
            .group_by(MONTH_COL)
            .agg(pl.col(VALUE_COL).mean())
            .sort(MONTH_COL)
        )

    @classmethod
    def _model_dataframe_for_hydro_lta(
        cls, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """Build hydro LTA model dataframe with stage-month mapping, adjusted for study period start."""

        months_column = cls._generate_model_dataframe_month_column(uow)
        stages_column = cls._generate_model_dataframe_stage_column(
            uow, len(months_column)
        )

        lta_df = pl.DataFrame(
            data={
                STAGE_COL: stages_column,
                MONTH_COL: months_column,
            }
        )
        lta_df = cls._resolve_starting_stage(lta_df, uow)
        return lta_df

    @classmethod
    def _generate_lta_hydro_inflow_series(
        cls, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Extrai a MLT para todas as UHEs.

        - codigo_usina (`int`)
        - usina (`str`)
        - codigo_ree (`int`)
        - ree (`str`)
        - codigo_submercado (`int`)
        - submercado (`str`)
        - estagio (`int`)
        - mes (`int`)
        - mlt (`float`)

        :return: A tabela como um DataFrame
        :rtype: pl.DataFrame
        """

        def _calc_hydro_lta_df(
            hydro_code: int,
            lta_model_df: pl.DataFrame,
            eer_code: int,
            submarket_code: int,
        ) -> pl.DataFrame:
            inflow = cls._generate_hydro_incremental_inflow_dataframe(
                hydro_code, uow
            )
            lta_inflow = cls._eval_monthly_lta(inflow)
            lta_lookup = lta_inflow.rename({VALUE_COL: LTA_COL}).with_columns(
                pl.lit(hydro_code).alias(HYDRO_CODE_COL)
            )
            lta_hydro_df = lta_model_df.join(lta_lookup, on=MONTH_COL)
            lta_hydro_df = lta_hydro_df.with_columns(
                pl.lit(eer_code).alias(EER_CODE_COL),
                pl.lit(submarket_code).alias(SUBMARKET_CODE_COL),
            )
            return lta_hydro_df

        with time_and_log(
            "Tempo para calculo da MLT por UHE", logger=cls.logger
        ):
            hydro_map = Deck.hydro_eer_submarket_map(uow)
            lta_model_df = cls._model_dataframe_for_hydro_lta(uow)
            lta_hydro_dfs: List[pl.DataFrame] = []
            for row in hydro_map.iter_rows(named=True):
                hydro_code = row[HYDRO_CODE_COL]
                lta_hydro_df = _calc_hydro_lta_df(
                    hydro_code,
                    lta_model_df,
                    row[EER_CODE_COL],
                    row[SUBMARKET_CODE_COL],
                )
                lta_hydro_dfs.append(lta_hydro_df)

            return pl.concat(lta_hydro_dfs).sort([STAGE_COL, HYDRO_CODE_COL])

    @classmethod
    def _resolve_starting_stage(
        cls, df: pl.DataFrame, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Adiciona a informação do estágio inicial do caso aos dados,
        realizando um deslocamento da coluna "estagio" para que o
        estágio inicial do caso seja 1.

        Também elimina estágios incluídos como consequência do formato
        dos dados lidos, que pertencem ao período pré-estudo.
        """
        offset = Deck.study_period_starting_month(uow) - 1
        return df.with_columns(
            (pl.col(STAGE_COL) - offset).alias(STAGE_COL)
        ).filter(pl.col(STAGE_COL) > 0)

    @classmethod
    def _generate_model_dataframe_month_column(
        cls, uow: AbstractUnitOfWork
    ) -> np.ndarray:
        """Generate month column for model stages spanning study period."""
        starting_date_with_tendency = (
            Deck.starting_date_with_past_tendency_period(uow)
        )
        ending_date_with_post_study_years = (
            Deck.ending_date_with_post_study_period(uow)
        )
        dates = pd.date_range(
            starting_date_with_tendency,
            ending_date_with_post_study_years,
            freq="MS",
        )
        return np.array([d.month for d in dates], dtype=np.int64)

    @classmethod
    def _generate_model_dataframe_stage_column(
        cls, uow: AbstractUnitOfWork, num_stages: int
    ) -> np.ndarray:
        """Generate stage column with past tendency period offset."""
        past_stages = Deck.num_stages_with_past_tendency_period(uow)
        stages_with_past_tendency = np.arange(
            -past_stages + 1, num_stages - past_stages + 1, dtype=np.int64
        )
        return stages_with_past_tendency

    @classmethod
    def _model_dataframe_for_eer_lta(
        cls, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """Build EER LTA model dataframe with stage-month-configuration mapping."""

        def __generate_configuration_column(
            uow: AbstractUnitOfWork,
        ) -> np.ndarray:
            configurations_df = Deck.configurations(uow)
            starting_date_with_tendency = (
                Deck.starting_date_with_past_tendency_period(uow)
            )
            ending_date_with_post_study_years = (
                Deck.ending_date_with_post_study_period(uow)
            )
            past_stages = Deck.num_stages_with_past_tendency_period(uow)
            additional_tendency_configurations = np.array([1] * past_stages)
            configurations = (
                configurations_df.filter(
                    (pl.col(START_DATE_COL) >= starting_date_with_tendency)
                    & (
                        pl.col(START_DATE_COL)
                        <= ending_date_with_post_study_years
                    )
                )[VALUE_COL]
                .to_numpy()
                .flatten()
            )
            return np.concatenate(
                [
                    additional_tendency_configurations,
                    configurations,
                ]
            )

        months_column = cls._generate_model_dataframe_month_column(uow)
        stages_column = cls._generate_model_dataframe_stage_column(
            uow, len(months_column)
        )
        configuration_column = __generate_configuration_column(uow)

        lta_df = pl.DataFrame(
            data={
                STAGE_COL: stages_column,
                CONFIG_COL: configuration_column,
                MONTH_COL: months_column,
            }
        )
        lta_df = cls._resolve_starting_stage(lta_df, uow)
        return lta_df

    @classmethod
    def _generate_lta_eer_energy_series(
        cls, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Extrai a MLT em energia para todos os REEs.

        - codigo_ree (`int`)
        - ree (`str`)
        - codigo_submercado (`int`)
        - submercado (`str`)
        - estagio (`int`)
        - configuracao (`int`)
        - mes (`int`)
        - mlt (`float`)

        :return: A tabela como um DataFrame
        :rtype: pl.DataFrame
        """

        def _energy_history_df(uow: AbstractUnitOfWork) -> pl.DataFrame:
            energy_history = Deck.engnat(uow)
            starting_year = Deck.study_period_starting_year(uow)
            history_final_year = starting_year - 1
            return energy_history.filter(
                pl.col("data").dt.year() < history_final_year
            )

        def _calc_eer_lta_df(
            file_eer_index: int,
            lta_model_df: pl.DataFrame,
            energy_history_df: pl.DataFrame,
            eer_code: int,
            submarket_code: int,
        ) -> pl.DataFrame:
            n_rows = lta_model_df.height
            eer_lta = np.zeros((n_rows,))
            for eer_idx, lta_line in enumerate(
                lta_model_df.iter_rows(named=True)
            ):
                mean_val = energy_history_df.filter(
                    (pl.col("configuracao") == lta_line[CONFIG_COL])
                    & (pl.col("ree") == file_eer_index)
                    & (pl.col("data").dt.month() == lta_line[MONTH_COL])
                )["valor"].mean()
                eer_lta[eer_idx] = (
                    float(mean_val) if mean_val is not None else 0.0  # type: ignore[arg-type]
                )
            lta_eer_df = lta_model_df.with_columns(
                pl.Series(LTA_COL, eer_lta),
                pl.lit(eer_code).alias(EER_CODE_COL),
                pl.lit(submarket_code).alias(SUBMARKET_CODE_COL),
            )
            return lta_eer_df

        with time_and_log(
            "Tempo para calculo da MLT por REE", logger=cls.logger
        ):
            energy_history = _energy_history_df(uow)
            eer_submarket_map = Deck.eer_submarket_map(uow)
            eer_order = Deck.eer_code_order(uow)
            eer_submarket_map = pl.DataFrame({EER_CODE_COL: eer_order}).join(
                eer_submarket_map, on=EER_CODE_COL, how="left"
            )
            lta_model_df = cls._model_dataframe_for_eer_lta(uow)
            lta_eer_dfs: List[pl.DataFrame] = []
            for idx, row in enumerate(eer_submarket_map.iter_rows(named=True)):
                lta_eer_df = _calc_eer_lta_df(
                    idx + 1,
                    lta_model_df,
                    energy_history,
                    row[EER_CODE_COL],
                    row[SUBMARKET_CODE_COL],
                )
                lta_eer_dfs.append(lta_eer_df)
            return pl.concat(lta_eer_dfs)

    @classmethod
    def _agg_lta_hydro_inflow_series(
        cls, variable: Variable, col: Optional[str], uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """Aggregate incremental inflow LTA by optional column."""
        hydro_lta = cls._get_lta_df(
            variable,
            SpatialResolution.USINA_HIDROELETRICA,
            uow,
        )
        col_list = [col] if col is not None else []
        group_cols = col_list + [STAGE_COL]
        return (
            hydro_lta.group_by(group_cols)
            .agg(pl.col(LTA_COL).sum())
            .select(group_cols + [LTA_COL])
            .sort(group_cols)
        )

    @classmethod
    def _agg_lta_eer_energy_series(
        cls, col: Optional[str], uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """Aggregate EER energy LTA by optional column."""
        eer_lta = cls._get_lta_df(
            Variable.ENA_ABSOLUTA,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
            uow,
        )
        col_list = [col] if col is not None else []
        group_cols = col_list + [STAGE_COL]
        return (
            eer_lta.group_by(group_cols)
            .agg(pl.col(LTA_COL).sum())
            .select(group_cols + [LTA_COL])
            .sort(group_cols)
        )

    @classmethod
    def _resolve_lta_submarket_energy_series(
        cls, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        with time_and_log(
            "Tempo para agregação da MLT de ENAA - SBM", logger=cls.logger
        ):
            return cls._agg_lta_eer_energy_series(SUBMARKET_CODE_COL, uow)

    @classmethod
    def _resolve_lta_sin_energy_series(
        cls, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        with time_and_log(
            "Tempo para agregação da MLT de ENAA - SIN", logger=cls.logger
        ):
            return cls._agg_lta_eer_energy_series(None, uow)

    @classmethod
    def _resolve_lta_eer_inflow_series(
        cls, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        with time_and_log(
            "Tempo para agregação da MLT de QINC - REE", logger=cls.logger
        ):
            return cls._agg_lta_hydro_inflow_series(
                Variable.VAZAO_INCREMENTAL, EER_CODE_COL, uow
            )

    @classmethod
    def _resolve_lta_submarket_inflow_series(
        cls, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        with time_and_log(
            "Tempo para agregação da MLT de QINC - SBM", logger=cls.logger
        ):
            return cls._agg_lta_hydro_inflow_series(
                Variable.VAZAO_INCREMENTAL, SUBMARKET_CODE_COL, uow
            )

    @classmethod
    def _resolve_lta_sin_inflow_series(
        cls, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        with time_and_log(
            "Tempo para agregação da MLT de QINC - SIN", logger=cls.logger
        ):
            return cls._agg_lta_hydro_inflow_series(
                Variable.VAZAO_INCREMENTAL, None, uow
            )

    @classmethod
    def _get_lta_df(
        cls,
        variable: Variable,
        spatial_resolution: SpatialResolution,
        uow: AbstractUnitOfWork,
    ) -> pl.DataFrame:
        """Get or compute LTA dataframe for variable and spatial resolution, using cache."""
        CACHING_FUNCTION_MAP: Dict[
            Tuple[Variable, SpatialResolution], Callable[..., Any]
        ] = {
            (
                Variable.ENA_ABSOLUTA,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): cls._generate_lta_eer_energy_series,
            (
                Variable.ENA_ABSOLUTA,
                SpatialResolution.SUBMERCADO,
            ): cls._resolve_lta_submarket_energy_series,
            (
                Variable.ENA_ABSOLUTA,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): cls._resolve_lta_sin_energy_series,
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.USINA_HIDROELETRICA,
            ): cls._generate_lta_hydro_inflow_series,
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.RESERVATORIO_EQUIVALENTE,
            ): cls._resolve_lta_eer_inflow_series,
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.SUBMERCADO,
            ): cls._resolve_lta_submarket_inflow_series,
            (
                Variable.VAZAO_INCREMENTAL,
                SpatialResolution.SISTEMA_INTERLIGADO,
            ): cls._resolve_lta_sin_inflow_series,
        }
        if cls.CACHED_MLT_VALUES.get((variable, spatial_resolution)) is None:
            cls.CACHED_MLT_VALUES[(variable, spatial_resolution)] = (
                CACHING_FUNCTION_MAP[(variable, spatial_resolution)](uow)
            )
        return cls.CACHED_MLT_VALUES.get(
            (variable, spatial_resolution), pl.DataFrame()
        )

    @classmethod
    def _format_scenario_data(
        cls, data: np.ndarray, num_scenarios: int, num_stages: int
    ) -> np.ndarray:
        """Tile and repeat data for scenario-stage expansion."""
        return np.tile(np.repeat(data, num_scenarios), (num_stages,))

    @classmethod
    def _add_energy_eer_data(
        cls,
        uow: AbstractUnitOfWork,
        energy_df: pl.DataFrame,
        dates: List[datetime],
    ) -> pl.DataFrame:
        """Add EER and submarket data with dates to energy dataframe."""

        def _add_entities(
            energy_df: pl.DataFrame,
            num_scenarios: int,
            num_stages: int,
            uow: AbstractUnitOfWork,
        ) -> pl.DataFrame:
            eer_order = Deck.eer_code_order(uow)
            eer_submarket_map_ordered = pl.DataFrame(
                {EER_CODE_COL: eer_order}
            ).join(Deck.eer_submarket_map(uow), on=EER_CODE_COL, how="left")
            for col in [
                EER_CODE_COL,
                SUBMARKET_CODE_COL,
            ]:
                energy_df = energy_df.with_columns(
                    pl.Series(
                        col,
                        cls._format_scenario_data(
                            eer_submarket_map_ordered[col].to_numpy(),
                            num_scenarios,
                            num_stages,
                        ),
                    )
                )
            return energy_df

        def _add_dates(
            energy_df: pl.DataFrame,
            dates: List[datetime],
            num_scenarios: int,
            num_eers: int,
        ) -> pl.DataFrame:
            end_dates = [d + relativedelta(months=1) for d in dates]
            sorted_start_dates: np.ndarray = np.repeat(
                np.array(dates), num_scenarios * num_eers
            )
            sorted_end_dates: np.ndarray = np.repeat(
                np.array(end_dates), num_scenarios * num_eers
            )
            return energy_df.with_columns(
                pl.Series(START_DATE_COL, sorted_start_dates),
                pl.Series(END_DATE_COL, sorted_end_dates),
            )

        num_scenarios = energy_df[SCENARIO_COL].n_unique()
        num_eers = energy_df[EER_CODE_COL].n_unique()
        num_stages = energy_df[STAGE_COL].n_unique()
        num_spans = (
            energy_df[SPAN_COL].n_unique()
            if SPAN_COL in energy_df.columns
            else 1
        )

        energy_df = _add_entities(
            energy_df, num_scenarios * num_spans, num_stages, uow
        )
        energy_df = _add_dates(
            energy_df, dates, num_scenarios * num_spans, num_eers
        )
        energy_df = cls._resolve_starting_stage(energy_df, uow)
        energy_df_columns = [
            EER_CODE_COL,
            SUBMARKET_CODE_COL,
            STAGE_COL,
            START_DATE_COL,
            END_DATE_COL,
            SCENARIO_COL,
            VALUE_COL,
        ]
        energy_df_columns += [SPAN_COL] if SPAN_COL in energy_df.columns else []
        return energy_df.select(energy_df_columns)

    @classmethod
    def _add_inflow_hydro_data(
        cls,
        uow: AbstractUnitOfWork,
        inflow_df: pl.DataFrame,
    ) -> pl.DataFrame:
        """Add hydro, EER, and submarket data with dates to inflow dataframe."""

        def _add_entities(
            inflow_df: pl.DataFrame,
            num_scenarios: int,
            num_stages: int,
            uow: AbstractUnitOfWork,
        ) -> pl.DataFrame:
            hydro_order = Deck.hydro_code_order(uow)
            hydro_eer_submarket_map = pl.DataFrame(
                {HYDRO_CODE_COL: hydro_order}
            ).join(
                Deck.hydro_eer_submarket_map(uow),
                on=HYDRO_CODE_COL,
                how="left",
            )
            for col in [
                HYDRO_CODE_COL,
                EER_CODE_COL,
                SUBMARKET_CODE_COL,
            ]:
                inflow_df = inflow_df.with_columns(
                    pl.Series(
                        col,
                        cls._format_scenario_data(
                            hydro_eer_submarket_map[col].to_numpy(),
                            num_scenarios,
                            num_stages,
                        ),
                    )
                )
            return inflow_df

        def _add_dates(
            inflow_df: pl.DataFrame,
            num_scenarios: int,
            num_hydros: int,
            num_stages: int,
            uow: AbstractUnitOfWork,
        ) -> pl.DataFrame:
            starting_date = Deck.starting_date_with_past_tendency_period(uow)
            ending_date = starting_date + relativedelta(months=num_stages - 1)
            dates = pd.date_range(
                starting_date,
                ending_date,
                freq="MS",
            )
            end_dates = [d + relativedelta(months=1) for d in dates]
            sorted_start_dates = np.repeat(dates, num_scenarios * num_hydros)
            sorted_end_dates = np.repeat(end_dates, num_scenarios * num_hydros)
            return inflow_df.with_columns(
                pl.Series(START_DATE_COL, sorted_start_dates),
                pl.Series(END_DATE_COL, sorted_end_dates),
            )

        num_scenarios = inflow_df[SCENARIO_COL].n_unique()
        num_hydros = inflow_df[HYDRO_CODE_COL].n_unique()
        num_stages = inflow_df[STAGE_COL].n_unique()
        num_spans = (
            inflow_df[SPAN_COL].n_unique()
            if SPAN_COL in inflow_df.columns
            else 1
        )

        inflow_df = _add_entities(
            inflow_df, num_scenarios * num_spans, num_stages, uow
        )
        inflow_df = _add_dates(
            inflow_df, num_scenarios * num_spans, num_hydros, num_stages, uow
        )
        inflow_df = cls._resolve_starting_stage(inflow_df, uow)
        inflow_df_columns = [
            STAGE_COL,
            START_DATE_COL,
            END_DATE_COL,
            SCENARIO_COL,
            HYDRO_CODE_COL,
            EER_CODE_COL,
            SUBMARKET_CODE_COL,
            VALUE_COL,
        ]
        inflow_df_columns += [SPAN_COL] if SPAN_COL in inflow_df.columns else []
        return inflow_df.select(inflow_df_columns)

    @classmethod
    def _post_resolve_energy_iteration(
        cls,
        generated_energy_df: pl.DataFrame,
        converted_energy_df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        hydro_simulation_stages: int,
        dates: List[datetime],
        it: Optional[int] = None,
    ) -> pl.DataFrame:
        """Merge generated and converted energy with EER/submarket data and iteration tag."""
        if converted_energy_df.height > 0 and generated_energy_df.height > 0:
            energy_df = pl.concat(
                [
                    converted_energy_df.filter(
                        pl.col(STAGE_COL) <= hydro_simulation_stages
                    ),
                    generated_energy_df.filter(
                        pl.col(STAGE_COL) > hydro_simulation_stages
                    ),
                ]
            )
        else:
            energy_df = generated_energy_df
        if energy_df.height > 0:
            energy_df = cls._add_energy_eer_data(uow, energy_df, dates)
            if it is not None:
                energy_df = energy_df.with_columns(
                    pl.lit(it).alias(ITERATION_COL)
                )
        return energy_df

    @classmethod
    def _post_resolve_inflow_iteration(
        cls,
        inflow_df: pl.DataFrame,
        uow: AbstractUnitOfWork,
        it: Optional[int] = None,
    ) -> pl.DataFrame:
        """Add hydro/EER/submarket data and iteration tag to inflow dataframe."""
        if inflow_df.height > 0:
            inflow_df = cls._add_inflow_hydro_data(uow, inflow_df)
            if it is not None:
                inflow_df = inflow_df.with_columns(
                    pl.lit(it).alias(ITERATION_COL)
                )
        return inflow_df

    @classmethod
    def _resolve_forward_energy_iteration(
        cls, uow: AbstractUnitOfWork, it: int
    ) -> pd.DataFrame:
        """Get forward-stage ENA data from energiaf and enavazf for iteration."""
        logger = Log.configure_process_logger(
            uow._queue, Variable.ENA_ABSOLUTA.value, it
        )
        logger.info(f"Obtendo energias forward da it. {it}")
        generated_energy_df = Deck.energiaf(it, uow)
        converted_energy_df = Deck.enavazf(it, uow)
        hydro_simulation_stages = Deck.num_hydro_simulation_stages_policy(uow)
        dates = Deck.internal_stages_starting_dates_policy_with_past_tendency(
            uow
        )

        return cls._post_resolve_energy_iteration(
            generated_energy_df,
            converted_energy_df,
            uow,
            hydro_simulation_stages,
            dates,
            it,
        ).to_pandas()

    @classmethod
    def _post_resolve(
        cls, resolve_responses: Dict[int, "pl.DataFrame | pd.DataFrame"]
    ) -> pl.DataFrame:
        """
        Realiza o pós-processamento para agregação dos dados de todos os
        DataFrames lidos de um conjunto de arquivos.
        """
        with time_and_log("Tempo para compactacao dos dados", cls.logger):
            valid_dfs: List[pl.DataFrame] = []
            for df in resolve_responses.values():
                if df is None:
                    continue
                if isinstance(df, pd.DataFrame):
                    if df.empty:
                        continue
                    valid_dfs.append(pl.from_pandas(df))
                else:
                    if df.height > 0:
                        valid_dfs.append(df)
            if not valid_dfs:
                return pl.DataFrame()
            return pl.concat(valid_dfs)

    @classmethod
    def _resolve_forward_energy(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        """
        Obtem os dados de ENA para a etapa forward em todas as iterações feitas
        pelo modelo.

        :return: Os dados como um DataFrame.
        :rtype: pl.DataFrame
        """
        num_iterations = Deck.num_iterations(uow)
        num_procs = int(Settings().processors)
        with time_and_log(
            message_root="Tempo para obter energias forward",
            logger=cls.logger,
        ):
            with ProcessPoolExecutor(
                max_workers=num_procs,
                mp_context=_mp.get_context(
                    "spawn" if _platform.system() == "Windows" else "forkserver"
                ),
            ) as executor:
                futures = {
                    it: executor.submit(
                        cls._resolve_forward_energy_iteration, uow, it
                    )
                    for it in range(1, num_iterations + 1)
                }
                dfs = {it: f.result(timeout=3600) for it, f in futures.items()}

        return cls._post_resolve(dfs)

    @classmethod
    def _resolve_forward_inflow_iteration(
        cls, uow: AbstractUnitOfWork, it: int
    ) -> pd.DataFrame:
        """
        Obtem os dados de QINC para a etapa forward em uma determinada
        iteração de interesse, considerando apenas os estágios individualizados,
        nos quais a vazão é lida do arquivo binário `vazaof.dat`. É adicionada
        uma coluna `iteracao` ao DataFrame resultante.

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        logger = Log.configure_process_logger(
            uow.queue, Variable.VAZAO_INCREMENTAL.value, it
        )
        logger.info(f"Obtendo vazões forward da it. {it}")
        inflow_df = Deck.vazaof(it, uow)
        return cls._post_resolve_inflow_iteration(
            inflow_df, uow, it
        ).to_pandas()

    @classmethod
    def _resolve_forward_inflow(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        """
        Obtem os dados de QINC para a etapa forward em todas as iterações
        feitas pelo modelo.

        :return: Os dados como um DataFrame.
        :rtype: pl.DataFrame
        """
        num_iterations = Deck.num_iterations(uow)
        num_procs = int(Settings().processors)
        with time_and_log(
            message_root="Tempo para obter vazoes forward",
            logger=cls.logger,
        ):
            with ProcessPoolExecutor(
                max_workers=num_procs,
                mp_context=_mp.get_context(
                    "spawn" if _platform.system() == "Windows" else "forkserver"
                ),
            ) as executor:
                futures = {
                    it: executor.submit(
                        cls._resolve_forward_inflow_iteration, uow, it
                    )
                    for it in range(1, num_iterations + 1)
                }
                dfs = {ir: f.result(timeout=3600) for ir, f in futures.items()}
        return cls._post_resolve(dfs)

    @classmethod
    def _resolve_backward_energy_iteration(
        cls, uow: AbstractUnitOfWork, it: int
    ) -> pd.DataFrame:
        """
        Obtem os dados de ENA para a etapa backward em uma determinada
        iteração de interesse, considerando já os estágios individualizados
        e agregados, nos quais a energia é lida do arquivo binário
        `enavazb.dat` e `energiab.dat`, respectivamente. É adicionada uma
        coluna `iteracao` ao DataFrame resultante.

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        logger = Log.configure_process_logger(
            uow._queue, Variable.ENA_ABSOLUTA.value, it
        )
        logger.info(f"Obtendo energias backward da it. {it}")
        generated_energy_df = Deck.energiab(it, uow)
        converted_energy_df = Deck.enavazb(it, uow)
        hydro_simulation_stages = Deck.num_hydro_simulation_stages_policy(uow)
        dates = Deck.internal_stages_starting_dates_policy(uow)

        return cls._post_resolve_energy_iteration(
            generated_energy_df,
            converted_energy_df,
            uow,
            hydro_simulation_stages,
            dates,
            it,
        ).to_pandas()

    @classmethod
    def _resolve_backward_energy(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        """
        Obtem os dados de ENA para a etapa backward em todas as iterações
        feitas pelo modelo.

        :return: Os dados como um DataFrame.
        :rtype: pl.DataFrame
        """
        num_iterations = Deck.num_iterations(uow)
        num_procs = int(Settings().processors)
        with time_and_log(
            message_root="Tempo para obter energias backward",
            logger=cls.logger,
        ):
            with ProcessPoolExecutor(
                max_workers=num_procs,
                mp_context=_mp.get_context(
                    "spawn" if _platform.system() == "Windows" else "forkserver"
                ),
            ) as executor:
                futures = {
                    it: executor.submit(
                        cls._resolve_backward_energy_iteration, uow, it
                    )
                    for it in range(1, num_iterations + 1)
                }
                dfs = {ir: f.result(timeout=3600) for ir, f in futures.items()}

        return cls._post_resolve(dfs)

    @classmethod
    def _resolve_backward_inflow_iteration(
        cls, uow: AbstractUnitOfWork, it: int
    ) -> pd.DataFrame:
        """
        Obtem os dados de QINC para a etapa backward em uma determinada
        iteração de interesse, considerando apenas os estágios individualizados,
        nos quais a vazão é lida do arquivo binário `vazaob.dat`. É adicionada
        uma coluna `iteracao` ao DataFrame resultante.

        :return: Os dados como um DataFrame.
        :rtype: pd.DataFrame
        """
        logger = Log.configure_process_logger(
            uow.queue, Variable.VAZAO_INCREMENTAL.value, it
        )
        logger.info(f"Obtendo vazões backward da it. {it}")
        inflow_df = Deck.vazaob(it, uow)
        return cls._post_resolve_inflow_iteration(
            inflow_df, uow, it
        ).to_pandas()

    @classmethod
    def _resolve_backward_inflow(cls, uow: AbstractUnitOfWork) -> pl.DataFrame:
        """
        Obtem os dados de QINC para a etapa backward em todas as iterações
        feitas pelo modelo.

        :return: Os dados como um DataFrame.
        :rtype: pl.DataFrame
        """
        num_iterations = Deck.num_iterations(uow)
        num_procs = int(Settings().processors)
        with time_and_log(
            message_root="Tempo para obter vazoes backward",
            logger=cls.logger,
        ):
            with ProcessPoolExecutor(
                max_workers=num_procs,
                mp_context=_mp.get_context(
                    "spawn" if _platform.system() == "Windows" else "forkserver"
                ),
            ) as executor:
                futures = {
                    it: executor.submit(
                        cls._resolve_backward_inflow_iteration, uow, it
                    )
                    for it in range(1, num_iterations + 1)
                }
                dfs = {ir: f.result(timeout=3600) for ir, f in futures.items()}
        return cls._post_resolve(dfs)

    @classmethod
    def _resolve_final_simulation_energy(
        cls, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Obtem os dados de ENA para a etapa de simulação final.

        :return: Os dados como um DataFrame.
        :rtype: pl.DataFrame
        """
        cls._log("Obtendo energias da simulação final")
        with time_and_log(
            message_root="Tempo para obter energias da simulacao final",
            logger=cls.logger,
        ):
            generated_energy_df = Deck.energias(uow)
            converted_energy_df = Deck.enavazs(uow)
        hydro_simulation_stages = (
            Deck.num_hydro_simulation_stages_final_simulation(uow)
        )
        dates = Deck.internal_stages_starting_dates_policy_with_past_tendency(
            uow
        )

        df = cls._post_resolve_energy_iteration(
            generated_energy_df,
            converted_energy_df,
            uow,
            hydro_simulation_stages,
            dates,
            it=None,
        )
        return cls._post_resolve({0: df})

    @classmethod
    def _resolve_final_simulation_inflow(
        cls, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        """
        Obtem os dados de QINC para a etapa de simulação final.

        :return: Os dados como um DataFrame.
        :rtype: pl.DataFrame
        """
        cls._log("Obtendo vazões da simulação final")
        with time_and_log(
            message_root="Tempo para obter vazoes da simulacao final",
            logger=cls.logger,
        ):
            inflow_df = Deck.vazaos(uow)

        df = cls._post_resolve_inflow_iteration(
            inflow_df,
            uow,
            it=None,
        )
        return cls._post_resolve({0: df})

    @classmethod
    def _get_cached_variable(
        cls,
        variable: Variable,
        step: Step,
        uow: AbstractUnitOfWork,
    ) -> pl.DataFrame:
        """
        Obtem um DataFrame com os dados de uma variável sintetizada,
        em uma determinada etapa, a partir do cache. Caso este dado
        não exista, ele é calculado a partir da função de resolução
        adequada, armazenado no cache e retornado.

        :return: Os dados da variável, para a etapa, como um DataFrame.
        :rtype: pl.DataFrame
        """
        CACHING_FUNCTION_MAP: Dict[
            Tuple[Variable, Step], Callable[..., Any]
        ] = {
            (Variable.ENA_ABSOLUTA, Step.FORWARD): cls._resolve_forward_energy,
            (
                Variable.ENA_ABSOLUTA,
                Step.BACKWARD,
            ): cls._resolve_backward_energy,
            (
                Variable.ENA_ABSOLUTA,
                Step.FINAL_SIMULATION,
            ): cls._resolve_final_simulation_energy,
            (
                Variable.VAZAO_INCREMENTAL,
                Step.FORWARD,
            ): cls._resolve_forward_inflow,
            (
                Variable.VAZAO_INCREMENTAL,
                Step.BACKWARD,
            ): cls._resolve_backward_inflow,
            (
                Variable.VAZAO_INCREMENTAL,
                Step.FINAL_SIMULATION,
            ): cls._resolve_final_simulation_inflow,
        }

        if cls.CACHED_SYNTHESIS.get((variable, step)) is None:
            cls.CACHED_SYNTHESIS[(variable, step)] = CACHING_FUNCTION_MAP[
                (variable, step)
            ](uow)
        return cls.CACHED_SYNTHESIS.get((variable, step), pl.DataFrame())

    @classmethod
    def _resolve_group(
        cls, group_col: List[str], df: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Realiza o agrupamento dos dados por meio de uma soma, cosiderando
        uma lista de colunas para agrupamento e excluindo a coluna "valor",
        que será sempre agregada.

        :return: Os dados agrupados como um DataFrame.
        :rtype: pl.DataFrame
        """
        if df.height > 0:
            cols = group_col + [
                c for c in cls.COMMON_COLUMNS if c in df.columns
            ]
            return (
                df.group_by(cols)
                .agg(pl.col(VALUE_COL).sum())
                .select(cols + [VALUE_COL])
            )
        else:
            return df

    @classmethod
    def _calc_lta(
        cls,
        df: pl.DataFrame,
        lta_df: pl.DataFrame,
        filter_col: Optional[str],
    ) -> pl.DataFrame:
        """
        Adiciona uma informação da MLT (Média de Longo
        Termo) para cada cenário sintetizado em dados pertencentes
        à etapa backward.

        :return: Os dados com MLT como um DataFrame.
        :rtype: pl.DataFrame
        """

        def _df_sorting_columns(
            df: pl.DataFrame, filter_col: Optional[str]
        ) -> List[str]:
            iteration_col = (
                [ITERATION_COL] if ITERATION_COL in df.columns else []
            )
            filter_col_list = [filter_col] if filter_col is not None else []
            span_col = [SPAN_COL] if SPAN_COL in df.columns else []
            return (
                iteration_col
                + [STAGE_COL]
                + filter_col_list
                + [SCENARIO_COL]
                + span_col
            )

        def _lta_df_sorting_columns(filter_col: Optional[str]) -> List[str]:
            filter_col_list = [filter_col] if filter_col is not None else []
            return [STAGE_COL] + filter_col_list

        df = df.sort(_df_sorting_columns(df, filter_col))
        lta_df = lta_df.sort(_lta_df_sorting_columns(filter_col))
        num_scenarios = df[SCENARIO_COL].n_unique()
        stages = df[STAGE_COL].unique().to_list()
        num_iterations = (
            df[ITERATION_COL].n_unique() if ITERATION_COL in df.columns else 1
        )
        num_spans = df[SPAN_COL].n_unique() if SPAN_COL in df.columns else 1
        elements = (
            df[filter_col].unique().to_list() if filter_col is not None else []
        )

        lta_df = lta_df.filter(pl.col(STAGE_COL).is_in(stages))
        if len(elements) > 0 and filter_col is not None:
            lta_df = lta_df.filter(pl.col(filter_col).is_in(elements))
        sorted_ltas = np.repeat(
            lta_df[LTA_COL].to_numpy(), num_scenarios * num_spans
        )

        lta_tiled = np.tile(sorted_ltas, num_iterations)
        df = df.with_columns(
            pl.Series(LTA_COL, lta_tiled),
        )
        df = df.with_columns(
            (pl.col(VALUE_COL) / pl.col(LTA_COL)).alias(LTA_VALUE_COL)
        )
        df = df.with_columns(
            pl.when(pl.col(LTA_VALUE_COL).is_infinite())
            .then(0.0)
            .otherwise(pl.col(LTA_VALUE_COL))
            .alias(LTA_VALUE_COL)
        )
        return df

    @classmethod
    def _resolve_lta(
        cls,
        synthesis: ScenarioSynthesis,
        df: pl.DataFrame,
        uow: AbstractUnitOfWork,
    ) -> pl.DataFrame:
        lta_df = cls._get_lta_df(
            synthesis.variable,
            synthesis.spatial_resolution,
            uow,
        )

        FILTER_MAP = {
            SpatialResolution.USINA_HIDROELETRICA: HYDRO_CODE_COL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE: EER_CODE_COL,
            SpatialResolution.SUBMERCADO: SUBMARKET_CODE_COL,
            SpatialResolution.SISTEMA_INTERLIGADO: None,
        }
        filter_col = FILTER_MAP[synthesis.spatial_resolution]
        return cls._calc_lta(df, lta_df, filter_col)

    @classmethod
    def _resolve_spatial_resolution(
        cls, synthesis: ScenarioSynthesis, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        RESOLUTION_MAP: Dict[SpatialResolution, List[str]] = {
            SpatialResolution.SISTEMA_INTERLIGADO: [],
            SpatialResolution.SUBMERCADO: [SUBMARKET_CODE_COL],
            SpatialResolution.RESERVATORIO_EQUIVALENTE: [
                EER_CODE_COL,
                SUBMARKET_CODE_COL,
            ],
            SpatialResolution.USINA_HIDROELETRICA: [
                HYDRO_CODE_COL,
                EER_CODE_COL,
                SUBMARKET_CODE_COL,
            ],
        }
        df = cls._get_cached_variable(synthesis.variable, synthesis.step, uow)
        df = cls._resolve_group(
            RESOLUTION_MAP[synthesis.spatial_resolution], df
        )
        return cls._resolve_lta(synthesis, df, uow)

    @classmethod
    def _export_metadata(
        cls,
        success_synthesis: List[ScenarioSynthesis],
        uow: AbstractUnitOfWork,
    ) -> None:
        """
        Realiza a exportação dos metadados dos cenários sintetizados, com
        a descrição de quais sínteses foram realizadas e algumas
        características de cada uma.

        - chave (`str`)
        - nome_curto_variavel (`str`)
        - nome_longo_variavel (`str`)
        - nome_curto_agregacao (`str`)
        - nome_longo_agregacao (`str`)
        - nome_curto_etapa (`str`)
        - nome_longo_etapa (`str`)
        - unidade (`str`)

        """
        metadata_df = pd.DataFrame(
            columns=[
                "chave",
                "nome_curto_variavel",
                "nome_longo_variavel",
                "nome_curto_agregacao",
                "nome_longo_agregacao",
                "nome_curto_etapa",
                "nome_longo_etapa",
                "unidade",
            ]
        )
        for s in success_synthesis:
            metadata_df.loc[metadata_df.shape[0]] = [
                str(s),
                s.variable.short_name,
                s.variable.long_name,
                s.spatial_resolution.short_name,
                s.spatial_resolution.long_name,
                s.step.short_name,
                s.step.long_name,
                UNITS[s].value if s in UNITS else "",
            ]
        with uow:
            existing_df = uow.export.read_df(SCENARIO_SYNTHESIS_METADATA_OUTPUT)
            if existing_df is not None:
                metadata_df = pd.concat(
                    [existing_df, metadata_df], ignore_index=True
                )
                metadata_df = metadata_df.drop_duplicates()
            uow.export.synthetize_df(
                metadata_df, SCENARIO_SYNTHESIS_METADATA_OUTPUT
            )

    @classmethod
    def _add_synthesis_stats(
        cls, s: ScenarioSynthesis, df: pl.DataFrame
    ) -> None:
        """
        Adiciona um DataFrame com estatísticas de uma síntese ao
        DataFrame de estatísticas da agregação espacial e etapa em questão.
        """
        df = df.with_columns(pl.lit(s.variable.value).alias(VARIABLE_COL))

        key = (s.spatial_resolution, s.step)

        if key not in cls.SYNTHESIS_STATS:
            cls.SYNTHESIS_STATS[key] = [df]
        else:
            cls.SYNTHESIS_STATS[key].append(df)

    @classmethod
    def _export_scenario_synthesis(
        cls, s: ScenarioSynthesis, df: pl.DataFrame, uow: AbstractUnitOfWork
    ) -> None:
        """
        Realiza a exportação dos dados para uma síntese dos
        cenários desejada. Opcionalmente, os dados são armazenados
        em cache para uso futuro e as estatísticas são adicionadas
        ao DataFrame de estatísticas da agregação espacial e etapa em questão.
        """
        with time_and_log(
            message_root="Tempo para exportacao dos dados", logger=cls.logger
        ):
            scenarios_pl = df.with_columns(
                pl.col(SCENARIO_COL).cast(pl.Int64)
            ).sort(
                s.sorting_synthesis_df_columns,
                maintain_order=True,
            )
            stats_df = calc_statistics(scenarios_pl)
            cls._add_synthesis_stats(s, stats_df)
            with uow:
                uow.export.synthetize_pl(scenarios_pl, str(s))

    @classmethod
    def _export_stats(
        cls,
        uow: AbstractUnitOfWork,
    ) -> None:
        """
        Realiza a exportação dos dados de estatísticas de síntese
        da operação. As estatísticas são exportadas para um arquivo
        único por agregação espacial e etapa, de nome
        `CENARIOS_{agregacao}_{etapa}`.
        """
        for (res, step), dfs in cls.SYNTHESIS_STATS.items():
            with uow:
                df = pl.concat(dfs)
                all_cols = df.columns
                columns_without_variable = [
                    c for c in all_cols if c != VARIABLE_COL
                ]
                df = df.select([VARIABLE_COL] + columns_without_variable)
                df = df.with_columns(pl.col(VARIABLE_COL).cast(pl.Utf8))
                filename = (
                    f"{SCENARIO_SYNTHESIS_STATS_ROOT}_{res.value}_{step.value}"
                )
                existing_df = uow.export.read_df(filename)
                if existing_df is not None:
                    dedup_subset = [
                        c
                        for c in df.columns
                        if c
                        not in [VALUE_COL, UPPER_BOUND_COL, LOWER_BOUND_COL]
                    ]
                    df = pl.concat([pl.from_pandas(existing_df), df]).unique(
                        subset=dedup_subset, keep="first"
                    )
                uow.export.synthetize_pl(df, filename)

    @classmethod
    def _preprocess_synthesis_variables(
        cls, variables: List[str], uow: AbstractUnitOfWork
    ) -> List[ScenarioSynthesis]:
        """
        Realiza o pré-processamento das variáveis de síntese fornecidas,
        filtrando as válidas para o caso em questão.
        """
        try:
            if len(variables) == 0:
                synthesis_variables = cls._default_args()
            else:
                all_variables = cls._match_wildcards(variables)
                synthesis_variables = cls._process_variable_arguments(
                    all_variables
                )
            valid_synthesis = cls.filter_valid_variables(
                synthesis_variables, uow
            )
        except Exception as e:
            print_exc()
            cls._log(str(e), level=ERROR)
            cls._log("Erro no pré-processamento das variáveis", ERROR)
            valid_synthesis = []
        return valid_synthesis

    @classmethod
    def _synthetize_single_variable(
        cls, s: ScenarioSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[ScenarioSynthesis]:
        """
        Realiza a síntese de cenários para uma variável
        fornecida.
        """
        filename = str(s)
        with time_and_log(
            message_root=f"Tempo para sintese de {filename}",
            logger=cls.logger,
        ):
            try:
                filename = str(s)
                cls._log(f"Realizando síntese de {filename}")
                df = cls._resolve_spatial_resolution(s, uow)
                if df is None:
                    return None
                elif isinstance(df, pl.DataFrame):
                    if df.height == 0:
                        cls._log("Erro ao realizar a síntese")
                        return None
                cls._export_scenario_synthesis(s, df, uow)
                return s
            except Exception as e:
                print_exc()
                cls._log(str(e), level=ERROR)
                return None

    @classmethod
    def enforce_version(cls, uow: AbstractUnitOfWork) -> None:
        version = Deck.pmo(uow).versao_modelo
        if version is not None:
            uow.version = version

    @classmethod
    def synthetize(cls, variables: List[str], uow: AbstractUnitOfWork) -> None:
        """
        Realiza a síntese dos cenários para as variáveis fornecidas,
        na agregação desejada e para a etapa escolhida. As variáveis são
        pré-processadas para filtrar as válidas para o caso em questão,
        e então são resolvidas de acordo com a síntese.
        """
        cls.logger = logging.getLogger("main")
        Deck.logger = cls.logger
        uow.subdir = SCENARIO_SYNTHESIS_SUBDIR
        with time_and_log(
            message_root="Tempo para síntese dos cenários", logger=cls.logger
        ):
            cls.enforce_version(uow)
            valid_synthesis = cls._preprocess_synthesis_variables(
                variables, uow
            )
            success_synthesis: List[ScenarioSynthesis] = []
            for s in valid_synthesis:
                r = cls._synthetize_single_variable(s, uow)
                if r:
                    success_synthesis.append(r)

            cls._export_stats(uow)
            cls._export_metadata(success_synthesis, uow)
