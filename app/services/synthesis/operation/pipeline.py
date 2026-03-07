import sys as _sys
from datetime import datetime
from logging import WARNING
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd
import polars as pl

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    EER_CODE_COL,
    END_DATE_COL,
    GROUPING_TMP_COL,
    OPERATION_SYNTHESIS_COMMON_COLUMNS,
    SCENARIO_COL,
    STAGE_COL,
    STAGE_DURATION_HOURS,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    THERMAL_CODE_COL,
    VALUE_COL,
)
from app.model.operation.operationsynthesis import OperationSynthesis
from app.model.operation.spatialresolution import SpatialResolution
from app.services.deck.context import DeckContext
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.dataframe import pl_to_pd
from app.utils.timing import time_and_log


def _pkg():
    return _sys.modules[__package__]


if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )


def _replace_scenario_info(
    df: pd.DataFrame,
    num_stages: int,
    num_scenarios: int,
    num_blocks: int,
    num_entities: int = 1,
) -> pd.DataFrame:
    """Replace scenario column."""
    df[SCENARIO_COL] = np.tile(
        np.tile(
            np.repeat(np.arange(1, num_scenarios + 1), num_blocks),
            num_stages,
        ),
        num_entities,
    )
    return df


def _add_stage_info(
    df: pd.DataFrame,
    num_stages: int,
    num_scenarios: int,
    num_blocks: int,
    end_dates: np.ndarray,
    num_entities: int = 1,
) -> pd.DataFrame:
    """Add STAGE_COL and END_DATE_COL to df."""
    stages = np.arange(1, num_stages + 1)
    df[STAGE_COL] = np.tile(
        np.repeat(stages, num_scenarios * num_blocks), num_entities
    )
    df[END_DATE_COL] = np.tile(
        np.repeat(np.asarray(end_dates), num_scenarios * num_blocks),
        num_entities,
    )
    return df


def _add_block_duration_info(
    df: pd.DataFrame,
    num_stages: int,
    num_scenarios: int,
    num_blocks: int,
    blocks: List[int],
    start_dates: List[datetime],
    df_block_lengths: pd.DataFrame,
    num_entities: int = 1,
) -> pd.DataFrame:
    """Add BLOCK_DURATION_COL to df using block length table."""
    df_bl = df_block_lengths.loc[df_block_lengths[BLOCK_COL].isin(blocks)]
    block_durations = np.zeros(
        (num_scenarios * num_blocks * num_stages,), dtype=np.float64
    )
    data_block_size = num_scenarios * num_blocks
    for i, d in enumerate(start_dates):
        date_durations = df_bl.loc[
            df_bl[START_DATE_COL] == d, VALUE_COL
        ].to_numpy()
        i_i = i * data_block_size
        block_durations[i_i : i_i + data_block_size] = np.tile(
            date_durations, num_scenarios
        )
    df[BLOCK_DURATION_COL] = (
        np.tile(block_durations, num_entities) * STAGE_DURATION_HOURS
    )
    return df


def _fetch_temporal_deck_data(
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext],
    num_stages: int,
):
    """Return (num_scenarios, start_dates, end_dates, df_block_lengths)."""
    if deck_context is not None:
        return (
            deck_context.num_scenarios,
            deck_context.starting_dates[:num_stages],
            deck_context.ending_dates[:num_stages],
            deck_context.block_lengths,
        )
    _Deck = _pkg().Deck
    return (
        _Deck.num_scenarios_final_simulation(uow),
        _Deck.internal_stages_starting_dates_final_simulation(uow)[:num_stages],
        _Deck.internal_stages_ending_dates_final_simulation(uow)[:num_stages],
        _Deck.block_lengths(uow),
    )


def get_unique_column_values_in_order(
    cls: "type[OperationSynthetizer]",
    df: pd.DataFrame,
    cols: List[str],
) -> Dict[str, list]:
    """Extract unique values in order."""
    return {col: df[col].unique().tolist() for col in cols}


def set_ordered_entities(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    entities: Dict[str, list],
) -> None:
    """Store ordered entities for a synthesis."""
    cls.ORDERED_SYNTHESIS_ENTITIES[s] = entities


def get_ordered_entities(
    cls: "type[OperationSynthetizer]", s: OperationSynthesis
) -> Dict[str, list]:
    """Get ordered entities for a synthesis."""
    return cls.ORDERED_SYNTHESIS_ENTITIES[s]


def resolve_temporal_resolution(
    df: Optional[pd.DataFrame],
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext] = None,
) -> Optional[pl.DataFrame]:
    """
    Adiciona informação temporal a um DataFrame de síntese.

    When `deck_context` is provided, pre-computed data is used; otherwise
    Deck methods are called directly.
    """
    if df is None:
        return None
    try:
        pl_df = (
            _pkg()
            .pd_to_pl(
                df.rename(
                    columns={"data": START_DATE_COL, "serie": SCENARIO_COL}
                )
            )
            .sort(
                [START_DATE_COL, SCENARIO_COL, BLOCK_COL], maintain_order=True
            )
        )
        num_stages = pl_df[START_DATE_COL].n_unique()
        blocks = pl_df[BLOCK_COL].unique(maintain_order=True).to_list()
        num_blocks = len(blocks)
        num_sc, _start_dates, end_dates, df_block_lengths = (
            _fetch_temporal_deck_data(uow, deck_context, num_stages)
        )
        pl_df = pl_df.with_columns(
            pl.Series(
                SCENARIO_COL,
                np.tile(
                    np.repeat(np.arange(1, num_sc + 1), num_blocks), num_stages
                ),
            )
        )
        pl_df = pl_df.with_columns(
            [
                pl.Series(
                    STAGE_COL,
                    np.repeat(
                        np.arange(1, num_stages + 1), num_sc * num_blocks
                    ),
                ),
                pl.Series(
                    END_DATE_COL,
                    np.repeat(
                        np.array(end_dates, dtype="datetime64[ms]"),
                        num_sc * num_blocks,
                    ),
                ),
            ]
        )
        # block_lengths now returns pl.DataFrame directly; pd_to_pl() removed
        # SHIM: this comment replaces the old pd_to_pl shim (epic-01 learnings item)
        bl_pl = df_block_lengths.filter(pl.col(BLOCK_COL).is_in(blocks)).rename(
            {VALUE_COL: BLOCK_DURATION_COL}
        )
        pl_df = pl_df.join(bl_pl, on=[START_DATE_COL, BLOCK_COL], how="left")
        pl_df = pl_df.with_columns(
            (pl.col(BLOCK_DURATION_COL) * STAGE_DURATION_HOURS).alias(
                BLOCK_DURATION_COL
            )
        )
        return pl_df.select(OPERATION_SYNTHESIS_COMMON_COLUMNS)
    except Exception as exc:
        from app.services.synthesis.operation.orchestrator import (
            OperationSynthetizer,
        )

        OperationSynthetizer._log(
            f"_resolve_temporal_resolution: Polars path failed ({exc}), "
            "falling back to pandas",
            WARNING,
        )
        # Pandas fallback path
        pd_df = df.rename(
            columns={"data": START_DATE_COL, "serie": SCENARIO_COL}
        ).copy()
        pd_df.sort_values(
            [START_DATE_COL, SCENARIO_COL, BLOCK_COL], inplace=True
        )
        num_stages = pd_df[START_DATE_COL].unique().shape[0]
        blocks = pd_df[BLOCK_COL].unique().tolist()
        num_blocks = len(blocks)
        num_sc, start_dates, end_dates, df_bl = _fetch_temporal_deck_data(
            uow, deck_context, num_stages
        )
        pd_df = _replace_scenario_info(pd_df, num_stages, num_sc, num_blocks)
        pd_df = _add_stage_info(
            pd_df, num_stages, num_sc, num_blocks, np.asarray(end_dates)
        )
        # block_lengths now returns pl.DataFrame; convert for pandas fallback path
        # SHIM: remove after polars migration of this module
        pd_df = _add_block_duration_info(
            pd_df,
            num_stages,
            num_sc,
            num_blocks,
            blocks,
            start_dates,
            df_bl.to_pandas(),
        )
        return pl.from_pandas(pd_df[OPERATION_SYNTHESIS_COMMON_COLUMNS])


def resolve_starting_stage(
    df: pd.DataFrame,
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext] = None,
) -> pd.DataFrame:
    """Desloca STAGE_COL para que o estágio inicial seja 1 e filtra estágios ≤ 0."""
    starting_month = (
        deck_context.study_period_starting_month
        if deck_context is not None
        else _pkg().Deck.study_period_starting_month(uow)
    )
    df.loc[:, STAGE_COL] -= starting_month - 1
    df = df.loc[df[STAGE_COL] > 0]
    return df


def resolve_starting_stage_polars(
    df: pl.DataFrame,
    deck_context: Optional[DeckContext],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    """Polars variant of resolve_starting_stage; falls back to pandas on error."""
    starting_month = (
        deck_context.study_period_starting_month
        if deck_context is not None
        else _pkg().Deck.study_period_starting_month(uow)
    )
    try:
        return df.with_columns(
            (pl.col(STAGE_COL) - (starting_month - 1)).alias(STAGE_COL)
        ).filter(pl.col(STAGE_COL) > 0)
    except Exception as exc:
        from app.services.synthesis.operation.orchestrator import (
            OperationSynthetizer,
        )

        OperationSynthetizer._log(
            f"_resolve_starting_stage_polars: Polars path failed ({exc}), "
            "falling back to pandas",
            WARNING,
        )
        pd_df = pl_to_pd(df)
        pd_df.loc[:, STAGE_COL] -= starting_month - 1
        pd_df = pd_df.loc[pd_df[STAGE_COL] > 0]
        return _pkg().pd_to_pl(pd_df)


def initial_stored_energy_df(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Extract initial stored energy data from pmo.dat in MWmes."""
    max_column = "earmax"
    _Deck = _pkg().Deck
    df = _Deck.initial_stored_energy(uow)
    df[EER_CODE_COL] = _Deck.eer_code_order(uow)
    if s.spatial_resolution == SpatialResolution.RESERVATORIO_EQUIVALENTE:
        return df.rename(columns={EER_CODE_COL: GROUPING_TMP_COL})

    df[max_column] = 100 * df["valor_MWmes"] / df["valor_percentual"]
    if s.spatial_resolution == SpatialResolution.SUBMERCADO:
        eers_sbm_map = (
            _Deck.eer_submarket_map(uow).to_pandas().set_index(EER_CODE_COL)
        )  # SHIM: remove after polars migration of this module
        df.dropna(inplace=True)
        df[GROUPING_TMP_COL] = df.apply(
            lambda line: eers_sbm_map.at[
                line[EER_CODE_COL], SUBMARKET_CODE_COL
            ],
            axis=1,
        )
    elif s.spatial_resolution == SpatialResolution.SISTEMA_INTERLIGADO:
        df[GROUPING_TMP_COL] = 1
    df = df.groupby(GROUPING_TMP_COL).sum(numeric_only=True).reset_index()
    df["valor_percentual"] = 100 * df["valor_MWmes"] / df[max_column]
    return df


def generate_scenarios(
    cls: "type[OperationSynthetizer]",
    df: pd.DataFrame,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Expand single scenario synthesis to multiple scenarios."""
    num_scenarios = _pkg().Deck.num_scenarios_final_simulation(uow)
    num_entries = df.shape[0]
    df = pd.concat([df] * num_scenarios, ignore_index=True)
    df["serie"] = np.repeat(np.arange(1, num_scenarios + 1), num_entries)
    return df


def resolve_temporal_resolution_GTER_UTE(
    cls: "type[OperationSynthetizer]",
    df: Optional[pd.DataFrame],
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext] = None,
) -> Optional[pd.DataFrame]:
    """Adiciona informação temporal para síntese de Geração Térmica por UTE."""
    if df is None:
        return None

    df = df.rename(
        columns={
            "data": START_DATE_COL,
            "serie": SCENARIO_COL,
            "classe": THERMAL_CODE_COL,
        }
    ).copy()
    df = df.sort_values(
        [THERMAL_CODE_COL, START_DATE_COL, SCENARIO_COL, BLOCK_COL],
    ).reset_index(drop=True)

    num_stages = df[START_DATE_COL].unique().shape[0]
    blocks = df[BLOCK_COL].unique().tolist()
    num_blocks = len(blocks)
    thermals = df[THERMAL_CODE_COL].unique().tolist()
    num_thermals = len(thermals)

    num_sc, start_dates, end_dates, df_bl = _fetch_temporal_deck_data(
        uow, deck_context, num_stages
    )
    end_dates_arr = np.asarray(end_dates)

    df = _replace_scenario_info(
        df, num_stages, num_sc, num_blocks, num_thermals
    )
    df = _add_stage_info(
        df, num_stages, num_sc, num_blocks, end_dates_arr, num_thermals
    )
    # block_lengths now returns pl.DataFrame; convert for pandas-based function
    # SHIM: remove after polars migration of this module
    df = _add_block_duration_info(
        df,
        num_stages,
        num_sc,
        num_blocks,
        blocks,
        start_dates,
        df_bl.to_pandas(),
        num_thermals,
    )
    return df


def post_resolve_GTER_UTE_entity(
    cls: "type[OperationSynthetizer]",
    df: Optional[pd.DataFrame],
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext] = None,
) -> Optional[pd.DataFrame]:
    """Post-process UTE thermal generation synthesis."""
    if df is None:
        return df
    df = resolve_temporal_resolution_GTER_UTE(cls, df, uow, deck_context)
    df = resolve_starting_stage(df, uow, deck_context)
    return df


def post_resolve_entity(
    cls: "type[OperationSynthetizer]",
    df: Optional[pd.DataFrame],
    s: OperationSynthesis,
    entity_column_values: Dict[str, Any],
    uow: AbstractUnitOfWork,
    internal_stubs: Dict = {},
    deck_context: Optional[DeckContext] = None,
) -> Optional[pl.DataFrame]:
    """Post-process entity extraction from NWLISTOP."""
    if df is None:
        return df
    df_pl = resolve_temporal_resolution(df, uow, deck_context)
    if df_pl is None:
        return None
    if entity_column_values:
        df_pl = df_pl.with_columns(
            [
                pl.lit(val).alias(col)
                for col, val in entity_column_values.items()
            ]
        )
    df_pl = resolve_starting_stage_polars(df_pl, deck_context, uow)
    if s.variable in internal_stubs:
        df_pl = internal_stubs[s.variable](df_pl, uow)
    return df_pl


def post_resolve(
    cls: "type[OperationSynthetizer]",
    resolve_responses: Dict[str, Optional[pl.DataFrame]],
    s: OperationSynthesis,
    uow: AbstractUnitOfWork,
    early_hooks: List[Callable] = [],
    late_hooks: List[Callable] = [],
) -> Optional[pd.DataFrame]:
    """Post-process all synthesis data and consolidate results."""
    with time_and_log(
        message_root="Tempo para compactacao dos dados", logger=cls.logger
    ):
        valid_dfs = [df for df in resolve_responses.values() if df is not None]
        if not valid_dfs:
            return None

        df = pl_to_pd(
            pl.concat(valid_dfs).sort(
                s.spatial_resolution.sorting_synthesis_df_columns,
                maintain_order=True,
            )
        ).reset_index(drop=True)

        for c in early_hooks:
            df = c(s, df, uow)

        entity_columns_order = get_unique_column_values_in_order(
            cls,
            df,
            s.spatial_resolution.sorting_synthesis_df_columns,
        )
        other_columns_order = get_unique_column_values_in_order(
            cls,
            pl_to_pd(valid_dfs[0]),
            s.spatial_resolution.non_entity_sorting_synthesis_df_columns,
        )
        set_ordered_entities(
            cls, s, {**entity_columns_order, **other_columns_order}
        )

        for c in late_hooks:
            df = c(s, df, uow)
    return df
