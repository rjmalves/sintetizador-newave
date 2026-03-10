import sys as _sys
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
from app.utils.timing import time_and_log


def _pkg() -> Any:
    return _sys.modules[__package__]


if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )


def _fetch_temporal_deck_data(
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext],
    num_stages: int,
) -> Any:
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
    df: pl.DataFrame,
    cols: List[str],
) -> Dict[str, List[Any]]:
    return {col: df[col].unique(maintain_order=True).to_list() for col in cols}


def set_ordered_entities(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    entities: Dict[str, List[Any]],
) -> None:
    cls.ORDERED_SYNTHESIS_ENTITIES[s] = entities


def get_ordered_entities(
    cls: "type[OperationSynthetizer]", s: OperationSynthesis
) -> Dict[str, List[Any]]:
    return cls.ORDERED_SYNTHESIS_ENTITIES[s]


def resolve_temporal_resolution(
    df: "Optional[pd.DataFrame | pl.DataFrame]",
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext] = None,
) -> Optional[pl.DataFrame]:
    if df is None:
        return None
    if isinstance(df, pl.DataFrame):
        # Already polars — rename polars-style if legacy column names present
        rename_map = {}
        if "data" in df.columns:
            rename_map["data"] = START_DATE_COL
        if "serie" in df.columns:
            rename_map["serie"] = SCENARIO_COL
        pl_df = df.rename(rename_map) if rename_map else df
    else:
        pl_df = pl.from_pandas(
            df.rename(columns={"data": START_DATE_COL, "serie": SCENARIO_COL})
        )
    pl_df = pl_df.sort(
        [START_DATE_COL, SCENARIO_COL, BLOCK_COL], maintain_order=True
    )
    num_stages = pl_df[START_DATE_COL].n_unique()
    blocks = pl_df[BLOCK_COL].unique(maintain_order=True).to_list()
    num_blocks = len(blocks)
    _, _start_dates, end_dates, df_block_lengths = _fetch_temporal_deck_data(
        uow, deck_context, num_stages
    )
    num_sc = pl_df.height // (num_blocks * num_stages)
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
                np.repeat(np.arange(1, num_stages + 1), num_sc * num_blocks),
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


def resolve_starting_stage_polars(
    df: pl.DataFrame,
    deck_context: Optional[DeckContext],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    starting_month = (
        deck_context.study_period_starting_month
        if deck_context is not None
        else _pkg().Deck.study_period_starting_month(uow)
    )
    return df.with_columns(
        (pl.col(STAGE_COL) - (starting_month - 1)).alias(STAGE_COL)
    ).filter(pl.col(STAGE_COL) > 0)


def initial_stored_energy_df(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    max_column = "earmax"
    _Deck = _pkg().Deck
    _raw = _Deck.initial_stored_energy(uow)
    df = _raw if isinstance(_raw, pl.DataFrame) else pl.from_pandas(_raw)
    df = df.with_columns(pl.Series(EER_CODE_COL, _Deck.eer_code_order(uow)))
    if s.spatial_resolution == SpatialResolution.RESERVATORIO_EQUIVALENTE:
        return df.rename({EER_CODE_COL: GROUPING_TMP_COL})

    df = df.with_columns(
        (
            pl.lit(100) * pl.col("valor_MWmes") / pl.col("valor_percentual")
        ).alias(max_column)
    )
    if s.spatial_resolution == SpatialResolution.SUBMERCADO:
        eer_sbm_map = _Deck.eer_submarket_map(uow).select(
            [EER_CODE_COL, SUBMARKET_CODE_COL]
        )
        df = df.drop_nulls()
        df = df.join(eer_sbm_map, on=EER_CODE_COL, how="left")
        df = df.rename({SUBMARKET_CODE_COL: GROUPING_TMP_COL})
    elif s.spatial_resolution == SpatialResolution.SISTEMA_INTERLIGADO:
        df = df.with_columns(pl.lit(1).alias(GROUPING_TMP_COL))
    df = df.group_by(GROUPING_TMP_COL, maintain_order=True).agg(
        [
            pl.col("valor_MWmes").sum(),
            pl.col(max_column).sum(),
        ]
    )
    df = df.with_columns(
        (pl.lit(100) * pl.col("valor_MWmes") / pl.col(max_column)).alias(
            "valor_percentual"
        )
    )
    return df


def generate_scenarios(
    cls: "type[OperationSynthetizer]",
    df: pl.DataFrame,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    num_scenarios = _pkg().Deck.num_scenarios_final_simulation(uow)
    num_entries = df.shape[0]
    df = pl.concat([df] * num_scenarios)
    df = df.with_columns(
        pl.Series(
            "serie", np.repeat(np.arange(1, num_scenarios + 1), num_entries)
        )
    )
    return df


def resolve_temporal_resolution_GTER_UTE(
    cls: "type[OperationSynthetizer]",
    df: Optional[pd.DataFrame],
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext] = None,
) -> Optional[pl.DataFrame]:
    if df is None:
        return None

    pl_df = pl.from_pandas(
        df.rename(
            columns={
                "data": START_DATE_COL,
                "serie": SCENARIO_COL,
                "classe": THERMAL_CODE_COL,
            }
        )
    ).sort(
        [THERMAL_CODE_COL, START_DATE_COL, SCENARIO_COL, BLOCK_COL],
        maintain_order=True,
    )

    num_stages = pl_df[START_DATE_COL].n_unique()
    blocks = pl_df[BLOCK_COL].unique(maintain_order=True).to_list()
    num_blocks = len(blocks)
    thermals = pl_df[THERMAL_CODE_COL].unique(maintain_order=True).to_list()
    num_thermals = len(thermals)

    _, _start_dates, end_dates, df_block_lengths = _fetch_temporal_deck_data(
        uow, deck_context, num_stages
    )
    num_sc = pl_df.height // (num_blocks * num_stages * num_thermals)

    pl_df = pl_df.with_columns(
        pl.Series(
            SCENARIO_COL,
            np.tile(
                np.tile(
                    np.repeat(np.arange(1, num_sc + 1), num_blocks),
                    num_stages,
                ),
                num_thermals,
            ),
        )
    )
    pl_df = pl_df.with_columns(
        [
            pl.Series(
                STAGE_COL,
                np.tile(
                    np.repeat(
                        np.arange(1, num_stages + 1), num_sc * num_blocks
                    ),
                    num_thermals,
                ),
            ),
            pl.Series(
                END_DATE_COL,
                np.tile(
                    np.repeat(
                        np.array(end_dates, dtype="datetime64[ms]"),
                        num_sc * num_blocks,
                    ),
                    num_thermals,
                ),
            ),
        ]
    )
    bl_pl = df_block_lengths.filter(pl.col(BLOCK_COL).is_in(blocks)).rename(
        {VALUE_COL: BLOCK_DURATION_COL}
    )
    pl_df = pl_df.join(bl_pl, on=[START_DATE_COL, BLOCK_COL], how="left")
    pl_df = pl_df.with_columns(
        (pl.col(BLOCK_DURATION_COL) * STAGE_DURATION_HOURS).alias(
            BLOCK_DURATION_COL
        )
    )
    return pl_df


def post_resolve_GTER_UTE_entity(
    cls: "type[OperationSynthetizer]",
    df: Optional[pd.DataFrame],
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext] = None,
) -> Optional[pl.DataFrame]:
    if df is None:
        return df
    pl_df = resolve_temporal_resolution_GTER_UTE(cls, df, uow, deck_context)
    if pl_df is None:
        return None
    pl_df = resolve_starting_stage_polars(pl_df, deck_context, uow)
    return pl_df


def post_resolve_entity(
    cls: "type[OperationSynthetizer]",
    df: "Optional[pd.DataFrame | pl.DataFrame]",
    s: OperationSynthesis,
    entity_column_values: Dict[str, Any],
    uow: AbstractUnitOfWork,
    internal_stubs: Dict[Any, Any] = {},
    deck_context: Optional[DeckContext] = None,
) -> Optional[pl.DataFrame]:
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
    early_hooks: List[Callable[..., Any]] = [],
    late_hooks: List[Callable[..., Any]] = [],
) -> Optional[pl.DataFrame]:
    with time_and_log(
        message_root="Tempo para compactacao dos dados", logger=cls.logger
    ):
        valid_dfs = [df for df in resolve_responses.values() if df is not None]
        if not valid_dfs:
            return None

        df = pl.concat(valid_dfs).sort(
            s.spatial_resolution.sorting_synthesis_df_columns,
            maintain_order=True,
        )

        for c in early_hooks:
            df = c(s, df, uow)

        entity_columns_order = get_unique_column_values_in_order(
            cls,
            df,
            s.spatial_resolution.sorting_synthesis_df_columns,
        )
        other_columns_order = get_unique_column_values_in_order(
            cls,
            valid_dfs[0],
            s.spatial_resolution.non_entity_sorting_synthesis_df_columns,
        )
        set_ordered_entities(
            cls, s, {**entity_columns_order, **other_columns_order}
        )

        for c in late_hooks:
            df = c(s, df, uow)
    return df
