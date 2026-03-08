from typing import TYPE_CHECKING, List

import pandas as pd
import polars as pl

from app.internal.constants import (
    OPERATION_SYNTHESIS_METADATA_OUTPUT,
    OPERATION_SYNTHESIS_STATS_ROOT,
    SCENARIO_COL,
    STRING_DF_TYPE,
    VARIABLE_COL,
)
from app.model.operation.operationsynthesis import (
    SYNTHESIS_DEPENDENCIES,
    UNITS,
    OperationSynthesis,
)
from app.services.deck.bounds import OperationVariableBounds
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.operations import calc_statistics
from app.utils.timing import time_and_log

if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )


def export_metadata(
    cls: "type[OperationSynthetizer]",
    success_synthesis: List[OperationSynthesis],
    uow: AbstractUnitOfWork,
) -> None:
    metadata_df = pd.DataFrame(
        columns=[
            "chave",
            "nome_curto_variavel",
            "nome_longo_variavel",
            "nome_curto_agregacao",
            "nome_longo_agregacao",
            "unidade",
            "calculado",
            "limitado",
        ]
    )
    for s in success_synthesis:
        metadata_df.loc[metadata_df.shape[0]] = [
            str(s),
            s.variable.short_name,
            s.variable.long_name,
            s.spatial_resolution.value,
            s.spatial_resolution.long_name,
            UNITS[s].value if s in UNITS else "",
            s in SYNTHESIS_DEPENDENCIES,
            OperationVariableBounds.is_bounded(s),
        ]
    with uow:
        existing_df = uow.export.read_df(OPERATION_SYNTHESIS_METADATA_OUTPUT)
        if existing_df is not None:
            metadata_df = pd.concat(
                [existing_df, metadata_df], ignore_index=True
            )
            metadata_df = metadata_df.drop_duplicates()
        uow.export.synthetize_df(
            metadata_df, OPERATION_SYNTHESIS_METADATA_OUTPUT
        )


def add_synthesis_stats(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    df: pl.DataFrame,
) -> None:
    df = df.with_columns(pl.lit(s.variable.value).alias(VARIABLE_COL))

    if s.spatial_resolution not in cls.SYNTHESIS_STATS:
        cls.SYNTHESIS_STATS[s.spatial_resolution] = [df]
    else:
        cls.SYNTHESIS_STATS[s.spatial_resolution].append(df)


def export_scenario_synthesis(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    df: pl.DataFrame,
    uow: AbstractUnitOfWork,
) -> None:
    from app.services.synthesis.operation.cache import store_in_cache_if_needed

    with time_and_log(
        message_root="Tempo para preparacao para exportacao",
        logger=cls.logger,
    ):
        scenarios_pl = df.with_columns(
            pl.col(SCENARIO_COL).cast(pl.Int64)
        ).sort(
            s.spatial_resolution.sorting_synthesis_df_columns,
            maintain_order=True,
        )
        stats_pl = calc_statistics(scenarios_pl)
        add_synthesis_stats(cls, s, stats_pl)
        store_in_cache_if_needed(cls, s, scenarios_pl)
    with time_and_log(
        message_root="Tempo para exportacao dos dados", logger=cls.logger
    ):
        with uow:
            uow.export.synthetize_pl(
                scenarios_pl.select(
                    s.spatial_resolution.all_synthesis_df_columns
                ),
                str(s),
            )


def export_stats(
    cls: "type[OperationSynthetizer]",
    uow: AbstractUnitOfWork,
) -> None:
    for res, dfs in cls.SYNTHESIS_STATS.items():
        with time_and_log(
            message_root="Tempo para exportacao"
            + f" das estatisticas de {res.value}",
            logger=cls.logger,
        ):
            with uow:
                df = pl.concat(dfs)
                df = df.select([VARIABLE_COL] + res.all_synthesis_df_columns)
                df = df.cast({VARIABLE_COL: pl.Utf8})
                df = df.sort([VARIABLE_COL] + res.sorting_synthesis_df_columns)
                stats_filename = f"{OPERATION_SYNTHESIS_STATS_ROOT}_{res.value}"
                existing_df = uow.export.read_df(stats_filename)
                if existing_df is not None:
                    df = pl.concat([pl.from_pandas(existing_df), df])
                    df = df.unique()
                uow.export.synthetize_pl(df, stats_filename)
