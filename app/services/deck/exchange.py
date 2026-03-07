from __future__ import annotations

from typing import Any, Dict

import polars as pl

from app.internal.constants import (
    BLOCK_COL,
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_TARGET_CODE_COL,
    START_DATE_COL,
    VALUE_COL,
)
from app.services.deck import misc, readers, temporal
from app.services.unitofwork import AbstractUnitOfWork


def _drops_exchange_direction_flag(
    bounds_df: pl.DataFrame,
) -> pl.DataFrame:
    return bounds_df.with_columns(
        [
            pl.when(pl.col("sentido") == 1)
            .then(pl.col(EXCHANGE_TARGET_CODE_COL))
            .otherwise(pl.col(EXCHANGE_SOURCE_CODE_COL))
            .alias(EXCHANGE_SOURCE_CODE_COL),
            pl.when(pl.col("sentido") == 1)
            .then(pl.col(EXCHANGE_SOURCE_CODE_COL))
            .otherwise(pl.col(EXCHANGE_TARGET_CODE_COL))
            .alias(EXCHANGE_TARGET_CODE_COL),
        ]
    ).drop("sentido")


def _cast_exchange_bounds_to_MWmes(
    exchange_block_bounds_df: pl.DataFrame,
    exchange_average_bounds_df: pl.DataFrame,
    block_length_df: pl.DataFrame,
) -> pl.DataFrame:
    avg_lookup = exchange_average_bounds_df.select(
        [
            EXCHANGE_SOURCE_CODE_COL,
            EXCHANGE_TARGET_CODE_COL,
            START_DATE_COL,
            pl.col(VALUE_COL).alias("_avg_value"),
        ]
    )
    exchange_block_bounds_df = (
        exchange_block_bounds_df.join(
            avg_lookup,
            on=[
                EXCHANGE_SOURCE_CODE_COL,
                EXCHANGE_TARGET_CODE_COL,
                START_DATE_COL,
            ],
            how="left",
        )
        .with_columns(
            (pl.col(VALUE_COL) * pl.col("_avg_value")).alias(VALUE_COL)
        )
        .drop("_avg_value")
    )

    block_lengths_lookup = block_length_df.sort(
        [START_DATE_COL, BLOCK_COL]
    ).select([START_DATE_COL, BLOCK_COL, pl.col(VALUE_COL).alias("_block_len")])
    exchange_block_bounds_df = (
        exchange_block_bounds_df.join(
            block_lengths_lookup,
            on=[START_DATE_COL, BLOCK_COL],
            how="left",
        )
        .with_columns(
            (pl.col(VALUE_COL) * pl.col("_block_len")).alias(VALUE_COL)
        )
        .drop("_block_len")
    )

    return exchange_block_bounds_df


def exchange_block_limits(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    def _eval_pat0(df_pat: pl.DataFrame) -> pl.DataFrame:
        df_pat_0 = df_pat.filter(pl.col(BLOCK_COL) == 1).with_columns(
            [
                pl.lit(0).cast(df_pat.schema[BLOCK_COL]).alias(BLOCK_COL),
                pl.lit(1.0).alias(VALUE_COL),
            ]
        )
        df_pat = pl.concat([df_pat, df_pat_0]).sort(
            [
                EXCHANGE_SOURCE_CODE_COL,
                EXCHANGE_TARGET_CODE_COL,
                START_DATE_COL,
                BLOCK_COL,
            ]
        )
        return df_pat

    val = cache.get("exchange_block_limits")
    if val is None:
        raw = readers.validate_data(
            deck_cls,
            readers.get_patamar(deck_cls, uow).intercambio_patamares,
            object,
            "limites de intercambio em P.U. por patamar (patamar.dat)",
        )
        val = pl.from_pandas(raw).rename(
            {
                "submercado_de": EXCHANGE_SOURCE_CODE_COL,
                "submercado_para": EXCHANGE_TARGET_CODE_COL,
                "data": START_DATE_COL,
            }
        )
        val = temporal.consider_post_study_years(deck_cls, cache, val, uow)
        val = _eval_pat0(val)
        cache["exchange_block_limits"] = val
    return val


def exchange_bounds(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("exchange_bounds")
    if val is None:
        raw = readers.validate_data(
            deck_cls,
            readers.get_sistema(deck_cls, uow).limites_intercambio,
            object,
            "limites de intercambio (sistema.dat)",
        )
        exchange_average_bounds_df = pl.from_pandas(raw).rename(
            {
                "submercado_de": EXCHANGE_SOURCE_CODE_COL,
                "submercado_para": EXCHANGE_TARGET_CODE_COL,
                "data": START_DATE_COL,
            }
        )
        exchange_average_bounds_df = _drops_exchange_direction_flag(
            exchange_average_bounds_df
        )
        exchange_block_bounds_df = exchange_block_limits(deck_cls, cache, uow)
        exchange_average_bounds_df = temporal.consider_post_study_years(
            deck_cls, cache, exchange_average_bounds_df, uow
        )
        block_length_df = misc.block_lengths(deck_cls, cache, uow)
        val = _cast_exchange_bounds_to_MWmes(
            exchange_block_bounds_df,
            exchange_average_bounds_df,
            block_length_df,
        )
        cache["exchange_bounds"] = val
    return val
