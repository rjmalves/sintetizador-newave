from typing import Any, Dict

import numpy as np
import pandas as pd

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
    bounds_df: pd.DataFrame,
) -> pd.DataFrame:
    filtro = bounds_df["sentido"] == 1
    (
        bounds_df.loc[filtro, EXCHANGE_SOURCE_CODE_COL],
        bounds_df.loc[filtro, EXCHANGE_TARGET_CODE_COL],
    ) = (
        bounds_df.loc[filtro, EXCHANGE_TARGET_CODE_COL],
        bounds_df.loc[filtro, EXCHANGE_SOURCE_CODE_COL],
    )
    return bounds_df.drop(columns=["sentido"])


def _cast_exchange_bounds_to_MWmes(
    exchange_block_bounds_df: pd.DataFrame,
    exchange_average_bounds_df: pd.DataFrame,
    block_length_df: pd.DataFrame,
) -> pd.DataFrame:
    exchange_block_bounds_df[VALUE_COL] = exchange_block_bounds_df.apply(
        lambda linha: (
            exchange_average_bounds_df.loc[
                (
                    exchange_average_bounds_df[EXCHANGE_SOURCE_CODE_COL]
                    == linha[EXCHANGE_SOURCE_CODE_COL]
                )
                & (
                    exchange_average_bounds_df[EXCHANGE_TARGET_CODE_COL]
                    == linha[EXCHANGE_TARGET_CODE_COL]
                )
                & (
                    exchange_average_bounds_df[START_DATE_COL]
                    == linha[START_DATE_COL]
                ),
                VALUE_COL,
            ].iloc[0]
            * linha[VALUE_COL]
        ),
        axis=1,
    )
    block_length_df = block_length_df.sort_values([START_DATE_COL, BLOCK_COL])
    n_pares_limites = exchange_block_bounds_df.drop_duplicates(
        [EXCHANGE_SOURCE_CODE_COL, EXCHANGE_TARGET_CODE_COL]
    ).shape[0]
    exchange_block_bounds_df[VALUE_COL] *= np.tile(
        block_length_df[VALUE_COL].to_numpy(), n_pares_limites
    )
    return exchange_block_bounds_df


def exchange_block_limits(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    """Load block-level exchange limits (P.U.) from patamar.dat."""

    def _eval_pat0(df_pat: pd.DataFrame) -> pd.DataFrame:
        df_pat_0 = df_pat.loc[df_pat[BLOCK_COL] == 1].copy()
        df_pat_0[BLOCK_COL] = 0
        df_pat_0[VALUE_COL] = 1.0
        df_pat = pd.concat([df_pat, df_pat_0], ignore_index=True)
        df_pat.sort_values(
            [
                EXCHANGE_SOURCE_CODE_COL,
                EXCHANGE_TARGET_CODE_COL,
                START_DATE_COL,
                BLOCK_COL,
            ],
            inplace=True,
        )
        return df_pat

    val = cache.get("exchange_block_limits")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_patamar(deck_cls, uow).intercambio_patamares,
            pd.DataFrame,
            "limites de intercambio em P.U. por patamar (patamar.dat)",
        )
        val = val.rename(
            columns={
                "submercado_de": EXCHANGE_SOURCE_CODE_COL,
                "submercado_para": EXCHANGE_TARGET_CODE_COL,
                "data": START_DATE_COL,
            }
        )
        val = temporal.consider_post_study_years(deck_cls, cache, val, uow)
        val = _eval_pat0(val)
        cache["exchange_block_limits"] = val
    return val.copy()


def exchange_bounds(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    """Load exchange bounds in MWmes from sistema.dat and patamar.dat."""
    val = cache.get("exchange_bounds")
    if val is None:
        exchange_average_bounds_df = readers.validate_data(
            deck_cls,
            readers.get_sistema(deck_cls, uow).limites_intercambio,
            pd.DataFrame,
            "limites de intercambio (sistema.dat)",
        )
        exchange_average_bounds_df = exchange_average_bounds_df.rename(
            columns={
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
        val = val.reset_index(drop=True)
        cache["exchange_bounds"] = val
    return val.copy()
