from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict

import polars as pl
from inewave.newave.modelos.modif import (
    CFUGA,
    CMONT,
    NUMCNJ,
    NUMMAQ,
    TURBMAXT,
    TURBMINT,
    VAZMIN,
    VAZMINT,
    VMAXT,
    VMINT,
    VOLMAX,
    VOLMIN,
)

if TYPE_CHECKING:
    import pandas as pd

from app.internal.constants import (
    BLOCK_COL,
    HEIGHT_POLY_COLS,
    HYDRO_CODE_COL,
    LOSS_COL,
    LOSS_KIND_COL,
    LOWER_BOUND_COL,
    LOWER_BOUND_UNIT_COL,
    LOWER_DROP_COL,
    RUN_OF_RIVER_REFERENCE_VOLUME_COL,
    SPEC_PRODUCTIVITY_COL,
    START_DATE_COL,
    UPPER_BOUND_COL,
    UPPER_BOUND_UNIT_COL,
    VOLUME_REGULATION_COL,
)
from app.model.operation.unit import Unit
from app.services.deck import accessors, entities, readers, temporal
from app.services.unitofwork import AbstractUnitOfWork


def _calc_turbined_flow_expr(max_conjuntos: int) -> pl.Expr:
    """Build a polars expression that sums num_machines * flow across all grupos."""
    terms = [
        pl.col(f"maquinas_conjunto_{i}").fill_null(0)
        * pl.col(f"vazao_nominal_conjunto_{i}").fill_null(0)
        for i in range(1, max_conjuntos + 1)
    ]
    return sum(terms)  # type: ignore[return-value]


def _max_conjuntos_from_df(df: pl.DataFrame) -> int:
    """Determine max number of conjuntos from DataFrame column names."""
    i = 1
    while f"maquinas_conjunto_{i}" in df.columns:
        i += 1
    return i - 1


def hydro_volume_bounds(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("hydro_volume_bounds")
    if val is None:
        hidr_df = accessors.hidr(deck_cls, cache, uow)
        hydro_codes = entities.hydro_code_order(deck_cls, cache, uow)
        df = hidr_df.filter(pl.col(HYDRO_CODE_COL).is_in(hydro_codes)).select(
            [HYDRO_CODE_COL, "volume_minimo", "volume_maximo"]
        )
        df = df.rename(
            {
                "volume_minimo": LOWER_BOUND_COL,
                "volume_maximo": UPPER_BOUND_COL,
            }
        )
        df = df.with_columns(
            [
                pl.lit(Unit.hm3_modif.value).alias(LOWER_BOUND_UNIT_COL),
                pl.lit(Unit.hm3_modif.value).alias(UPPER_BOUND_UNIT_COL),
            ]
        )
        map_df = entities.hydro_eer_submarket_map(deck_cls, cache, uow)
        val = df.join(map_df, on=HYDRO_CODE_COL)
        cache["hydro_volume_bounds"] = val
    return val


def hydro_volume_bounds_with_changes(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("hydro_volume_bounds_with_changes")
    if val is None:
        hm3_df = hydro_volume_bounds(deck_cls, cache, uow)
        # convert to pandas for readers boundary
        df_pd: pd.DataFrame = hm3_df.to_pandas().set_index(HYDRO_CODE_COL)
        df_pd = readers.apply_modif_changes_to_hydros(
            deck_cls,
            cache,
            df_pd,
            LOWER_BOUND_COL,
            LOWER_BOUND_UNIT_COL,
            VOLMIN,
            uow,
        )
        df_pd = readers.apply_modif_changes_to_hydros(
            deck_cls,
            cache,
            df_pd,
            UPPER_BOUND_COL,
            UPPER_BOUND_UNIT_COL,
            VOLMAX,
            uow,
        )
        df_pd = df_pd.reset_index()
        df = pl.from_pandas(df_pd)
        # Cast percentage bounds to hm3 using vectorized polars expressions
        hm3_ref = hm3_df.select(
            [HYDRO_CODE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL]
        ).rename(
            {
                LOWER_BOUND_COL: "_hm3_lower",
                UPPER_BOUND_COL: "_hm3_upper",
            }
        )
        df = df.join(hm3_ref, on=HYDRO_CODE_COL)
        for col, unit_col in zip(
            [LOWER_BOUND_COL, UPPER_BOUND_COL],
            [LOWER_BOUND_UNIT_COL, UPPER_BOUND_UNIT_COL],
        ):
            df = df.with_columns(
                pl.when(pl.col(unit_col) == Unit.perc_modif.value)
                .then(
                    pl.col(col)
                    * (pl.col("_hm3_upper") - pl.col("_hm3_lower"))
                    / 100.0
                    + pl.col("_hm3_lower")
                )
                .otherwise(pl.col(col))
                .alias(col)
            )
            df = df.with_columns(
                pl.when(pl.col(unit_col) == Unit.perc_modif.value)
                .then(pl.lit(Unit.hm3_modif.value))
                .otherwise(pl.col(unit_col))
                .alias(unit_col)
            )
        df = df.drop(["_hm3_lower", "_hm3_upper"])
        val = df
        cache["hydro_volume_bounds_with_changes"] = val
    return val


def _hydro_volume_bounds_in_stages(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
    consider_lower_changes: bool = True,
) -> pl.DataFrame:
    hm3_df = hydro_volume_bounds_with_changes(deck_cls, cache, uow)
    dates = temporal.stages_starting_dates_final_simulation(
        deck_cls, cache, uow
    )
    dates_df = pl.DataFrame({START_DATE_COL: dates})
    hm3_expanded = hm3_df.join(dates_df, how="cross").sort(
        [HYDRO_CODE_COL, START_DATE_COL]
    )
    # convert to pandas for readers boundary
    df_pd: pd.DataFrame = hm3_expanded.to_pandas()
    if consider_lower_changes:
        df_pd = readers.apply_modif_changes_to_hydros_in_stages(
            deck_cls,
            cache,
            df_pd,
            LOWER_BOUND_COL,
            LOWER_BOUND_UNIT_COL,
            VMINT,
            uow,
        )
    df_pd = readers.apply_modif_changes_to_hydros_in_stages(
        deck_cls,
        cache,
        df_pd,
        UPPER_BOUND_COL,
        UPPER_BOUND_UNIT_COL,
        VMAXT,
        uow,
    )
    df = pl.from_pandas(df_pd)
    # Cast percentage bounds to hm3 using join on [HYDRO_CODE_COL, START_DATE_COL]
    hm3_ref = hm3_expanded.select(
        [HYDRO_CODE_COL, START_DATE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL]
    ).rename(
        {
            LOWER_BOUND_COL: "_hm3_lower",
            UPPER_BOUND_COL: "_hm3_upper",
        }
    )
    df = df.join(hm3_ref, on=[HYDRO_CODE_COL, START_DATE_COL])
    for col, unit_col in zip(
        [LOWER_BOUND_COL, UPPER_BOUND_COL],
        [LOWER_BOUND_UNIT_COL, UPPER_BOUND_UNIT_COL],
    ):
        df = df.with_columns(
            pl.when(pl.col(unit_col) == Unit.perc_modif.value)
            .then(
                pl.col(col)
                * (pl.col("_hm3_upper") - pl.col("_hm3_lower"))
                / 100.0
                + pl.col("_hm3_lower")
            )
            .otherwise(pl.col(col))
            .alias(col)
        )
        df = df.with_columns(
            pl.when(pl.col(unit_col) == Unit.perc_modif.value)
            .then(pl.lit(Unit.hm3_modif.value))
            .otherwise(pl.col(unit_col))
            .alias(unit_col)
        )
    df = df.drop(["_hm3_lower", "_hm3_upper"])
    return df


def hydro_volume_bounds_in_stages(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("hydro_volume_bounds_in_stages")
    if val is None:
        val = _hydro_volume_bounds_in_stages(deck_cls, cache, uow, True)
        cache["hydro_volume_bounds_in_stages"] = val
    return val


def hydro_volume_bounds_in_stages_for_rescaling(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("hydro_volume_bounds_in_stages_for_rescaling")
    if val is None:
        val = _hydro_volume_bounds_in_stages(deck_cls, cache, uow, False)
        cache["hydro_volume_bounds_in_stages_for_rescaling"] = val
    return val


def _apply_turbined_flow_changes_pandas(
    df_pd: "pd.DataFrame",
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> "pd.DataFrame":
    """Apply NUMCNJ/NUMMAQ modif changes on a pandas DataFrame (indexed by HYDRO_CODE_COL)."""
    modif = accessors.modif(deck_cls, cache, uow)
    for idx in df_pd.index:
        changes = modif.modificacoes_usina(idx)
        if changes is not None:
            ngrp = [r for r in changes if isinstance(r, NUMCNJ)]
            if ngrp:
                df_pd.at[idx, "numero_conjuntos_maquinas"] = ngrp[-1].numero
            for r in [r for r in changes if isinstance(r, NUMMAQ)]:
                df_pd.at[idx, f"maquinas_conjunto_{r.conjunto}"] = (
                    r.numero_maquinas
                )
    return df_pd


def hydro_turbined_flow_bounds(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("hydro_turbined_flow_bounds")
    if val is None:
        hidr_df = accessors.hidr(deck_cls, cache, uow)
        hydro_codes = entities.hydro_code_order(deck_cls, cache, uow)
        df = hidr_df.filter(pl.col(HYDRO_CODE_COL).is_in(hydro_codes))
        max_conjuntos = _max_conjuntos_from_df(df)
        expr = _calc_turbined_flow_expr(max_conjuntos)
        df = df.with_columns(
            [
                expr.cast(pl.Float64).alias(UPPER_BOUND_COL),
                pl.lit(0.0).alias(LOWER_BOUND_COL),
            ]
        )
        df = df.select(
            [HYDRO_CODE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL]
        ).with_columns(
            [
                pl.lit(Unit.m3s.value).alias(LOWER_BOUND_UNIT_COL),
                pl.lit(Unit.m3s.value).alias(UPPER_BOUND_UNIT_COL),
            ]
        )
        map_df = entities.hydro_eer_submarket_map(deck_cls, cache, uow)
        val = df.join(map_df, on=HYDRO_CODE_COL)
        cache["hydro_turbined_flow_bounds"] = val
    return val


def hydro_turbined_flow_bounds_with_changes(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("hydro_turbined_flow_bounds_with_changes")
    if val is None:
        hidr_df = accessors.hidr(deck_cls, cache, uow)
        hydro_codes = entities.hydro_code_order(deck_cls, cache, uow)
        df_all = hidr_df.filter(pl.col(HYDRO_CODE_COL).is_in(hydro_codes))
        # Apply NUMCNJ/NUMMAQ changes via pandas boundary (cell-level mutation)
        df_pd: pd.DataFrame = df_all.to_pandas().set_index(HYDRO_CODE_COL)
        df_pd = _apply_turbined_flow_changes_pandas(df_pd, deck_cls, cache, uow)
        df_pd = df_pd.reset_index()
        df = pl.from_pandas(df_pd)
        max_conjuntos = _max_conjuntos_from_df(df)
        expr = _calc_turbined_flow_expr(max_conjuntos)
        df = df.with_columns(
            [
                expr.cast(pl.Float64).alias(UPPER_BOUND_COL),
                pl.lit(0.0).alias(LOWER_BOUND_COL),
            ]
        )
        df = df.select(
            [HYDRO_CODE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL]
        ).with_columns(
            [
                pl.lit(Unit.m3s.value).alias(LOWER_BOUND_UNIT_COL),
                pl.lit(Unit.m3s.value).alias(UPPER_BOUND_UNIT_COL),
            ]
        )
        map_df = entities.hydro_eer_submarket_map(deck_cls, cache, uow)
        val = df.join(map_df, on=HYDRO_CODE_COL)
        cache["hydro_turbined_flow_bounds_with_changes"] = val
    return val


def hydro_turbined_flow_bounds_in_stages(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("hydro_turbined_flow_bounds_in_stages")
    if val is None:
        from app.services.deck import misc as misc_mod

        m3s_df = hydro_turbined_flow_bounds_with_changes(deck_cls, cache, uow)
        m3s_df = _expand_hydro_to_stages(m3s_df, deck_cls, cache, uow)
        # convert to pandas for readers boundary
        df_pd: pd.DataFrame = m3s_df.to_pandas()
        df_pd = readers.apply_modif_changes_to_hydros_in_stages(
            deck_cls,
            cache,
            df_pd,
            LOWER_BOUND_COL,
            LOWER_BOUND_UNIT_COL,
            TURBMINT,
            uow,
        )
        df_pd = readers.apply_modif_changes_to_hydros_in_stages(
            deck_cls,
            cache,
            df_pd,
            UPPER_BOUND_COL,
            UPPER_BOUND_UNIT_COL,
            TURBMAXT,
            uow,
        )
        m3s_df = pl.from_pandas(df_pd)
        m3s_df = _expand_to_blocks(m3s_df, deck_cls, cache, uow, misc_mod)
        val = m3s_df
        cache["hydro_turbined_flow_bounds_in_stages"] = val
    return val


def hydro_outflow_bounds(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("hydro_outflow_bounds")
    if val is None:
        hidr_df = accessors.hidr(deck_cls, cache, uow)
        hydro_codes = entities.hydro_code_order(deck_cls, cache, uow)
        df = hidr_df.filter(pl.col(HYDRO_CODE_COL).is_in(hydro_codes)).select(
            [HYDRO_CODE_COL, "vazao_minima_historica"]
        )
        df = df.rename({"vazao_minima_historica": LOWER_BOUND_COL})
        df = df.with_columns(pl.col(LOWER_BOUND_COL).cast(pl.Float64))
        df = df.with_columns(
            [
                pl.lit(float("inf")).alias(UPPER_BOUND_COL),
                pl.lit(Unit.m3s.value).alias(LOWER_BOUND_UNIT_COL),
                pl.lit(Unit.m3s.value).alias(UPPER_BOUND_UNIT_COL),
            ]
        )
        map_df = entities.hydro_eer_submarket_map(deck_cls, cache, uow)
        val = df.join(map_df, on=HYDRO_CODE_COL)
        cache["hydro_outflow_bounds"] = val
    return val


def hydro_outflow_bounds_with_changes(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("hydro_outflow_bounds_with_changes")
    if val is None:
        # TODO - analisar modif.dat
        base_df = hydro_outflow_bounds(deck_cls, cache, uow)
        df_pd: pd.DataFrame = base_df.to_pandas().set_index(HYDRO_CODE_COL)
        df_pd = readers.apply_modif_changes_to_hydros(
            deck_cls,
            cache,
            df_pd,
            LOWER_BOUND_COL,
            LOWER_BOUND_UNIT_COL,
            VAZMIN,
            uow,
        )
        df_pd = df_pd.reset_index()
        val = pl.from_pandas(df_pd)
        cache["hydro_outflow_bounds_with_changes"] = val
    return val


def hydro_outflow_bounds_in_stages(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("hydro_outflow_bounds_in_stages")
    if val is None:
        from app.services.deck import misc as misc_mod

        m3s_df = hydro_outflow_bounds_with_changes(deck_cls, cache, uow)
        m3s_df = _expand_hydro_to_stages(m3s_df, deck_cls, cache, uow)
        df_pd: pd.DataFrame = m3s_df.to_pandas()
        df_pd = readers.apply_modif_changes_to_hydros_in_stages(
            deck_cls,
            cache,
            df_pd,
            LOWER_BOUND_COL,
            LOWER_BOUND_UNIT_COL,
            VAZMINT,
            uow,
        )
        m3s_df = pl.from_pandas(df_pd)
        num_blocks = misc_mod.num_blocks(deck_cls, cache, uow) + 1
        dates_block = pl.DataFrame({BLOCK_COL: list(range(num_blocks))})
        m3s_df = m3s_df.join(dates_block, how="cross")
        m3s_df = m3s_df.sort([HYDRO_CODE_COL, START_DATE_COL, BLOCK_COL])
        val = m3s_df
        cache["hydro_outflow_bounds_in_stages"] = val
    return val


def hydro_drops(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("hydro_drops")
    if val is None:
        hidr_df = accessors.hidr(deck_cls, cache, uow)
        hydro_codes = entities.hydro_code_order(deck_cls, cache, uow)
        cols = (
            [HYDRO_CODE_COL]
            + HEIGHT_POLY_COLS
            + [
                LOWER_DROP_COL,
                LOSS_KIND_COL,
                LOSS_COL,
                VOLUME_REGULATION_COL,
                RUN_OF_RIVER_REFERENCE_VOLUME_COL,
                SPEC_PRODUCTIVITY_COL,
            ]
        )
        df = hidr_df.filter(pl.col(HYDRO_CODE_COL).is_in(hydro_codes)).select(
            cols
        )
        map_df = entities.hydro_eer_submarket_map(deck_cls, cache, uow)
        val = df.join(map_df, on=HYDRO_CODE_COL)
        cache["hydro_drops"] = val
    return val


def hydro_drops_in_stages(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("hydro_drops_in_stages")
    if val is None:
        df = hydro_drops(deck_cls, cache, uow)
        df = _expand_hydro_to_stages(df, deck_cls, cache, uow)
        df_pd: pd.DataFrame = df.to_pandas()
        df_pd = readers.apply_modif_changes_to_hydros_in_stages(
            deck_cls,
            cache,
            df_pd,
            HEIGHT_POLY_COLS[0],
            LOWER_BOUND_UNIT_COL,
            CMONT,
            uow,
        )
        df_pd = readers.apply_modif_changes_to_hydros_in_stages(
            deck_cls,
            cache,
            df_pd,
            LOWER_DROP_COL,
            UPPER_BOUND_UNIT_COL,
            CFUGA,
            uow,
        )
        val = pl.from_pandas(df_pd)
        cache["hydro_drops_in_stages"] = val
    return val


def _expand_hydro_to_stages(
    df: pl.DataFrame,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    dates = temporal.stages_starting_dates_final_simulation(
        deck_cls, cache, uow
    )
    dates_df = pl.DataFrame({START_DATE_COL: dates})
    expanded = df.join(dates_df, how="cross")
    return expanded.sort([HYDRO_CODE_COL, START_DATE_COL])


def _expand_to_blocks(
    df: pl.DataFrame,
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
    misc_mod: Any,
) -> pl.DataFrame:
    num_blocks = misc_mod.num_blocks(deck_cls, cache, uow) + 1
    blocks_df = pl.DataFrame({BLOCK_COL: list(range(num_blocks))})
    expanded = df.join(blocks_df, how="cross")
    return expanded.sort([HYDRO_CODE_COL, START_DATE_COL, BLOCK_COL])
