from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np
import polars as pl

from app.internal.constants import (
    EER_CODE_COL,
    EER_NAME_COL,
    FOLLOWING_HYDRO_COL,
    HEIGHT_POLY_COLS,
    HM3_M3S_MONTHLY_FACTOR,
    HYDRO_CODE_COL,
    HYDRO_NAME_COL,
    LOSS_COL,
    LOSS_KIND_COL,
    LOWER_BOUND_COL,
    LOWER_DROP_COL,
    NET_DROP_COL,
    PRODUCTIVITY_TMP_COL,
    RUN_OF_RIVER_REFERENCE_VOLUME_COL,
    SPEC_PRODUCTIVITY_COL,
    START_DATE_COL,
    UPPER_BOUND_COL,
    UPPER_DROP_COL,
    VOLUME_FOR_PRODUCTIVITY_TMP_COL,
    VOLUME_REGULATION_COL,
)
from app.services.deck import accessors, entities, readers, temporal
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.graph import Graph


def evaluate_productivity(
    df: pl.DataFrame,
    volume_col: str = VOLUME_FOR_PRODUCTIVITY_TMP_COL,
) -> pl.DataFrame:
    """Evaluate hydraulic productivity for each plant row."""
    n = df.shape[0]

    coefs = [df[c].to_numpy() for c in HEIGHT_POLY_COLS]
    vol_array = df[volume_col].fill_null(0.0).to_numpy()
    reg_array = df[VOLUME_REGULATION_COL].to_numpy()
    lower_array = df[LOWER_BOUND_COL].to_numpy()
    upper_array = df[UPPER_BOUND_COL].to_numpy()
    ror_ref_array = df[RUN_OF_RIVER_REFERENCE_VOLUME_COL].to_numpy()

    upper_drop = np.empty(n, dtype=np.float64)
    for i in range(n):
        row_coefs = [coefs[j][i] for j in range(len(HEIGHT_POLY_COLS))]
        if reg_array[i] == "M":
            # Regulated plant: integrate polynomial over [min_v, vol] and divide
            coefs_integral = [0.0] + [
                c / (k + 1) for k, c in enumerate(row_coefs)
            ]
            min_v = lower_array[i]
            max_v = upper_array[i]
            net_v = max_v - min_v
            pct_v = vol_array[i] / net_v if net_v > 0 else 0.0
            rev_int = list(reversed(coefs_integral))
            min_int = float(np.polyval(rev_int, min_v))
            max_int = float(np.polyval(rev_int, pct_v * net_v + min_v))
            denom = pct_v * net_v
            upper_drop[i] = (max_int - min_int) / denom if denom > 0 else 0.0
        else:
            upper_drop[i] = float(
                np.polyval(list(reversed(row_coefs)), ror_ref_array[i])
            )

    df = df.with_columns(pl.Series(UPPER_DROP_COL, upper_drop))
    df = df.with_columns(
        (pl.col(UPPER_DROP_COL) - pl.col(LOWER_DROP_COL)).alias(NET_DROP_COL)
    )
    df = df.with_columns(pl.col(volume_col).fill_null(0.0).alias(volume_col))
    loss_kind_array = df[LOSS_KIND_COL].to_numpy()
    loss_array = df[LOSS_COL].to_numpy()
    net_drop_array = df[NET_DROP_COL].to_numpy()
    spec_prod_array = df[SPEC_PRODUCTIVITY_COL].to_numpy()

    productivity = np.empty(n, dtype=np.float64)
    for i in range(n):
        if loss_kind_array[i] == 1:
            adjusted_drop = net_drop_array[i] * (1.0 - loss_array[i])
        elif loss_kind_array[i] == 2:
            adjusted_drop = net_drop_array[i] - loss_array[i]
        else:
            adjusted_drop = net_drop_array[i]
        productivity[i] = spec_prod_array[i] * adjusted_drop

    productivity *= HM3_M3S_MONTHLY_FACTOR
    df = df.with_columns(pl.Series(PRODUCTIVITY_TMP_COL, productivity))
    return df


def accumulate_productivity(df: pl.DataFrame) -> pl.DataFrame:
    """BFS-based cascade accumulation of productivity values."""
    codes: list[int] = df[HYDRO_CODE_COL].to_list()
    following: list[int] = df[FOLLOWING_HYDRO_COL].to_list()
    prod: list[float] = df[PRODUCTIVITY_TMP_COL].to_list()
    code_to_idx: dict[int, int] = {c: i for i, c in enumerate(codes)}

    edges = list(zip(following, codes))
    bfs = Graph(edges, directed=True).bfs(0)[1:]
    for hydro_code in bfs:
        idx = code_to_idx.get(hydro_code)
        if idx is None:
            continue
        downstream = following[idx]
        if downstream == 0:
            continue
        d_idx = code_to_idx.get(downstream)
        if d_idx is None:
            continue
        prod[idx] += prod[d_idx]

    return df.with_columns(pl.Series(PRODUCTIVITY_TMP_COL, prod))


def _hydro_accumulated_productivity_at_volume(
    deck_cls: Any,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
    df: pl.DataFrame,
    volume_col: str = VOLUME_FOR_PRODUCTIVITY_TMP_COL,
) -> pl.DataFrame:
    from app.services.deck import hydro as hydro_mod

    hidr = accessors.hidr(deck_cls, cache, uow)
    hidr_cols = [
        HYDRO_CODE_COL,
        RUN_OF_RIVER_REFERENCE_VOLUME_COL,
        LOSS_COL,
        LOSS_KIND_COL,
        LOWER_DROP_COL,
        SPEC_PRODUCTIVITY_COL,
        VOLUME_REGULATION_COL,
    ] + HEIGHT_POLY_COLS
    hidr_sel = hidr.select(hidr_cols)

    volume_bounds = hydro_mod.hydro_volume_bounds_with_changes(
        deck_cls, cache, uow
    ).select([HYDRO_CODE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL])

    hydros_following = entities.hydros(deck_cls, cache, uow).select(
        [HYDRO_CODE_COL, FOLLOWING_HYDRO_COL]
    )

    df_cols = df.columns
    work = df.join(hidr_sel, on=HYDRO_CODE_COL, how="inner")
    work = work.join(volume_bounds, on=HYDRO_CODE_COL, how="inner")
    work = work.join(hydros_following, on=HYDRO_CODE_COL, how="inner")
    work = evaluate_productivity(work, volume_col=volume_col)
    work = accumulate_productivity(work)
    return work.select(df_cols + [PRODUCTIVITY_TMP_COL])


def _initial_stored_energy_from_pmo(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> Optional[pl.DataFrame]:
    df_pmo_pd = accessors.pmo(deck_cls, cache, uow).energia_armazenada_inicial
    if not hasattr(df_pmo_pd, "rename"):
        return None
    eers = entities.eers(deck_cls, cache, uow)
    df_pmo = pl.from_pandas(df_pmo_pd).rename({"nome_ree": EER_NAME_COL})
    df_pmo = df_pmo.join(
        eers.select([EER_CODE_COL, EER_NAME_COL]),
        on=EER_NAME_COL,
        how="left",
    )
    df_pmo = df_pmo.sort(EER_CODE_COL)
    return df_pmo.select([EER_NAME_COL, "valor_MWmes", "valor_percentual"])


def _initial_stored_energy_from_confhd_hidr(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> Optional[pl.DataFrame]:
    from app.services.deck import hydro as hydro_mod

    ABSOLUTE_VALUE_COL = "valor_hm3"
    ABSOLUTE_VALUE_FINAL_COL = "valor_MWmes"
    PERCENT_VALUE_COL = "valor_percentual"
    MAX_PRODUCTIVITY_COL = "prod_max"
    MAX_STORED_VOLUME_COL = "varmax"
    MAXIMUM_STORED_ENERGY_COL = "earmax"

    def _join_drop_data(df: pl.DataFrame) -> pl.DataFrame:
        drops_pl = hydro_mod.hydro_drops_in_stages(deck_cls, cache, uow)
        stage_date = temporal.stages_starting_dates_final_simulation(
            deck_cls, cache, uow
        )[0]
        drops_df = drops_pl.filter(pl.col(START_DATE_COL) == stage_date)
        return df.join(drops_df.drop("usina"), on=HYDRO_CODE_COL, how="inner")

    def _join_bounds_data(df: pl.DataFrame) -> pl.DataFrame:
        bounds_df = hydro_mod.hydro_volume_bounds_with_changes(
            deck_cls, cache, uow
        ).select([HYDRO_CODE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL])
        return df.join(bounds_df, on=HYDRO_CODE_COL, how="inner")

    def _volume_to_energy(df: pl.DataFrame) -> pl.DataFrame:
        df = (
            df.with_columns(
                pl.when(pl.col(VOLUME_REGULATION_COL) != "M")
                .then(pl.lit(0.0))
                .otherwise(pl.col(ABSOLUTE_VALUE_COL))
                .alias(ABSOLUTE_VALUE_COL)
            )
            .with_columns(
                (
                    pl.col(ABSOLUTE_VALUE_COL) * pl.col(PRODUCTIVITY_TMP_COL)
                ).alias(ABSOLUTE_VALUE_COL)
            )
            .with_columns(
                pl.when(pl.col(VOLUME_REGULATION_COL) != "M")
                .then(pl.lit(0.0))
                .otherwise(pl.col(MAX_STORED_VOLUME_COL))
                .alias(MAX_STORED_VOLUME_COL)
            )
            .with_columns(
                (
                    pl.col(MAX_STORED_VOLUME_COL) * pl.col(MAX_PRODUCTIVITY_COL)
                ).alias(MAXIMUM_STORED_ENERGY_COL)
            )
        )
        return df

    def _cast_to_eers(df: pl.DataFrame) -> pl.DataFrame:
        map_df = entities.hydro_eer_submarket_map(deck_cls, cache, uow).select(
            [HYDRO_CODE_COL, EER_CODE_COL, EER_NAME_COL]
        )
        df = df.join(map_df, on=HYDRO_CODE_COL, how="inner")
        aggregated = (
            df.select(
                [
                    EER_CODE_COL,
                    EER_NAME_COL,
                    ABSOLUTE_VALUE_COL,
                    MAXIMUM_STORED_ENERGY_COL,
                ]
            )
            .group_by([EER_CODE_COL, EER_NAME_COL])
            .agg(
                pl.col(ABSOLUTE_VALUE_COL).sum(),
                pl.col(MAXIMUM_STORED_ENERGY_COL).sum(),
            )
        )
        eer_codes = entities.eer_code_order(deck_cls, cache, uow)
        present_codes = aggregated[EER_CODE_COL].to_list()
        missing_codes = [e for e in eer_codes if e not in present_codes]

        if missing_codes:
            eers_df = entities.eers(deck_cls, cache, uow)
            missing_names = eers_df.filter(
                pl.col(EER_CODE_COL).is_in(missing_codes)
            ).select([EER_CODE_COL, EER_NAME_COL])
            nan_col = pl.lit(None).cast(pl.Float64)
            missing_df = missing_names.with_columns(
                nan_col.alias(ABSOLUTE_VALUE_COL),
                pl.lit(100.0).alias(PERCENT_VALUE_COL),
                nan_col.alias(MAXIMUM_STORED_ENERGY_COL),
            )
            aggregated = pl.concat(
                [aggregated, missing_df.drop(PERCENT_VALUE_COL)],
                how="diagonal",
            )

        return aggregated.sort(EER_CODE_COL)

    hydros_df = entities.hydros(deck_cls, cache, uow)
    # initial_stored_volume returns pl.DataFrame (hydro_code, name, valor_hm3, valor_percentual)
    stored_vol = initial_stored_volume(deck_cls, cache, uow)

    absolute_df = stored_vol
    absolute_df = _join_drop_data(absolute_df)
    absolute_df = _join_bounds_data(absolute_df)
    absolute_df = absolute_df.join(
        hydros_df.select([HYDRO_CODE_COL, FOLLOWING_HYDRO_COL]),
        on=HYDRO_CODE_COL,
        how="inner",
    )
    absolute_df = evaluate_productivity(
        absolute_df, volume_col=ABSOLUTE_VALUE_COL
    )
    absolute_df = accumulate_productivity(absolute_df)

    sv_cols = stored_vol.columns
    percent_df = _join_bounds_data(stored_vol)
    percent_df = percent_df.with_columns(
        (pl.col(UPPER_BOUND_COL) - pl.col(LOWER_BOUND_COL)).alias(
            ABSOLUTE_VALUE_COL
        )
    ).select(sv_cols)
    percent_df = _join_drop_data(percent_df)
    percent_df = _join_bounds_data(percent_df)
    percent_df = percent_df.join(
        hydros_df.select([HYDRO_CODE_COL, FOLLOWING_HYDRO_COL]),
        on=HYDRO_CODE_COL,
        how="inner",
    )
    percent_df = percent_df.with_columns(
        (pl.col(UPPER_BOUND_COL) - pl.col(LOWER_BOUND_COL)).alias(
            ABSOLUTE_VALUE_COL
        )
    )
    percent_df = evaluate_productivity(
        percent_df, volume_col=ABSOLUTE_VALUE_COL
    )
    percent_df = accumulate_productivity(percent_df)
    percent_df = percent_df.rename(
        {
            ABSOLUTE_VALUE_COL: MAX_STORED_VOLUME_COL,
            PRODUCTIVITY_TMP_COL: MAX_PRODUCTIVITY_COL,
        }
    )

    return (
        absolute_df.join(
            percent_df.select(
                [HYDRO_CODE_COL, MAX_STORED_VOLUME_COL, MAX_PRODUCTIVITY_COL]
            ),
            on=HYDRO_CODE_COL,
            how="inner",
        )
        .pipe(lambda df: _volume_to_energy(df))
        .pipe(
            lambda df: _cast_to_eers(
                df.select(
                    [
                        HYDRO_CODE_COL,
                        ABSOLUTE_VALUE_COL,
                        MAX_STORED_VOLUME_COL,
                        MAXIMUM_STORED_ENERGY_COL,
                    ]
                )
            )
        )
        .with_columns(
            (
                pl.col(ABSOLUTE_VALUE_COL)
                / pl.col(MAXIMUM_STORED_ENERGY_COL)
                * 100.0
            ).alias(PERCENT_VALUE_COL)
        )
        .rename({ABSOLUTE_VALUE_COL: ABSOLUTE_VALUE_FINAL_COL})
        .select([EER_NAME_COL, ABSOLUTE_VALUE_FINAL_COL, PERCENT_VALUE_COL])
    )


def initial_stored_energy(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("initial_stored_energy")
    if val is None:
        val = _initial_stored_energy_from_pmo(deck_cls, cache, uow)
        if val is None:
            val = _initial_stored_energy_from_confhd_hidr(deck_cls, cache, uow)
        val = readers.validate_data(
            deck_cls,
            val,
            pl.DataFrame,
            "energia armazenada inicial por REE (pmo.dat ou calculada)",
        )
        assert isinstance(val, pl.DataFrame)
        cache["initial_stored_energy"] = val
    return val


def _initial_stored_volume_from_pmo(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> Optional[pl.DataFrame]:
    df_pd = accessors.pmo(deck_cls, cache, uow).volume_armazenado_inicial
    if df_pd is None:
        return None
    df = pl.from_pandas(df_pd).rename(
        {
            "codigo_usina": HYDRO_CODE_COL,
            "nome_usina": HYDRO_NAME_COL,
        }
    )
    return df


def _initial_stored_volume_from_confhd_hidr(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> Optional[pl.DataFrame]:
    from app.services.deck import hydro as hydro_mod

    confhd = readers.get_confhd(deck_cls, uow)
    usinas = confhd.usinas
    assert usinas is not None
    confhd_df = pl.from_pandas(
        usinas[[HYDRO_CODE_COL, "nome_usina", "volume_inicial_percentual"]]
    ).rename(
        {
            "nome_usina": HYDRO_NAME_COL,
            "volume_inicial_percentual": "valor_percentual",
        }
    )

    hidr = accessors.hidr(deck_cls, cache, uow)
    volume_bounds = hydro_mod.hydro_volume_bounds_with_changes(
        deck_cls, cache, uow
    ).select([HYDRO_CODE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL])

    df = confhd_df.join(hidr, on=HYDRO_CODE_COL, how="inner").join(
        volume_bounds, on=HYDRO_CODE_COL, how="inner"
    )

    return (
        df.with_columns(
            (
                pl.col("valor_percentual")
                / 100.0
                * (pl.col(UPPER_BOUND_COL) - pl.col(LOWER_BOUND_COL))
            ).alias("valor_hm3")
        )
        .with_columns(
            pl.when(pl.col("tipo_regulacao") != "M")
            .then(pl.lit(None).cast(pl.Float64))
            .otherwise(pl.col("valor_hm3"))
            .alias("valor_hm3")
        )
        .with_columns(
            pl.when(pl.col("tipo_regulacao") != "M")
            .then(pl.lit(0.0))
            .otherwise(pl.col("valor_percentual"))
            .alias("valor_percentual")
        )
        .select(
            [HYDRO_CODE_COL, HYDRO_NAME_COL, "valor_hm3", "valor_percentual"]
        )
    )


def _initial_stored_volume_pre_study_condition(
    deck_cls: Any,
    cache: Dict[str, Any],
    df: pl.DataFrame,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    if temporal.num_pre_study_period_years(deck_cls, cache, uow) > 0:
        df = df.with_columns(
            pl.when(pl.col("valor_hm3").is_not_null())
            .then(pl.lit(100.0))
            .otherwise(pl.col("valor_percentual"))
            .alias("valor_percentual")
        )
    return df


def initial_stored_volume(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("initial_stored_volume")
    if val is None:
        val = _initial_stored_volume_from_pmo(deck_cls, cache, uow)
        if val is None:
            val = _initial_stored_volume_from_confhd_hidr(deck_cls, cache, uow)
        val = readers.validate_data(
            deck_cls,
            val,
            pl.DataFrame,
            "volume armazenado inicial por UHE (pmo.dat ou calculado)",
        )
        assert isinstance(val, pl.DataFrame)
        val = _initial_stored_volume_pre_study_condition(
            deck_cls, cache, val, uow
        )
        cache["initial_stored_volume"] = val
    return val
