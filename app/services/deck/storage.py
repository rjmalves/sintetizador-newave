from functools import partial
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

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
    df: pd.DataFrame,
    volume_col: str = VOLUME_FOR_PRODUCTIVITY_TMP_COL,
) -> pd.DataFrame:
    """Compute turbine productivity for each hydro in df."""

    def _upper_drop_at_volume(line: pd.Series) -> float:
        coefs = [line[c] for c in HEIGHT_POLY_COLS]
        if line[VOLUME_REGULATION_COL] == "M":
            coefs_integral = [0] + [c / (i + 1) for i, c in enumerate(coefs)]
            min_v = line[LOWER_BOUND_COL]
            max_v = line[UPPER_BOUND_COL]
            net_v = max_v - min_v
            pct_v = line[volume_col] / net_v if net_v > 0 else 0
            rev_int = list(reversed(coefs_integral))
            min_int = np.polyval(rev_int, min_v)
            max_int = np.polyval(rev_int, pct_v * net_v + min_v)
            return (
                (max_int - min_int) / (pct_v * net_v)
                if pct_v * net_v > 0
                else 0.0
            )
        else:
            return float(
                np.polyval(
                    list(reversed(coefs)),
                    line[RUN_OF_RIVER_REFERENCE_VOLUME_COL],
                )
            )

    def _fill_run_of_river(line: pd.Series) -> float:
        return 0.0 if pd.isna(line[volume_col]) else line[volume_col]

    def _apply_losses(line: pd.Series, col: str):
        if line[LOSS_KIND_COL] == 1:
            return line[col] * (1 - line[LOSS_COL])
        elif line[LOSS_KIND_COL] == 2:
            return line[col] - line[LOSS_COL]

    df[UPPER_DROP_COL] = df.apply(_upper_drop_at_volume, axis=1)
    df[NET_DROP_COL] = df[UPPER_DROP_COL] - df[LOWER_DROP_COL]
    df[volume_col] = df.apply(_fill_run_of_river, axis=1)
    df[PRODUCTIVITY_TMP_COL] = df[SPEC_PRODUCTIVITY_COL] * df.apply(
        partial(_apply_losses, col=NET_DROP_COL), axis=1
    )
    df[PRODUCTIVITY_TMP_COL] *= HM3_M3S_MONTHLY_FACTOR
    return df


def accumulate_productivity(df: pd.DataFrame) -> pd.DataFrame:
    """Propagate productivity downstream through the cascade."""
    np_edges = list(
        df.reset_index()[[FOLLOWING_HYDRO_COL, HYDRO_CODE_COL]].to_numpy()
    )
    edges = [tuple(e) for e in np_edges]
    bfs = Graph(edges, directed=True).bfs(0)[1:]
    for hydro_code in bfs:
        downstream = df.at[hydro_code, FOLLOWING_HYDRO_COL]
        if downstream == 0:
            continue
        df.at[hydro_code, PRODUCTIVITY_TMP_COL] += df.at[
            downstream, PRODUCTIVITY_TMP_COL
        ]
    return df


def _hydro_accumulated_productivity_at_volume(
    deck_cls,
    cache: Dict[str, Any],
    uow: AbstractUnitOfWork,
    df: pd.DataFrame,
    volume_col: str = VOLUME_FOR_PRODUCTIVITY_TMP_COL,
) -> pd.DataFrame:
    from app.services.deck import hydro as hydro_mod

    hidr = accessors.hidr(deck_cls, cache, uow)
    hidr_cols = [
        RUN_OF_RIVER_REFERENCE_VOLUME_COL,
        LOSS_COL,
        LOSS_KIND_COL,
        LOWER_DROP_COL,
        SPEC_PRODUCTIVITY_COL,
        VOLUME_REGULATION_COL,
    ]
    df = df.copy()
    df_cols = df.columns.tolist()
    df = df.join(hidr[hidr_cols + HEIGHT_POLY_COLS], how="inner")
    df = df.join(
        hydro_mod.hydro_volume_bounds_with_changes(deck_cls, cache, uow),
        how="inner",
    )
    df = df.join(
        entities.hydros(deck_cls, cache, uow)[[FOLLOWING_HYDRO_COL]],
        how="inner",
    )
    df = evaluate_productivity(df, volume_col=volume_col)
    df = accumulate_productivity(df)
    return df[df_cols + [PRODUCTIVITY_TMP_COL]]


def _initial_stored_energy_from_pmo(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> Optional[pd.DataFrame]:
    df_pmo = accessors.pmo(deck_cls, cache, uow).energia_armazenada_inicial
    if isinstance(df_pmo, pd.DataFrame):
        eers = entities.eers(deck_cls, cache, uow).reset_index()
        df_pmo[EER_CODE_COL] = df_pmo["nome_ree"].apply(
            lambda x: eers.loc[eers[EER_NAME_COL] == x, EER_CODE_COL].iloc[0]
        )
        df_pmo = df_pmo.rename(columns={"nome_ree": EER_NAME_COL})
        df_pmo = df_pmo.set_index([EER_CODE_COL]).sort_index()
    return df_pmo


def _initial_stored_energy_from_confhd_hidr(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> Optional[pd.DataFrame]:
    from app.services.deck import hydro as hydro_mod

    ABSOLUTE_VALUE_COL = "valor_hm3"
    ABSOLUTE_VALUE_FINAL_COL = "valor_MWmes"
    PERCENT_VALUE_COL = "valor_percentual"
    MAX_PRODUCTIVITY_COL = "prod_max"
    MAX_STORED_VOLUME_COL = "varmax"
    MAXIMUM_STORED_ENERGY_COL = "earmax"

    def _join_drop_data(df: pd.DataFrame) -> pd.DataFrame:
        drops_df = hydro_mod.hydro_drops_in_stages(deck_cls, cache, uow)
        stage_date = temporal.stages_starting_dates_final_simulation(
            deck_cls, cache, uow
        )[0]
        drops_df = drops_df.loc[
            drops_df[START_DATE_COL] == stage_date
        ].set_index(HYDRO_CODE_COL)
        return df.drop(columns=["usina"]).join(drops_df, how="inner")

    def _join_bounds_data(df: pd.DataFrame) -> pd.DataFrame:
        bounds_df = hydro_mod.hydro_volume_bounds_with_changes(
            deck_cls, cache, uow
        )
        return df.join(
            bounds_df[[LOWER_BOUND_COL, UPPER_BOUND_COL]], how="inner"
        )

    def _volume_to_energy(df: pd.DataFrame) -> pd.DataFrame:
        df.loc[df[VOLUME_REGULATION_COL] != "M", ABSOLUTE_VALUE_COL] = 0.0
        df[ABSOLUTE_VALUE_COL] *= df[PRODUCTIVITY_TMP_COL]
        df.loc[df[VOLUME_REGULATION_COL] != "M", MAX_STORED_VOLUME_COL] = 0.0
        df[MAXIMUM_STORED_ENERGY_COL] = (
            df[MAX_STORED_VOLUME_COL] * df[MAX_PRODUCTIVITY_COL]
        )
        return df

    def _cast_to_eers(df: pd.DataFrame) -> pd.DataFrame:
        df = df.join(
            entities.hydro_eer_submarket_map(deck_cls, cache, uow).drop(
                columns=["usina"]
            ),
            how="inner",
        )
        df = (
            df[
                [
                    EER_CODE_COL,
                    EER_NAME_COL,
                    ABSOLUTE_VALUE_COL,
                    MAXIMUM_STORED_ENERGY_COL,
                ]
            ]
            .groupby([EER_CODE_COL, EER_NAME_COL])
            .sum()
        ).reset_index()
        eer_codes = entities.eer_code_order(deck_cls, cache, uow)
        eers = entities.eers(deck_cls, cache, uow)
        missing_eers = [
            e for e in eer_codes if e not in df[EER_CODE_COL].tolist()
        ]
        missing_df = pd.DataFrame(
            {
                EER_CODE_COL: missing_eers,
                EER_NAME_COL: eers.loc[missing_eers, EER_NAME_COL].tolist(),
                ABSOLUTE_VALUE_COL: [np.nan] * len(missing_eers),
                PERCENT_VALUE_COL: [100.0] * len(missing_eers),
            }
        )
        if not missing_df.empty:
            df = pd.concat([df, missing_df], ignore_index=True)
        df[EER_CODE_COL] = df[EER_CODE_COL].astype(int)
        return df.set_index(EER_CODE_COL)

    hydros_df = entities.hydros(deck_cls, cache, uow)
    df = initial_stored_volume(deck_cls, cache, uow).set_index(HYDRO_CODE_COL)

    absolute_df = df.copy()
    absolute_df = _join_drop_data(absolute_df)
    absolute_df = _join_bounds_data(absolute_df)
    absolute_df = absolute_df.join(
        hydros_df[[FOLLOWING_HYDRO_COL]], how="inner"
    )
    absolute_df = evaluate_productivity(
        absolute_df, volume_col=ABSOLUTE_VALUE_COL
    )
    absolute_df = accumulate_productivity(absolute_df)

    df_cols = df.columns
    percent_df = _join_bounds_data(df.copy())
    percent_df[ABSOLUTE_VALUE_COL] = (
        percent_df[UPPER_BOUND_COL] - percent_df[LOWER_BOUND_COL]
    )
    percent_df = percent_df[df_cols]
    percent_df = _join_drop_data(percent_df)
    percent_df = _join_bounds_data(percent_df)
    percent_df = percent_df.join(hydros_df[[FOLLOWING_HYDRO_COL]], how="inner")
    percent_df[ABSOLUTE_VALUE_COL] = (
        percent_df[UPPER_BOUND_COL] - percent_df[LOWER_BOUND_COL]
    )
    percent_df = evaluate_productivity(
        percent_df, volume_col=ABSOLUTE_VALUE_COL
    )
    percent_df = accumulate_productivity(percent_df)
    percent_df = percent_df.rename(
        columns={
            ABSOLUTE_VALUE_COL: MAX_STORED_VOLUME_COL,
            PRODUCTIVITY_TMP_COL: MAX_PRODUCTIVITY_COL,
        }
    )

    df = absolute_df.join(
        percent_df[[MAX_STORED_VOLUME_COL, MAX_PRODUCTIVITY_COL]], how="inner"
    )
    df = _volume_to_energy(df)
    df = _cast_to_eers(
        df[
            [
                ABSOLUTE_VALUE_COL,
                MAX_STORED_VOLUME_COL,
                MAXIMUM_STORED_ENERGY_COL,
            ]
        ]
    )
    df[PERCENT_VALUE_COL] = (
        df[ABSOLUTE_VALUE_COL] / df[MAXIMUM_STORED_ENERGY_COL] * 100.0
    )
    df = df.rename(columns={ABSOLUTE_VALUE_COL: ABSOLUTE_VALUE_FINAL_COL})
    return df[[EER_NAME_COL, ABSOLUTE_VALUE_FINAL_COL, PERCENT_VALUE_COL]]


def initial_stored_energy(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("initial_stored_energy")
    if val is None:
        val = _initial_stored_energy_from_pmo(deck_cls, cache, uow)
        if val is None:
            val = _initial_stored_energy_from_confhd_hidr(deck_cls, cache, uow)
        val = readers.validate_data(
            deck_cls,
            val,
            pd.DataFrame,
            "energia armazenada inicial por REE (pmo.dat ou calculada)",
        )
        cache["initial_stored_energy"] = val
    return val.copy()


def _initial_stored_volume_from_pmo(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> Optional[pd.DataFrame]:
    df = accessors.pmo(deck_cls, cache, uow).volume_armazenado_inicial
    if df is None:
        return df
    return df.rename(
        columns={"codigo_usina": HYDRO_CODE_COL, "nome_usina": HYDRO_NAME_COL}
    )


def _initial_stored_volume_from_confhd_hidr(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> Optional[pd.DataFrame]:
    from app.services.deck import hydro as hydro_mod

    confhd = readers.get_confhd(deck_cls, uow)
    df = confhd.usinas[
        [HYDRO_CODE_COL, "nome_usina", "volume_inicial_percentual"]
    ].set_index(HYDRO_CODE_COL)
    df = df.rename(
        columns={
            "nome_usina": HYDRO_NAME_COL,
            "volume_inicial_percentual": "valor_percentual",
        }
    )
    hidr = accessors.hidr(deck_cls, cache, uow)
    volume_bounds = hydro_mod.hydro_volume_bounds_with_changes(
        deck_cls, cache, uow
    )[[LOWER_BOUND_COL, UPPER_BOUND_COL]]
    df = df.join(hidr, how="inner")
    df = df.join(volume_bounds, how="inner")
    df["valor_hm3"] = df.apply(
        lambda line: (
            line["valor_percentual"]
            / 100.0
            * (line[UPPER_BOUND_COL] - line[LOWER_BOUND_COL])
        ),
        axis=1,
    )
    df.loc[df["tipo_regulacao"] != "M", "valor_hm3"] = np.nan
    df.loc[df["tipo_regulacao"] != "M", "valor_percentual"] = 0.0
    return df[[HYDRO_NAME_COL, "valor_hm3", "valor_percentual"]].reset_index()


def _initial_stored_volume_pre_study_condition(
    deck_cls, cache: Dict[str, Any], df: pd.DataFrame, uow: AbstractUnitOfWork
) -> pd.DataFrame:
    if temporal.num_pre_study_period_years(deck_cls, cache, uow) > 0:
        df.loc[~df["valor_hm3"].isna(), "valor_percentual"] = 100.0
    return df


def initial_stored_volume(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("initial_stored_volume")
    if val is None:
        val = _initial_stored_volume_from_pmo(deck_cls, cache, uow)
        if val is None:
            val = _initial_stored_volume_from_confhd_hidr(deck_cls, cache, uow)
        val = readers.validate_data(
            deck_cls,
            val,
            pd.DataFrame,
            "volume armazenado inicial por UHE (pmo.dat ou calculado)",
        )
        val = _initial_stored_volume_pre_study_condition(
            deck_cls, cache, val, uow
        )
        cache["initial_stored_volume"] = val
    return val.copy()
