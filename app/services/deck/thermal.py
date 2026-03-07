from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from app.internal.constants import (
    LOWER_BOUND_COL,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    THERMAL_CODE_COL,
    THERMAL_NAME_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
)
from app.services.deck import accessors, entities, readers, temporal
from app.services.unitofwork import AbstractUnitOfWork


def _apply_thermal_single_change(
    df: pd.DataFrame,
    thermal_code: int,
    start_date: datetime,
    end_date: datetime,
    col: str,
    value: float,
) -> None:
    df_filter = (
        (df[THERMAL_CODE_COL] == thermal_code)
        & (df[START_DATE_COL] >= start_date)
        & (df[START_DATE_COL] <= end_date)
    )
    df.loc[df_filter, col] = value


def _apply_thermal_bounds_maintenance_and_changes(
    deck_cls, cache: Dict[str, Any], df: pd.DataFrame, uow: AbstractUnitOfWork
) -> pd.DataFrame:
    def _apply_thermal_changes(df: pd.DataFrame) -> pd.DataFrame:
        expt = accessors.expt(
            deck_cls, cache, uow
        ).to_pandas()  # SHIM: remove after polars migration of this module
        stage_dates = temporal.stages_starting_dates_final_simulation(
            deck_cls, cache, uow
        )
        final_date = stage_dates[-1]
        expt["data_fim"] = expt["data_fim"].fillna(final_date)
        col_map: dict[str, str] = {
            "POTEF": "potencia_instalada",
            "FCMAX": "fator_capacidade_maximo",
            "TEIFT": "teif",
            "GTMIN": LOWER_BOUND_COL,
            "IPTER": "indisponibilidade_programada",
        }
        for _, line in expt.iterrows():
            _apply_thermal_single_change(
                df,
                line["codigo_usina"],
                line["data_inicio"],
                line["data_fim"],
                col_map[line["tipo"]],
                line["modificacao"],
            )
        return df

    def _apply_maintenance(df: pd.DataFrame) -> pd.DataFrame:
        manutt = accessors.manutt(
            deck_cls, cache, uow
        ).to_pandas()  # SHIM: remove after polars migration of this module
        thermal_codes = manutt[THERMAL_CODE_COL].unique()
        maintenance_end_date = temporal.thermal_maintenance_end_date(
            deck_cls, cache, uow
        )
        for code in thermal_codes:
            thermal_df = df.loc[
                (df[THERMAL_CODE_COL] == code)
                & (df[START_DATE_COL] < maintenance_end_date),
                [START_DATE_COL, "potencia_instalada"],
            ].copy()
            last_month: pd.Timestamp = thermal_df[START_DATE_COL].max()
            last_day = last_month.daysinmonth
            thermal_df.loc[-1, START_DATE_COL] = last_month.replace(
                day=last_day
            )
            thermal_df = (
                thermal_df.set_index(START_DATE_COL).resample("D").ffill()
            )
            thermal_df["potencia_instalada"] = thermal_df[
                "potencia_instalada"
            ].ffill()
            thermal_maintenance_df = manutt.loc[
                manutt[THERMAL_CODE_COL] == code
            ]
            for _, line in thermal_maintenance_df.iterrows():
                start_date = line["data_inicio"]
                num_days = line["duracao"]
                value = line["potencia"]
                end_date = start_date + timedelta(days=num_days - 1)
                thermal_df.loc[start_date:end_date, "potencia_instalada"] -= (
                    value
                )
            thermal_df = thermal_df.resample("MS").mean().reset_index()
            df.loc[
                (df[THERMAL_CODE_COL] == code)
                & (df[START_DATE_COL].isin(thermal_df[START_DATE_COL])),
                "potencia_instalada",
            ] = thermal_df["potencia_instalada"].to_numpy()
        return df

    maintenance_end_date = temporal.thermal_maintenance_end_date(
        deck_cls, cache, uow
    )
    df.loc[
        df[START_DATE_COL] < maintenance_end_date,
        "indisponibilidade_programada",
    ] = 0.0
    df = _apply_thermal_changes(df)
    df = _apply_maintenance(df)
    return df


def _thermal_generation_bounds_term_manutt_expt(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    def _expand_to_stages(df: pd.DataFrame) -> pd.DataFrame:
        num_thermals = df.shape[0]
        dates = np.array(
            temporal.stages_starting_dates_final_simulation(
                deck_cls, cache, uow
            )
        )
        num_stages = len(dates)
        df = pd.concat([df] * num_stages, ignore_index=True)
        df[START_DATE_COL] = np.repeat(dates, num_thermals)
        return df.sort_values([THERMAL_CODE_COL, START_DATE_COL]).reset_index(
            drop=True
        )

    def _add_term_lower_bounds(
        df: pd.DataFrame, term: pd.DataFrame
    ) -> pd.DataFrame:
        stage_dates = temporal.stages_starting_dates_final_simulation(
            deck_cls, cache, uow
        )
        initial_month = stage_dates[0].month
        term = term.loc[term["mes"] >= initial_month].copy()
        last_term_month = term["mes"].max()
        last_term_block = term.loc[term["mes"] == last_term_month]
        num_repeats = len(stage_dates) - (12 - initial_month) - 1
        term_repeats: list[pd.DataFrame] = []
        for n in range(1, num_repeats):
            last_term_block_month = last_term_block.copy()
            last_term_block_month["mes"] += n
            term_repeats.append(last_term_block_month)
        term = pd.concat([term, *term_repeats], ignore_index=True)
        term = term.sort_values([THERMAL_CODE_COL, "mes"])
        df[LOWER_BOUND_COL] = term["geracao_minima"].to_numpy()
        return df

    def _enforce_null_lower_bounds_on_changes(df: pd.DataFrame) -> pd.DataFrame:
        expt = accessors.expt(
            deck_cls, cache, uow
        ).to_pandas()  # SHIM: remove after polars migration of this module
        thermals_to_nullify = expt.loc[
            expt["tipo"] == "GTMIN", "codigo_usina"
        ].unique()
        maintenance_end_date = temporal.thermal_maintenance_end_date(
            deck_cls, cache, uow
        )
        for code in thermals_to_nullify:
            df.loc[
                (df[THERMAL_CODE_COL] == code)
                & (df[START_DATE_COL] >= maintenance_end_date),
                LOWER_BOUND_COL,
            ] = 0.0
        return df

    def _enforce_null_upper_bounds_on_changes(df: pd.DataFrame) -> pd.DataFrame:
        expt = accessors.expt(
            deck_cls, cache, uow
        ).to_pandas()  # SHIM: remove after polars migration of this module
        thermals_to_nullify = expt.loc[
            expt["tipo"] == "POTEF", "codigo_usina"
        ].unique()
        maintenance_end_date = temporal.thermal_maintenance_end_date(
            deck_cls, cache, uow
        )
        for code in thermals_to_nullify:
            df.loc[
                (df[THERMAL_CODE_COL] == code)
                & (df[START_DATE_COL] >= maintenance_end_date),
                "potencia_instalada",
            ] = 0.0
        return df

    def _eval_upper_bounds(df: pd.DataFrame) -> pd.DataFrame:
        df[UPPER_BOUND_COL] = (
            df["potencia_instalada"]
            * (df["fator_capacidade_maximo"] / 100.0)
            * (100.0 - df["indisponibilidade_programada"])
            / 100.0
            * (100.0 - df["teif"])
            / 100.0
        )
        return df

    term = accessors.term(
        deck_cls, cache, uow
    ).to_pandas()  # SHIM: remove after polars migration of this module
    bounds_df = (
        term.drop_duplicates(subset=["codigo_usina", "nome_usina"])
        .copy()
        .sort_values(THERMAL_CODE_COL)
    )
    bounds_df = _expand_to_stages(bounds_df)
    bounds_df = _add_term_lower_bounds(bounds_df, term)
    bounds_df = _enforce_null_lower_bounds_on_changes(bounds_df)
    bounds_df = _enforce_null_upper_bounds_on_changes(bounds_df)
    bounds_df = _apply_thermal_bounds_maintenance_and_changes(
        deck_cls, cache, bounds_df, uow
    )
    bounds_df = _eval_upper_bounds(bounds_df)
    return bounds_df[
        [
            THERMAL_CODE_COL,
            "nome_usina",
            START_DATE_COL,
            LOWER_BOUND_COL,
            UPPER_BOUND_COL,
        ]
    ].copy()


def _thermal_generation_bounds_pmo(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> Optional[pd.DataFrame]:
    pmo = accessors.pmo(deck_cls, cache, uow)
    bounds_df = pmo.geracao_minima_usinas_termicas
    if bounds_df is None or not isinstance(bounds_df, pd.DataFrame):
        return bounds_df
    bounds_df = bounds_df.rename(
        columns={"data": START_DATE_COL, "valor_MWmed": LOWER_BOUND_COL}
    )
    upper_bounds = pmo.geracao_maxima_usinas_termicas
    if isinstance(upper_bounds, pd.DataFrame):
        bounds_df[UPPER_BOUND_COL] = upper_bounds["valor_MWmed"].to_numpy()
    start_date = temporal.stages_starting_dates_final_simulation(
        deck_cls, cache, uow
    )[0]
    bounds_df = bounds_df.loc[
        bounds_df[START_DATE_COL] >= start_date
    ].reset_index(drop=True)
    bounds_df = temporal.consider_post_study_years(
        deck_cls, cache, bounds_df, uow
    )
    return bounds_df


def thermal_generation_bounds(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    def _add_submarket_data(df: pd.DataFrame) -> pd.DataFrame:
        thermal_map = entities.thermal_submarket_map(deck_cls, cache, uow)
        df = df.rename(
            columns={
                "nome_usina": THERMAL_NAME_COL,
                "codigo_usina": THERMAL_CODE_COL,
            }
        )
        return df.join(
            thermal_map[[SUBMARKET_CODE_COL, SUBMARKET_NAME_COL]],
            on=THERMAL_CODE_COL,
        )

    val = cache.get("thermal_generation_bounds")
    if val is None:
        bounds_df = _thermal_generation_bounds_pmo(deck_cls, cache, uow)
        if bounds_df is None:
            bounds_df = _thermal_generation_bounds_term_manutt_expt(
                deck_cls, cache, uow
            )
        val = _add_submarket_data(bounds_df)
        cache["thermal_generation_bounds"] = val
    return val.copy()


def thermal_costs(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    def _build_base_costs_df() -> pd.DataFrame:
        clast_df = accessors.clast(
            deck_cls, cache, uow
        ).to_pandas()  # SHIM: remove after polars migration of this module
        clast_df = clast_df.rename(
            columns={"codigo_usina": THERMAL_CODE_COL, "valor": VALUE_COL}
        )
        starting_year = temporal.study_period_starting_year(
            deck_cls, cache, uow
        )
        num_thermals = len(clast_df[THERMAL_CODE_COL].unique())
        num_years = len(clast_df["indice_ano_estudo"].unique())
        clast_df = clast_df.loc[clast_df.index.repeat(12)].reset_index(
            drop=True
        )
        clast_df["mes"] = np.tile(list(range(1, 13)), num_thermals * num_years)
        clast_df[START_DATE_COL] = clast_df.apply(
            lambda line: datetime(
                starting_year + line["indice_ano_estudo"] - 1, line["mes"], 1
            ),
            axis=1,
        )
        return clast_df.drop(
            columns=[
                "nome_usina",
                "tipo_combustivel",
                "indice_ano_estudo",
                "mes",
            ]
        )

    def _apply_thermal_cost_changes(df: pd.DataFrame) -> pd.DataFrame:
        clast_changes_df = readers.validate_data(
            deck_cls,
            readers.get_clast(deck_cls, uow).modificacoes,
            pd.DataFrame,
            "modificações dos custos de térmicas (clast.dat)",
        )
        stage_dates = temporal.stages_starting_dates_final_simulation(
            deck_cls, cache, uow
        )
        clast_changes_df["data_fim"] = clast_changes_df["data_fim"].fillna(
            stage_dates[-1]
        )
        for _, line in clast_changes_df.iterrows():
            _apply_thermal_single_change(
                df,
                line["codigo_usina"],
                line["data_inicio"],
                line["data_fim"],
                VALUE_COL,
                line["custo"],
            )
        return df

    val = cache.get("thermal_costs")
    if val is None:
        df = _build_base_costs_df()
        _apply_thermal_cost_changes(df)
        stage_dates = temporal.stages_starting_dates_final_simulation(
            deck_cls, cache, uow
        )
        df = df.loc[df[START_DATE_COL].isin(stage_dates)].reset_index(drop=True)
        val = df[[THERMAL_CODE_COL, START_DATE_COL, VALUE_COL]]
        cache["thermal_costs"] = val
    return val
