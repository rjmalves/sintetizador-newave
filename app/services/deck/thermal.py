from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import polars as pl

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
    df: pl.DataFrame,
    thermal_code: int,
    start_date: datetime,
    end_date: datetime,
    col: str,
    value: float,
) -> pl.DataFrame:
    return df.with_columns(
        pl.when(
            (pl.col(THERMAL_CODE_COL) == thermal_code)
            & (pl.col(START_DATE_COL) >= start_date)
            & (pl.col(START_DATE_COL) <= end_date)
        )
        .then(pl.lit(value))
        .otherwise(pl.col(col))
        .alias(col)
    )


def _apply_thermal_bounds_maintenance_and_changes(
    deck_cls: Any,
    cache: Dict[str, Any],
    df: pl.DataFrame,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    def _apply_thermal_changes(df: pl.DataFrame) -> pl.DataFrame:
        expt = accessors.expt(deck_cls, cache, uow)
        stage_dates = temporal.stages_starting_dates_final_simulation(
            deck_cls, cache, uow
        )
        final_date = stage_dates[-1]
        expt = expt.with_columns(
            pl.col("data_fim").fill_null(pl.lit(final_date))
        )
        col_map: dict[str, str] = {
            "POTEF": "potencia_instalada",
            "FCMAX": "fator_capacidade_maximo",
            "TEIFT": "teif",
            "GTMIN": LOWER_BOUND_COL,
            "IPTER": "indisponibilidade_programada",
        }
        for row in expt.iter_rows(named=True):
            mapped_col = col_map.get(row["tipo"])
            if mapped_col is None:
                continue
            df = _apply_thermal_single_change(
                df,
                row["codigo_usina"],
                row["data_inicio"],
                row["data_fim"],
                mapped_col,
                row["modificacao"],
            )
        return df

    def _apply_maintenance(df: pl.DataFrame) -> pl.DataFrame:
        manutt = accessors.manutt(deck_cls, cache, uow)
        if manutt.is_empty():
            return df
        thermal_codes = manutt[THERMAL_CODE_COL].unique().to_list()
        maintenance_end_date = temporal.thermal_maintenance_end_date(
            deck_cls, cache, uow
        )
        updated_rows: list[pl.DataFrame] = []
        for code in thermal_codes:
            # Extract monthly power-installed for this thermal unit within maintenance window
            thermal_monthly = df.filter(
                (pl.col(THERMAL_CODE_COL) == code)
                & (pl.col(START_DATE_COL) < maintenance_end_date)
            ).select([START_DATE_COL, "potencia_instalada"])

            if thermal_monthly.is_empty():
                continue

            # Build daily date range from first month to last day of last month
            first_date: datetime = thermal_monthly[START_DATE_COL].min()  # type: ignore[assignment]
            last_month: datetime = thermal_monthly[START_DATE_COL].max()  # type: ignore[assignment]
            last_day = (last_month.replace(day=28) + timedelta(days=4)).replace(
                day=1
            ) - timedelta(days=1)
            last_day_date = last_month.replace(day=last_day.day)

            daily_dates = pl.date_range(
                first_date,
                last_day_date,
                interval="1d",
                eager=True,
            ).cast(pl.Datetime("us"))

            daily_df = pl.DataFrame({START_DATE_COL: daily_dates})

            # Join monthly power to daily range and forward-fill
            daily_df = daily_df.join(
                thermal_monthly,
                on=START_DATE_COL,
                how="left",
            ).with_columns(pl.col("potencia_instalada").forward_fill())

            # Apply maintenance subtractions
            maint_for_code = manutt.filter(pl.col(THERMAL_CODE_COL) == code)
            for mrow in maint_for_code.iter_rows(named=True):
                m_start: datetime = mrow["data_inicio"]
                num_days: int = mrow["duracao"]
                power_val: float = mrow["potencia"]
                m_end = m_start + timedelta(days=num_days - 1)
                daily_df = daily_df.with_columns(
                    pl.when(
                        (pl.col(START_DATE_COL) >= m_start)
                        & (pl.col(START_DATE_COL) <= m_end)
                    )
                    .then(pl.col("potencia_instalada") - power_val)
                    .otherwise(pl.col("potencia_instalada"))
                    .alias("potencia_instalada")
                )

            monthly_df = (
                daily_df.with_columns(
                    pl.col(START_DATE_COL)
                    .dt.truncate("1mo")
                    .alias("_month_start")
                )
                .group_by("_month_start")
                .agg(pl.col("potencia_instalada").mean())
                .rename({"_month_start": START_DATE_COL})
                .sort(START_DATE_COL)
            )

            updated_rows.append(
                monthly_df.with_columns(pl.lit(code).alias(THERMAL_CODE_COL))
            )

        if not updated_rows:
            return df

        all_updates = pl.concat(updated_rows)
        df = (
            df.join(
                all_updates.rename({"potencia_instalada": "_new_potencia"}),
                on=[THERMAL_CODE_COL, START_DATE_COL],
                how="left",
            )
            .with_columns(
                pl.when(pl.col("_new_potencia").is_not_null())
                .then(pl.col("_new_potencia"))
                .otherwise(pl.col("potencia_instalada"))
                .alias("potencia_instalada")
            )
            .drop("_new_potencia")
        )
        return df

    maintenance_end_date = temporal.thermal_maintenance_end_date(
        deck_cls, cache, uow
    )
    df = df.with_columns(
        pl.when(pl.col(START_DATE_COL) < maintenance_end_date)
        .then(pl.lit(0.0))
        .otherwise(pl.col("indisponibilidade_programada"))
        .alias("indisponibilidade_programada")
    )
    df = _apply_thermal_changes(df)
    df = _apply_maintenance(df)
    return df


def _thermal_generation_bounds_term_manutt_expt(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    def _expand_to_stages(df: pl.DataFrame) -> pl.DataFrame:
        dates = temporal.stages_starting_dates_final_simulation(
            deck_cls, cache, uow
        )
        dates_df = pl.DataFrame({START_DATE_COL: dates})
        return df.join(dates_df, how="cross").sort(
            [THERMAL_CODE_COL, START_DATE_COL]
        )

    def _add_term_lower_bounds(
        df: pl.DataFrame, term: pl.DataFrame
    ) -> pl.DataFrame:
        stage_dates = temporal.stages_starting_dates_final_simulation(
            deck_cls, cache, uow
        )
        initial_month = stage_dates[0].month
        term_filtered = term.filter(pl.col("mes") >= initial_month)
        last_term_month = term_filtered["mes"].max()
        last_term_block = term_filtered.filter(pl.col("mes") == last_term_month)
        num_repeats = len(stage_dates) - (12 - initial_month) - 1
        term_parts: list[pl.DataFrame] = [term_filtered]
        for n in range(1, num_repeats):
            extra = last_term_block.with_columns(
                (pl.col("mes") + n).alias("mes")
            )
            term_parts.append(extra)
        term_full = pl.concat(term_parts).sort([THERMAL_CODE_COL, "mes"])
        # Assign lower bounds by position (same thermal code order as expanded df)
        lower_bounds = term_full["geracao_minima"].to_numpy()
        return df.with_columns(pl.Series(LOWER_BOUND_COL, lower_bounds))

    def _enforce_null_lower_bounds_on_changes(df: pl.DataFrame) -> pl.DataFrame:
        expt = accessors.expt(deck_cls, cache, uow)
        thermals_to_nullify = (
            expt.filter(pl.col("tipo") == "GTMIN")["codigo_usina"]
            .unique()
            .to_list()
        )
        maintenance_end_date = temporal.thermal_maintenance_end_date(
            deck_cls, cache, uow
        )
        return df.with_columns(
            pl.when(
                pl.col(THERMAL_CODE_COL).is_in(thermals_to_nullify)
                & (pl.col(START_DATE_COL) >= maintenance_end_date)
            )
            .then(pl.lit(0.0))
            .otherwise(pl.col(LOWER_BOUND_COL))
            .alias(LOWER_BOUND_COL)
        )

    def _enforce_null_upper_bounds_on_changes(df: pl.DataFrame) -> pl.DataFrame:
        expt = accessors.expt(deck_cls, cache, uow)
        thermals_to_nullify = (
            expt.filter(pl.col("tipo") == "POTEF")["codigo_usina"]
            .unique()
            .to_list()
        )
        maintenance_end_date = temporal.thermal_maintenance_end_date(
            deck_cls, cache, uow
        )
        return df.with_columns(
            pl.when(
                pl.col(THERMAL_CODE_COL).is_in(thermals_to_nullify)
                & (pl.col(START_DATE_COL) >= maintenance_end_date)
            )
            .then(pl.lit(0.0))
            .otherwise(pl.col("potencia_instalada"))
            .alias("potencia_instalada")
        )

    def _eval_upper_bounds(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            (
                pl.col("potencia_instalada")
                * (pl.col("fator_capacidade_maximo") / 100.0)
                * (100.0 - pl.col("indisponibilidade_programada"))
                / 100.0
                * (100.0 - pl.col("teif"))
                / 100.0
            ).alias(UPPER_BOUND_COL)
        )

    term = accessors.term(deck_cls, cache, uow)
    bounds_df = term.unique(
        subset=["codigo_usina", "nome_usina"], maintain_order=True
    ).sort(THERMAL_CODE_COL)
    bounds_df = _expand_to_stages(bounds_df)
    bounds_df = _add_term_lower_bounds(bounds_df, term)
    bounds_df = _enforce_null_lower_bounds_on_changes(bounds_df)
    bounds_df = _enforce_null_upper_bounds_on_changes(bounds_df)
    bounds_df = _apply_thermal_bounds_maintenance_and_changes(
        deck_cls, cache, bounds_df, uow
    )
    bounds_df = _eval_upper_bounds(bounds_df)
    return bounds_df.select(
        [
            THERMAL_CODE_COL,
            "nome_usina",
            START_DATE_COL,
            LOWER_BOUND_COL,
            UPPER_BOUND_COL,
        ]
    )


def _thermal_generation_bounds_pmo(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> Optional[pl.DataFrame]:
    pmo = accessors.pmo(deck_cls, cache, uow)
    bounds_pd = pmo.geracao_minima_usinas_termicas
    if bounds_pd is None or not hasattr(bounds_pd, "rename"):
        return None
    bounds_df = pl.from_pandas(bounds_pd).rename(
        {"data": START_DATE_COL, "valor_MWmed": LOWER_BOUND_COL}
    )
    upper_pd = pmo.geracao_maxima_usinas_termicas
    if upper_pd is not None and hasattr(upper_pd, "rename"):
        upper_df = pl.from_pandas(upper_pd)
        bounds_df = bounds_df.with_columns(
            upper_df["valor_MWmed"].alias(UPPER_BOUND_COL)
        )
    start_date = temporal.stages_starting_dates_final_simulation(
        deck_cls, cache, uow
    )[0]
    bounds_df = bounds_df.filter(pl.col(START_DATE_COL) >= start_date)
    bounds_df = temporal.consider_post_study_years(
        deck_cls, cache, bounds_df, uow
    )
    return bounds_df


def thermal_generation_bounds(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    def _add_submarket_data(df: pl.DataFrame) -> pl.DataFrame:
        thermal_map = entities.thermal_submarket_map(deck_cls, cache, uow)
        df = df.rename(
            {
                "nome_usina": THERMAL_NAME_COL,
                "codigo_usina": THERMAL_CODE_COL,
            }
        )
        return df.join(
            thermal_map.select(
                [THERMAL_CODE_COL, SUBMARKET_CODE_COL, SUBMARKET_NAME_COL]
            ),
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
    return val


def thermal_costs(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    def _build_base_costs_df() -> pl.DataFrame:
        clast_df = accessors.clast(deck_cls, cache, uow).rename(
            {"codigo_usina": THERMAL_CODE_COL, "valor": VALUE_COL}
        )
        starting_year = temporal.study_period_starting_year(
            deck_cls, cache, uow
        )
        months = list(range(1, 13))
        expanded = clast_df.join(pl.DataFrame({"mes": months}), how="cross")
        expanded = expanded.with_columns(
            pl.map_batches(
                [
                    pl.lit(starting_year) + pl.col("indice_ano_estudo") - 1,
                    pl.col("mes"),
                ],
                lambda cols: pl.Series(
                    [
                        datetime(int(y), int(m), 1)
                        for y, m in zip(cols[0].to_list(), cols[1].to_list())
                    ]
                ),
            ).alias(START_DATE_COL)
        )
        return expanded.drop(
            ["nome_usina", "tipo_combustivel", "indice_ano_estudo", "mes"]
        )

    def _apply_thermal_cost_changes(df: pl.DataFrame) -> pl.DataFrame:
        clast_changes_pd = readers.validate_data(
            deck_cls,
            readers.get_clast(deck_cls, uow).modificacoes,
            object,
            "modificações dos custos de térmicas (clast.dat)",
        )
        clast_changes_df = pl.from_pandas(clast_changes_pd)
        stage_dates = temporal.stages_starting_dates_final_simulation(
            deck_cls, cache, uow
        )
        clast_changes_df = clast_changes_df.with_columns(
            pl.col("data_fim").fill_null(pl.lit(stage_dates[-1]))
        )
        for row in clast_changes_df.iter_rows(named=True):
            df = _apply_thermal_single_change(
                df,
                row["codigo_usina"],
                row["data_inicio"],
                row["data_fim"],
                VALUE_COL,
                row["custo"],
            )
        return df

    val = cache.get("thermal_costs")
    if val is None:
        df = _build_base_costs_df()
        df = _apply_thermal_cost_changes(df)
        stage_dates = temporal.stages_starting_dates_final_simulation(
            deck_cls, cache, uow
        )
        df = df.filter(pl.col(START_DATE_COL).is_in(stage_dates))
        val = df.select([THERMAL_CODE_COL, START_DATE_COL, VALUE_COL])
        cache["thermal_costs"] = val
    return val
