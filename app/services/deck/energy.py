from __future__ import annotations

from typing import Any, Dict

import polars as pl

from app.internal.constants import (
    CONFIG_COL,
    EER_CODE_COL,
    EER_NAME_COL,
    FOLLOWING_HYDRO_COL,
    HYDRO_CODE_COL,
    LOWER_BOUND_COL,
    PRODUCTIVITY_TMP_COL,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
    VOLUME_REGULATION_COL,
)
from app.services.deck import accessors, entities, readers, temporal
from app.services.unitofwork import AbstractUnitOfWork


def convergence(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("convergence")
    if val is None:
        pmo = accessors.pmo(deck_cls, cache, uow)
        raw = readers.validate_data(
            deck_cls,
            pmo.convergencia,
            object,
            "processo iterativo de convergencia (pmo.dat)",
        )
        val = pl.from_pandas(raw)
        cache["convergence"] = val
    return val


def stored_energy_upper_bounds_inputs(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame | None:
    """
    Calcula os limites superiores de armazenamento de energia a partir
    dos dados de entrada (CONFHD, HIDR), sem usar o PMO.
    """
    # Import lazily to avoid circular: hydro imports entities which doesn't
    # import energy; storage imports entities which doesn't import energy.
    from app.services.deck import hydro as hydro_mod
    from app.services.deck import storage as storage_mod

    ABSOLUTE_VALUE_COL = "valor_hm3"

    def _join_drop_data(df: pl.DataFrame, stage_date: object) -> pl.DataFrame:
        drops_pl = hydro_mod.hydro_drops_in_stages(deck_cls, cache, uow)
        drops_df = drops_pl.filter(pl.col(START_DATE_COL) == stage_date).drop(
            "usina"
        )
        return df.join(drops_df, on=HYDRO_CODE_COL, how="inner")

    def _join_hydros_data(df: pl.DataFrame) -> pl.DataFrame:
        hydros_df = entities.hydros(deck_cls, cache, uow).select(
            [HYDRO_CODE_COL, FOLLOWING_HYDRO_COL]
        )
        return df.join(hydros_df, on=HYDRO_CODE_COL, how="inner")

    def _join_bounds_data(df: pl.DataFrame) -> pl.DataFrame:
        bounds_df = hydro_mod.hydro_volume_bounds_with_changes(
            deck_cls, cache, uow
        ).select([HYDRO_CODE_COL, LOWER_BOUND_COL, UPPER_BOUND_COL])
        return df.join(bounds_df, on=HYDRO_CODE_COL, how="inner")

    def _volume_to_energy(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.when(pl.col(VOLUME_REGULATION_COL) != "M")
            .then(pl.lit(0.0))
            .otherwise(pl.col(ABSOLUTE_VALUE_COL))
            .alias(ABSOLUTE_VALUE_COL)
        ).with_columns(
            (pl.col(ABSOLUTE_VALUE_COL) * pl.col(PRODUCTIVITY_TMP_COL)).alias(
                ABSOLUTE_VALUE_COL
            )
        )

    configuration_df = temporal.configurations(deck_cls, cache, uow)
    dates = temporal.stages_starting_dates_final_simulation(
        deck_cls, cache, uow
    )

    df_init = storage_mod.initial_stored_volume(deck_cls, cache, uow)
    stage_pl_dfs: list[pl.DataFrame] = []
    for row in configuration_df.iter_rows(named=True):
        configuration_date = row[START_DATE_COL]
        if configuration_date not in dates:
            continue
        stage_df = df_init
        stage_df = _join_drop_data(stage_df, configuration_date)
        stage_df = _join_bounds_data(stage_df)
        stage_df = _join_hydros_data(stage_df)
        stage_df = stage_df.with_columns(
            (pl.col(UPPER_BOUND_COL) - pl.col(LOWER_BOUND_COL)).alias(
                ABSOLUTE_VALUE_COL
            )
        )
        stage_df = storage_mod.evaluate_productivity(
            stage_df, volume_col=ABSOLUTE_VALUE_COL
        )
        stage_df = storage_mod.accumulate_productivity(stage_df)
        stage_df = stage_df.with_columns(
            [
                pl.lit(row[VALUE_COL]).alias(CONFIG_COL),
                pl.lit(configuration_date).alias(START_DATE_COL),
            ]
        )
        stage_pl_dfs.append(stage_df)

    df_pl = pl.concat(stage_pl_dfs)
    df_pl = _volume_to_energy(df_pl)

    eer_submarket_map_df = entities.eer_submarket_map(deck_cls, cache, uow)
    hydro_eer_map_df = entities.hydro_eer_submarket_map(deck_cls, cache, uow)
    df_pl = df_pl.join(
        hydro_eer_map_df.select(
            [
                HYDRO_CODE_COL,
                EER_CODE_COL,
                EER_NAME_COL,
                SUBMARKET_CODE_COL,
                SUBMARKET_NAME_COL,
            ]
        ),
        on=HYDRO_CODE_COL,
        how="inner",
    )

    df_pl = df_pl.group_by(
        [
            START_DATE_COL,
            CONFIG_COL,
            EER_CODE_COL,
            EER_NAME_COL,
            SUBMARKET_CODE_COL,
            SUBMARKET_NAME_COL,
        ]
    ).agg(pl.col(ABSOLUTE_VALUE_COL).sum())

    eer_codes = entities.eer_code_order(deck_cls, cache, uow)
    existing_eer_codes = df_pl[EER_CODE_COL].unique().to_list()
    missing_eer_codes = [c for c in eer_codes if c not in existing_eer_codes]

    if missing_eer_codes:
        date_config_df = df_pl.select([START_DATE_COL, CONFIG_COL]).unique()
        missing_eer_meta = eer_submarket_map_df.filter(
            pl.col(EER_CODE_COL).is_in(missing_eer_codes)
        )
        missing_df = date_config_df.join(
            missing_eer_meta, how="cross"
        ).with_columns(pl.lit(0.0).alias(ABSOLUTE_VALUE_COL))
        df_pl = pl.concat([df_pl, missing_df])

    df_pl = df_pl.sort([EER_CODE_COL, START_DATE_COL, CONFIG_COL])
    df_pl = df_pl.rename({ABSOLUTE_VALUE_COL: VALUE_COL})
    df_pl = df_pl.select(
        [
            START_DATE_COL,
            CONFIG_COL,
            EER_CODE_COL,
            EER_NAME_COL,
            SUBMARKET_CODE_COL,
            SUBMARKET_NAME_COL,
            VALUE_COL,
        ]
    )
    return df_pl


def stored_energy_upper_bounds_pmo(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame | None:
    """
    Obtem os limites superiores de armazenamento de energia para
    cada REE em MWmes, para o período de estudo.
    """

    def _filter_study_period(df: pl.DataFrame) -> pl.DataFrame:
        dates = temporal.stages_starting_dates_final_simulation(
            deck_cls, cache, uow
        )
        return df.filter(pl.col(START_DATE_COL).is_between(dates[0], dates[-1]))

    def _add_entity_data(df: pl.DataFrame) -> pl.DataFrame:
        eers_list = entities.eer_code_order(deck_cls, cache, uow)
        eer_df = pl.DataFrame({EER_CODE_COL: eers_list})
        df = df.join(eer_df, how="cross")
        entity_map = entities.eer_submarket_map(deck_cls, cache, uow)
        return df.join(entity_map, on=EER_CODE_COL, how="left")

    def _add_values(
        df: pl.DataFrame, maximum_storage_df: pl.DataFrame
    ) -> pl.DataFrame:
        lookup = maximum_storage_df.select(
            [EER_NAME_COL, CONFIG_COL, pl.col(VALUE_COL).alias("_max_val")]
        )
        df = df.join(lookup, on=[EER_NAME_COL, CONFIG_COL], how="left")
        return df.with_columns(pl.col("_max_val").alias(VALUE_COL)).drop(
            "_max_val"
        )

    raw_maximum_storage = accessors.pmo(
        deck_cls, cache, uow
    ).energia_armazenada_maxima

    if raw_maximum_storage is None:
        return None

    maximum_storage_df = pl.from_pandas(raw_maximum_storage).rename(
        {
            "nome_ree": EER_NAME_COL,
            "configuracao": CONFIG_COL,
            "valor_MWmes": VALUE_COL,
        }
    )
    configs_df = temporal.configurations(deck_cls, cache, uow).rename(
        {VALUE_COL: CONFIG_COL}
    )
    configs_df = _filter_study_period(configs_df)
    configs_df = _add_entity_data(configs_df)
    configs_df = _add_values(configs_df, maximum_storage_df)
    return configs_df.sort([EER_CODE_COL, START_DATE_COL])


def stored_energy_upper_bounds(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("stored_energy_upper_bounds")
    if val is None:
        bounds_df = stored_energy_upper_bounds_pmo(deck_cls, cache, uow)
        if bounds_df is None:
            bounds_df = stored_energy_upper_bounds_inputs(deck_cls, cache, uow)
        assert isinstance(bounds_df, pl.DataFrame)
        val = bounds_df
        cache["stored_energy_upper_bounds"] = val
    return val


def eer_stored_energy_lower_bounds(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    """
    Obtem os limites inferiores de armazenamento de energia para
    cada REE, convertendo os valores de percentual para MWmes,
    para o período de estudo.
    """

    def _add_missing_eer_bounds(df: pl.DataFrame) -> pl.DataFrame:
        start_date = temporal.stages_starting_dates_final_simulation(
            deck_cls, cache, uow
        )[0]
        df = df.filter(pl.col(START_DATE_COL) >= start_date)
        eers_minimum_storage = df[EER_CODE_COL].unique().to_list()
        eer_codes = entities.eer_code_order(deck_cls, cache, uow)
        missing_eers = list(set(eer_codes).difference(eers_minimum_storage))
        if not missing_eers:
            return df
        template_eer = eers_minimum_storage[0]
        template_df = df.filter(pl.col(EER_CODE_COL) == template_eer)
        eer_dtype = df.schema[EER_CODE_COL]
        missing_dfs = [
            template_df.with_columns(
                [
                    pl.lit(eer_code).cast(eer_dtype).alias(EER_CODE_COL),
                    pl.lit(0.0).alias(VALUE_COL),
                ]
            )
            for eer_code in missing_eers
        ]
        return pl.concat([df] + missing_dfs)

    def _cast_perc_to_absolute(df: pl.DataFrame) -> pl.DataFrame:
        upper_bound_df = stored_energy_upper_bounds(deck_cls, cache, uow)
        df = df.sort([EER_CODE_COL, START_DATE_COL])
        upper_bound_df = upper_bound_df.sort([EER_CODE_COL, START_DATE_COL])
        upper_lookup = upper_bound_df.select(
            [
                EER_CODE_COL,
                START_DATE_COL,
                pl.col(VALUE_COL).alias("_upper_val"),
            ]
        )
        return (
            df.join(upper_lookup, on=[EER_CODE_COL, START_DATE_COL], how="left")
            .with_columns(
                (pl.col(VALUE_COL) * pl.col("_upper_val") / 100.0).alias(
                    VALUE_COL
                )
            )
            .drop("_upper_val")
        )

    def _add_entity_data(df: pl.DataFrame) -> pl.DataFrame:
        entity_map = entities.eer_submarket_map(deck_cls, cache, uow)
        return df.join(entity_map, on=EER_CODE_COL, how="left")

    val = cache.get("eer_stored_energy_lower_bounds")
    if val is None:
        raw = readers.validate_data(
            deck_cls,
            accessors.curva(deck_cls, cache, uow).curva_seguranca,
            object,
            "curva de seguranca (curva.dat)",
        )
        minimum_perc_storage_df = pl.from_pandas(raw).rename(
            {"data": START_DATE_COL}
        )
        lower_bound_df = _add_missing_eer_bounds(minimum_perc_storage_df)
        lower_bound_df = _cast_perc_to_absolute(lower_bound_df)
        lower_bound_df = _add_entity_data(lower_bound_df)
        val = temporal.consider_post_study_years(
            deck_cls, cache, lower_bound_df, uow
        )
        cache["eer_stored_energy_lower_bounds"] = val
    return val
