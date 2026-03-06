from typing import Any, Dict

import numpy as np
import pandas as pd

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
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("convergence")
    if val is None:
        pmo = accessors.pmo(deck_cls, cache, uow)
        val = readers.validate_data(
            deck_cls,
            pmo.convergencia,
            pd.DataFrame,
            "processo iterativo de convergencia (pmo.dat)",
        )
        cache["convergence"] = val
    return val.copy()


def stored_energy_upper_bounds_inputs(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame | None:
    """
    Calcula os limites superiores de armazenamento de energia a partir
    dos dados de entrada (CONFHD, HIDR), sem usar o PMO.
    """
    # Import lazily to avoid circular: hydro imports entities which doesn't
    # import energy; storage imports entities which doesn't import energy.
    from app.services.deck import hydro as hydro_mod
    from app.services.deck import storage as storage_mod

    ABSOLUTE_VALUE_COL = "valor_hm3"

    def _join_drop_data(df: pd.DataFrame, stage_date: object) -> pd.DataFrame:
        drops_df = hydro_mod.hydro_drops_in_stages(deck_cls, cache, uow)
        drops_df = drops_df.loc[
            drops_df[START_DATE_COL] == stage_date
        ].set_index(HYDRO_CODE_COL)
        return df.drop(columns=["usina"]).join(drops_df, how="inner")

    def _join_hydros_data(df: pd.DataFrame) -> pd.DataFrame:
        hydros_df = entities.hydros(deck_cls, cache, uow)
        return df.join(hydros_df[[FOLLOWING_HYDRO_COL]], how="inner")

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
        return df

    def _cast_to_eers_and_fill_missing(
        df: pd.DataFrame, configurations_df: pd.DataFrame
    ) -> pd.DataFrame:
        df = (
            df[
                [
                    START_DATE_COL,
                    CONFIG_COL,
                    EER_CODE_COL,
                    EER_NAME_COL,
                    SUBMARKET_CODE_COL,
                    SUBMARKET_NAME_COL,
                    ABSOLUTE_VALUE_COL,
                ]
            ]
            .groupby(
                [
                    START_DATE_COL,
                    CONFIG_COL,
                    EER_CODE_COL,
                    EER_NAME_COL,
                    SUBMARKET_CODE_COL,
                    SUBMARKET_NAME_COL,
                ]
            )
            .sum()
        ).reset_index()
        eer_codes = entities.eer_code_order(deck_cls, cache, uow)
        eers_df = entities.eer_submarket_map(deck_cls, cache, uow)
        missing_eers = [
            eer for eer in eer_codes if eer not in df[EER_CODE_COL].tolist()
        ]
        missing_dfs: list[pd.DataFrame] = []
        dates = df[START_DATE_COL].unique()
        for eer in missing_eers:
            missing_df = pd.DataFrame(
                {
                    START_DATE_COL: dates,
                    CONFIG_COL: configurations_df.loc[
                        configurations_df[START_DATE_COL].isin(dates),
                        VALUE_COL,
                    ].to_numpy(),
                    EER_CODE_COL: [eer] * len(dates),
                    EER_NAME_COL: [eers_df.at[eer, EER_NAME_COL]] * len(dates),
                    SUBMARKET_CODE_COL: [eers_df.at[eer, SUBMARKET_CODE_COL]]
                    * len(dates),
                    SUBMARKET_NAME_COL: [eers_df.at[eer, SUBMARKET_NAME_COL]]
                    * len(dates),
                    ABSOLUTE_VALUE_COL: [0.0] * len(dates),
                }
            )
            missing_dfs.append(missing_df)
        df = pd.concat([df] + missing_dfs, ignore_index=True)
        df = df.sort_values([EER_CODE_COL, START_DATE_COL, CONFIG_COL])
        return df

    df = storage_mod.initial_stored_volume(deck_cls, cache, uow).set_index(
        HYDRO_CODE_COL
    )
    dfs: list[pd.DataFrame] = []
    configuration_df = temporal.configurations(deck_cls, cache, uow)
    dates = temporal.stages_starting_dates_final_simulation(
        deck_cls, cache, uow
    )
    for _, line in configuration_df.iterrows():
        configuration_date = line[START_DATE_COL]
        if configuration_date not in dates:
            continue
        # Calcula prodts no máximo
        stage_df = df.copy()
        stage_df = _join_drop_data(stage_df, configuration_date)
        stage_df = _join_bounds_data(stage_df)
        stage_df = _join_hydros_data(stage_df)
        stage_df[ABSOLUTE_VALUE_COL] = (
            stage_df[UPPER_BOUND_COL] - stage_df[LOWER_BOUND_COL]
        )
        stage_df = storage_mod.evaluate_productivity(
            stage_df, volume_col=ABSOLUTE_VALUE_COL
        )
        stage_df = storage_mod.accumulate_productivity(stage_df)
        stage_df[CONFIG_COL] = line[VALUE_COL]
        dfs.append(stage_df)

    df = pd.concat(dfs, ignore_index=True)
    df = _volume_to_energy(df)
    df = _cast_to_eers_and_fill_missing(df, configuration_df)

    df = df.rename(columns={ABSOLUTE_VALUE_COL: VALUE_COL})

    df = df[
        [
            START_DATE_COL,
            CONFIG_COL,
            EER_CODE_COL,
            EER_NAME_COL,
            SUBMARKET_CODE_COL,
            SUBMARKET_NAME_COL,
            VALUE_COL,
        ]
    ].reset_index(drop=True)
    return df


def stored_energy_upper_bounds_pmo(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame | None:
    """
    Obtem os limites superiores de armazenamento de energia para
    cada REE em MWmes, para o período de estudo.
    """

    def _filter_study_period(df: pd.DataFrame) -> pd.DataFrame:
        dates = temporal.stages_starting_dates_final_simulation(
            deck_cls, cache, uow
        )
        df = df.loc[df[START_DATE_COL].between(dates[0], dates[-1])]
        return df

    def _add_entity_data(df: pd.DataFrame) -> pd.DataFrame:
        eers_list = entities.eer_code_order(deck_cls, cache, uow)
        num_configs = df.shape[0]
        df = pd.concat([df] * len(eers_list), ignore_index=True)
        df[EER_CODE_COL] = np.repeat(eers_list, num_configs)
        entity_map = entities.eer_submarket_map(deck_cls, cache, uow)
        return df.join(entity_map, on=EER_CODE_COL)

    def _add_values(
        df: pd.DataFrame, maximum_storage_df: pd.DataFrame
    ) -> pd.DataFrame:
        df[VALUE_COL] = df.apply(
            lambda line: maximum_storage_df.loc[
                (maximum_storage_df[EER_NAME_COL] == line[EER_NAME_COL])
                & (maximum_storage_df[CONFIG_COL] == line[CONFIG_COL]),
                VALUE_COL,
            ].iloc[0],
            axis=1,
        )
        return df

    maximum_storage_df = accessors.pmo(
        deck_cls, cache, uow
    ).energia_armazenada_maxima

    if maximum_storage_df is None:
        return None

    maximum_storage_df = maximum_storage_df.rename(
        columns={
            "nome_ree": EER_NAME_COL,
            "data": START_DATE_COL,
            "valor_MWmes": VALUE_COL,
        }
    )
    configs_df = temporal.configurations(deck_cls, cache, uow)
    configs_df = configs_df.rename(
        columns={
            VALUE_COL: CONFIG_COL,
        }
    )
    configs_df = _filter_study_period(configs_df)
    configs_df = _add_entity_data(configs_df)
    configs_df = _add_values(configs_df, maximum_storage_df)
    stored_energy_upper_bounds_df = configs_df.sort_values(
        [
            EER_CODE_COL,
            START_DATE_COL,
        ]
    )

    return stored_energy_upper_bounds_df.reset_index(drop=True)


def stored_energy_upper_bounds(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("stored_energy_upper_bounds")
    if val is None:
        bounds_df = stored_energy_upper_bounds_pmo(deck_cls, cache, uow)
        if bounds_df is None:
            bounds_df = stored_energy_upper_bounds_inputs(deck_cls, cache, uow)
        val = bounds_df
        cache["stored_energy_upper_bounds"] = val
    return val.copy()


def eer_stored_energy_lower_bounds(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    """
    Obtem os limites inferiores de armazenamento de energia para
    cada REE, convertendo os valores de percentual para MWmes,
    para o período de estudo.
    """

    def _add_missing_eer_bounds(df: pd.DataFrame) -> pd.DataFrame:
        df = df.loc[
            df[START_DATE_COL]
            >= temporal.stages_starting_dates_final_simulation(
                deck_cls, cache, uow
            )[0]
        ]
        eers_minimum_storage = df[EER_CODE_COL].unique().tolist()
        eer_codes = entities.eer_code_order(deck_cls, cache, uow)
        missing_eers = list(set(eer_codes).difference(eers_minimum_storage))
        lower_bound_dfs = [df]
        for c in missing_eers:
            eer_df = df.loc[df[EER_CODE_COL] == eers_minimum_storage[0]].copy()
            eer_df[EER_CODE_COL] = c
            eer_df[VALUE_COL] = 0.0
            lower_bound_dfs.append(eer_df)
        lower_bound_df = pd.concat(lower_bound_dfs, ignore_index=True)
        lower_bound_df = lower_bound_df.sort_values(
            [
                EER_CODE_COL,
                START_DATE_COL,
            ]
        )
        return lower_bound_df

    def _cast_perc_to_absolute(df: pd.DataFrame) -> pd.DataFrame:
        upper_bound_df = stored_energy_upper_bounds(deck_cls, cache, uow)
        df = df.sort_values([EER_CODE_COL, START_DATE_COL]).reset_index(
            drop=True
        )
        upper_bound_df = upper_bound_df.sort_values(
            [
                EER_CODE_COL,
                START_DATE_COL,
            ]
        ).reset_index(drop=True)
        df[VALUE_COL] = df[VALUE_COL] * upper_bound_df[VALUE_COL] / 100.0
        return df

    def _add_entity_data(df: pd.DataFrame) -> pd.DataFrame:
        entity_map = entities.eer_submarket_map(deck_cls, cache, uow)
        return df.join(entity_map, on=EER_CODE_COL)

    val = cache.get("eer_stored_energy_lower_bounds")
    if val is None:
        minimum_perc_storage_df = readers.validate_data(
            deck_cls,
            accessors.curva(deck_cls, cache, uow).curva_seguranca,
            pd.DataFrame,
            "curva de seguranca (curva.dat)",
        )
        minimum_perc_storage_df = minimum_perc_storage_df.rename(
            columns={"data": START_DATE_COL}
        )
        lower_bound_df = _add_missing_eer_bounds(minimum_perc_storage_df)
        lower_bound_df = _cast_perc_to_absolute(lower_bound_df)
        val = _add_entity_data(lower_bound_df)
        val = temporal.consider_post_study_years(deck_cls, cache, val, uow)
        cache["eer_stored_energy_lower_bounds"] = val
    return val.copy()
