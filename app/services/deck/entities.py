from typing import Any, Dict, List

import numpy as np
import pandas as pd

from app.internal.constants import (
    BLOCK_COL,
    EER_CODE_COL,
    EER_NAME_COL,
    HYDRO_CODE_COL,
    HYDRO_NAME_COL,
    LOWER_BOUND_COL,
    LOWER_BOUND_UNIT_COL,
    START_DATE_COL,
    STRING_DF_TYPE,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    THERMAL_CODE_COL,
    THERMAL_NAME_COL,
    UPPER_BOUND_COL,
    UPPER_BOUND_UNIT_COL,
    VALUE_COL,
)
from app.model.operation.unit import Unit
from app.services.deck import readers, temporal
from app.services.unitofwork import AbstractUnitOfWork


def submarkets(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("submarkets")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_sistema(deck_cls, uow).custo_deficit,
            pd.DataFrame,
            "submercados e custos de deficit (sistema.dat)",
        )
        val = val.rename(
            columns={
                "codigo_submercado": SUBMARKET_CODE_COL,
                "nome_submercado": SUBMARKET_NAME_COL,
            }
        )
        val = val.drop_duplicates(subset=[SUBMARKET_CODE_COL]).reset_index(
            drop=True
        )
        val = val.astype({SUBMARKET_NAME_COL: STRING_DF_TYPE})
        val = val.set_index(SUBMARKET_CODE_COL)
        cache["submarkets"] = val
    return val.copy()


def eers(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("eers")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_ree(deck_cls, uow).rees,
            pd.DataFrame,
            "REEs e periodo individualizado (ree.dat)",
        )
        val = val.rename(
            columns={
                "codigo": EER_CODE_COL,
                "nome": EER_NAME_COL,
                "submercado": SUBMARKET_CODE_COL,
            }
        )
        val = val.astype({EER_NAME_COL: STRING_DF_TYPE})
        val = val.set_index(EER_CODE_COL)
        cache["eers"] = val
    return val.copy()


def hydros(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("hydros")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_confhd(deck_cls, uow).usinas,
            pd.DataFrame,
            "cadastro de usinas hidreletricas (confhd.dat)",
        )
        val = val.rename(
            columns={
                "codigo_usina": HYDRO_CODE_COL,
                "nome_usina": HYDRO_NAME_COL,
                "ree": EER_CODE_COL,
            }
        )
        val = val.astype({HYDRO_NAME_COL: STRING_DF_TYPE})
        val = val.set_index(HYDRO_CODE_COL)
        cache["hydros"] = val
    return val.copy()


def thermals(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("thermals")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_conft(deck_cls, uow).usinas,
            pd.DataFrame,
            "cadastro de usinas termeletricas (conft.dat)",
        )
        val = val.drop(columns="codigo_usina")
        val = val.rename(
            columns={
                "classe": THERMAL_CODE_COL,
                "nome_usina": THERMAL_NAME_COL,
                "submercado": SUBMARKET_CODE_COL,
            }
        )
        val = val.astype({THERMAL_NAME_COL: STRING_DF_TYPE})
        val = val.set_index(THERMAL_CODE_COL)
        cache["thermals"] = val
    return val.copy()


def eer_code_order(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> List[int]:
    val = cache.get("eer_code_order")
    if val is None:
        val = eers(deck_cls, cache, uow).index.tolist()
        cache["eer_code_order"] = val
    return val


def hydro_code_order(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> List[int]:
    val = cache.get("hydro_code_order")
    if val is None:
        val = hydros(deck_cls, cache, uow).index.tolist()
        cache["hydro_code_order"] = val
    return val


def hydro_eer_submarket_map(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("hydro_eer_submarket_map")
    if val is None:
        hydros_df = hydros(deck_cls, cache, uow)
        eers_df = eers(deck_cls, cache, uow)
        submarkets_df = submarkets(deck_cls, cache, uow)
        val = hydros_df[[HYDRO_NAME_COL, EER_CODE_COL]].copy()
        val = val.join(
            eers_df[[EER_NAME_COL, SUBMARKET_CODE_COL]], on=EER_CODE_COL
        )
        val = val.join(
            submarkets_df[[SUBMARKET_NAME_COL]], on=SUBMARKET_CODE_COL
        )
        cache["hydro_eer_submarket_map"] = val
    return val.copy()


def eer_submarket_map(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("eer_submarket_map")
    if val is None:
        aux = hydro_eer_submarket_map(deck_cls, cache, uow)
        aux = aux.drop_duplicates(subset=[EER_CODE_COL]).reset_index(drop=True)[
            [
                EER_CODE_COL,
                EER_NAME_COL,
                SUBMARKET_CODE_COL,
                SUBMARKET_NAME_COL,
            ]
        ]
        val = aux.set_index(EER_CODE_COL)
        cache["eer_submarket_map"] = val
    return val.copy()


def thermal_submarket_map(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    val = cache.get("thermal_submarket_map")
    if val is None:
        thermals_df = thermals(deck_cls, cache, uow).reset_index()
        thermals_df = thermals_df.set_index(THERMAL_NAME_COL)
        submarkets_df = submarkets(deck_cls, cache, uow)

        val = pd.DataFrame(
            data={
                THERMAL_CODE_COL: thermals_df[THERMAL_CODE_COL].tolist(),
                THERMAL_NAME_COL: thermals_df.index.tolist(),
                SUBMARKET_CODE_COL: thermals_df[SUBMARKET_CODE_COL].tolist(),
            }
        )
        val[SUBMARKET_NAME_COL] = val[SUBMARKET_CODE_COL].apply(
            lambda c: submarkets_df.at[c, SUBMARKET_NAME_COL]
        )
        val = val.set_index(THERMAL_CODE_COL)
        cache["thermal_submarket_map"] = val
    return val.copy()


def hybrid_policy(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> bool:
    val = cache.get("hybrid_policy")
    if val is None:
        eers_df = eers(deck_cls, cache, uow)
        raw = bool(eers_df["ano_fim_individualizado"].isna().sum() == 0)
        val = readers.validate_data(
            deck_cls,
            raw,
            bool,
            "fim do horizonte individualizado (ree.dat)",
        )
        cache["hybrid_policy"] = val
    return val


def flow_diversion(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    # Import misc here to avoid circular: misc does not import entities

    def _filter_stages(df: pd.DataFrame) -> pd.DataFrame:
        return df.loc[
            df[START_DATE_COL].isin(
                temporal.stages_starting_dates_final_simulation(
                    deck_cls, cache, uow
                )
            )
        ].copy()

    def _add_missing_hydros(df: pd.DataFrame) -> pd.DataFrame:
        hydros_df = hydros(deck_cls, cache, uow)
        hydro_codes = hydros_df.index.tolist()
        hydro_codes_in_df = df[HYDRO_CODE_COL].unique().tolist()
        missing_hydros = [c for c in hydro_codes if c not in hydro_codes_in_df]
        dfs_missing_hydros: list[pd.DataFrame] = []
        for c in missing_hydros:
            df_hydro = df.loc[df[HYDRO_CODE_COL] == hydro_codes_in_df[0]].copy()
            df_hydro[HYDRO_CODE_COL] = c
            df_hydro[VALUE_COL] = 0.0
            dfs_missing_hydros.append(df_hydro)
        df = pd.concat([df] + dfs_missing_hydros, ignore_index=True)
        df = df.sort_values([HYDRO_CODE_COL, START_DATE_COL])
        return df

    def _make_bound_columns(df: pd.DataFrame) -> pd.DataFrame:
        df[VALUE_COL] *= -1
        df[LOWER_BOUND_COL] = df[VALUE_COL]
        df[UPPER_BOUND_COL] = df[VALUE_COL]
        df[LOWER_BOUND_UNIT_COL] = Unit.m3s.value
        df[UPPER_BOUND_UNIT_COL] = Unit.m3s.value
        return df

    val = cache.get("flow_diversion")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_dsvagua(deck_cls, uow).desvios,
            pd.DataFrame,
            "retiradas de agua das usinas hidreletricas (dsvagua.dat)",
        )
        val = val.rename(
            columns={
                "codigo_usina": HYDRO_CODE_COL,
                "data": START_DATE_COL,
            }
        )
        val = (
            val.groupby([HYDRO_CODE_COL, START_DATE_COL])[VALUE_COL]
            .sum()
            .reset_index()
        )
        val = _filter_stages(val)
        val = _add_missing_hydros(val)
        val = _make_bound_columns(val)
        val[BLOCK_COL] = 0
        val = val.reset_index(drop=True)
        val = temporal.consider_post_study_years(deck_cls, cache, val, uow)
        cache["flow_diversion"] = val
    return val.copy()


def non_simulated_generation(
    deck_cls, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pd.DataFrame:
    # Import misc here to avoid circular: misc does not import entities
    from app.services.deck import misc as misc_mod

    def _generate_MWmed_generation_by_block() -> pd.DataFrame:
        tmp_col = "unsi_block"
        generation = readers.validate_data(
            deck_cls,
            readers.get_sistema(deck_cls, uow).geracao_usinas_nao_simuladas,
            pd.DataFrame,
            "geração de UNSI (sistema.dat)",
        )
        generation = generation.rename(
            columns={
                "data": START_DATE_COL,
                "codigo_submercado": SUBMARKET_CODE_COL,
                "indice_bloco": tmp_col,
                "valor": VALUE_COL,
            }
        )
        generation = generation.sort_values(
            [
                SUBMARKET_CODE_COL,
                tmp_col,
                START_DATE_COL,
            ]
        ).reset_index(drop=True)
        factors = readers.validate_data(
            deck_cls,
            readers.get_patamar(deck_cls, uow).usinas_nao_simuladas,
            pd.DataFrame,
            "profundidades da geração de UNSI (patamar.dat)",
        )
        factors = factors.rename(
            columns={
                "data": START_DATE_COL,
                "codigo_submercado": SUBMARKET_CODE_COL,
                "indice_bloco": tmp_col,
                "patamar": BLOCK_COL,
                "valor": VALUE_COL,
            }
        )
        df = factors.sort_values(
            [
                SUBMARKET_CODE_COL,
                tmp_col,
                START_DATE_COL,
                BLOCK_COL,
            ]
        ).reset_index(drop=True)
        num_blocks_val = misc_mod.num_blocks(deck_cls, cache, uow)
        block_generations = generation.loc[
            generation.index.repeat(num_blocks_val), VALUE_COL
        ].to_numpy()
        df[VALUE_COL] = df[VALUE_COL].to_numpy() * block_generations
        df = (
            df.groupby(
                [
                    START_DATE_COL,
                    SUBMARKET_CODE_COL,
                    BLOCK_COL,
                ]
            )
            .sum()
            .reset_index()
        )
        return df.drop(columns=[tmp_col])

    def _cast_generation_to_MWmes(
        generation_df: pd.DataFrame,
        block_length_df: pd.DataFrame,
    ) -> pd.DataFrame:
        block_length_df = block_length_df.sort_values(
            [
                START_DATE_COL,
                BLOCK_COL,
            ]
        )
        num_submarkets = generation_df.drop_duplicates(
            [
                SUBMARKET_CODE_COL,
            ]
        ).shape[0]
        generation_df[VALUE_COL] *= np.tile(
            block_length_df[VALUE_COL].to_numpy(), num_submarkets
        )
        return generation_df

    def _eval_pat0(df: pd.DataFrame) -> pd.DataFrame:
        df_block_0 = (
            df.groupby([SUBMARKET_CODE_COL, START_DATE_COL])
            .mean()
            .reset_index()
        )
        df_block_0[BLOCK_COL] = 0
        df = pd.concat([df, df_block_0], ignore_index=True)
        df.sort_values(
            [
                SUBMARKET_CODE_COL,
                START_DATE_COL,
                BLOCK_COL,
            ],
            inplace=True,
        )
        return df.reset_index(drop=True)

    val = cache.get("non_simulated_generation")
    if val is None:
        generation_df = _generate_MWmed_generation_by_block()
        generation_df = _eval_pat0(generation_df)
        block_length_df = misc_mod.block_lengths(deck_cls, cache, uow)
        generation_df = _cast_generation_to_MWmes(
            generation_df, block_length_df
        )
        generation_df = temporal.consider_post_study_years(
            deck_cls, cache, generation_df, uow
        )
        val = generation_df
        cache["non_simulated_generation"] = val
    return val.copy()
