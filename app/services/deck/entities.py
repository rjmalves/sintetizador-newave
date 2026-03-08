from typing import Any, Dict, List

import numpy as np
import polars as pl

from app.internal.constants import (
    BLOCK_COL,
    EER_CODE_COL,
    EER_NAME_COL,
    HYDRO_CODE_COL,
    HYDRO_NAME_COL,
    LOWER_BOUND_COL,
    LOWER_BOUND_UNIT_COL,
    START_DATE_COL,
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
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("submarkets")
    if val is None:
        raw = readers.validate_data(
            deck_cls,
            readers.get_sistema(deck_cls, uow).custo_deficit,
            object,
            "submercados e custos de deficit (sistema.dat)",
        )
        val = pl.from_pandas(raw)
        val = val.rename(
            {
                "codigo_submercado": SUBMARKET_CODE_COL,
                "nome_submercado": SUBMARKET_NAME_COL,
            }
        )
        val = val.unique(subset=[SUBMARKET_CODE_COL], maintain_order=True)
        val = val.with_columns(pl.col(SUBMARKET_NAME_COL).cast(pl.Utf8))
        cache["submarkets"] = val
    return val


def eers(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("eers")
    if val is None:
        raw = readers.validate_data(
            deck_cls,
            readers.get_ree(deck_cls, uow).rees,
            object,
            "REEs e periodo individualizado (ree.dat)",
        )
        val = pl.from_pandas(raw)
        val = val.rename(
            {
                "codigo": EER_CODE_COL,
                "nome": EER_NAME_COL,
                "submercado": SUBMARKET_CODE_COL,
            }
        )
        val = val.with_columns(pl.col(EER_NAME_COL).cast(pl.Utf8))
        cache["eers"] = val
    return val


def hydros(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("hydros")
    if val is None:
        raw = readers.validate_data(
            deck_cls,
            readers.get_confhd(deck_cls, uow).usinas,
            object,
            "cadastro de usinas hidreletricas (confhd.dat)",
        )
        val = pl.from_pandas(raw)
        val = val.rename(
            {
                "codigo_usina": HYDRO_CODE_COL,
                "nome_usina": HYDRO_NAME_COL,
                "ree": EER_CODE_COL,
            }
        )
        val = val.with_columns(pl.col(HYDRO_NAME_COL).cast(pl.Utf8))
        cache["hydros"] = val
    return val


def thermals(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("thermals")
    if val is None:
        raw = readers.validate_data(
            deck_cls,
            readers.get_conft(deck_cls, uow).usinas,
            object,
            "cadastro de usinas termeletricas (conft.dat)",
        )
        val = pl.from_pandas(raw)
        val = val.drop("codigo_usina")
        val = val.rename(
            {
                "classe": THERMAL_CODE_COL,
                "nome_usina": THERMAL_NAME_COL,
                "submercado": SUBMARKET_CODE_COL,
            }
        )
        val = val.with_columns(pl.col(THERMAL_NAME_COL).cast(pl.Utf8))
        cache["thermals"] = val
    return val


def eer_code_order(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> List[int]:
    val: List[int] | None = cache.get("eer_code_order")
    if val is not None:
        return val
    val = eers(deck_cls, cache, uow)[EER_CODE_COL].to_list()
    cache["eer_code_order"] = val
    return val


def hydro_code_order(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> List[int]:
    val: List[int] | None = cache.get("hydro_code_order")
    if val is not None:
        return val
    val = hydros(deck_cls, cache, uow)[HYDRO_CODE_COL].to_list()
    cache["hydro_code_order"] = val
    return val


def hydro_eer_submarket_map(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("hydro_eer_submarket_map")
    if val is None:
        hydros_df = hydros(deck_cls, cache, uow)
        eers_df = eers(deck_cls, cache, uow)
        submarkets_df = submarkets(deck_cls, cache, uow)
        val = hydros_df.select([HYDRO_CODE_COL, HYDRO_NAME_COL, EER_CODE_COL])
        val = val.join(
            eers_df.select([EER_CODE_COL, EER_NAME_COL, SUBMARKET_CODE_COL]),
            on=EER_CODE_COL,
        )
        val = val.join(
            submarkets_df.select([SUBMARKET_CODE_COL, SUBMARKET_NAME_COL]),
            on=SUBMARKET_CODE_COL,
        )
        cache["hydro_eer_submarket_map"] = val
    return val


def eer_submarket_map(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("eer_submarket_map")
    if val is None:
        aux = hydro_eer_submarket_map(deck_cls, cache, uow)
        val = (
            aux.unique(subset=[EER_CODE_COL])
            .select(
                [
                    EER_CODE_COL,
                    EER_NAME_COL,
                    SUBMARKET_CODE_COL,
                    SUBMARKET_NAME_COL,
                ]
            )
            .sort(EER_CODE_COL)
        )
        cache["eer_submarket_map"] = val
    return val


def thermal_submarket_map(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("thermal_submarket_map")
    if val is None:
        thermals_df = thermals(deck_cls, cache, uow)
        submarkets_df = submarkets(deck_cls, cache, uow)
        val = thermals_df.select(
            [THERMAL_CODE_COL, THERMAL_NAME_COL, SUBMARKET_CODE_COL]
        ).join(
            submarkets_df.select([SUBMARKET_CODE_COL, SUBMARKET_NAME_COL]),
            on=SUBMARKET_CODE_COL,
        )
        cache["thermal_submarket_map"] = val
    return val


def hybrid_policy(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> bool:
    val = cache.get("hybrid_policy")
    if val is None:
        eers_df = eers(deck_cls, cache, uow)
        null_count = eers_df.select(
            pl.col("ano_fim_individualizado").is_null().sum()
        ).item()
        raw = bool(null_count == 0)
        val = readers.validate_data(
            deck_cls,
            raw,
            bool,
            "fim do horizonte individualizado (ree.dat)",
        )
        cache["hybrid_policy"] = val
    return val


def flow_diversion(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    def _filter_stages(df: pl.DataFrame) -> pl.DataFrame:
        stage_dates = temporal.stages_starting_dates_final_simulation(
            deck_cls, cache, uow
        )
        return df.filter(pl.col(START_DATE_COL).is_in(stage_dates))

    def _add_missing_hydros(df: pl.DataFrame) -> pl.DataFrame:
        hydros_df = hydros(deck_cls, cache, uow)
        hydro_codes = hydros_df[HYDRO_CODE_COL].to_list()
        hydro_codes_in_df = df[HYDRO_CODE_COL].unique().to_list()
        missing_hydros = [c for c in hydro_codes if c not in hydro_codes_in_df]
        if not missing_hydros:
            return df.sort([HYDRO_CODE_COL, START_DATE_COL])

        # Add missing hydros to dataframe
        template = df.filter(pl.col(HYDRO_CODE_COL) == hydro_codes_in_df[0])
        hydro_dtype = df.schema[HYDRO_CODE_COL]
        missing_dfs = [
            template.with_columns(
                [
                    pl.lit(c).cast(hydro_dtype).alias(HYDRO_CODE_COL),
                    pl.lit(0.0).alias(VALUE_COL),
                ]
            )
            for c in missing_hydros
        ]
        return pl.concat([df] + missing_dfs).sort(
            [HYDRO_CODE_COL, START_DATE_COL]
        )

    def _make_bound_columns(df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns((pl.col(VALUE_COL) * -1).alias(VALUE_COL))
        df = df.with_columns(
            [
                pl.col(VALUE_COL).alias(LOWER_BOUND_COL),
                pl.col(VALUE_COL).alias(UPPER_BOUND_COL),
                pl.lit(Unit.m3s.value).alias(LOWER_BOUND_UNIT_COL),
                pl.lit(Unit.m3s.value).alias(UPPER_BOUND_UNIT_COL),
            ]
        )
        return df

    val = cache.get("flow_diversion")
    if val is None:
        raw = readers.validate_data(
            deck_cls,
            readers.get_dsvagua(deck_cls, uow).desvios,
            object,
            "retiradas de agua das usinas hidreletricas (dsvagua.dat)",
        )
        val = pl.from_pandas(raw)
        val = val.rename(
            {
                "codigo_usina": HYDRO_CODE_COL,
                "data": START_DATE_COL,
            }
        )
        val = val.group_by([HYDRO_CODE_COL, START_DATE_COL]).agg(
            pl.col(VALUE_COL).sum()
        )
        val = _filter_stages(val)
        val = _add_missing_hydros(val)
        val = _make_bound_columns(val)
        val = val.with_columns(pl.lit(0).alias(BLOCK_COL))
        val = temporal.consider_post_study_years(deck_cls, cache, val, uow)
        cache["flow_diversion"] = val
    return val


def non_simulated_generation(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    from app.services.deck import misc as misc_mod

    def _generate_MWmed_generation_by_block() -> pl.DataFrame:
        tmp_col = "unsi_block"
        generation = readers.validate_data(
            deck_cls,
            readers.get_sistema(deck_cls, uow).geracao_usinas_nao_simuladas,
            object,
            "geração de UNSI (sistema.dat)",
        )
        generation = pl.from_pandas(generation)
        generation = generation.rename(
            {
                "data": START_DATE_COL,
                "codigo_submercado": SUBMARKET_CODE_COL,
                "indice_bloco": tmp_col,
                "valor": VALUE_COL,
            }
        )
        generation = generation.sort(
            [SUBMARKET_CODE_COL, tmp_col, START_DATE_COL]
        )
        factors = readers.validate_data(
            deck_cls,
            readers.get_patamar(deck_cls, uow).usinas_nao_simuladas,
            object,
            "profundidades da geração de UNSI (patamar.dat)",
        )
        factors = pl.from_pandas(factors)
        factors = factors.rename(
            {
                "data": START_DATE_COL,
                "codigo_submercado": SUBMARKET_CODE_COL,
                "indice_bloco": tmp_col,
                "patamar": BLOCK_COL,
                "valor": VALUE_COL,
            }
        )
        df = factors.sort(
            [SUBMARKET_CODE_COL, tmp_col, START_DATE_COL, BLOCK_COL]
        )
        num_blocks_val = misc_mod.num_blocks(deck_cls, cache, uow)
        # Repeat generation rows to match the num_blocks factor structure
        gen_values = np.repeat(generation[VALUE_COL].to_numpy(), num_blocks_val)
        df = df.with_columns(
            (pl.Series(VALUE_COL, gen_values) * pl.col(VALUE_COL)).alias(
                VALUE_COL
            )
        )
        df = df.group_by([START_DATE_COL, SUBMARKET_CODE_COL, BLOCK_COL]).agg(
            pl.col(VALUE_COL).sum()
        )
        return df.drop(tmp_col) if tmp_col in df.columns else df

    def _cast_generation_to_MWmes(
        generation_df: pl.DataFrame,
        block_length_df: pl.DataFrame,
    ) -> pl.DataFrame:
        block_length_df = block_length_df.sort([START_DATE_COL, BLOCK_COL])
        num_submarkets = generation_df[SUBMARKET_CODE_COL].n_unique()
        block_lengths_arr = np.tile(
            block_length_df[VALUE_COL].to_numpy(), num_submarkets
        )
        generation_df = generation_df.with_columns(
            (pl.col(VALUE_COL) * pl.Series("_bl", block_lengths_arr)).alias(
                VALUE_COL
            )
        )
        return (
            generation_df.drop("_bl")
            if "_bl" in generation_df.columns
            else generation_df
        )

    def _eval_pat0(df: pl.DataFrame) -> pl.DataFrame:
        cols = df.columns
        df_block_0 = (
            df.group_by([SUBMARKET_CODE_COL, START_DATE_COL])
            .agg(pl.col(VALUE_COL).mean())
            .with_columns(pl.lit(0).cast(df.schema[BLOCK_COL]).alias(BLOCK_COL))
            .select(cols)
        )
        df = pl.concat([df, df_block_0]).sort(
            [SUBMARKET_CODE_COL, START_DATE_COL, BLOCK_COL]
        )
        return df

    val = cache.get("non_simulated_generation")
    if val is None:
        generation_df = _generate_MWmed_generation_by_block()
        generation_df = _eval_pat0(generation_df)
        block_length_df = misc_mod.block_lengths(deck_cls, cache, uow)
        generation_df = _cast_generation_to_MWmes(
            generation_df, block_length_df
        )
        val = temporal.consider_post_study_years(
            deck_cls, cache, generation_df, uow
        )
        cache["non_simulated_generation"] = val
    return val
