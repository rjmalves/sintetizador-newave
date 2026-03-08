from typing import Any, Dict

import polars as pl

from app.internal.constants import (
    BLOCK_COL,
    START_DATE_COL,
    VALUE_COL,
)
from app.services.deck import accessors, readers, temporal
from app.services.unitofwork import AbstractUnitOfWork


def costs(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("costs")
    if val is None:
        pmo = accessors.pmo(deck_cls, cache, uow)
        raw = readers.validate_data(
            deck_cls,
            pmo.custo_operacao_series_simuladas,
            object,
            "custo de operacao das series simuladas (pmo.dat)",
        )
        val = pl.from_pandas(raw)
        cache["costs"] = val
    return val


def num_iterations(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    val = cache.get("num_iterations")
    if val is None:
        from app.services.deck import energy as energy_mod

        df = energy_mod.convergence(deck_cls, cache, uow)
        val = readers.validate_data(
            deck_cls,
            int(df["iteracao"].max() or 0),  # type: ignore[arg-type]
            int,
            "numero de iteracoes na convergencia (pmo.dat)",
        )
        cache["num_iterations"] = val
    return int(val)


def runtimes(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    val = cache.get("runtimes")
    if val is None:
        arq = accessors.newavetim(deck_cls, cache, uow)
        raw = readers.validate_data(
            deck_cls,
            arq.tempos_etapas,
            object,
            "tempos por etapa da execucao (newave.tim)",
        )
        val = pl.from_pandas(raw)
        cache["runtimes"] = val
    return val


def num_blocks(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    val = cache.get("num_blocks")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            readers.get_patamar(deck_cls, uow).numero_patamares,
            int,
            "numero de patamares (patamar.dat)",
        )
        cache["num_blocks"] = val
    return val  # type: ignore[return-value]


def block_lengths(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> pl.DataFrame:
    def _eval_pat0(df_pat: pl.DataFrame) -> pl.DataFrame:
        df_pat_0 = (
            df_pat.group_by(START_DATE_COL)
            .agg(pl.col(VALUE_COL).sum())
            .with_columns(
                pl.lit(0).cast(df_pat.schema[BLOCK_COL]).alias(BLOCK_COL)
            )
            .select(df_pat.columns)
        )
        df_pat = pl.concat([df_pat, df_pat_0]).sort([START_DATE_COL, BLOCK_COL])
        return df_pat

    val = cache.get("block_lengths")
    if val is None:
        raw = readers.validate_data(
            deck_cls,
            readers.get_patamar(deck_cls, uow).duracao_mensal_patamares,
            object,
            "duracao mensal em P.U. dos patamares (patamar.dat)",
        )
        val = pl.from_pandas(raw).rename(
            {"data": START_DATE_COL, "patamar": BLOCK_COL}
        )
        val = temporal.consider_post_study_years(deck_cls, cache, val, uow)
        val = _eval_pat0(val)
        cache["block_lengths"] = val
    return val


def models_wind_generation(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    val = cache.get("models_wind_generation")
    if val is None:
        val = readers.validate_data(
            deck_cls,
            accessors.dger(deck_cls, cache, uow).considera_geracao_eolica != 0,
            int,
            "consideracao de incerteza eolica (dger.dat)",
        )
        cache["models_wind_generation"] = val
    return val


def scenario_generation_model_type(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    return temporal.scenario_generation_model_type(deck_cls, cache, uow)


def scenario_generation_model_max_order(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    return temporal.scenario_generation_model_max_order(deck_cls, cache, uow)


def num_forward_series(
    deck_cls: Any, cache: Dict[str, Any], uow: AbstractUnitOfWork
) -> int:
    val = cache.get("num_forward_series")
    if val is None:
        dger = accessors.dger(deck_cls, cache, uow)
        val = readers.validate_data(
            deck_cls,
            dger.num_forwards,
            int,
            "número de séries forward (dger.dat)",
        )
        cache["num_forward_series"] = val
    return val  # type: ignore[return-value]
