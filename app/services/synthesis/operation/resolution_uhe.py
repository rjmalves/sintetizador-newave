from concurrent.futures import ProcessPoolExecutor
from logging import WARNING
from typing import TYPE_CHECKING, Optional

import pandas as pd
import polars as pl

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    EER_CODE_COL,
    HYDRO_CODE_COL,
    HYDRO_NAME_COL,
    SCENARIO_COL,
    STAGE_COL,
    STAGE_DURATION_HOURS,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    VALUE_COL,
)
from app.model.operation.operationsynthesis import OperationSynthesis
from app.model.operation.variable import Variable
from app.services.deck.context import DeckContext
from app.services.deck.deck import Deck
from app.services.synthesis.operation.pipeline import (
    post_resolve,
    post_resolve_entity,
)
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.dataframe import pd_to_pl, pl_to_pd
from app.utils.log import Log
from app.utils.timing import time_and_log

if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )


def _calc_block_0_weighted_mean(
    cls: "type[OperationSynthetizer]",
    df: pl.DataFrame,
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext] = None,
) -> pl.DataFrame:
    """
    Calcula um valor médio ponderado para o estágio a partir
    de valores fornecidos por patamar de alguma variável operativa
    de uma UHE. Accepts and returns pl.DataFrame.
    """
    try:
        unique_cols_for_block_0 = [
            HYDRO_CODE_COL,
            STAGE_COL,
            SCENARIO_COL,
        ]
        # Compute weighted value: value * block_duration / STAGE_DURATION_HOURS
        weighted = df.with_columns(
            (
                pl.col(VALUE_COL)
                * pl.col(BLOCK_DURATION_COL)
                / STAGE_DURATION_HOURS
            ).alias(VALUE_COL)
        )
        # Sum weighted values per (hydro, stage, scenario) -> block-0 row
        block_0 = weighted.group_by(
            unique_cols_for_block_0, maintain_order=True
        ).agg(pl.col(VALUE_COL).sum())
        # Take one representative row per group to carry the other columns,
        # then overwrite BLOCK_COL and BLOCK_DURATION_COL
        non_group_cols = [
            c
            for c in df.columns
            if c not in unique_cols_for_block_0
            and c not in (VALUE_COL, BLOCK_COL, BLOCK_DURATION_COL)
        ]
        representative = df.group_by(
            unique_cols_for_block_0, maintain_order=True
        ).agg([pl.first(c) for c in non_group_cols])
        block_col_dtype = df[BLOCK_COL].dtype
        block_0 = block_0.join(
            representative, on=unique_cols_for_block_0, how="left"
        ).with_columns(
            [
                pl.lit(0).cast(block_col_dtype).alias(BLOCK_COL),
                pl.lit(float(STAGE_DURATION_HOURS)).alias(BLOCK_DURATION_COL),
            ]
        )
        # Reorder columns to match original schema, then concatenate
        block_0 = block_0.select(df.columns)
        return pl.concat([block_0, df]).sort(
            unique_cols_for_block_0 + [BLOCK_COL], maintain_order=True
        )
    except Exception as exc:
        cls._log(
            f"_calc_block_0_weighted_mean: Polars path failed ({exc}), "
            "falling back to pandas",
            WARNING,
        )
        pd_df = pl_to_pd(df)
        if deck_context is not None:
            n_blocks = deck_context.num_blocks
        else:
            n_blocks = Deck.num_blocks(uow)
        unique_cols_for_block_0 = [
            HYDRO_CODE_COL,
            STAGE_COL,
            SCENARIO_COL,
        ]
        df_block_0 = pd_df.copy()
        df_block_0[VALUE_COL] = (
            df_block_0[VALUE_COL] * df_block_0[BLOCK_DURATION_COL]
        ) / STAGE_DURATION_HOURS
        df_base = pd_df.iloc[::n_blocks].reset_index(drop=True).copy()
        df_base[BLOCK_COL] = 0
        df_base[BLOCK_DURATION_COL] = STAGE_DURATION_HOURS
        arr = df_block_0[VALUE_COL].to_numpy()
        n_linhas = arr.shape[0]
        n_elementos_distintos = n_linhas // n_blocks
        df_base[VALUE_COL] = arr.reshape((n_elementos_distintos, -1)).sum(
            axis=1
        )
        df_block_0 = pd.concat([df_base, pd_df], ignore_index=True, copy=True)
        df_block_0 = df_block_0.sort_values(
            unique_cols_for_block_0 + [BLOCK_COL]
        )
        return pd_to_pl(df_block_0)


def _limit_stages_with_hydro(
    deck_context: Optional[DeckContext],
    s: OperationSynthesis,
    df: pd.DataFrame,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    if deck_context is not None:
        ending_date = deck_context.hydro_simulation_ending_date
    else:
        ending_date = Deck.hydro_simulation_stages_ending_date_final_simulation(
            uow
        )
    df = df.loc[df[START_DATE_COL] < ending_date,].reset_index(drop=True)
    return df


def resolve_UHE_entity(
    cls: "type[OperationSynthetizer]",
    uow: AbstractUnitOfWork,
    synthesis: OperationSynthesis,
    uhe_index: int,
    uhe_name: str,
    deck_context: Optional[DeckContext] = None,
) -> Optional[pl.DataFrame]:
    """
    Obtem os dados da síntese de operação para uma UHE
    a partir do arquivo de saída do NWLISTOP.
    """

    def _calc_block_0_weighted_mean_stub(
        df: pl.DataFrame, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        return _calc_block_0_weighted_mean(cls, df, uow, deck_context)

    logger_name = f"{synthesis.variable.value}_{uhe_name}"
    logger = Log.configure_process_logger(uow.queue, logger_name, uhe_index)

    with uow:
        logger.debug(f"Processando arquivo da UHE: {uhe_index} - {uhe_name}")
        df = uow.files.get_nwlistop(
            synthesis.variable,
            synthesis.spatial_resolution,
            uhe=uhe_index,
        )

    internal_stubs = {
        Variable.COTA_JUSANTE: _calc_block_0_weighted_mean_stub,  # noqa
        Variable.QUEDA_LIQUIDA: _calc_block_0_weighted_mean_stub,  # noqa
        Variable.VAZAO_TURBINADA: _calc_block_0_weighted_mean_stub,  # noqa
        Variable.VAZAO_VERTIDA: _calc_block_0_weighted_mean_stub,  # noqa
        Variable.VAZAO_DESVIADA: _calc_block_0_weighted_mean_stub,  # noqa
    }

    if deck_context is not None:
        aux_df = deck_context.hydro_eer_submarket_map.to_pandas().set_index(
            HYDRO_CODE_COL
        )  # SHIM: remove after polars migration of this module
    else:
        aux_df = (
            Deck.hydro_eer_submarket_map(uow)
            .to_pandas()
            .set_index(HYDRO_CODE_COL)
        )  # SHIM: remove after polars migration of this module

    return post_resolve_entity(
        cls,
        df,
        synthesis,
        {
            HYDRO_CODE_COL: uhe_index,
            EER_CODE_COL: aux_df.at[uhe_index, EER_CODE_COL],
            SUBMARKET_CODE_COL: aux_df.at[uhe_index, SUBMARKET_CODE_COL],
        },
        uow,
        internal_stubs=internal_stubs,
        deck_context=deck_context,
    )


def resolve_UHE(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext] = None,
    executor: Optional[ProcessPoolExecutor] = None,
) -> Optional[pd.DataFrame]:
    """
    Resolve a síntese de operação para uma variável operativa
    de uma UHE a partir dos arquivos de saída do NWLISTOP.
    """

    def _limit_stages_hook(
        s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        return _limit_stages_with_hydro(deck_context, s, df, uow)

    hydros = (
        Deck.hydros(uow)
        .to_pandas()
        .reset_index(drop=True)
        .sort_values(HYDRO_CODE_COL)
    )  # SHIM: remove after polars migration of this module
    hydros_idx = hydros[HYDRO_CODE_COL]
    hydros_name = hydros[HYDRO_NAME_COL]

    with time_and_log(
        message_root="Tempo para ler dados de UHE",
        logger=cls.logger,
    ):
        assert executor is not None, (
            "__resolve_UHE requires a ProcessPoolExecutor; "
            "use synthetize() which creates the group-level executor"
        )
        futures = {
            name: executor.submit(
                cls._resolve_UHE_entity,
                uow,
                synthesis,
                idx,
                name,
                deck_context,
            )
            for idx, name in zip(hydros_idx, hydros_name)
        }
        dfs = {ir: f.result(timeout=3600) for ir, f in futures.items()}

    df = post_resolve(
        cls,
        dfs,
        synthesis,
        uow,
        early_hooks=[_limit_stages_hook],
    )
    return df
