from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Optional

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


def _limit_stages_with_hydro(
    deck_context: Optional[DeckContext],
    s: OperationSynthesis,
    df: pl.DataFrame,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    if deck_context is not None:
        ending_date = deck_context.hydro_simulation_ending_date
    else:
        ending_date = Deck.hydro_simulation_stages_ending_date_final_simulation(
            uow
        )
    return df.filter(pl.col(START_DATE_COL) < ending_date)


def resolve_UHE_entity(
    cls: "type[OperationSynthetizer]",
    uow: AbstractUnitOfWork,
    synthesis: OperationSynthesis,
    uhe_index: int,
    uhe_name: str,
    deck_context: Optional[DeckContext] = None,
) -> Optional[pl.DataFrame]:
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
        Variable.COTA_JUSANTE: _calc_block_0_weighted_mean_stub,
        Variable.QUEDA_LIQUIDA: _calc_block_0_weighted_mean_stub,
        Variable.VAZAO_TURBINADA: _calc_block_0_weighted_mean_stub,
        Variable.VAZAO_VERTIDA: _calc_block_0_weighted_mean_stub,
        Variable.VAZAO_DESVIADA: _calc_block_0_weighted_mean_stub,
    }

    if deck_context is not None:
        aux_df = deck_context.hydro_eer_submarket_map
    else:
        aux_df = Deck.hydro_eer_submarket_map(uow)

    row = aux_df.filter(pl.col(HYDRO_CODE_COL) == uhe_index)
    eer_code = row.item(0, EER_CODE_COL)
    sbm_code = row.item(0, SUBMARKET_CODE_COL)

    return post_resolve_entity(
        cls,
        df,
        synthesis,
        {
            HYDRO_CODE_COL: uhe_index,
            EER_CODE_COL: eer_code,
            SUBMARKET_CODE_COL: sbm_code,
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
) -> Optional[pl.DataFrame]:
    def _limit_stages_hook(
        s: OperationSynthesis, df: pl.DataFrame, uow: AbstractUnitOfWork
    ) -> pl.DataFrame:
        return _limit_stages_with_hydro(deck_context, s, df, uow)

    hydros = Deck.hydros(uow).sort(HYDRO_CODE_COL)
    hydros_idx = hydros[HYDRO_CODE_COL].to_list()
    hydros_name = hydros[HYDRO_NAME_COL].to_list()

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
