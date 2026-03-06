from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Optional

import pandas as pd
import polars as pl

from app.internal.constants import (
    EXCHANGE_SOURCE_CODE_COL,
    EXCHANGE_TARGET_CODE_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
)
from app.model.operation.operationsynthesis import OperationSynthesis
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


def resolve_SBP_entity(
    cls: "type[OperationSynthetizer]",
    uow: AbstractUnitOfWork,
    synthesis: OperationSynthesis,
    sbm1_index: int,
    sbm1_name: str,
    sbm2_index: int,
    sbm2_name: str,
    deck_context: Optional[DeckContext] = None,
) -> Optional[pl.DataFrame]:
    """
    Obtém os dados da síntese de operação para um par de submercados
    a partir do arquivo de saída do NWLISTOP.
    """
    if sbm1_index >= sbm2_index:
        return None
    logger_name = f"{synthesis.variable.value}_{sbm1_name}_{sbm2_name}"
    logger = Log.configure_process_logger(
        uow.queue, logger_name, sbm1_index + 10 * sbm2_index
    )
    with uow:
        logger.debug(
            "Processando arquivo do par de submercados:"
            + f" {sbm1_index}[{sbm1_name}] - {sbm2_index}[{sbm2_name}]"
        )
        df = uow.files.get_nwlistop(
            synthesis.variable,
            synthesis.spatial_resolution,
            submercados=(sbm1_index, sbm2_index),
        )
    return post_resolve_entity(
        cls,
        df,
        synthesis,
        {
            EXCHANGE_SOURCE_CODE_COL: sbm1_index,
            EXCHANGE_TARGET_CODE_COL: sbm2_index,
        },
        uow,
        deck_context=deck_context,
    )


def resolve_SBP(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext] = None,
    executor: Optional[ProcessPoolExecutor] = None,
) -> Optional[pd.DataFrame]:
    """
    Resolve a síntese de operação para uma variável operativa
    de um par de submercados a partir dos arquivos de saída do NWLISTOP.
    """

    submarkets = Deck.submarkets(uow).reset_index()
    sbms_idx = submarkets[SUBMARKET_CODE_COL].unique()
    sbms_name = [
        submarkets.loc[
            submarkets[SUBMARKET_CODE_COL] == s, SUBMARKET_NAME_COL
        ].iloc[0]
        for s in sbms_idx
    ]

    with time_and_log(
        message_root="Tempo para obter dados de SBP", logger=cls.logger
    ):
        assert executor is not None, (
            "__resolve_SBP requires a ProcessPoolExecutor; "
            "use synthetize() which creates the group-level executor"
        )
        futures = {
            f"{idx1}-{idx2}": executor.submit(
                cls._resolve_SBP_entity,
                uow,
                synthesis,
                idx1,
                name1,
                idx2,
                name2,
                deck_context,
            )
            for idx1, name1 in zip(sbms_idx, sbms_name)
            for idx2, name2 in zip(sbms_idx, sbms_name)
        }
        dfs = {ir: f.result(timeout=3600) for ir, f in futures.items()}

    df = post_resolve(
        cls,
        dfs,
        synthesis,
        uow,
    )
    return df
