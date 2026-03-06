from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Optional

import pandas as pd
import polars as pl

from app.internal.constants import (
    EER_CODE_COL,
    EER_NAME_COL,
    SUBMARKET_CODE_COL,
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


def resolve_REE_entity(
    cls: "type[OperationSynthetizer]",
    uow: AbstractUnitOfWork,
    synthesis: OperationSynthesis,
    ree_index: int,
    ree_name: str,
    deck_context: Optional[DeckContext] = None,
) -> Optional[pl.DataFrame]:
    """
    Obtem os dados da síntese de operação para um REE
    a partir do arquivo de saída do NWLISTOP.
    """

    logger_name = f"{synthesis.variable.value}_{ree_name}"
    logger = Log.configure_process_logger(uow.queue, logger_name, ree_index)
    with uow:
        logger.debug(f"Processando arquivo do REE: {ree_index} - {ree_name}")
        df = uow.files.get_nwlistop(
            synthesis.variable,
            synthesis.spatial_resolution,
            ree=ree_index,
        )

    if deck_context is not None:
        aux_df = deck_context.eer_submarket_map
    else:
        aux_df = Deck.eer_submarket_map(uow)

    return post_resolve_entity(
        cls,
        df,
        synthesis,
        {
            EER_CODE_COL: ree_index,
            SUBMARKET_CODE_COL: aux_df.at[ree_index, SUBMARKET_CODE_COL],
        },
        uow,
        deck_context=deck_context,
    )


def resolve_REE(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext] = None,
    executor: Optional[ProcessPoolExecutor] = None,
) -> Optional[pd.DataFrame]:
    """
    Resolve a síntese de operação para uma variável operativa
    de um REE a partir dos arquivos de saída do NWLISTOP.
    """

    eers = Deck.eers(uow).reset_index().sort_values(EER_CODE_COL)
    eers_idx = eers[EER_CODE_COL]
    eers_name = eers[EER_NAME_COL]

    with time_and_log(
        message_root="Tempo para ler dados de REE", logger=cls.logger
    ):
        assert executor is not None, (
            "__resolve_REE requires a ProcessPoolExecutor; "
            "use synthetize() which creates the group-level executor"
        )
        futures = {
            idx: executor.submit(
                cls._resolve_REE_entity,
                uow,
                synthesis,
                idx,
                name,
                deck_context,
            )
            for idx, name in zip(eers_idx, eers_name)
        }
        dfs = {ir: f.result(timeout=3600) for ir, f in futures.items()}

    df = post_resolve(
        cls,
        dfs,
        synthesis,
        uow,
    )
    return df
