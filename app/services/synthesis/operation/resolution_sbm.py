from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Optional

import polars as pl

from app.internal.constants import (
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


def resolve_SBM_entity(
    cls: "type[OperationSynthetizer]",
    uow: AbstractUnitOfWork,
    synthesis: OperationSynthesis,
    sbm_index: int,
    sbm_name: str,
    deck_context: Optional[DeckContext] = None,
) -> Optional[pl.DataFrame]:
    logger_name = f"{synthesis.variable.value}_{sbm_name}"
    logger = Log.configure_process_logger(uow.queue, logger_name, sbm_index)
    with uow:
        logger.debug(
            f"Processando arquivo do submercado: {sbm_index} - {sbm_name}"
        )
        df = uow.files.get_nwlistop(
            synthesis.variable,
            synthesis.spatial_resolution,
            submercado=sbm_index,
        )
    return post_resolve_entity(
        cls,
        df,
        synthesis,
        {
            SUBMARKET_CODE_COL: sbm_index,
        },
        uow,
        deck_context=deck_context,
    )


def resolve_SBM(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext] = None,
    executor: Optional[ProcessPoolExecutor] = None,
) -> Optional[pl.DataFrame]:
    submarkets = Deck.submarkets(uow).sort(SUBMARKET_CODE_COL)
    real_submarkets = submarkets.filter(pl.col("ficticio") == 0)
    sbms_idx = real_submarkets[SUBMARKET_CODE_COL].to_list()
    name_map = dict(
        zip(
            real_submarkets[SUBMARKET_CODE_COL].to_list(),
            real_submarkets[SUBMARKET_NAME_COL].to_list(),
        )
    )
    sbms_name = [name_map[s] for s in sbms_idx]

    with time_and_log(
        message_root="Tempo para obter dados de SBM", logger=cls.logger
    ):
        assert executor is not None, (
            "__resolve_SBM requires a ProcessPoolExecutor; "
            "use synthetize() which creates the group-level executor"
        )
        futures = {
            idx: executor.submit(
                cls._resolve_SBM_entity,
                uow,
                synthesis,
                idx,
                name,
                deck_context,
            )
            for idx, name in zip(sbms_idx, sbms_name)
        }
        dfs = {ir: f.result(timeout=3600) for ir, f in futures.items()}

    df = post_resolve(
        cls,
        dfs,
        synthesis,
        uow,
    )
    return df
