from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Optional

import pandas as pd
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
    """
    Obtem os dados da síntese de operação para um submercado
    a partir do arquivo de saída do NWLISTOP.
    """

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
) -> Optional[pd.DataFrame]:
    """
    Resolve a síntese de operação para uma variável operativa
    de um submercado a partir dos arquivos de saída do NWLISTOP.
    """

    submarkets = (
        Deck.submarkets(uow).to_pandas().reset_index(drop=True)
    )  # SHIM: remove after polars migration of this module
    real_submarkets = submarkets.loc[
        submarkets["ficticio"] == 0, :
    ].sort_values(SUBMARKET_CODE_COL)
    sbms_idx = real_submarkets[SUBMARKET_CODE_COL].unique()
    sbms_name = [
        real_submarkets.loc[
            real_submarkets[SUBMARKET_CODE_COL] == s, SUBMARKET_NAME_COL
        ].iloc[0]
        for s in sbms_idx
    ]

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
