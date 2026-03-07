from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Optional

import pandas as pd

from app.internal.constants import (
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
)
from app.model.operation.operationsynthesis import OperationSynthesis
from app.model.operation.variable import Variable
from app.services.deck.context import DeckContext
from app.services.deck.deck import Deck
from app.services.synthesis.operation.pipeline import (
    post_resolve,
    post_resolve_GTER_UTE_entity,
)
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.dataframe import pd_to_pl
from app.utils.log import Log
from app.utils.timing import time_and_log

if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )


def resolve_GTER_UTE_entity(
    cls: "type[OperationSynthetizer]",
    uow: AbstractUnitOfWork,
    synthesis: OperationSynthesis,
    sbm_index: int,
    sbm_name: str,
    deck_context: Optional[DeckContext] = None,
) -> Optional[pd.DataFrame]:
    """
    Obtém os dados da síntese de operação para todas as UTE
    de um submercado a partir do arquivo de saída do NWLISTOP.
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
    if df is not None:
        df[SUBMARKET_CODE_COL] = sbm_index
    return post_resolve_GTER_UTE_entity(cls, df, uow, deck_context)


def resolve_GTER_UTE(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext] = None,
    executor: Optional[ProcessPoolExecutor] = None,
) -> pd.DataFrame:
    """
    Obtem os dados da síntese de operação da geração térmica
    para as UTE de um submercado a partir dos arquivos
    de saída do NWLISTOP.
    """

    def _sort_thermals(
        s: OperationSynthesis, df: pd.DataFrame, uow: AbstractUnitOfWork
    ) -> pd.DataFrame:
        df = df.sort_values(
            s.spatial_resolution.sorting_synthesis_df_columns
        ).reset_index(drop=True)
        return df

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

    synthesis = OperationSynthesis(
        variable=synthesis.variable,
        spatial_resolution=synthesis.spatial_resolution,
    )

    with time_and_log(
        message_root="Tempo para ler dados de UTE",
        logger=cls.logger,
    ):
        assert executor is not None, (
            "_resolve_GTER_UTE requires a ProcessPoolExecutor; "
            "use synthetize() which creates the group-level executor"
        )
        futures = {
            idx: executor.submit(
                cls._resolve_GTER_UTE_entity,
                uow,
                synthesis,
                idx,
                name,
                deck_context,
            )
            for idx, name in zip(sbms_idx, sbms_name)
        }
        dfs = {ir: f.result(timeout=3600) for ir, f in futures.items()}

    # _post_resolve_GTER_UTE_entity returns pd.DataFrame (out of scope for this ticket).
    # Convert to pl.DataFrame at the boundary before passing to _post_resolve.
    dfs_pl = {
        k: pd_to_pl(v) if isinstance(v, pd.DataFrame) else v
        for k, v in dfs.items()
    }
    df = post_resolve(
        cls,
        dfs_pl,
        synthesis,
        uow,
        early_hooks=[_sort_thermals],
    )
    return df


def resolve_UTE(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext] = None,
    executor: Optional[ProcessPoolExecutor] = None,
) -> Optional[pd.DataFrame]:
    """
    Resolve a síntese de operação para uma variável operativa
    de uma UTE a partir dos arquivos de saída do NWLISTOP.
    """
    if synthesis.variable == Variable.GERACAO_TERMICA:
        return resolve_GTER_UTE(cls, synthesis, uow, deck_context, executor)
    else:
        raise RuntimeError("Variável não suportada para UTEs")
