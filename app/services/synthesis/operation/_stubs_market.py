"""
Private market/GUNS stub resolvers for stubs.py.

resolve_SBM_entity_MER_MERL, _resolve_SBM_MER_MERL, stub_MER_MERL, and
stub_GUNS are moved here to keep stubs.py within the 500-line limit.
"""

from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Callable, Dict, Optional

import pandas as pd
import polars as pl

from app.internal.constants import (
    BLOCK_COL,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
)
from app.model.operation.operationsynthesis import OperationSynthesis
from app.model.operation.spatialresolution import SpatialResolution
from app.model.settings import Settings
from app.services.deck.deck import Deck
from app.services.synthesis.operation.pipeline import (
    generate_scenarios,
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


def resolve_SBM_entity_MER_MERL(
    cls: "type[OperationSynthetizer]",
    uow: AbstractUnitOfWork,
    synthesis: OperationSynthesis,
    sbm_index: int,
    sbm_name: str,
) -> Optional[pl.DataFrame]:
    """Obtém dados de síntese para um submercado (variáveis MER e MERL)."""
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
    df = generate_scenarios(cls, df, uow)
    return post_resolve_entity(
        cls, df, synthesis, {SUBMARKET_CODE_COL: sbm_index}, uow
    )


def _resolve_SBM_MER_MERL(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> Optional[pd.DataFrame]:
    """
    Resolves MER/MERL synthesis at SBM spatial resolution using a dedicated
    ProcessPoolExecutor. Extracted from the former nested closure inside
    stub_MER_MERL to eliminate the architectural anomaly of a nested executor.
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
    n_procs = int(Settings().processors)
    with time_and_log(
        message_root="Tempo para obter dados de SBM", logger=cls.logger
    ):
        with ProcessPoolExecutor(max_workers=n_procs) as executor:
            futures = {
                idx: executor.submit(
                    cls._resolve_SBM_entity_MER_MERL, uow, synthesis, idx, name
                )
                for idx, name in zip(sbms_idx, sbms_name)
            }
            dfs = {ir: f.result(timeout=3600) for ir, f in futures.items()}
    return post_resolve(cls, dfs, synthesis, uow)


def stub_MER_MERL(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Processa a síntese de mercado de energia no formato compatível com NWLISTOP."""

    def _resolve_SIN_MER_MERL(
        synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pd.DataFrame]:
        with time_and_log(
            message_root="Tempo para obter dados do SIN", logger=cls.logger
        ):
            with uow:
                df = uow.files.get_nwlistop(
                    synthesis.variable, synthesis.spatial_resolution, ""
                )
            df = generate_scenarios(cls, df, uow)
            df = post_resolve_entity(cls, df, synthesis, {}, uow)
        return post_resolve(cls, {"SIN": df}, synthesis, uow)

    RESOLUTION_FUNCTION_MAP: Dict[SpatialResolution, Callable] = {
        SpatialResolution.SISTEMA_INTERLIGADO: _resolve_SIN_MER_MERL,
        SpatialResolution.SUBMERCADO: lambda s, u: _resolve_SBM_MER_MERL(
            cls, s, u
        ),
    }
    res = RESOLUTION_FUNCTION_MAP[synthesis.spatial_resolution](synthesis, uow)
    return res if res is not None else pd.DataFrame()


def stub_GUNS(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """Processa a síntese de geração de usinas não simuladas."""

    def _resolve_SIN(
        synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pd.DataFrame]:
        df = Deck.non_simulated_generation(
            uow
        ).to_pandas()  # SHIM: remove after polars migration of this module
        df = generate_scenarios(cls, df, uow)
        df = (
            df.groupby([START_DATE_COL, BLOCK_COL, "serie"])
            .sum()
            .reset_index()
            .drop(columns=[SUBMARKET_CODE_COL])
        )
        df = post_resolve_entity(cls, df, synthesis, {}, uow)
        return post_resolve(cls, {"SIN": df}, synthesis, uow)

    def _resolve_SBM(
        synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pd.DataFrame]:
        df = Deck.non_simulated_generation(
            uow
        ).to_pandas()  # SHIM: remove after polars migration of this module
        df = generate_scenarios(cls, df, uow)
        df = df.sort_values(
            [SUBMARKET_CODE_COL, START_DATE_COL, "serie", BLOCK_COL]
        )
        dfs: dict = {}
        for sbm_code in df[SUBMARKET_CODE_COL].unique().tolist():
            sbm_df = df.loc[df[SUBMARKET_CODE_COL] == sbm_code].reset_index(
                drop=True
            )
            sbm_df = post_resolve_entity(
                cls, sbm_df, synthesis, {SUBMARKET_CODE_COL: sbm_code}, uow
            )
            dfs[str(sbm_code)] = sbm_df
        return post_resolve(cls, dfs, synthesis, uow)

    RESOLUTION_FUNCTION_MAP: Dict[SpatialResolution, Callable] = {
        SpatialResolution.SISTEMA_INTERLIGADO: _resolve_SIN,
        SpatialResolution.SUBMERCADO: _resolve_SBM,
    }
    res = RESOLUTION_FUNCTION_MAP[synthesis.spatial_resolution](synthesis, uow)
    return res if res is not None else pd.DataFrame()
