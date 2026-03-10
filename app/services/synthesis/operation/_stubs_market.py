"""Private market/GUNS stub resolvers for stubs.py."""

import multiprocessing as _mp
import platform as _platform
from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

import polars as pl

from app.internal.constants import (
    BLOCK_COL,
    START_DATE_COL,
    SUBMARKET_CODE_COL,
    SUBMARKET_NAME_COL,
    VALUE_COL,
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
    df_pl = generate_scenarios(cls, pl.from_pandas(df), uow)
    return post_resolve_entity(
        cls, df_pl, synthesis, {SUBMARKET_CODE_COL: sbm_index}, uow
    )


def _resolve_SBM_MER_MERL(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
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
    n_procs = int(Settings().processors)
    with time_and_log(
        message_root="Tempo para obter dados de SBM", logger=cls.logger
    ):
        with ProcessPoolExecutor(
            max_workers=n_procs,
            mp_context=_mp.get_context(
                "spawn" if _platform.system() == "Windows" else "forkserver"
            ),
        ) as executor:
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
) -> pl.DataFrame:
    def _resolve_SIN_MER_MERL(
        synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pl.DataFrame]:
        with time_and_log(
            message_root="Tempo para obter dados do SIN", logger=cls.logger
        ):
            with uow:
                df = uow.files.get_nwlistop(
                    synthesis.variable, synthesis.spatial_resolution, ""
                )
            df_pl = generate_scenarios(cls, pl.from_pandas(df), uow)
            df_result = post_resolve_entity(cls, df_pl, synthesis, {}, uow)
        return post_resolve(cls, {"SIN": df_result}, synthesis, uow)

    RESOLUTION_FUNCTION_MAP: Dict[SpatialResolution, Callable[..., Any]] = {
        SpatialResolution.SISTEMA_INTERLIGADO: _resolve_SIN_MER_MERL,
        SpatialResolution.SUBMERCADO: lambda s, u: _resolve_SBM_MER_MERL(
            cls, s, u
        ),
    }
    res = RESOLUTION_FUNCTION_MAP[synthesis.spatial_resolution](synthesis, uow)
    return res if res is not None else pl.DataFrame()


def stub_GUNS(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    def _resolve_SIN(
        synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pl.DataFrame]:
        df = Deck.non_simulated_generation(uow)
        df = generate_scenarios(cls, df, uow)
        df = df.group_by([START_DATE_COL, BLOCK_COL, "serie"]).agg(
            pl.col(VALUE_COL).sum()
        )
        df_result = post_resolve_entity(cls, df, synthesis, {}, uow)
        return post_resolve(cls, {"SIN": df_result}, synthesis, uow)

    def _resolve_SBM(
        synthesis: OperationSynthesis, uow: AbstractUnitOfWork
    ) -> Optional[pl.DataFrame]:
        df = Deck.non_simulated_generation(uow)
        df = generate_scenarios(cls, df, uow)
        df = df.sort([SUBMARKET_CODE_COL, START_DATE_COL, "serie", BLOCK_COL])
        dfs: dict[str, Any] = {}
        for sbm_code in df[SUBMARKET_CODE_COL].unique().to_list():
            sbm_df = df.filter(pl.col(SUBMARKET_CODE_COL) == sbm_code)
            sbm_df_result = post_resolve_entity(
                cls, sbm_df, synthesis, {SUBMARKET_CODE_COL: sbm_code}, uow
            )
            dfs[str(sbm_code)] = sbm_df_result
        return post_resolve(cls, dfs, synthesis, uow)

    RESOLUTION_FUNCTION_MAP: Dict[SpatialResolution, Callable[..., Any]] = {
        SpatialResolution.SISTEMA_INTERLIGADO: _resolve_SIN,
        SpatialResolution.SUBMERCADO: _resolve_SBM,
    }
    res = RESOLUTION_FUNCTION_MAP[synthesis.spatial_resolution](synthesis, uow)
    return res if res is not None else pl.DataFrame()
