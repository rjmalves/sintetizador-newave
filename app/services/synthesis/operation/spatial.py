from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Optional

import pandas as pd

from app.model.operation.operationsynthesis import OperationSynthesis
from app.model.operation.spatialresolution import SpatialResolution
from app.services.deck.context import DeckContext
from app.services.synthesis.operation.bounds import resolve_bounds
from app.services.synthesis.operation.resolution_pee import resolve_PEE
from app.services.synthesis.operation.resolution_ree import resolve_REE
from app.services.synthesis.operation.resolution_sbm import resolve_SBM
from app.services.synthesis.operation.resolution_sbp import resolve_SBP
from app.services.synthesis.operation.resolution_sin import resolve_SIN
from app.services.synthesis.operation.resolution_uhe import resolve_UHE
from app.services.synthesis.operation.resolution_ute import resolve_UTE
from app.services.unitofwork import AbstractUnitOfWork

if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )


def resolve_spatial_resolution(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext] = None,
    executor: Optional[ProcessPoolExecutor] = None,
) -> pd.DataFrame:
    """
    Despacha a função de resolução espacial para ler os dados a partir
    da saída do NWLISTOP, pós-processar e organizar em um DataFrame
    de acordo com a agregação desejada.
    """
    RESOLUTION_FUNCTION_MAP = {
        SpatialResolution.SISTEMA_INTERLIGADO: resolve_SIN,
        SpatialResolution.SUBMERCADO: resolve_SBM,
        SpatialResolution.PAR_SUBMERCADOS: resolve_SBP,
        SpatialResolution.RESERVATORIO_EQUIVALENTE: resolve_REE,
        SpatialResolution.USINA_HIDROELETRICA: resolve_UHE,
        SpatialResolution.USINA_TERMELETRICA: resolve_UTE,
        SpatialResolution.PARQUE_EOLICO_EQUIVALENTE: resolve_PEE,
    }
    solver = RESOLUTION_FUNCTION_MAP[synthesis.spatial_resolution]
    # resolve_SIN does not use a pool; for all other resolvers we pass
    # the optional shared executor so that the caller can reuse a pool
    # across multiple variables at the same spatial resolution.
    if synthesis.spatial_resolution == SpatialResolution.SISTEMA_INTERLIGADO:
        res = solver(cls, synthesis, uow, deck_context)
    else:
        res = solver(cls, synthesis, uow, deck_context, executor)
    return res if res is not None else pd.DataFrame()


def resolve_synthesis(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext] = None,
    executor: Optional[ProcessPoolExecutor] = None,
) -> pd.DataFrame:
    """
    Realiza a resolução de uma síntese, opcionalmente adicionando
    limites superiores e inferiores aos valores de cada linha.
    """
    df = resolve_spatial_resolution(cls, s, uow, deck_context, executor)
    if df is not None:
        df = resolve_bounds(cls, s, df, uow)
    return df
