from logging import DEBUG, ERROR
from typing import TYPE_CHECKING

import pandas as pd

from app.model.operation.operationsynthesis import OperationSynthesis
from app.utils.timing import time_and_log

if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )


def get_from_cache(
    cls: "type[OperationSynthetizer]", s: OperationSynthesis
) -> pd.DataFrame:
    """
    Extrai o resultado de uma síntese da cache caso exista, lançando
    um erro caso contrário.
    """
    if s not in cls.CACHED_SYNTHESIS:
        cls._log(f"Erro na leitura do cache - {str(s)}", ERROR)
        raise RuntimeError()
    cls._log(f"Lendo do cache - {str(s)}", DEBUG)
    res = cls.CACHED_SYNTHESIS[s]
    if res is None:
        cls._log(f"Erro na leitura do cache - {str(s)}", ERROR)
        raise RuntimeError()
    return res


def get_from_cache_if_exists(
    cls: "type[OperationSynthetizer]", s: OperationSynthesis
) -> pd.DataFrame:
    """
    Obtém uma síntese da operação a partir da cache, caso esta
    exista. Caso contrário, retorna um DataFrame vazio.
    """
    if s in cls.CACHED_SYNTHESIS.keys():
        return get_from_cache(cls, s)
    else:
        return pd.DataFrame()


def store_in_cache_if_needed(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    df: pd.DataFrame,
) -> None:
    """
    Adiciona um DataFrame com os dados de uma síntese à cache
    caso esta seja uma variável que deva ser armazenada.
    """
    if s in cls.SYNTHESIS_TO_CACHE:
        with time_and_log(
            message_root="Tempo para armazenamento na cache",
            logger=cls.logger,
        ):
            cls.CACHED_SYNTHESIS[s] = df
