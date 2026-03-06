from typing import TYPE_CHECKING

import pandas as pd

from app.model.operation.operationsynthesis import OperationSynthesis
from app.services.deck.bounds import OperationVariableBounds
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.dataframe import pd_to_pl, pl_to_pd
from app.utils.timing import time_and_log

if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )


def resolve_bounds(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    df: pd.DataFrame,
    uow: AbstractUnitOfWork,
) -> pd.DataFrame:
    """
    Realiza o cálculo dos limites superiores e inferiores para
    a síntese caso esta seja uma variável limitada.

    Converts the incoming ``pd.DataFrame`` to ``pl.DataFrame`` at the
    boundary so that ``resolve_bounds`` can operate in Polars and
    converts the result back to ``pd.DataFrame`` for downstream
    consumers. The conversion pair lives here rather than inside
    ``resolve_bounds`` so that callers which already hold a
    ``pl.DataFrame`` (e.g. ``_resolve_stub``) can pass through without
    a double conversion.
    """
    with time_and_log(
        message_root="Tempo para calculo dos limites",
        logger=cls.logger,
    ):
        df_pl = pd_to_pl(df)
        df_pl = OperationVariableBounds.resolve_bounds(
            s,
            df_pl,
            cls._get_ordered_entities(s),
            uow,
        )
        df = pl_to_pd(df_pl)

    return df
