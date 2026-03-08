from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from app.model.operation.operationsynthesis import OperationSynthesis
from app.services.deck.bounds import OperationVariableBounds
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.timing import time_and_log

if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )


def resolve_bounds(
    cls: "type[OperationSynthetizer]",
    s: OperationSynthesis,
    df: pl.DataFrame,
    uow: AbstractUnitOfWork,
) -> pl.DataFrame:
    with time_and_log(
        message_root="Tempo para calculo dos limites",
        logger=cls.logger,
    ):
        df = OperationVariableBounds.resolve_bounds(
            s,
            df,
            cls._get_ordered_entities(s),
            uow,
        )

    return df
