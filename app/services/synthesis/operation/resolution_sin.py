from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Optional

import pandas as pd

from app.model.operation.operationsynthesis import OperationSynthesis
from app.services.deck.context import DeckContext
from app.services.synthesis.operation.pipeline import (
    post_resolve,
    post_resolve_entity,
)
from app.services.unitofwork import AbstractUnitOfWork
from app.utils.timing import time_and_log

if TYPE_CHECKING:
    from app.services.synthesis.operation.orchestrator import (
        OperationSynthetizer,
    )


def resolve_SIN(
    cls: "type[OperationSynthetizer]",
    synthesis: OperationSynthesis,
    uow: AbstractUnitOfWork,
    deck_context: Optional[DeckContext] = None,
    executor: Optional[ProcessPoolExecutor] = None,
) -> Optional[pd.DataFrame]:
    with time_and_log(
        message_root="Tempo para obter dados do SIN", logger=cls.logger
    ):
        with uow:
            df = uow.files.get_nwlistop(
                synthesis.variable,
                synthesis.spatial_resolution,
                "",
            )
        df = post_resolve_entity(
            cls, df, synthesis, {}, uow, deck_context=deck_context
        )
    return post_resolve(cls, {"SIN": df}, synthesis, uow)
