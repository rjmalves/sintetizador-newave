from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, List

import polars as pl

if TYPE_CHECKING:
    from app.services.unitofwork import AbstractUnitOfWork


@dataclass
class DeckContext:
    block_lengths: pl.DataFrame
    num_scenarios: int
    num_blocks: int
    starting_dates: List[datetime]
    ending_dates: List[datetime]
    eer_submarket_map: pl.DataFrame
    hydro_eer_submarket_map: pl.DataFrame
    study_period_starting_month: int
    hydro_simulation_ending_date: datetime

    def __post_init__(self) -> None:
        for field_name, value in self.__dict__.items():
            if value is None:
                raise ValueError(
                    f"DeckContext field '{field_name}' must not be None"
                )

    @classmethod
    def from_deck(cls, uow: "AbstractUnitOfWork") -> "DeckContext":
        from app.services.deck.deck import Deck

        return cls(
            block_lengths=Deck.block_lengths(uow),
            num_scenarios=Deck.num_scenarios_final_simulation(uow),
            num_blocks=Deck.num_blocks(uow),
            starting_dates=Deck.internal_stages_starting_dates_final_simulation(
                uow
            ),
            ending_dates=Deck.internal_stages_ending_dates_final_simulation(
                uow
            ),
            eer_submarket_map=Deck.eer_submarket_map(uow),
            hydro_eer_submarket_map=Deck.hydro_eer_submarket_map(uow),
            study_period_starting_month=Deck.study_period_starting_month(uow),
            hydro_simulation_ending_date=Deck.hydro_simulation_stages_ending_date_final_simulation(
                uow
            ),
        )
