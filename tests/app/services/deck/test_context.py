import pickle
from datetime import datetime

import polars as pl
import pytest

from app.services.deck.context import DeckContext
from app.services.unitofwork import factory
from tests.conftest import DECK_TEST_DIR, q

uow = factory("FS", DECK_TEST_DIR, q)


def _make_minimal_context() -> DeckContext:
    """Build a DeckContext with minimal synthetic data for unit tests."""
    return DeckContext(
        block_lengths=pl.DataFrame(
            {
                "data_inicio": [datetime(2023, 10, 1)],
                "patamar": [1],
                "valor": [1.0],
            }
        ),
        num_scenarios=7,
        num_blocks=1,
        starting_dates=[datetime(2023, 10, 1)],
        ending_dates=[datetime(2023, 11, 1)],
        eer_submarket_map=pl.DataFrame(
            {"codigo_ree": [1], "codigo_submercado": [1]}
        ),
        hydro_eer_submarket_map=pl.DataFrame(
            {"codigo_usina": [10], "codigo_ree": [1], "codigo_submercado": [1]}
        ),
        study_period_starting_month=10,
        hydro_simulation_ending_date=datetime(2028, 12, 1),
    )


def test_deck_context_from_deck(test_settings):
    """DeckContext.from_deck() must create a valid context from a real UoW."""
    ctx = DeckContext.from_deck(uow)
    assert isinstance(ctx.block_lengths, pl.DataFrame)
    assert isinstance(ctx.num_scenarios, int)
    assert isinstance(ctx.num_blocks, int)
    assert isinstance(ctx.starting_dates, list)
    assert isinstance(ctx.ending_dates, list)
    assert isinstance(ctx.eer_submarket_map, pl.DataFrame)
    assert isinstance(ctx.hydro_eer_submarket_map, pl.DataFrame)
    assert isinstance(ctx.study_period_starting_month, int)
    assert isinstance(ctx.hydro_simulation_ending_date, datetime)
    assert ctx.num_scenarios > 0
    assert ctx.num_blocks > 0
    assert len(ctx.starting_dates) > 0
    assert len(ctx.ending_dates) > 0


def test_deck_context_pickle_round_trip(test_settings):
    """DeckContext must survive a pickle round-trip with all fields intact."""
    ctx = DeckContext.from_deck(uow)
    serialized = pickle.dumps(ctx)
    deserialized = pickle.loads(serialized)

    assert deserialized.num_scenarios == ctx.num_scenarios
    assert deserialized.num_blocks == ctx.num_blocks
    assert (
        deserialized.study_period_starting_month
        == ctx.study_period_starting_month
    )
    assert (
        deserialized.hydro_simulation_ending_date
        == ctx.hydro_simulation_ending_date
    )
    assert deserialized.starting_dates == ctx.starting_dates
    assert deserialized.ending_dates == ctx.ending_dates
    assert deserialized.block_lengths.equals(ctx.block_lengths)
    assert deserialized.eer_submarket_map.equals(ctx.eer_submarket_map)
    assert deserialized.hydro_eer_submarket_map.equals(
        ctx.hydro_eer_submarket_map
    )


def test_deck_context_none_field_raises():
    """DeckContext.__post_init__ must raise ValueError if any field is None."""
    with pytest.raises((ValueError, TypeError)):
        DeckContext(
            block_lengths=None,  # type: ignore[arg-type]
            num_scenarios=7,
            num_blocks=1,
            starting_dates=[datetime(2023, 10, 1)],
            ending_dates=[datetime(2023, 11, 1)],
            eer_submarket_map=pl.DataFrame(),
            hydro_eer_submarket_map=pl.DataFrame(),
            study_period_starting_month=10,
            hydro_simulation_ending_date=datetime(2028, 12, 1),
        )


def test_deck_context_fields_match_specification(test_settings):
    """DeckContext must contain exactly the fields listed in the ticket spec."""
    ctx = DeckContext.from_deck(uow)
    expected_fields = {
        "block_lengths",
        "num_scenarios",
        "num_blocks",
        "starting_dates",
        "ending_dates",
        "eer_submarket_map",
        "hydro_eer_submarket_map",
        "study_period_starting_month",
        "hydro_simulation_ending_date",
    }
    actual_fields = set(ctx.__dataclass_fields__.keys())
    assert actual_fields == expected_fields
