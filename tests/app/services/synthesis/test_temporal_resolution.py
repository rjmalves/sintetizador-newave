"""
Unit tests for _resolve_temporal_resolution (ticket-008).

Tests verify that the Polars-backed implementation returns results
numerically identical to the pandas fallback and that error handling
falls back correctly.
"""

from datetime import datetime
from typing import List
from unittest.mock import MagicMock, patch

import logging

import numpy as np
import pandas as pd
import polars as pl
from polars.exceptions import ComputeError
import pytest

from app.internal.constants import (
    BLOCK_COL,
    BLOCK_DURATION_COL,
    END_DATE_COL,
    OPERATION_SYNTHESIS_COMMON_COLUMNS,
    SCENARIO_COL,
    STAGE_COL,
    STAGE_DURATION_HOURS,
    START_DATE_COL,
    VALUE_COL,
)
from app.services.deck.context import DeckContext
from app.services.synthesis.operation import OperationSynthetizer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NUM_STAGES = 3
NUM_SCENARIOS = 5
NUM_BLOCKS = 2


def _make_starting_dates(num_stages: int) -> List[datetime]:
    """Generate monthly starting dates beginning 2023-01-01."""
    from dateutil.relativedelta import relativedelta

    base = datetime(2023, 1, 1)
    return [base + relativedelta(months=i) for i in range(num_stages)]


def _make_ending_dates(start_dates: List[datetime]) -> List[datetime]:
    """Ending dates are one month after the corresponding starting dates."""
    from dateutil.relativedelta import relativedelta

    return [d + relativedelta(months=1) for d in start_dates]


def _make_block_lengths(
    start_dates: List[datetime],
    blocks: List[int],
    duration_per_block: float = 0.5,
) -> pl.DataFrame:
    """
    Build a block_lengths DataFrame with columns [START_DATE_COL, BLOCK_COL, VALUE_COL].
    Each block in each stage gets the same `duration_per_block` value.
    """
    rows = []
    for d in start_dates:
        for b in blocks:
            rows.append(
                {START_DATE_COL: d, BLOCK_COL: b, VALUE_COL: duration_per_block}
            )
    return pl.from_pandas(pd.DataFrame(rows))


def _make_input_df(
    start_dates: List[datetime],
    num_scenarios: int,
    blocks: List[int],
    value_seed: float = 1.0,
) -> pd.DataFrame:
    """
    Build the raw DataFrame that inewave's get_nwlistop would return.
    Columns: ["data", "serie", BLOCK_COL, VALUE_COL].
    """
    rows = []
    for d in start_dates:
        for s in range(1, num_scenarios + 1):
            for b in blocks:
                rows.append(
                    {
                        "data": d,
                        "serie": s,
                        BLOCK_COL: b,
                        VALUE_COL: value_seed * (s + b),
                    }
                )
    return pd.DataFrame(rows)


def _make_deck_context(
    start_dates: List[datetime],
    end_dates: List[datetime],
    num_scenarios: int,
    blocks: List[int],
    duration_per_block: float = 0.5,
) -> DeckContext:
    block_lengths = _make_block_lengths(start_dates, blocks, duration_per_block)
    ctx = DeckContext(
        block_lengths=block_lengths,
        num_scenarios=num_scenarios,
        num_blocks=len(blocks),
        starting_dates=start_dates,
        ending_dates=end_dates,
        eer_submarket_map=pl.DataFrame(
            {"codigo_ree": [1], "codigo_submercado": [1]}
        ),
        hydro_eer_submarket_map=pl.DataFrame(
            {"codigo_usina": [1], "codigo_ree": [1], "codigo_submercado": [1]}
        ),
        study_period_starting_month=1,
        hydro_simulation_ending_date=datetime(2024, 12, 1),
    )
    return ctx


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def resolution_setup():
    """Return (input_df, deck_context, start_dates, end_dates, blocks)."""
    blocks = [1, 2]
    start_dates = _make_starting_dates(NUM_STAGES)
    end_dates = _make_ending_dates(start_dates)
    input_df = _make_input_df(start_dates, NUM_SCENARIOS, blocks)
    ctx = _make_deck_context(start_dates, end_dates, NUM_SCENARIOS, blocks)
    return input_df, ctx, start_dates, end_dates, blocks


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestResolveTemporalResolution:
    """Tests for OperationSynthetizer._resolve_temporal_resolution."""

    def test_output_shape(self, resolution_setup):
        """Output must have exactly NUM_STAGES * NUM_SCENARIOS * NUM_BLOCKS rows."""
        input_df, ctx, *_ = resolution_setup
        uow_mock = MagicMock()

        result = OperationSynthetizer._resolve_temporal_resolution(
            input_df, uow_mock, ctx
        )

        expected_rows = NUM_STAGES * NUM_SCENARIOS * NUM_BLOCKS
        assert result.shape[0] == expected_rows, (
            f"Expected {expected_rows} rows, got {result.shape[0]}"
        )

    def test_output_columns(self, resolution_setup):
        """Output must have exactly OPERATION_SYNTHESIS_COMMON_COLUMNS in order."""
        input_df, ctx, *_ = resolution_setup
        uow_mock = MagicMock()

        result = OperationSynthetizer._resolve_temporal_resolution(
            input_df, uow_mock, ctx
        )

        assert list(result.columns) == OPERATION_SYNTHESIS_COMMON_COLUMNS

    def test_scenario_col_values(self, resolution_setup):
        """SCENARIO_COL must contain values 1..NUM_SCENARIOS, sorted then tiled."""
        input_df, ctx, *_ = resolution_setup
        uow_mock = MagicMock()

        result = OperationSynthetizer._resolve_temporal_resolution(
            input_df, uow_mock, ctx
        )

        # Within each stage, scenarios repeat: 1,1,2,2,...,5,5 (once per block)
        expected_scenarios = np.tile(
            np.repeat(np.arange(1, NUM_SCENARIOS + 1), NUM_BLOCKS),
            NUM_STAGES,
        )
        np.testing.assert_array_equal(
            result[SCENARIO_COL].to_numpy(), expected_scenarios
        )

    def test_stage_col_values(self, resolution_setup):
        """STAGE_COL must contain values 1..NUM_STAGES, each repeated NUM_SCENARIOS*NUM_BLOCKS times."""
        input_df, ctx, *_ = resolution_setup
        uow_mock = MagicMock()

        result = OperationSynthetizer._resolve_temporal_resolution(
            input_df, uow_mock, ctx
        )

        expected_stages = np.repeat(
            np.arange(1, NUM_STAGES + 1), NUM_SCENARIOS * NUM_BLOCKS
        )
        np.testing.assert_array_equal(
            result[STAGE_COL].to_numpy(), expected_stages
        )

    def test_block_duration_col_values(self, resolution_setup):
        """BLOCK_DURATION_COL must equal duration_per_block * STAGE_DURATION_HOURS."""
        input_df, ctx, *_ = resolution_setup
        uow_mock = MagicMock()
        duration_per_block = 0.5

        result = OperationSynthetizer._resolve_temporal_resolution(
            input_df, uow_mock, ctx
        )

        expected_duration = duration_per_block * STAGE_DURATION_HOURS
        np.testing.assert_allclose(
            result[BLOCK_DURATION_COL].to_numpy(),
            expected_duration,
            rtol=1e-10,
        )

    def test_polars_path_matches_pandas_fallback(self, resolution_setup):
        """Both Polars and pandas fallback paths must produce numerically identical pl.DataFrame results."""
        input_df, ctx, *_ = resolution_setup
        uow_mock = MagicMock()

        # Get Polars path result (normal execution) — returns pl.DataFrame
        result_polars = OperationSynthetizer._resolve_temporal_resolution(
            input_df.copy(), uow_mock, ctx
        )

        # Force pandas fallback by patching pd_to_pl to raise inside _add_temporal_info_polars.
        # The fallback uses pl.from_pandas directly, so it still returns a pl.DataFrame.
        with patch(
            "app.services.synthesis.operation.pd_to_pl",
            side_effect=ComputeError("forced failure"),
        ):
            result_fallback = OperationSynthetizer._resolve_temporal_resolution(
                input_df.copy(), uow_mock, ctx
            )

        assert isinstance(result_polars, pl.DataFrame)
        assert isinstance(result_fallback, pl.DataFrame)

        for col in OPERATION_SYNTHESIS_COMMON_COLUMNS:
            if col in (START_DATE_COL, END_DATE_COL):
                # Compare datetime values independent of numpy precision (ms vs us)
                np.testing.assert_array_equal(
                    result_polars[col].cast(pl.Datetime("us")).to_numpy(),
                    result_fallback[col].cast(pl.Datetime("us")).to_numpy(),
                    err_msg=f"Mismatch in column '{col}'",
                )
            else:
                np.testing.assert_array_equal(
                    result_polars[col].to_numpy(),
                    result_fallback[col].to_numpy(),
                    err_msg=f"Mismatch in column '{col}'",
                )

    def test_deck_context_prevents_deck_calls(self, resolution_setup):
        """When deck_context is provided, no Deck methods should be called."""
        input_df, ctx, *_ = resolution_setup
        uow_mock = MagicMock()

        with patch("app.services.synthesis.operation.Deck") as mock_deck:
            OperationSynthetizer._resolve_temporal_resolution(
                input_df, uow_mock, ctx
            )
            mock_deck.num_scenarios_final_simulation.assert_not_called()
            mock_deck.internal_stages_starting_dates_final_simulation.assert_not_called()
            mock_deck.internal_stages_ending_dates_final_simulation.assert_not_called()
            mock_deck.block_lengths.assert_not_called()

    def test_polars_error_falls_back_to_pandas_with_warning(
        self, resolution_setup, caplog
    ):
        """When Polars path raises ComputeError, a warning is logged and result is a pl.DataFrame."""
        input_df, ctx, *_ = resolution_setup
        uow_mock = MagicMock()

        # OperationSynthetizer._log only fires when cls.logger is not None.
        # Wire it to a real logger so caplog can capture the warning.
        test_logger = logging.getLogger("test_temporal_resolution")
        original_logger = OperationSynthetizer.logger
        OperationSynthetizer.logger = test_logger
        try:
            with patch(
                "app.services.synthesis.operation.pd_to_pl",
                side_effect=ComputeError("test compute error"),
            ):
                with caplog.at_level(
                    logging.WARNING, logger="test_temporal_resolution"
                ):
                    result = OperationSynthetizer._resolve_temporal_resolution(
                        input_df.copy(), uow_mock, ctx
                    )
        finally:
            OperationSynthetizer.logger = original_logger

        # Result must be a pl.DataFrame from the fallback path (pandas -> pl.from_pandas)
        assert result is not None
        assert isinstance(result, pl.DataFrame)
        assert result.shape[0] == NUM_STAGES * NUM_SCENARIOS * NUM_BLOCKS
        assert list(result.columns) == OPERATION_SYNTHESIS_COMMON_COLUMNS

        # Warning must mention "falling back to pandas"
        warning_messages = [
            r.message for r in caplog.records if r.levelno == logging.WARNING
        ]
        assert any(
            "falling back to pandas" in str(m) for m in warning_messages
        ), (
            f"Expected 'falling back to pandas' in warnings, got: {warning_messages}"
        )

    def test_none_input_returns_none(self):
        """None input must return None without raising."""
        uow_mock = MagicMock()
        result = OperationSynthetizer._resolve_temporal_resolution(
            None, uow_mock, None
        )
        assert result is None
