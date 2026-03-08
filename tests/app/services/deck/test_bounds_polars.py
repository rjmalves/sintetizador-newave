"""Unit tests for OperationVariableBounds.resolve_bounds Polars interface."""

import math
from datetime import datetime
from unittest.mock import patch

import numpy as np
import pandas as pd
import polars as pl

from app.internal.constants import (
    BLOCK_COL,
    EER_CODE_COL,
    LOWER_BOUND_COL,
    SCENARIO_COL,
    STAGE_COL,
    START_DATE_COL,
    UPPER_BOUND_COL,
    VALUE_COL,
)
from app.model.operation.operationsynthesis import OperationSynthesis
from app.model.operation.spatialresolution import SpatialResolution
from app.model.operation.variable import Variable
from app.services.deck.bounds import OperationVariableBounds


def _make_small_pl_df(n_rows: int = 6) -> pl.DataFrame:
    """Build a minimal pl.DataFrame that resembles a synthesis output."""
    return pl.DataFrame(
        {
            STAGE_COL: list(range(1, n_rows + 1)),
            SCENARIO_COL: [1] * n_rows,
            VALUE_COL: [float(i) for i in range(n_rows)],
        }
    )


class TestResolveBoundsUnboundedVariable:
    """resolve_bounds for a variable not present in MAPPINGS."""

    def test_returns_polars_dataframe(self):
        df = _make_small_pl_df()
        # CUSTO_MARGINAL_OPERACAO is not in MAPPINGS so it is unbounded.
        s = OperationSynthesis(
            Variable.CUSTO_MARGINAL_OPERACAO,
            SpatialResolution.SUBMERCADO,
        )
        result = OperationVariableBounds.resolve_bounds(
            s,
            df,
            {},
            uow=None,  # type: ignore[arg-type]
        )
        assert isinstance(result, pl.DataFrame)

    def test_lower_bound_is_negative_inf(self):
        df = _make_small_pl_df()
        s = OperationSynthesis(
            Variable.CUSTO_MARGINAL_OPERACAO,
            SpatialResolution.SUBMERCADO,
        )
        result = OperationVariableBounds.resolve_bounds(
            s,
            df,
            {},
            uow=None,  # type: ignore[arg-type]
        )
        for val in result[LOWER_BOUND_COL].to_list():
            assert math.isinf(val) and val < 0

    def test_upper_bound_is_positive_inf(self):
        df = _make_small_pl_df()
        s = OperationSynthesis(
            Variable.CUSTO_MARGINAL_OPERACAO,
            SpatialResolution.SUBMERCADO,
        )
        result = OperationVariableBounds.resolve_bounds(
            s,
            df,
            {},
            uow=None,  # type: ignore[arg-type]
        )
        for val in result[UPPER_BOUND_COL].to_list():
            assert math.isinf(val) and val > 0

    def test_original_columns_preserved(self):
        df = _make_small_pl_df()
        s = OperationSynthesis(
            Variable.CUSTO_MARGINAL_OPERACAO,
            SpatialResolution.SUBMERCADO,
        )
        result = OperationVariableBounds.resolve_bounds(
            s,
            df,
            {},
            uow=None,  # type: ignore[arg-type]
        )
        for col in [STAGE_COL, SCENARIO_COL, VALUE_COL]:
            assert col in result.columns

    def test_row_count_unchanged(self):
        n = 10
        df = _make_small_pl_df(n)
        s = OperationSynthesis(
            Variable.CUSTO_MARGINAL_OPERACAO,
            SpatialResolution.SUBMERCADO,
        )
        result = OperationVariableBounds.resolve_bounds(
            s,
            df,
            {},
            uow=None,  # type: ignore[arg-type]
        )
        assert len(result) == n


class TestResolveBoundsBoundedVariable:
    """resolve_bounds for ENERGIA_ARMAZENADA_ABSOLUTA_FINAL at REE resolution."""

    @staticmethod
    def _make_bounded_df() -> pl.DataFrame:
        """Build a pl.DataFrame for 2 REEs, 2 stages, 2 scenarios, 1 block."""
        dates = [datetime(2024, 1, 1), datetime(2024, 2, 1)]
        rows = []
        for eer in [1, 2]:
            for s_idx, d in enumerate(dates, start=1):
                for scen in [1, 2]:
                    rows.append(
                        {
                            EER_CODE_COL: eer,
                            START_DATE_COL: d,
                            STAGE_COL: s_idx,
                            SCENARIO_COL: scen,
                            BLOCK_COL: 1,
                            VALUE_COL: float(eer * 100 + s_idx * 10 + scen),
                        }
                    )
        return pl.DataFrame(rows)

    @staticmethod
    def _make_bounds_df() -> pd.DataFrame:
        """Mock upper/lower bounds: 2 REEs x 2 stages."""
        dates = [datetime(2024, 1, 1), datetime(2024, 2, 1)]
        rows = []
        for eer in [1, 2]:
            for d in dates:
                rows.append(
                    {
                        EER_CODE_COL: eer,
                        START_DATE_COL: d,
                        VALUE_COL: float(eer * 1000),
                    }
                )
        return pd.DataFrame(rows)

    @patch("app.services.deck.bounds.Deck")
    def test_bounded_returns_polars_with_non_null_bounds(self, mock_deck):
        upper_df = self._make_bounds_df()
        lower_df = self._make_bounds_df().copy()
        lower_df[VALUE_COL] = 0.0
        mock_deck.stored_energy_upper_bounds.return_value = upper_df
        mock_deck.eer_stored_energy_lower_bounds.return_value = lower_df

        df = self._make_bounded_df()
        s = OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        )
        ordered = {
            EER_CODE_COL: [1, 2],
            STAGE_COL: [1, 2],
            SCENARIO_COL: [1, 2],
            BLOCK_COL: [1],
        }
        result = OperationVariableBounds.resolve_bounds(
            s,
            df,
            ordered,
            uow=None,  # type: ignore[arg-type]
        )
        assert isinstance(result, pl.DataFrame)
        assert LOWER_BOUND_COL in result.columns
        assert UPPER_BOUND_COL in result.columns
        assert result[LOWER_BOUND_COL].null_count() == 0
        assert result[UPPER_BOUND_COL].null_count() == 0
        # Lower bounds should be 0.0, upper bounds should be positive
        assert all(v >= 0.0 for v in result[LOWER_BOUND_COL].to_list())
        assert all(v > 0.0 for v in result[UPPER_BOUND_COL].to_list())

    @patch("app.services.deck.bounds.Deck")
    def test_polars_path_matches_pandas_path(self, mock_deck):
        """C3: Polars-path output matches direct pandas helper output."""
        upper_df = self._make_bounds_df()
        lower_df = self._make_bounds_df().copy()
        lower_df[VALUE_COL] = 0.0

        df = self._make_bounded_df()
        s = OperationSynthesis(
            Variable.ENERGIA_ARMAZENADA_ABSOLUTA_FINAL,
            SpatialResolution.RESERVATORIO_EQUIVALENTE,
        )
        ordered = {
            EER_CODE_COL: [1, 2],
            STAGE_COL: [1, 2],
            SCENARIO_COL: [1, 2],
            BLOCK_COL: [1],
        }

        # Polars path: resolve_bounds accepts pl.DataFrame
        mock_deck.stored_energy_upper_bounds.return_value = upper_df.copy()
        mock_deck.eer_stored_energy_lower_bounds.return_value = lower_df.copy()
        polars_result = OperationVariableBounds.resolve_bounds(
            s,
            df,
            ordered,
            uow=None,  # type: ignore[arg-type]
        )

        # Direct pandas path: call the MAPPINGS lambda directly with pd.DataFrame
        mock_deck.stored_energy_upper_bounds.return_value = upper_df.copy()
        mock_deck.eer_stored_energy_lower_bounds.return_value = lower_df.copy()
        pandas_result = OperationVariableBounds.MAPPINGS[s](
            df.to_pandas(), None, ordered
        )

        polars_pd = (
            polars_result.to_pandas()
            .sort_values([EER_CODE_COL, STAGE_COL, SCENARIO_COL, BLOCK_COL])
            .reset_index(drop=True)
        )
        pandas_pd = pandas_result.sort_values(
            [EER_CODE_COL, STAGE_COL, SCENARIO_COL, BLOCK_COL]
        ).reset_index(drop=True)

        np.testing.assert_allclose(
            polars_pd[LOWER_BOUND_COL].values,
            pandas_pd[LOWER_BOUND_COL].values,
            atol=1e-6,
        )
        np.testing.assert_allclose(
            polars_pd[UPPER_BOUND_COL].values,
            pandas_pd[UPPER_BOUND_COL].values,
            atol=1e-6,
        )
        np.testing.assert_allclose(
            polars_pd[VALUE_COL].values,
            pandas_pd[VALUE_COL].values,
            atol=1e-6,
        )
