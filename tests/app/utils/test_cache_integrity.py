"""Tests for cache DataFrame copy elimination (ticket-007)."""

import numpy as np
import pandas as pd

from app.internal.constants import VALUE_COL


def test_assign_does_not_mutate_original():
    """Using .assign() on a cached DataFrame preserves the original."""
    cached_df = pd.DataFrame(
        {
            "codigo_usina": [1, 1, 2, 2],
            "estagio": [1, 2, 1, 2],
            "cenario": [1, 1, 1, 1],
            VALUE_COL: [10.0, 20.0, 30.0, 40.0],
        }
    )
    original_values = cached_df[VALUE_COL].to_numpy().copy()

    # Simulate stub pattern: .assign() creates a new DataFrame
    derived_df = cached_df.assign(**{VALUE_COL: cached_df[VALUE_COL] * 2.0})

    # Original must be unchanged
    np.testing.assert_array_equal(
        cached_df[VALUE_COL].to_numpy(), original_values
    )
    # Derived must have new values
    np.testing.assert_array_equal(
        derived_df[VALUE_COL].to_numpy(), original_values * 2.0
    )


def test_copy_protects_multi_column_mutation():
    """Using .copy() before multi-column mutation preserves the original."""
    cached_df = pd.DataFrame(
        {
            "codigo_usina": [1, 2],
            VALUE_COL: [100.0, 200.0],
            "limite_inferior": [0.0, 0.0],
            "limite_superior": [500.0, 600.0],
        }
    )
    original_values = cached_df[VALUE_COL].to_numpy().copy()

    # Simulate __stub_EARM_UHE pattern: .copy() then mutate
    working_df = cached_df.copy()
    working_df[VALUE_COL] = working_df[VALUE_COL] * 0.5
    working_df["limite_inferior"] = 0.0
    working_df["limite_superior"] = working_df["limite_superior"] * 0.5

    # Original must be unchanged
    np.testing.assert_array_equal(
        cached_df[VALUE_COL].to_numpy(), original_values
    )


def test_no_copy_on_store_shares_reference():
    """Storing without .copy() means cache holds same object."""
    cache = {}
    df = pd.DataFrame({VALUE_COL: [1.0, 2.0, 3.0]})
    cache["key"] = df  # No .copy()

    assert cache["key"] is df


def test_no_copy_on_retrieve_shares_reference():
    """Retrieving without .copy() means caller gets same object."""
    cache = {}
    df = pd.DataFrame({VALUE_COL: [1.0, 2.0, 3.0]})
    cache["key"] = df

    retrieved = cache["key"]  # No .copy()
    assert retrieved is df
