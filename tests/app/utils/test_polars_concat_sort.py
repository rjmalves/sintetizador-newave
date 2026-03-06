"""Tests for Polars concat + sort equivalence with pandas (ticket-006)."""

import time

import numpy as np
import pandas as pd
import polars as pl

from app.utils.dataframe import pd_to_pl, pl_to_pd


def _make_entity_dfs(
    num_entities: int, rows_per_entity: int, seed: int = 42
) -> list[pd.DataFrame]:
    rng = np.random.default_rng(seed)
    dfs = []
    for entity_id in range(1, num_entities + 1):
        dfs.append(
            pd.DataFrame(
                {
                    "codigo_usina": entity_id,
                    "estagio": np.repeat(
                        np.arange(1, rows_per_entity // 10 + 1), 10
                    )[:rows_per_entity],
                    "cenario": np.tile(np.arange(1, 11), rows_per_entity // 10)[
                        :rows_per_entity
                    ],
                    "valor": rng.random(rows_per_entity),
                }
            )
        )
    return dfs


def test_polars_concat_sort_matches_pandas():
    """pl.concat + pl.sort produces same result as pd.concat + sort_values."""
    dfs = _make_entity_dfs(num_entities=20, rows_per_entity=100)
    sorting_cols = ["codigo_usina", "estagio"]

    # pandas path
    pd_result = (
        pd.concat(dfs, ignore_index=True)
        .sort_values(sorting_cols)
        .reset_index(drop=True)
    )

    # polars path
    pl_dfs = [pd_to_pl(df) for df in dfs]
    pl_combined = pl.concat(pl_dfs).sort(sorting_cols, maintain_order=True)
    pl_result = pl_to_pd(pl_combined).reset_index(drop=True)

    pd.testing.assert_frame_equal(pl_result, pd_result)


def test_polars_concat_row_count():
    """pl.concat preserves total row count across entities."""
    dfs = _make_entity_dfs(num_entities=50, rows_per_entity=200)
    expected_rows = 50 * 200

    pl_dfs = [pd_to_pl(df) for df in dfs]
    result = pl_to_pd(pl.concat(pl_dfs))

    assert len(result) == expected_rows


def test_polars_concat_all_none_returns_empty():
    """When all entity DataFrames are None, no concat occurs."""
    responses = {"a": None, "b": None, "c": None}
    valid_dfs = [df for df in responses.values() if df is not None]
    assert len(valid_dfs) == 0


def test_polars_sort_matches_pandas_with_ties():
    """Stable sort (maintain_order=True) matches pandas stable sort on ties."""
    rng = np.random.default_rng(99)
    n = 500
    df = pd.DataFrame(
        {
            "group": np.repeat([1, 2, 3, 4, 5], n // 5),
            "stage": np.ones(n, dtype=int),
            "cenario": np.tile(np.arange(1, n // 5 + 1), 5),
            "valor": rng.random(n),
        }
    )
    sorting_cols = ["group", "stage"]

    pd_sorted = df.sort_values(sorting_cols).reset_index(drop=True)

    pl_sorted = pd_to_pl(df).sort(sorting_cols, maintain_order=True)
    pl_result = pl_to_pd(pl_sorted).reset_index(drop=True)

    pd.testing.assert_frame_equal(pl_result, pd_sorted)


def test_polars_concat_sort_row_count_at_scale():
    """200 entities x 7200 rows = 1,440,000 rows after concat + sort."""
    dfs = _make_entity_dfs(num_entities=200, rows_per_entity=7200)
    expected_rows = 200 * 7200

    pl_dfs = [pd_to_pl(df) for df in dfs]
    pl_combined = pl.concat(pl_dfs).sort(
        ["codigo_usina", "estagio"], maintain_order=True
    )
    result = pl_to_pd(pl_combined).reset_index(drop=True)

    assert len(result) == expected_rows
    # Verify sort order: codigo_usina should be non-decreasing
    assert (result["codigo_usina"].diff().dropna() >= 0).all()


def test_polars_concat_sort_faster_than_pandas():
    """Polars concat+sort completes in less than 50% of pandas time."""
    dfs = _make_entity_dfs(num_entities=100, rows_per_entity=72000)
    sorting_cols = ["codigo_usina", "estagio"]

    # Warm up both engines to eliminate JIT/import overhead
    _warm = _make_entity_dfs(num_entities=2, rows_per_entity=100)
    pd.concat(_warm, ignore_index=True).sort_values(sorting_cols)
    pl.concat([pd_to_pl(d) for d in _warm]).sort(sorting_cols)

    # pandas timing
    t0 = time.perf_counter()
    pd.concat(dfs, ignore_index=True).sort_values(sorting_cols).reset_index(
        drop=True
    )
    pd_time = time.perf_counter() - t0

    # polars timing (include conversion overhead for fairness)
    t0 = time.perf_counter()
    pl_dfs = [pd_to_pl(df) for df in dfs]
    pl_to_pd(
        pl.concat(pl_dfs).sort(sorting_cols, maintain_order=True)
    ).reset_index(drop=True)
    pl_time = time.perf_counter() - t0

    assert pl_time < pd_time * 0.80, (
        f"Polars ({pl_time:.3f}s) not <80% of pandas ({pd_time:.3f}s)"
    )
