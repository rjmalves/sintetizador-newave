# Accumulated Learnings — Epic 02 (covers Epic 01 + Epic 02)

## Architecture & Boundaries

- `FSUnitOfWork` uses absolute paths internally; `chdir()` was safely removed by deleting 4 lines with no downstream breakage (`app/utils/fs.py`)
- `DeckContext` dataclass pre-computes deck data in the main process and passes it via pickle to subprocesses; keep it small (~KB) and use `Optional[DeckContext] = None` for gradual adoption (`app/services/deck/context.py`)
- Polars is used only inside functions; all function signatures remain `pd.DataFrame -> pd.DataFrame`; conversion at entry/exit via `app/utils/dataframe.py`
- No `import polars` inside subprocess-dispatched entity resolution methods (`__resolve_SIN`, `__resolve_UHE`, etc.) — Polars thread pool must not compete with Python process pool

## Polars Integration Patterns

- Single import point for all conversions: `from app.utils.dataframe import pd_to_pl, pl_to_pd` — never call `pl.from_pandas()` directly in service files
- Single-pass `group_by(...).agg([23 exprs]).unpivot()` for multi-statistic aggregations; avoids 23 sequential DataFrame scans (`app/utils/operations.py` — `_calc_statistics_polars`)
- Inline sort chain: `pl_to_pd(pl.concat([pd_to_pl(df) for df in dfs]).sort(cols, maintain_order=True))` — used in both `operation.py` and `scenario.py` `_post_resolve` methods
- Always `maintain_order=True` in Polars `group_by` and `sort` — Polars default is non-stable, which breaks row-by-row test comparisons
- Polars internal implementations named with `_polars` suffix (e.g., `_calc_statistics_polars`) so the pandas fallback is clearly separate
- Wrap Polars hot paths in try/except with logger warning and pandas fallback (`calc_statistics` in `app/utils/operations.py`)
- `unpivot` is the Polars >= 1.0 API (previously `melt`); use `unpivot` throughout

## Cache Safety

- `CACHED_SYNTHESIS` stores pandas DataFrames with no `.copy()` on store or retrieve (`operation.py` lines 2201, 2408)
- Any method reading from cache that modifies `VALUE_COL` must use `df.assign(**{VALUE_COL: ...})` — never mutate in-place; all stubs follow this pattern (`__stub_QDEF`, `__stub_VDEF`, `__stub_VEVAP`, `__stub_CTO`, `__stub_EVER`, conversion helpers)

## Data Pipeline

- `calc_statistics()` moved from per-entity to post-concatenation (Epic 01, ticket-003); now called once per variable instead of ~200 times for UHE
- Statistics relocation reduced test failures from 136 to 15 by cutting paths that previously hit the `Settings().installdir` None issue
- Always `.to_numpy().copy()` when extracting a numpy array from a cached DataFrame that will be mutated in-place (see initial storage computation in `operation.py`)

## Testing & Quality

- 15 pre-existing test failures remain due to `Settings().installdir = None` in the test environment — not caused by this epic; fix by mocking the settings fixture
- Missing: a test that calls `calc_statistics` and compares its Polars output to the pandas fallback on the same input — highest-risk correctness gap
- `pd_to_pl_lazy` is exported from `app/utils/dataframe.py` but unused in production code — either use in epic-03 lazy chains or remove

## Operational Notes

- Epic-02 changes are uncommitted: `app/utils/dataframe.py` and `tests/app/utils/` are untracked; make a single commit before starting epic-03 refinement
- `_calc_quantiles` and `_calc_mean_std` (pandas fallback paths) are retained in `app/utils/operations.py` intentionally — remove only after Polars path is proven stable in production
- `scenario.py` `_post_resolve` uses `pl.concat` without sort (sort happens in export step) — operation.py `_post_resolve` sorts immediately after concat due to spatial-entity ordering requirement
