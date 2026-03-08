# Accumulated Learnings After Epic 04

## Conversion Patterns

- Convert-at-cache-insertion: `pl.from_pandas(val)` before `cache[key] = val`; no `.copy()` needed -- polars is immutable
- One-shot series accessors: `pl.from_pandas(arq.series).rename({"old": "new"})` -- no `columns=` keyword
- Empty fallback with schema: `pl.DataFrame(schema={"col": pl.Int64, ...})` not `pl.DataFrame(columns=[...])`
- Cross-join stage expansion: `df.join(pl.DataFrame({START_DATE_COL: dates}), how="cross").sort(...)` -- `app/services/deck/hydro.py` lines 505-513
- `pl.when().then().otherwise()` replaces all `.loc[condition, col] = value` mutations -- `app/services/deck/thermal.py` lines 22-39
- Resample: `pl.date_range() + left-join + forward_fill() + dt.truncate("1mo") + group_by().agg(.mean())` -- `app/services/deck/thermal.py` lines 75-171
- LTA/groupby: `.group_by(MONTH_COL).agg(pl.col(VAL).mean()).sort(MONTH_COL)` -- `.sort()` is required; `.group_by()` does NOT preserve order -- `app/services/synthesis/scenario.py` lines 182-189
- Scalar extraction: `df.filter(pl.col(col) == val)[other_col].item(0)` replaces `df.loc[mask].iloc[0]` -- `app/services/synthesis/scenario.py` lines 133-168
- Scalar multiply: `df.with_columns(pl.col(COL) * SCALAR)` replaces `df[COL] *= SCALAR` -- `app/services/synthesis/system.py` line 126
- Per-entity lookup dict: `dict(zip(df[KEY].to_list(), df[VAL].to_list()))` built before executor loop; never `.filter().item()` in a hot loop -- `app/services/synthesis/operation/resolution_sbm.py` lines 68-74

## Critical Gotchas

- **Pandas index dropped**: `pl.from_pandas(df)` silently drops the index; use `include_index=True` when index is a domain identifier -- `app/services/deck/accessors.py` line 208
- **Integer column names (RESOLVED in epic-04)**: `pl.from_pandas()` converts int column names to strings; access `Deck.vazoes()` columns with `vazoes[str(station_code)]` -- `app/services/synthesis/scenario.py` lines 135, 157
- **`df.is_empty()`**: polars uses method call; `df.empty` is a property (pandas only); using it on polars silently never triggers -- `app/services/synthesis/operation/orchestrator.py` line 373

## ProcessPoolExecutor Serialization Constraint

- Worker functions must `.to_pandas()` before returning; `_post_resolve()` calls `pl.from_pandas()` on receipt
- Two round-trip conversions per iteration file; future improvement: verify polars pickle compatibility and eliminate
- Sites: `app/services/synthesis/scenario.py` lines 831, 903, 960, 1008 (workers) and 843-849 (`_post_resolve`)

## Export Boundary Pattern

- `uow.export.synthetize_df()` always expects `pd.DataFrame`; convert explicitly: `uow.export.synthetize_df(df.to_pandas(), filename)`
- `export_metadata()` in all synthesizer classes retains pandas for tiny metadata tables -- permanent boundary
- `pd.date_range()` in `scenario.py` (lines 169, 298, 718) is used as a date utility only; acceptable to retain `import pandas as pd` for this use

## Shim Strategy -- COMPLETE FOR SYNTHESIS LAYER

- All synthesis `# SHIM` annotations removed; `scenario.py`, `execution.py`, `system.py`, `operation/*` have zero shims
- Three deck-layer shims remain as permanent debt markers: `deck.py` line 301, `readers.py` line 418, `bounds.py` line 1525 -- do not remove without porting those modules
- `app/utils/dataframe.py` (`pd_to_pl`/`pl_to_pd`) deleted; re-export from `operation/__init__.py` also removed

## Readers Boundary Pattern

- `readers.apply_modif_changes_to_hydros*()` accepts indexed `pd.DataFrame`
- Pattern: `df.to_pandas().set_index(HYDRO_CODE_COL)` / call reader / `pl.from_pandas(df_pd.reset_index())`
- Sites: `app/services/deck/hydro.py` lines 97-118, 171-191, 306-309, 401-411, 426-436, 481-500

## Conventions

- Temporary join columns use `_` prefix and are `.drop()`ped after use -- `app/services/deck/exchange.py` lines 43-83
- `if TYPE_CHECKING: import pandas as pd` guards runtime-clean annotations on functions still accepting `pd.DataFrame` -- `app/services/deck/temporal.py`, `hydro.py`
- `isinstance(df, pl.DataFrame)` dispatch in `_synthetize_single_variable()` handles mixed `pl.DataFrame | pd.DataFrame` return from `_resolve()` -- `execution.py` lines 179-183, `system.py` lines 194-198

## Remaining Migration Work

- `execution.py` / `system.py`: `_resolve_program`, `_resolve_title`, `_resolve_version`, `_resolve_convergence`, `__resolve_EST`, `__resolve_CVU` still return `pd.DataFrame`
- `bounds.py` and the `deck.py`/`readers.py` SHIM pair are the last unported deck-layer modules
- `pd.date_range()` in `scenario.py` can be replaced with `pl.date_range(..., eager=True).to_list()` to fully remove pandas from that file
- `TestPostResolveNoPdToPl` class in `test_entity_pipeline.py` was deleted without replacement; add a test verifying `_post_resolve()` correctly handles a `{int: pl.DataFrame}` dict input

## Quality Scoring Artifact

- Tickets 010-014 all scored `scope_adherence: 0.0` or `0.5` due to ripple-effect files (orchestrator, tests, utils) not listed in "Key Files to Modify"
- Mitigation: list every ripple-effect file explicitly in "Key Files" before dispatch
