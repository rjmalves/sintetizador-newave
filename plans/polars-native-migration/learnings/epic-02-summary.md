# Accumulated Learnings After Epic 02

## Conversion Patterns

- Convert-at-cache-insertion: call `pl.from_pandas(val)` before `cache[key] = val`; return the cached reference directly (no `.copy()` needed -- polars is immutable)
- One-shot series accessors: chain inline as `pl.from_pandas(arq.series).rename({...})`
- Polars rename syntax: `.rename({"old": "new"})` -- no `columns=` keyword, unlike pandas
- Empty fallback with schema: `pl.DataFrame(schema={"col": pl.Int64, ...})` not `pl.DataFrame(columns=[...])`
- Cross-join for stage expansion: `df.join(pl.DataFrame({START_DATE_COL: dates}), how="cross").sort(...)` replaces `pd.concat([df] * N)` + `np.repeat()` -- used in `app/services/deck/hydro.py` lines 505-513
- `pl.when().then().otherwise()` replaces all `.loc[condition, col] = value` mutations -- canonical example at `app/services/deck/thermal.py` lines 22-39
- `df.iter_rows(named=True)` replaces `iterrows()` when iteration is semantically required (ordered change application, per-entity maintenance windows)
- Extract-to-numpy for per-row math: `df[c].to_numpy()`, compute, `df.with_columns(pl.Series(name, result))` -- used in `app/services/deck/storage.py` lines 44-103
- Resample-to-daily-then-monthly: `pl.date_range() + left-join + forward_fill() + dt.truncate("1mo") + group_by().agg(.mean())` replaces pandas `resample("D").ffill()` then `resample("MS").mean()` -- full example at `app/services/deck/thermal.py` lines 75-171
- `map_batches([col1, col2], lambda)` for date construction from year+month columns when `pl.date()` expression is unavailable -- `app/services/deck/thermal.py` lines 365-379

## Critical Gotcha: Pandas Index

- `pl.from_pandas(df)` silently drops the pandas index
- Use `pl.from_pandas(df, include_index=True)` when the source DataFrame has meaningful data in its index
- Canonical example: `hidr.cadastro` uses plant code as index; `accessors.hidr()` uses `include_index=True` so `codigo_usina` appears as a column (`app/services/deck/accessors.py` line 208)
- After epic-02, domain modules (entities, hydro, storage) no longer use `set_index()`; all joins are explicit `on=col` polars joins

## Critical Gotcha: Integer Column Names

- `pl.from_pandas()` converts integer column names to strings
- `Deck.vazoes()` has flow-station-code integers as column names; after `.to_pandas()` the consumer must cast back: `[int(c) for c in df.columns]`
- SHIM at `app/services/synthesis/scenario.py` lines 134-136 marks this
- Resolution: switch to long-form (tidy) table before migrating `scenario.py` in epic-04

## Shim Strategy

- Every `.to_pandas()` call added to a not-yet-migrated consumer is annotated `# SHIM: remove after polars migration of this module`
- After epic-02, the only remaining shims are in synthesis pipeline modules (`resolution_*.py`, `_stubs_*.py`, `pipeline.py`, `scenario.py`, `execution.py`, `system.py`)
- Domain modules (entities, temporal, misc, exchange, energy, hydro, storage, thermal, policy) have zero pandas shims
- Readers-boundary crossings in hydro.py (`to_pandas()` + `pl.from_pandas()` around `apply_modif_changes_*` calls) are permanent until readers.py itself is migrated; annotated `# convert to pandas for readers boundary`

## Readers Boundary Pattern

- `readers.apply_modif_changes_to_hydros()` and `readers.apply_modif_changes_to_hydros_in_stages()` accept `pd.DataFrame` indexed by `HYDRO_CODE_COL`
- Boundary crossing pattern: `df_pd = df.to_pandas().set_index(HYDRO_CODE_COL)` / call reader / `df_pd = df_pd.reset_index()` / `df = pl.from_pandas(df_pd)`
- Sites: `app/services/deck/hydro.py` lines 97-118, 171-191, 306-309, 401-411, 426-436, 481-500

## Temporary Column Naming Convention

- When a join introduces a temporary lookup column, name it with `_` prefix (e.g., `_avg_value`, `_hm3_lower`, `_upper_val`, `_new_potencia`) and explicitly `.drop()` it after use
- Prevents silent column collisions when the same column name exists in both DataFrames
- `app/services/deck/exchange.py` lines 43-83, `app/services/deck/hydro.py` lines 120-150

## `TYPE_CHECKING` Guard for Runtime-Clean Pandas Annotations

- Functions that still accept a pandas DataFrame parameter use `from __future__ import annotations` + `if TYPE_CHECKING: import pandas as pd`
- Ensures pandas is never imported at runtime in migrated modules
- Affected: `app/services/deck/temporal.py` lines 1-11 (three functions still accept `eers_df: "pd.DataFrame"`), `app/services/deck/hydro.py` lines 1-22

## Type Annotations vs Runtime Behavior

- `DeckContext.block_lengths` is annotated `pl.DataFrame` and now returns `pl.DataFrame` from `misc.block_lengths()` -- the type inconsistency from epic-01 is resolved; remove `pd_to_pl()` call in `pipeline.py` line ~216 when migrating epic-03

## Scope Reality vs Plan

- Ticket-003 predicted 40-50 shim sites; actual was ~25 across 5 files
- Epic-02 scope was accurate: all 9 domain modules ported with no unplanned regressions
- Quality scores: ticket-007 scored 0.65 (extra files modified beyond declared scope); tickets-006 and 008 scored 0.775; others 0.87-0.90
- Main quality penalty across all tickets: zero new test code added (test_delta = 0.0 or 0.5); tests pass via existing integration suite

## Epic-03 Priorities (synthesis pipeline)

- Remove `# SHIM` annotations from `resolution_*.py`, `_stubs_*.py`, `pipeline.py` -- all deck sources are now polars
- Remove `pd_to_pl()` call in `pipeline.py` for `block_lengths` -- now natively polars
- `resolution_sbm.py` uses `submarkets.loc[...]` filter -- replace with polars `.filter()`
- `scenario.py` wide-format vazoes DataFrame needs redesign to tidy format before migration (integer-column-name problem)
- `temporal.py` three functions still accept `eers_df: "pd.DataFrame"` -- resolve when policy.py is refactored to pass cached polars eers
