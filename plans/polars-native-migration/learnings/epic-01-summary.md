# Accumulated Learnings After Epic 01

## Conversion Patterns

- Convert-at-cache-insertion: call `pl.from_pandas(val)` before `cache[key] = val`; return the cached reference directly (no `.copy()` needed -- polars is immutable)
- One-shot series accessors: chain inline as `pl.from_pandas(arq.series).rename({...})`
- Polars rename syntax: `.rename({"old": "new"})` -- no `columns=` keyword, unlike pandas
- Empty fallback with schema: `pl.DataFrame(schema={"col": pl.Int64, ...})` not `pl.DataFrame(columns=[...])`

## Critical Gotcha: Pandas Index

- `pl.from_pandas(df)` silently drops the pandas index
- Use `pl.from_pandas(df, include_index=True)` when the source DataFrame has meaningful data in its index
- Canonical example: `hidr.cadastro` uses plant code as index; `accessors.hidr()` uses `include_index=True` so `codigo_usina` appears as a column (`app/services/deck/accessors.py` line 208)
- Consumer shims: `.to_pandas().reset_index()` to recover the indexed form (`app/services/deck/hydro.py` lines 56, 209, 248, 304, 388)
- Before any `pl.from_pandas()` call on a new DataFrame, check whether `.index.name` is a domain identifier

## Critical Gotcha: Integer Column Names

- `pl.from_pandas()` converts integer column names to strings
- `Deck.vazoes()` has flow-station-code integers as column names; after `.to_pandas()` the consumer must cast back: `[int(c) for c in df.columns]`
- SHIM at `app/services/synthesis/scenario.py` lines 134-136 marks this
- Resolution: switch to long-form (tidy) table before migrating `scenario.py` in epic 04

## Shim Strategy

- Every `.to_pandas()` call added to a not-yet-migrated consumer is annotated `# SHIM: remove after polars migration of this module`
- Shim sites: `hydro.py` (5), `storage.py` (2), `thermal.py` (6), `stubs.py` (2), `scenario.py` (10)
- Modules with NO shims needed: `entities.py`, `misc.py`, `exchange.py`, `energy.py`, `policy.py` -- these read inewave directly or rely on entity functions, not the polars-returning cached accessors
- Remove shims as part of each migrating ticket; do not leave them post-migration

## Type Annotations vs Runtime Behavior

- `DeckContext.block_lengths` is annotated `pl.DataFrame` but `Deck.block_lengths()` actually returns `pd.DataFrame` (misc.py not yet migrated)
- Python does not enforce annotations at runtime; no crash, but `pipeline.py` double-converts via `pd_to_pl()` (line 216)
- Fix this inconsistency when migrating `misc.py` in epic 02 ticket-005

## Scope Reality vs Plan

- Ticket-003 predicted 40-50 shim sites across 15 files; actual result was ~25 sites across 5 files
- Several files listed as needing shims (`exchange.py`, `misc.py`, `policy.py`, `energy.py`, `resolution_*.py`) did not consume any polars-returning accessor directly

## Epic 02 Priorities

- Migrate `entities.py` first (ticket-004): its functions feed `hydro.py` and `storage.py` SHIM sites; migrating it collapses the pandas-index vs polars-column duality
- Migrate `misc.py` in ticket-005: resolves the `DeckContext.block_lengths` type inconsistency and removes the `pd_to_pl()` double-conversion in `pipeline.py`
