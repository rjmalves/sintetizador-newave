# Epic 03: Synthesis Pipeline

## Goal

Port the synthesis operation pipeline (`pipeline.py`, `bounds.py`, `export.py`, `cache.py`, `spatial.py`, `resolution_*.py`, `operations.py`, `stubs.py`) to native polars, removing all pd/pl conversion wrappers, pandas fallback paths, and dual implementations.

## Scope

- Remove pandas fallback paths from `resolve_temporal_resolution` and `resolve_starting_stage_polars`
- Remove the pandas `resolve_starting_stage` function entirely
- Port `post_resolve` to return `pl.DataFrame` instead of `pd.DataFrame`
- Port `initial_stored_energy_df`, `generate_scenarios`, `post_resolve_GTER_UTE` to polars
- Remove pd/pl conversion wrapper in `bounds.py` (synthesis)
- Port `cache.py` to store `pl.DataFrame`
- Port `export.py` to accept polars natively (remove pd_to_pl/pl_to_pd conversions)
- Port `calc_statistics` in `operations.py` to accept `pl.DataFrame` directly
- Update all resolution modules to work with polars DataFrames from Deck
- Update `orchestrator.py` type annotations

## Tickets

| ID         | Title                                                  | Effort |
| ---------- | ------------------------------------------------------ | ------ |
| ticket-010 | Port pipeline.py to native polars                      | 5      |
| ticket-011 | Port synthesis bounds, cache, and export to polars     | 3      |
| ticket-012 | Port resolution modules and spatial dispatch to polars | 3      |

## Dependencies

- Depends on Epic 2 (deck domain modules return polars)
- Epic 4 depends on this epic

## Success Criteria

- Zero pandas fallback paths in pipeline.py
- Zero `pd_to_pl()` / `pl_to_pd()` calls in the synthesis pipeline
- `post_resolve` returns `pl.DataFrame`
- Cache stores `pl.DataFrame`
- All 349+ existing tests pass
