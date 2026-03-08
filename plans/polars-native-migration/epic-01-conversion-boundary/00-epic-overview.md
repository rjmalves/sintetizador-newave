# Epic 01: Conversion Boundary

## Goal

Establish the polars conversion boundary at `accessors.py`, making all Deck facade DataFrame-returning methods return `pl.DataFrame` instead of `pd.DataFrame`. Update `DeckContext` to store polars DataFrames. This is the foundational change that all subsequent epics depend on.

## Scope

- Convert all cached DataFrame accessors in `accessors.py` to store and return `pl.DataFrame`
- Remove defensive `.copy()` calls (polars immutability makes them unnecessary)
- Convert uncached series-file accessors (energiaf, enavazf, etc.) to return `pl.DataFrame`
- Update `deck.py` facade return type annotations from `pd.DataFrame` to `pl.DataFrame`
- Convert `DeckContext` fields from `pd.DataFrame` to `pl.DataFrame`
- Update all direct consumers of Deck that expect `pd.DataFrame` to accept `pl.DataFrame` (temporary compatibility shims where needed)

## Tickets

| ID         | Title                                                       | Effort |
| ---------- | ----------------------------------------------------------- | ------ |
| ticket-001 | Convert cached accessors to return polars DataFrames        | 3      |
| ticket-002 | Convert uncached series accessors and DeckContext to polars | 3      |
| ticket-003 | Add polars compatibility shims to downstream consumers      | 3      |

## Dependencies

- No external dependencies
- Epic 2 depends on this epic being complete

## Success Criteria

- All Deck facade methods that return DataFrames return `pl.DataFrame`
- DeckContext stores `pl.DataFrame`
- All 349+ existing tests pass (consumers have compatibility shims)
- Zero `.copy()` calls in accessors.py
