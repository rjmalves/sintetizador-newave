# ticket-002 Convert uncached series accessors and DeckContext to polars

## Context

### Background

After ticket-001 converts cached accessors to return polars, the uncached series accessors (energiaf, enavazf, vazaof, energiab, enavazb, vazaob, energias, enavazs, vazaos) still return `pd.DataFrame`. These functions read large binary time-series files from inewave and are called repeatedly during synthesis. Additionally, `DeckContext` stores `pd.DataFrame` fields (`block_lengths`, `eer_submarket_map`, `hydro_eer_submarket_map`) that get converted to polars downstream. This ticket converts both to polars.

### Relation to Epic

This is the second ticket of Epic 1 (Conversion Boundary). It completes the accessor layer conversion by handling the uncached series accessors and DeckContext, so that all data flowing out of the deck layer is polars.

### Current State

`app/services/deck/accessors.py` lines 239-395 contain 9 uncached series accessor functions. Each:

1. Calls a `readers.get_*()` function that returns an inewave file object
2. Accesses `.series` to get a `pd.DataFrame`
3. Renames columns using `df.rename(columns={...})`
4. Returns `pd.DataFrame` or empty `pd.DataFrame()`

`app/services/deck/context.py` defines `DeckContext` with three `pd.DataFrame` fields:

- `block_lengths: pd.DataFrame`
- `eer_submarket_map: pd.DataFrame`
- `hydro_eer_submarket_map: pd.DataFrame`

The `from_deck()` classmethod populates these from Deck methods. After ticket-001, those Deck methods already return `pl.DataFrame`, so the type annotations in DeckContext should change to match.

## Specification

### Requirements

1. Each uncached series accessor must convert its pandas result to `pl.DataFrame` before returning
2. Empty DataFrame fallbacks must use `pl.DataFrame()` instead of `pd.DataFrame()`
3. Column renames must use polars `.rename()` syntax
4. Return type annotations must change from `pd.DataFrame` to `pl.DataFrame`
5. `DeckContext` field type annotations must change from `pd.DataFrame` to `pl.DataFrame`
6. `DeckContext.from_deck()` must work correctly with the polars returns from Deck (after ticket-001)
7. The `import pandas as pd` in `context.py` must be replaced with `import polars as pl`

### Inputs/Props

- inewave series file objects with `.series` attribute returning `pd.DataFrame`
- polars library

### Outputs/Behavior

- All uncached series accessors return `pl.DataFrame`
- DeckContext stores `pl.DataFrame` in its DataFrame fields
- The `from_deck()` classmethod still works without modification (Deck already returns polars after ticket-001)

### Error Handling

- If `.series` is None, return `pl.DataFrame()` (empty polars DataFrame)
- Column rename errors should propagate (same behavior as current pandas code)

## Acceptance Criteria

- [ ] Given the accessor function `energiaf()` in `accessors.py`, when called with a valid iteration and uow, then it returns a `pl.DataFrame` with columns including `EER_CODE_COL` and `SCENARIO_COL`
- [ ] Given the accessor function `vazaof()` in `accessors.py`, when the underlying file is missing (returns None), then it returns an empty `pl.DataFrame` (verified by `result.is_empty()` and `isinstance(result, pl.DataFrame)`)
- [ ] Given `DeckContext`, when inspecting its field type annotations, then `block_lengths`, `eer_submarket_map`, and `hydro_eer_submarket_map` are annotated as `pl.DataFrame`
- [ ] Given `context.py`, when searching for `import pandas`, then zero matches are found (pandas import removed, replaced with polars)
- [ ] Given `deck.py`, when inspecting return type annotations for `energiaf`, `enavazf`, `vazaof`, `energiab`, `enavazb`, `vazaob`, `energias`, `enavazs`, `vazaos`, then all are `pl.DataFrame`

## Implementation Guide

### Suggested Approach

1. In `accessors.py`, for each of the 9 uncached series accessor functions:
   a. After accessing `.series` and getting a pandas DataFrame, convert with `pl.from_pandas()`
   b. Change `.rename(columns={...})` to polars `.rename({...})`
   c. Change empty fallback from `pd.DataFrame()` to `pl.DataFrame()`
   d. Update return type annotation to `pl.DataFrame`
2. In `context.py`:
   a. Replace `import pandas as pd` with `import polars as pl`
   b. Change field type annotations from `pd.DataFrame` to `pl.DataFrame`
   c. The `from_deck()` classmethod body does not need changes (Deck already returns polars)
3. In `deck.py`, update return type annotations for all 9 series accessor facade methods

### Key Files to Modify

- `app/services/deck/accessors.py` -- convert uncached series accessors
- `app/services/deck/context.py` -- change field types to polars
- `app/services/deck/deck.py` -- update type annotations for series accessors

### Patterns to Follow

- `pl.from_pandas(df).rename({"old": "new"})` for conversion + rename in one chain
- `pl.DataFrame()` for empty DataFrames (no schema needed for empty fallbacks)

### Pitfalls to Avoid

- The `.rename()` syntax differs: pandas uses `columns={"old": "new"}`, polars uses `.rename({"old": "new"})`
- Do NOT modify `readers.py` -- it stays pandas internally
- The `from_deck()` classmethod may need the `__post_init__` validation updated if it checks for `pd.DataFrame` type explicitly (it does not currently -- it only checks for None)

## Testing Requirements

### Unit Tests

- Verify that `energiaf()`, `vazaof()`, etc. return `pl.DataFrame` instances
- Verify that `DeckContext` can be instantiated with `pl.DataFrame` fields

### Integration Tests

- Not applicable (downstream consumers updated in ticket-003)

### E2E Tests

- Not applicable

## Dependencies

- **Blocked By**: ticket-001-convert-cached-accessors-polars.md
- **Blocks**: ticket-003-add-polars-compatibility-shims.md

## Effort Estimate

**Points**: 3
**Confidence**: High

## Out of Scope

- Converting readers.py to polars (stays pandas, talks to inewave)
- Updating downstream consumers (ticket-003)
- Converting domain modules (Epic 2)
