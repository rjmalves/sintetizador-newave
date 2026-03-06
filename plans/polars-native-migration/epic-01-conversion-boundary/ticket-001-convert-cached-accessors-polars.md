# ticket-001 Convert cached accessors to return polars DataFrames

## Context

### Background

The sintetizador-newave codebase completed a performance overhaul that introduced polars for hot-path operations but kept pandas as the internal representation. The accessor layer (`accessors.py`) reads inewave files (which return pandas DataFrames), caches them as pandas, and returns them with defensive `.copy()` calls. Every downstream consumer that needs polars must call `pd_to_pl()`, creating unnecessary copies. This ticket establishes the polars conversion boundary by converting all cached DataFrame accessors to store and return `pl.DataFrame`.

### Relation to Epic

This is the first ticket of Epic 1 (Conversion Boundary). It converts the data-at-rest in the accessor cache from pandas to polars, which is the foundational change enabling all subsequent migration work.

### Current State

`app/services/deck/accessors.py` contains 10 cached accessor functions that return `pd.DataFrame`:

- `confhd()` (line 89): returns `val.copy()` of a `pd.DataFrame`
- `clast()` (line 106): returns `val.copy()` of a `pd.DataFrame`
- `term()` (line 123): returns `val.copy()` of a `pd.DataFrame`
- `manutt()` (line 140): returns `val.copy()` of a `pd.DataFrame`
- `expt()` (line 171): returns `val.copy()` of a `pd.DataFrame`
- `hidr()` (line 188): returns `val.copy()` of a `pd.DataFrame`
- `engnat()` (line 226): returns `val` (no copy) of a `pd.DataFrame`
- `vazoes()` (line 398): returns `val.copy()` of a `pd.DataFrame`
- Plus the non-DataFrame cached accessors (dger, pmo, curva, modif, newavetim, study_title, version) that return inewave objects or strings -- these do NOT change.

The Deck facade (`app/services/deck/deck.py`) exposes these as classmethods with `pd.DataFrame` return type annotations.

## Specification

### Requirements

1. Each cached DataFrame accessor function in `accessors.py` must convert the inewave pandas result to `pl.DataFrame` using `pl.from_pandas()` before storing in cache
2. The `.copy()` calls must be removed (polars DataFrames are immutable; returning the cached reference is safe)
3. The return type annotation of each function must change from `pd.DataFrame` to `pl.DataFrame`
4. The corresponding Deck facade methods in `deck.py` must update their return type annotations from `pd.DataFrame` to `pl.DataFrame`
5. The `manutt()` function creates an empty `pd.DataFrame` as a fallback -- this must become an empty `pl.DataFrame` with the same column names
6. For `engnat()` and `vazoes()`, which may return large DataFrames, the conversion must happen at cache insertion time (not on every return)

### Inputs/Props

- inewave file parsing results (pandas DataFrames from `readers.py`)
- The module `polars` (already a project dependency)

### Outputs/Behavior

- All cached DataFrame accessors return `pl.DataFrame`
- Cache stores `pl.DataFrame` instances
- No `.copy()` calls remain on DataFrame returns
- Downstream consumers will receive `pl.DataFrame` instead of `pd.DataFrame` (they will break until compatibility shims are added in ticket-003)

### Error Handling

- If `pl.from_pandas()` raises on an unexpected dtype, let it propagate (the existing `readers.validate_data` already validates types before this point)
- The `manutt()` fallback for None must create `pl.DataFrame(schema={...})` with the correct column names and types

## Acceptance Criteria

- [ ] Given the accessor function `confhd()` in `accessors.py`, when it is called, then it returns a `pl.DataFrame` (verified by `isinstance(result, pl.DataFrame)`)
- [ ] Given the accessor function `hidr()` in `accessors.py`, when it is called twice, then both calls return the same object identity (`result1 is result2` is True, confirming no copy)
- [ ] Given the `manutt()` function when the underlying file has no maintenance data, then it returns an empty `pl.DataFrame` with columns `["codigo_empresa", "nome_empresa", "codigo_usina", "nome_usina", "codigo_unidade", "data_inicio", "duracao", "potencia"]`
- [ ] Given `deck.py`, when inspecting the return type annotation of `Deck.confhd()`, then it is `pl.DataFrame` (not `pd.DataFrame`)
- [ ] Given `accessors.py`, when searching for `.copy()` calls on DataFrame returns, then zero matches are found

## Implementation Guide

### Suggested Approach

1. Add `import polars as pl` to `accessors.py` (pandas import stays for the inewave boundary)
2. For each cached accessor that returns `pd.DataFrame`:
   a. After `readers.validate_data(...)` stores the pandas result in `val`, add `val = pl.from_pandas(val)` before `cache[key] = val`
   b. Remove the `return val.copy()` and replace with `return val`
   c. Change the return type annotation from `pd.DataFrame` to `pl.DataFrame`
3. For `manutt()`: change the fallback empty DataFrame from `pd.DataFrame(columns=[...])` to `pl.DataFrame(schema={"codigo_empresa": pl.Int64, "nome_empresa": pl.Utf8, "codigo_usina": pl.Int64, "nome_usina": pl.Utf8, "codigo_unidade": pl.Int64, "data_inicio": pl.Datetime, "duracao": pl.Int64, "potencia": pl.Float64})`
4. Update `deck.py` return type annotations for: `confhd`, `clast`, `term`, `manutt`, `expt`, `hidr`, `engnat`, `vazoes`
5. Do NOT update the non-DataFrame accessors (dger, pmo, curva, modif, newavetim, study_title, version)

### Key Files to Modify

- `app/services/deck/accessors.py` -- convert cached accessors
- `app/services/deck/deck.py` -- update type annotations

### Patterns to Follow

- `pl.from_pandas(df)` for conversion
- `pl.DataFrame(schema={...})` for empty DataFrames with schema
- Return cached polars DataFrame directly (no `.copy()`)

### Pitfalls to Avoid

- Do NOT convert the non-DataFrame cached accessors (dger, pmo, curva, modif return inewave objects)
- Do NOT convert the uncached series accessors (energiaf, enavazf, etc.) -- those are ticket-002
- Do NOT yet update consumers of these functions -- that is ticket-003
- The `manutt` empty DataFrame schema types must match what inewave would return; use `pl.Int64` for integer columns, `pl.Utf8` for string columns, `pl.Datetime` for date columns, `pl.Float64` for float columns

## Testing Requirements

### Unit Tests

- Tests in `tests/app/services/synthesis/test_operation.py` and `tests/app/services/synthesis/test_scenario.py` that mock Deck accessor methods will break because they mock with `pd.DataFrame`. These breakages are expected and will be fixed in ticket-003. This ticket should verify that the accessor functions themselves produce correct polars output by running any direct accessor tests.

### Integration Tests

- Not applicable at this stage (downstream breakage is expected until ticket-003)

### E2E Tests

- Not applicable

## Dependencies

- **Blocked By**: None
- **Blocks**: ticket-002-convert-uncached-accessors-deckcontext-polars.md, ticket-003-add-polars-compatibility-shims.md

## Effort Estimate

**Points**: 3
**Confidence**: High

## Out of Scope

- Converting uncached series accessors (energiaf, enavazf, etc.) -- ticket-002
- Converting DeckContext -- ticket-002
- Updating downstream consumers of Deck -- ticket-003
- Modifying readers.py (stays pandas internally)
