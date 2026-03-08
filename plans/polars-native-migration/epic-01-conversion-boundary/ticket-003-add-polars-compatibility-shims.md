# ticket-003 Add polars compatibility shims to downstream consumers

## Context

### Background

After tickets 001 and 002, all Deck facade DataFrame methods return `pl.DataFrame`. However, every downstream consumer (deck domain modules, synthesis pipeline, scenario synthesis, tests) still expects `pd.DataFrame`. This ticket adds minimal compatibility shims: each consumer site that receives a Deck DataFrame and performs pandas operations on it gets a `df.to_pandas()` call at the boundary, preserving current behavior. These shims are temporary -- they will be removed as each module is migrated to polars in Epics 2-4.

### Relation to Epic

This is the third and final ticket of Epic 1 (Conversion Boundary). It restores the full test suite to passing by shimming all downstream consumers, making the polars boundary transparent until the consumers themselves are migrated.

### Current State

After tickets 001-002, the following modules call Deck methods and expect `pd.DataFrame`:

**Deck domain modules** (will be migrated in Epic 2):

- `entities.py` lines 52, 76, 100, 125, 153-155, 191-193, 243, 278-279: uses pandas operations on Deck returns (confhd, eer_submarket_map, hydro_eer_submarket_map, etc.)
- `energy.py`: uses Deck returns with pandas join, groupby, apply
- `hydro.py`: uses Deck returns with pandas join, set_index, apply
- `storage.py`: uses Deck returns with pandas join, apply
- `thermal.py`: uses Deck returns with pandas operations
- `exchange.py`: uses misc.block_lengths() with pandas operations
- `misc.py`: uses Deck returns with pandas operations
- `policy.py`: uses Deck returns with pandas operations
- `temporal.py`: uses Deck returns (mostly scalar accessors, some DataFrames)

**Synthesis pipeline** (will be migrated in Epic 3):

- `pipeline.py`: `_fetch_temporal_deck_data` returns DeckContext.block_lengths (now polars); `initial_stored_energy_df`, `generate_scenarios`, `post_resolve_GTER_UTE` use Deck returns
- `resolution_sbm.py`, `resolution_ree.py`, `resolution_uhe.py`, `resolution_ute.py`: use `Deck.submarkets()`, `Deck.eers()`, `Deck.hydros()`, `Deck.thermals()` with pandas `.reset_index()`, `.loc[]`
- `export.py`: uses pandas operations on DataFrames
- `stubs.py`, `_stubs_market.py`: use Deck returns with pandas operations

**Scenario synthesis** (will be migrated in Epic 4):

- `scenario.py`: uses Deck returns extensively with pandas operations

**Test files**:

- `tests/app/services/synthesis/test_operation.py`: creates pd.DataFrame mocks
- `tests/app/services/synthesis/test_scenario.py`: creates pd.DataFrame mocks

## Specification

### Requirements

1. Every site where a Deck facade method returning `pl.DataFrame` is consumed by pandas code must get a `.to_pandas()` conversion shim
2. For DeckContext consumers in `pipeline.py` (`_fetch_temporal_deck_data`), the returned block_lengths must be converted to pandas if the caller uses pandas operations on it
3. Resolution modules that call `Deck.submarkets()`, `Deck.eers()`, `Deck.hydros()`, etc. must convert to pandas at their call sites
4. Test fixtures that mock Deck methods with `pd.DataFrame` must either: (a) wrap the mock return in `pl.from_pandas()`, or (b) the consumer already has a shim
5. After all shims are added, the full test suite (349+ tests) must pass

### Inputs/Props

- `pl.DataFrame` instances from Deck methods
- Downstream modules expecting `pd.DataFrame`

### Outputs/Behavior

- All downstream consumers work correctly with shimmed conversions
- Full test suite passes
- Each shim is clearly marked with a comment `# SHIM: remove after polars migration of this module`

### Error Handling

- `.to_pandas()` should not fail on any valid `pl.DataFrame` -- no special error handling needed
- If a test mock returns `pd.DataFrame` and the tested code expects `pl.DataFrame`, wrap with `pl.from_pandas()` in the mock

## Acceptance Criteria

- [ ] Given the full test suite (`pytest tests/`), when run after this ticket, then all 349+ tests pass with exit code 0
- [ ] Given `entities.py`, when it calls `accessors.confhd()` (which now returns `pl.DataFrame`), then it has a `.to_pandas()` shim and the entity catalog functions work correctly
- [ ] Given `pipeline.py` function `_fetch_temporal_deck_data`, when `deck_context` is provided, then `deck_context.block_lengths` (now `pl.DataFrame`) is converted to pandas for the caller's use
- [ ] Given a search for `# SHIM:` comments in the codebase, then each shim is annotated with the removal condition
- [ ] Given `resolution_sbm.py` function `resolve_SBM`, when it calls `Deck.submarkets(uow)`, then it converts the result to pandas before calling `.reset_index()`, `.loc[]`, etc.

## Implementation Guide

### Suggested Approach

The most efficient approach is to add `.to_pandas()` at each call site where a Deck method result is used with pandas operations. There are approximately 40-50 such call sites spread across ~15 files.

**Strategy**: For each file, find all calls to Deck methods that return DataFrames, and add `.to_pandas()` after each call. Mark each with `# SHIM: remove after polars migration of this module`.

Specific patterns:

```python
# Before:
df = Deck.confhd(uow)
df.set_index(...)  # pandas operation

# After:
df = Deck.confhd(uow).to_pandas()  # SHIM: remove after polars migration of this module
df.set_index(...)  # pandas operation still works
```

For DeckContext consumers in pipeline.py:

```python
# The block_lengths field is now pl.DataFrame
# Add .to_pandas() where pandas operations are used on it
df_block_lengths = deck_context.block_lengths.to_pandas()  # SHIM
```

For test mocks: wrap `pd.DataFrame` mock returns with `pl.from_pandas()`:

```python
# Before:
mock_deck.confhd.return_value = pd.DataFrame(...)

# After:
mock_deck.confhd.return_value = pl.from_pandas(pd.DataFrame(...))
```

### Key Files to Modify

- `app/services/deck/entities.py` -- shim Deck accessor calls
- `app/services/deck/energy.py` -- shim accessors/entity calls
- `app/services/deck/hydro.py` -- shim accessors/entity calls
- `app/services/deck/storage.py` -- shim accessors/entity calls
- `app/services/deck/thermal.py` -- shim accessors/entity calls
- `app/services/deck/exchange.py` -- shim misc calls
- `app/services/deck/misc.py` -- shim accessors calls
- `app/services/deck/policy.py` -- shim entity/misc calls
- `app/services/deck/temporal.py` -- shim accessors calls (if any DataFrame returns)
- `app/services/synthesis/operation/pipeline.py` -- shim DeckContext and Deck calls
- `app/services/synthesis/operation/resolution_sbm.py` -- shim Deck.submarkets()
- `app/services/synthesis/operation/resolution_ree.py` -- shim Deck.eers()
- `app/services/synthesis/operation/resolution_uhe.py` -- shim Deck.hydros()
- `app/services/synthesis/operation/resolution_ute.py` -- shim Deck.thermals()
- `app/services/synthesis/operation/export.py` -- shim if needed
- `app/services/synthesis/scenario.py` -- shim Deck calls
- `tests/app/services/synthesis/test_operation.py` -- update mocks
- `tests/app/services/synthesis/test_scenario.py` -- update mocks

### Patterns to Follow

- `.to_pandas()` for converting polars back to pandas at consumer boundary
- `pl.from_pandas(pd.DataFrame(...))` for test mocks
- `# SHIM: remove after polars migration of this module` comment on every shim

### Pitfalls to Avoid

- Do NOT shim functions that return scalars (int, str, bool, datetime, list) -- only DataFrame-returning methods need shims
- Do NOT shim inewave object accessors (dger, pmo, curva, modif, newavetim) -- these were not changed
- Be careful with functions that chain: `Deck.eers(uow).index.tolist()` -- the `.to_pandas()` must go before the pandas method chain
- Some entity functions return DataFrames that are used both with pandas `.at[]` lookups and with polars joins -- for now, shim to pandas everywhere

## Testing Requirements

### Unit Tests

- Run the full test suite: `pytest tests/ -x` to verify all 349+ tests pass
- No new tests needed -- the purpose of this ticket is to restore existing tests to passing

### Integration Tests

- The full test suite serves as integration validation

### E2E Tests

- Not applicable

## Dependencies

- **Blocked By**: ticket-001-convert-cached-accessors-polars.md, ticket-002-convert-uncached-accessors-deckcontext-polars.md
- **Blocks**: ticket-004-port-entities-polars.md (Epic 2)

## Effort Estimate

**Points**: 3
**Confidence**: High

## Out of Scope

- Actually migrating any downstream module to polars (that's Epics 2-4)
- Modifying readers.py
- Optimizing the shim conversions (they are temporary)
