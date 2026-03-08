# ticket-002 Pre-compute Deck Data and Pass to Subprocesses

## Context

### Background

The `Deck` class (`app/services/deck/deck.py`, 4283 lines) uses a class-level dictionary `DECK_DATA_CACHING: Dict[str, Any] = {}` to cache parsed deck files (dger, confhd, patamar, ree, sistema, etc.). This cache works in the main process, but when `multiprocessing.Pool` spawns subprocesses, each subprocess gets a fresh copy of the class with an empty cache. Every subprocess therefore re-reads and re-parses the same deck files.

For a UHE synthesis with ~200 plants, this means each of the ~200 subprocess invocations re-reads dger.dat, confhd.dat, patamar.dat, etc. These are not nwlistop output files -- they are configuration/input files that are identical across all entities. Re-reading them hundreds of times is pure waste.

### Relation to Epic

This is the second ticket in Epic 01 (Foundation Fixes). It depends on ticket-001 (chdir removal) because the current subprocess flow uses `with uow:` which triggers `chdir()`. With chdir removed, the UoW is safe to use in subprocess entity resolution methods.

### Current State

- `Deck.DECK_DATA_CACHING` (line 123 of deck.py): class-level dict populated lazily by methods like `Deck.dger()`, `Deck.pmo()`, `Deck.confhd()`, etc.
- Each `Deck._get_*` method (e.g., `_get_dger` at line 131, `_get_confhd` at line 183) wraps the file read in `with uow:`, triggering UoW context entry
- Each public `Deck.*` method (e.g., `Deck.dger()` at line 464) checks cache, calls `_get_*` if miss, stores in cache
- `OperationSynthetizer._resolve_SBM_entity` (line 444-475 of operation.py): called in subprocess, accesses `Deck.block_lengths(uow)` via `_resolve_temporal_resolution`, which reads patamar.dat
- `OperationSynthetizer._resolve_UHE_entity` (line 668-742 of operation.py): called in subprocess, accesses `Deck.hydro_eer_submarket_map(uow)`, `Deck.num_blocks(uow)`, etc.
- Subprocess entry point methods like `__resolve_UHE` (line 745) create a `Pool`, then call `pool.apply_async(cls._resolve_UHE_entity, (uow, synthesis, idx, name))` for each UHE

## Specification

### Requirements

1. Create a `DeckContext` dataclass (or similar) that holds all pre-computed deck data needed by subprocess entity resolution methods
2. Before entering the multiprocessing pool, call a method that pre-computes all required deck data from the main process (where caching works)
3. Pass the `DeckContext` to each subprocess instead of having subprocesses call `Deck.*()` methods that trigger re-reads
4. Modify the entity resolution methods (`_resolve_SBM_entity`, `_resolve_REE_entity`, `_resolve_UHE_entity`, `_resolve_SBP_entity`, `_resolve_PEE_entity`, `_resolve_UTE_entity`) to accept and use the `DeckContext` instead of calling `Deck.*` via UoW
5. The `DeckContext` must be pickle-serializable since it is sent to subprocesses
6. All existing tests must pass

### Inputs/Props

- `DeckContext` contains the subset of deck data used by entity resolution methods:
  - `block_lengths: pd.DataFrame` (from `Deck.block_lengths()`)
  - `num_scenarios: int` (from `Deck.num_scenarios_final_simulation()`)
  - `num_blocks: int` (from `Deck.num_blocks()`)
  - `starting_dates: List[datetime]` (from `Deck.internal_stages_starting_dates_final_simulation()`)
  - `ending_dates: List[datetime]` (from `Deck.internal_stages_ending_dates_final_simulation()`)
  - `eer_submarket_map: pd.DataFrame` (from `Deck.eer_submarket_map()`)
  - `hydro_eer_submarket_map: pd.DataFrame` (from `Deck.hydro_eer_submarket_map()`)
  - `study_period_starting_month: int` (from `Deck.study_period_starting_month()`)
  - `hydro_simulation_ending_date: datetime` (from `Deck.hydro_simulation_stages_ending_date_final_simulation()`)

### Outputs/Behavior

- Deck files are read exactly once in the main process during the preprocessing phase
- Subprocess entity resolution methods receive pre-computed data -- no file I/O for deck files
- The `Deck.DECK_DATA_CACHING` dict is still populated in the main process (preserving existing behavior for non-parallelized paths)
- The overall synthesis results are identical to before

### Error Handling

- If a required deck file fails to read during pre-computation, the existing `RuntimeError` exceptions in `Deck._get_*` methods propagate to the main process, failing fast before any subprocess work begins
- The `DeckContext` dataclass should validate that all required fields are non-None on construction

## Acceptance Criteria

- [ ] Given a `DeckContext` is created from the main process, when it is serialized via `pickle.dumps()` and deserialized via `pickle.loads()`, then all fields are preserved with identical values
- [ ] Given `_resolve_UHE_entity` is called in a subprocess with a `DeckContext`, when temporal resolution is applied, then `Deck.block_lengths()` is NOT called (the data comes from the context)
- [ ] Given a full operation synthesis with `--processadores 4` runs, when monitoring file reads via added debug logging, then deck files (dger.dat, confhd.dat, patamar.dat, etc.) are each read exactly once total
- [ ] Given `pytest tests/` is run, when all tests execute, then all tests pass with zero failures
- [ ] Given the `DeckContext` dataclass file is inspected, when examining its fields, then it contains exactly the fields listed in the Inputs/Props section with proper type annotations

## Implementation Guide

### Suggested Approach

1. Create a new file `app/services/deck/context.py` with a `DeckContext` dataclass:

   ```python
   from dataclasses import dataclass
   from datetime import datetime
   from typing import List
   import pandas as pd

   @dataclass
   class DeckContext:
       block_lengths: pd.DataFrame
       num_scenarios: int
       num_blocks: int
       starting_dates: List[datetime]
       ending_dates: List[datetime]
       eer_submarket_map: pd.DataFrame
       hydro_eer_submarket_map: pd.DataFrame
       study_period_starting_month: int
       hydro_simulation_ending_date: datetime

       @classmethod
       def from_deck(cls, uow) -> "DeckContext":
           from app.services.deck.deck import Deck
           return cls(
               block_lengths=Deck.block_lengths(uow),
               num_scenarios=Deck.num_scenarios_final_simulation(uow),
               num_blocks=Deck.num_blocks(uow),
               starting_dates=Deck.internal_stages_starting_dates_final_simulation(uow),
               ending_dates=Deck.internal_stages_ending_dates_final_simulation(uow),
               eer_submarket_map=Deck.eer_submarket_map(uow),
               hydro_eer_submarket_map=Deck.hydro_eer_submarket_map(uow),
               study_period_starting_month=Deck.study_period_starting_month(uow),
               hydro_simulation_ending_date=Deck.hydro_simulation_stages_ending_date_final_simulation(uow),
           )
   ```

2. In `OperationSynthetizer.synthetize()` (line 2512 of operation.py), after `enforce_version(uow)` and before the variable loop, create the `DeckContext`:

   ```python
   deck_context = DeckContext.from_deck(uow)
   ```

3. Pass `deck_context` through to `_synthetize_single_variable`, then to `_resolve_synthesis` / `_resolve_spatial_resolution`, then to the `__resolve_*` methods, then to the `pool.apply_async` calls as an additional argument.

4. Modify `_resolve_SBM_entity`, `_resolve_REE_entity`, `_resolve_UHE_entity`, etc. to accept `deck_context: DeckContext` and use its fields instead of calling `Deck.*()`.

5. Modify `_resolve_temporal_resolution` and `_resolve_starting_stage` to accept `DeckContext` instead of `uow` for deck data access. These are called inside entity resolution methods.

6. The `_resolve_temporal_resolution` nested functions (`_add_block_duration_info`, `_replace_scenario_info`, `_add_stage_info`, `_add_temporal_info`) currently call `Deck.block_lengths(uow)`, `Deck.num_scenarios_final_simulation(uow)`, `Deck.internal_stages_starting_dates_final_simulation(uow)`, and `Deck.internal_stages_ending_dates_final_simulation(uow)`. Replace these with reads from `deck_context`.

7. Note: the `DeckContext.from_deck()` factory still uses the existing `Deck` methods which populate `DECK_DATA_CACHING` in the main process. This preserves the existing caching behavior for any non-parallelized paths that still call `Deck.*()` directly.

### Key Files to Modify

- `app/services/deck/context.py` (new file)
- `app/services/synthesis/operation.py` (pass context through call chain, modify entity resolution methods)
- `app/services/synthesis/scenario.py` (same pattern if it uses multiprocessing -- verify)

### Patterns to Follow

- Follow the existing dataclass pattern used by `OperationSynthesis` in `app/model/operation/operationsynthesis.py`
- For the `from_deck` factory, follow the existing `factory()` classmethod pattern used in model classes

### Pitfalls to Avoid

- Do NOT modify the `Deck` class itself beyond what is necessary -- it is used in many places. The context is an extraction layer, not a replacement.
- Do NOT try to share `DeckContext` via `multiprocessing.Manager` -- the dataclass will be pickled and sent to each subprocess, which is simpler and faster for this data size (a few KB)
- Do NOT remove the `Deck.DECK_DATA_CACHING` mechanism -- it still serves the main process and non-parallelized code paths
- Be careful with `pd.DataFrame` fields in the dataclass -- they are pickle-serializable but you must verify the exact DataFrames used are not too large (they should be small -- deck data, not synthesis data)
- The `_resolve_UHE_entity` method also calls `Deck.hydro_eer_submarket_map(uow)` and `Deck.num_blocks(uow)` -- make sure these are included in the context

## Testing Requirements

### Unit Tests

- Test that `DeckContext.from_deck()` creates a valid context from a mock UoW
- Test that `DeckContext` round-trips through pickle serialization
- Test that entity resolution methods produce the same output with `DeckContext` as they did with direct `Deck.*()` calls

### Integration Tests

- Run the existing test suite (`pytest tests/`) -- all must pass
- Verify the `test_operation.py` tests still pass, as these exercise the full entity resolution pipeline

### E2E Tests (if applicable)

- Not required for this ticket

## Dependencies

- **Blocked By**: ticket-001-eliminate-chdir-from-unitofwork.md
- **Blocks**: None (but ticket-003 can run in parallel as it is independent)

## Effort Estimate

**Points**: 5
**Confidence**: High

## Out of Scope

- Changing the parallelism model itself (multiprocessing.Pool -> concurrent.futures) -- that is Epic 04
- Modifying the `Deck` class internal caching mechanism
- Pre-computing nwlistop data (that is fundamentally different -- each entity has its own file)
- The `ScenarioSynthetizer` parallel path -- evaluate whether it uses the same pattern, but only modify if it follows the same subprocess deck re-read issue
