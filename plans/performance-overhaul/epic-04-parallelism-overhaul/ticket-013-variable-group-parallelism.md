# ticket-013 Implement Variable-Group Parallelism by Spatial Resolution

## Context

### Background

The current synthesis loop in `OperationSynthetizer.synthetize()` (operation.py line 2872) processes variables sequentially: for each variable, it creates a `ProcessPoolExecutor` (after ticket-012), dispatches all entities in parallel, waits for completion, then moves to the next variable. For UHE variables, this means spawning ~200 subprocesses per variable, and there are ~30 UHE variables. Each subprocess invocation incurs process spawn overhead, pickle serialization of `uow` and `DeckContext`, and subprocess teardown.

Variables at the same spatial resolution (e.g., all UHE variables) share the same entity list, the same DeckContext, and the same file access patterns. By grouping variables by spatial resolution and sharing a single executor across the group, the subprocess pool can be created once per resolution instead of once per variable, reducing spawn overhead proportionally to the number of variables in the group.

### Relation to Epic

This is the core performance improvement ticket for Epic 04. While ticket-012 modernizes the API, this ticket restructures the parallelism granularity to reduce the number of executor create/destroy cycles. It depends on ticket-012 for the `ProcessPoolExecutor` API.

### Current State

- `synthetize()` (operation.py line 2872) iterates `synthesis_with_dependencies` sequentially:
  ```python
  for s in synthesis_with_dependencies:
      r = cls._synthetize_single_variable(s, uow, deck_context)
  ```
- `_synthetize_single_variable` (line 2820) calls `_resolve_synthesis` -> `_resolve_spatial_resolution` -> `__resolve_UHE`/`__resolve_SBM`/etc., each of which creates its own `ProcessPoolExecutor`
- `_preprocess_synthesis_variables` (line 2790) calls `_add_synthesis_dependencies` which topologically orders variables so that dependencies (e.g., `VOLUME_RETIRADO` before `VAZAO_RETIRADA`) come first
- Stub variables (those in `SYNTHESIS_DEPENDENCIES` values) are computed from cache and must run after their dependencies
- `CACHED_SYNTHESIS` (class-level dict, line 76) stores results for dependency resolution
- `ScenarioSynthetizer.synthetize()` (scenario.py line 1630) has a similar sequential loop but parallelizes per-iteration (not per-entity), and does not share entity lists across variables -- it is out of scope for this restructuring

## Specification

### Requirements

1. Group the `synthesis_with_dependencies` list by `spatial_resolution` value (SIN, SBM, REE, UHE, SBP, UTE, PEE) in `synthetize()`
2. Within each spatial resolution group, create a single `ProcessPoolExecutor` and reuse it for all variables in the group
3. Preserve the dependency ordering from `_add_synthesis_dependencies`: if variable A depends on variable B, B must complete and be cached before A starts, even if they share a spatial resolution
4. Within a dependency-respecting ordering, variables at the same resolution that have no inter-dependency can share the executor (the entity-level parallelism is the same, but the executor is created once)
5. Do NOT change `ScenarioSynthetizer` -- its parallelism is per-iteration, not per-entity, and grouping by spatial resolution does not apply

### Inputs/Props

- `synthesis_with_dependencies: List[OperationSynthesis]` -- already topologically sorted by `_add_synthesis_dependencies`
- Each `OperationSynthesis` has `.spatial_resolution: SpatialResolution` and `.variable: Variable`

### Outputs/Behavior

- Identical synthesis results: same DataFrames, same cache contents, same exported files
- Fewer `ProcessPoolExecutor` create/destroy cycles (one per spatial resolution group instead of one per variable)
- The entity lists (hydros, submarkets, EERs, etc.) are fetched once per resolution group and reused across variables in the group

### Error Handling

- If any variable in a group fails, the error is logged (existing pattern in `_synthetize_single_variable` line 2856) and the remaining variables in the group continue processing
- If a dependency variable fails and its result is not cached, dependent stub variables will receive an empty DataFrame from `_get_from_cache` and be skipped (existing behavior)

## Acceptance Criteria

- [ ] Given `OperationSynthetizer.synthetize()` called with 3 UHE variables that have no inter-dependencies, when the method completes, then `ProcessPoolExecutor.__init__` is called exactly once for the UHE resolution group (verified via mock)
- [ ] Given `OperationSynthetizer.synthetize()` called with variables `VOLUME_RETIRADO_UHE` and `VAZAO_RETIRADA_UHE` where the latter depends on the former, when the method completes, then `VOLUME_RETIRADO_UHE` is fully cached before `VAZAO_RETIRADA_UHE` begins processing
- [ ] Given the existing test suite, when `python -m pytest tests/ -x` is run, then all tests pass with exit code 0
- [ ] Given `app/services/synthesis/scenario.py`, when the file is inspected, then no changes have been made compared to the ticket-012 baseline (scenario.py is out of scope)
- [ ] Given `app/services/synthesis/operation.py`, when searched for the pattern `ProcessPoolExecutor(` inside any `__resolve_*` method, then zero matches are found (executors are created at the group level, not inside individual resolve methods)

## Implementation Guide

### Suggested Approach

1. **Extract entity list fetching from `__resolve_*` methods**: Currently each `__resolve_SBM`, `__resolve_REE`, `__resolve_UHE`, etc. fetches the entity list (e.g., `Deck.hydros(uow)`) and creates its own pool. Refactor so that:
   - A new method `_get_entities_for_resolution(spatial_resolution, uow)` returns the entity list (indices + names) for a given resolution
   - The `__resolve_*` methods accept an `executor: ProcessPoolExecutor` parameter instead of creating their own

2. **Refactor `__resolve_*` methods to accept an executor**: Change the signature from:

   ```python
   def __resolve_UHE(cls, synthesis, uow, deck_context=None) -> Optional[pd.DataFrame]:
   ```

   to:

   ```python
   def __resolve_UHE(cls, synthesis, uow, deck_context=None, executor=None) -> Optional[pd.DataFrame]:
   ```

   When `executor` is `None` (for backward compatibility in tests or single-variable calls), create a local one. When provided, use it.

3. **Group variables in `synthetize()`**: After `_preprocess_synthesis_variables`, partition the list into dependency-respecting batches:
   - Walk the topologically sorted list. For each variable, if it has no unsatisfied dependencies, add it to the current batch for its resolution.
   - When a variable depends on an uncached predecessor, flush the current batch (process all variables in the batch sharing the same executor), then start a new batch.
   - A simpler approach: process the list sequentially as today, but share a single executor across consecutive variables with the same spatial resolution. Create a new executor only when the resolution changes or a dependency boundary is crossed.

4. **Implement the "shared executor" approach** (recommended simpler path):

   ```python
   current_resolution = None
   current_executor = None
   for s in synthesis_with_dependencies:
       if s.spatial_resolution != current_resolution:
           if current_executor is not None:
               current_executor.shutdown(wait=True)
           current_resolution = s.spatial_resolution
           current_executor = ProcessPoolExecutor(max_workers=n_procs)
       r = cls._synthetize_single_variable(s, uow, deck_context, executor=current_executor)
       ...
   if current_executor is not None:
       current_executor.shutdown(wait=True)
   ```

5. **Thread the executor through the call chain**: `_synthetize_single_variable` -> `_resolve_synthesis` -> `_resolve_spatial_resolution` -> `__resolve_*` all need to accept and forward the optional `executor` parameter.

### Key Files to Modify

- `app/services/synthesis/operation.py` -- `synthetize()`, `_synthetize_single_variable`, `_resolve_spatial_resolution`, all `__resolve_*` methods, `_resolve_GTER_UTE`

### Patterns to Follow

- Follow the existing `Optional[DeckContext] = None` parameter pattern for the new `Optional[ProcessPoolExecutor] = None` parameter
- Follow the existing `RESOLUTION_FUNCTION_MAP` dispatch pattern in `_resolve_spatial_resolution` (line 2267)
- Entity list fetching can stay inside the `__resolve_*` methods for now (extracting it is optional optimization)

### Pitfalls to Avoid

- Do NOT parallelize across variables themselves (e.g., running UHE variables concurrently) -- they share `CACHED_SYNTHESIS` which is a class-level dict and not thread/process-safe. The per-entity parallelism within each variable is the parallelism target.
- Do NOT modify `ScenarioSynthetizer` -- its parallelism model (per-iteration) is different and does not benefit from variable grouping
- Do NOT break the dependency ordering: stub variables (e.g., `VAZAO_RETIRADA`) that read from `CACHED_SYNTHESIS` must run after their dependencies are cached
- The `_resolve_SBM_MER_MERL` nested function uses its own pool -- this nested function can either accept the group executor or keep creating its own (the MER/MERL path is rarely used). Recommend accepting the executor for consistency.
- `__resolve_SIN` does not use a pool at all (it reads a single file) -- it does not need an executor parameter, but should still work in the shared-executor context

## Testing Requirements

### Unit Tests

- Add a test in `tests/app/services/synthesis/test_operation.py` that mocks `ProcessPoolExecutor` and verifies it is instantiated once when `synthetize()` processes 2 variables with the same spatial resolution consecutively
- Run the existing test suite to verify no regressions

### Integration Tests

- Not applicable at unit test level -- behavioral equivalence is verified by existing tests

### E2E Tests (if applicable)

- Not applicable

## Dependencies

- **Blocked By**: ticket-012-replace-multiprocessing-with-futures.md
- **Blocks**: None

## Effort Estimate

**Points**: 4
**Confidence**: Medium
