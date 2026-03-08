# ticket-012 Replace multiprocessing.Pool with concurrent.futures

## Context

### Background

The codebase currently uses `multiprocessing.Pool` with `apply_async()` for all entity-level parallelism across both `OperationSynthetizer` and `ScenarioSynthetizer`. This pattern has several drawbacks: no context manager protocol for the pool lifecycle, poor exception propagation (exceptions are only raised on `.get()`), and no ability to swap executor implementations (e.g., thread-based for I/O workloads). The `concurrent.futures` module from the standard library addresses all of these issues with `ProcessPoolExecutor`, `submit()`, and `Future` objects.

With DeckContext pre-computed in Epic 01 and Polars handling multi-threaded operations internally in Epics 02-03, the subprocess payload is already minimal (entity index/name + UoW + DeckContext). This ticket modernizes the pool management API without changing the parallelism granularity (per-entity within each variable), which will be restructured in ticket-013.

### Relation to Epic

This is the foundational ticket for Epic 04 (Parallelism Overhaul). It replaces the parallelism primitive used by all subsequent tickets. Ticket-013 (variable-group parallelism) and ticket-014 (thread-based I/O evaluation) both depend on the `ProcessPoolExecutor` API introduced here.

### Current State

- `operation.py` imports `from multiprocessing import Pool` (line 4) and uses `Pool(processes=n_procs)` with `pool.apply_async()` in 7 locations:
  - `__resolve_SBM` (line 638)
  - `__resolve_SBP` (line 722)
  - `__resolve_REE` (line 810)
  - `__resolve_UHE` (line 1007)
  - `_resolve_GTER_UTE` (line 2154)
  - `_resolve_SBM_MER_MERL` (nested function, line 1785)
  - One additional SBM pool in the MER/MERL nested function
- `scenario.py` imports `from multiprocessing import Pool` (line 4) and uses it in 4 locations:
  - `_resolve_forward_energy` (line 1015)
  - `_resolve_forward_inflow` (line 1061)
  - `_resolve_backward_energy` (line 1118)
  - `_resolve_backward_inflow` (line 1165)
- All Pool usages follow the identical pattern:
  ```python
  with Pool(processes=n_procs) as pool:
      async_res = {key: pool.apply_async(fn, args) for ...}
      dfs = {k: r.get(timeout=3600) for k, r in async_res.items()}
  ```
- `n_procs` is always read from `int(Settings().processors)` at the call site
- `Settings().processors` reads from the `PROCESSADORES` env var, defaulting to 1

## Specification

### Requirements

1. Replace every `multiprocessing.Pool` + `apply_async` usage in `operation.py` and `scenario.py` with `concurrent.futures.ProcessPoolExecutor` + `submit()`
2. Replace `from multiprocessing import Pool` with `from concurrent.futures import ProcessPoolExecutor, as_completed` in both files
3. Maintain the same parallelism granularity: one subprocess per entity (SBM, REE, UHE, etc.) or per iteration (scenario forward/backward)
4. Maintain the same timeout behavior (3600s per future)
5. Propagate exceptions immediately via `Future.result(timeout=3600)` instead of silently deferring them to `.get()`
6. Keep `n_procs` sourced from `Settings().processors` at each call site (do not introduce a shared executor yet -- that is ticket-013's scope)

### Inputs/Props

- No new inputs. The same arguments are passed to the same entity resolver functions.
- `DeckContext` serialization over pickle is unchanged (it is already a simple dataclass with pandas DataFrames and primitive types, proven to work in Epics 01-03).

### Outputs/Behavior

- Identical DataFrames produced by each `__resolve_*` and `_resolve_*` method
- Identical logging behavior via `Log.configure_process_logger()` in subprocesses
- Any exception in a subprocess is raised when `future.result()` is called, instead of being silently stored until `async_result.get()`

### Error Handling

- If a subprocess raises an exception, `future.result(timeout=3600)` re-raises it in the main process. The existing `try/except` in `_synthetize_single_variable` (operation.py line 2836) and `_synthetize_single_variable` (scenario.py line 1606) already catch and log `Exception`, so no additional error handling is needed.
- `concurrent.futures.TimeoutError` replaces `multiprocessing.TimeoutError` for the 3600s timeout. Both are subclasses of `Exception`, so the existing handlers catch them.

## Acceptance Criteria

- [ ] Given the file `app/services/synthesis/operation.py`, when searched for `from multiprocessing import Pool`, then zero matches are found
- [ ] Given the file `app/services/synthesis/scenario.py`, when searched for `from multiprocessing import Pool`, then zero matches are found
- [ ] Given the file `app/services/synthesis/operation.py`, when searched for `Pool(`, then zero matches are found
- [ ] Given the file `app/services/synthesis/scenario.py`, when searched for `Pool(`, then zero matches are found
- [ ] Given the file `app/services/synthesis/operation.py`, when searched for `from concurrent.futures import`, then exactly one match is found at the imports section
- [ ] Given the file `app/services/synthesis/scenario.py`, when searched for `from concurrent.futures import`, then exactly one match is found at the imports section
- [ ] Given the existing test suite, when `python -m pytest tests/ -x` is run, then all tests pass with exit code 0
- [ ] Given a single-processor configuration (`PROCESSADORES=1`), when `OperationSynthetizer.synthetize()` is called with a UHE variable, then the result is identical to the pre-change baseline (the executor still works with `max_workers=1`)

## Implementation Guide

### Suggested Approach

1. In `operation.py`, replace the import:

   ```python
   # Before
   from multiprocessing import Pool
   # After
   from concurrent.futures import ProcessPoolExecutor
   ```

2. For each of the 7 Pool usage sites in `operation.py`, apply this mechanical transformation:

   ```python
   # Before
   with Pool(processes=n_procs) as pool:
       async_res = {key: pool.apply_async(fn, args) for ...}
       dfs = {k: r.get(timeout=3600) for k, r in async_res.items()}

   # After
   with ProcessPoolExecutor(max_workers=n_procs) as executor:
       futures = {key: executor.submit(fn, *args) for ...}
       dfs = {k: f.result(timeout=3600) for k, f in futures.items()}
   ```

   Note: `apply_async(fn, (a, b, c))` becomes `executor.submit(fn, a, b, c)` -- the args tuple is unpacked with `*`.

3. Apply the same transformation to the 4 Pool usage sites in `scenario.py`.

4. Verify that the `_resolve_SBM_MER_MERL` nested function inside `__resolve_UTE` (operation.py around line 1785) is also converted -- it defines its own local Pool.

5. Remove `from multiprocessing import Pool` from both files. Note: `multiprocessing` is still used indirectly via `Log.configure_process_logger` which takes a `multiprocessing.Queue` -- do NOT remove any `multiprocessing` imports that are used elsewhere (check for `from multiprocessing.queues import Queue` usage via `uow.queue`).

### Key Files to Modify

- `app/services/synthesis/operation.py` -- 7 Pool sites + import change
- `app/services/synthesis/scenario.py` -- 4 Pool sites + import change

### Patterns to Follow

- Follow the existing pattern of creating a new executor per `__resolve_*` call (do not share executors across calls -- that is ticket-013's scope)
- Maintain the `with` context manager pattern for executor lifecycle
- Keep the dict comprehension pattern for submitting futures (preserves the key-to-result mapping)

### Pitfalls to Avoid

- Do NOT remove imports of `multiprocessing` that are used for things other than `Pool` (e.g., `uow.queue` is a `multiprocessing.Queue`)
- Do NOT change the function signatures of entity resolver methods (`_resolve_SBM_entity`, `_resolve_UHE_entity`, etc.) -- they must remain pickle-serializable top-level or classmethod callables
- Do NOT introduce `as_completed()` iteration in this ticket -- the current pattern collects all results in a dict comprehension, and changing iteration order is out of scope
- Do NOT change `Settings().processors` to an executor-level parameter -- ticket-013 handles executor lifecycle
- The `_resolve_SBM_entity_MER_MERL` method (used by the nested `_resolve_SBM_MER_MERL` function) does not receive `deck_context` -- do not add it

## Testing Requirements

### Unit Tests

- Run the existing test suite: `python -m pytest tests/app/services/synthesis/test_operation.py tests/app/services/synthesis/test_scenario.py -x`
- No new unit tests are required for this mechanical refactoring; the existing tests cover the entity resolution methods that are being called through the new executor

### Integration Tests

- Not applicable -- this is a drop-in API replacement with identical behavior

### E2E Tests (if applicable)

- Not applicable

## Dependencies

- **Blocked By**: ticket-011-migrate-parquet-export-polars.md
- **Blocks**: ticket-013-variable-group-parallelism.md, ticket-014-evaluate-thread-io-parallelism.md

## Effort Estimate

**Points**: 2
**Confidence**: High
