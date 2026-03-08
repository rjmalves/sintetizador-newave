# ticket-014 Evaluate and Implement Thread-Based I/O Parallelism

## Context

### Background

Entity resolution in `OperationSynthetizer` consists of two phases: (1) file I/O via `uow.files.get_nwlistop()` which reads binary nwlistop files using `cfinterface`/`inewave` C-extension parsers, and (2) post-processing via `_post_resolve_entity()` which applies temporal resolution, entity columns, and starting stage adjustments using Polars. After Epics 02-03, the post-processing phase is Polars-native and inherently multi-threaded within a single process.

`ProcessPoolExecutor` (introduced in ticket-012) incurs per-process overhead: fork/spawn, pickle serialization of `uow` + `DeckContext`, and inter-process result transfer of `pl.DataFrame` objects. If the file reading phase releases the GIL (which C-extension I/O typically does), `ThreadPoolExecutor` would eliminate all of this overhead since threads share the process memory space.

This ticket evaluates the GIL behavior of `cfinterface`/`inewave` file reads and, if threads are viable, implements `ThreadPoolExecutor` as the default executor for operation synthesis entity resolution.

### Relation to Epic

This is an optimization investigation ticket that runs in parallel with ticket-013 (both depend only on ticket-012). If threads prove viable, the executor type can be changed independently of the variable-grouping restructuring. If threads are not viable (GIL contention), this ticket documents the findings and closes with no code changes.

### Current State

- After ticket-012, all entity resolution uses `ProcessPoolExecutor` with `executor.submit()`
- Entity resolver methods (`_resolve_SBM_entity`, `_resolve_UHE_entity`, etc.) call:
  1. `Log.configure_process_logger(uow.queue, ...)` -- uses `multiprocessing.Queue` (not thread-safe for `QueueHandler` without adaptation)
  2. `with uow:` context manager (calls `FSUnitOfWork.__enter__` which opens a `FilesRepository`)
  3. `uow.files.get_nwlistop(...)` -- the hot I/O path, calls inewave's `BlockFile.read(path)` under the hood
  4. `cls._post_resolve_entity(df, ...)` -- Polars post-processing
- `uow.queue` is a `multiprocessing.Queue` used for subprocess logging; threads would need `queue.Queue` or a `logging.handlers.QueueHandler` with a thread-safe queue
- `FSUnitOfWork` stores absolute paths (Epic 01 fix) and is stateless beyond path configuration -- thread-safe for concurrent reads
- `Log.configure_process_logger` creates a new logger per worker -- thread-safe as long as the queue is thread-safe

## Specification

### Requirements

1. Write a benchmark script that times entity resolution for a representative UHE variable (e.g., `GHID_UHE` with ~200 entities) using:
   - `ProcessPoolExecutor` with `max_workers=N` (baseline from ticket-012)
   - `ThreadPoolExecutor` with `max_workers=N`
   - Sequential execution (no parallelism, `max_workers=1`)
     where `N` matches the default `PROCESSADORES` setting
2. If `ThreadPoolExecutor` is slower than or equal to `ProcessPoolExecutor` (indicating GIL contention in `cfinterface` reads), document the findings in a `BENCHMARKS.md` file in the epic directory and close with no production code changes
3. If `ThreadPoolExecutor` is faster:
   a. Replace `ProcessPoolExecutor` with `ThreadPoolExecutor` in the `__resolve_*` methods of `operation.py` for entity resolution
   b. Adapt the logging setup: replace `multiprocessing.Queue`-based logging with `queue.Queue` or thread-safe `logging` (since threads share the process logger hierarchy, `Log.configure_process_logger` may need adjustment)
   c. Verify that `FSUnitOfWork` is safe for concurrent thread access (no shared mutable state)
4. Keep `ScenarioSynthetizer` on `ProcessPoolExecutor` -- its per-iteration parallelism reads different binary files (vazaof.dat, energiab.dat) and the inewave parsers for those may have different GIL characteristics

### Inputs/Props

- Benchmark input: a NEWAVE output directory with nwlistop files for at least one UHE variable
- Configuration: `PROCESSADORES` env var for worker count

### Outputs/Behavior

- If threads are adopted: identical DataFrames with reduced wall-clock time due to no spawn overhead
- If threads are not adopted: `BENCHMARKS.md` documenting the comparison and rationale

### Error Handling

- Benchmark script handles `FileNotFoundError` for missing nwlistop files gracefully (skip benchmark, log warning)
- If thread-based execution produces different results (data race), this is a signal that threads are not safe -- fall back to `ProcessPoolExecutor`

## Acceptance Criteria

- [ ] Given the benchmark script `plans/performance-overhaul/epic-04-parallelism-overhaul/benchmarks/bench_executor.py`, when run with `python bench_executor.py --case-dir /path/to/newave/output --variable GHID_UHE`, then it prints timing results for Process, Thread, and Sequential executor modes
- [ ] Given the benchmark results, when `ThreadPoolExecutor` is faster than `ProcessPoolExecutor` by more than 10%, then `app/services/synthesis/operation.py` uses `ThreadPoolExecutor` for all `__resolve_*` entity resolution methods
- [ ] Given the benchmark results, when `ThreadPoolExecutor` is NOT faster than `ProcessPoolExecutor` by more than 10%, then `app/services/synthesis/operation.py` remains unchanged and `plans/performance-overhaul/epic-04-parallelism-overhaul/BENCHMARKS.md` documents the findings
- [ ] Given the existing test suite, when `python -m pytest tests/ -x` is run, then all tests pass with exit code 0
- [ ] Given the file `app/services/synthesis/scenario.py`, when inspected, then it still uses `ProcessPoolExecutor` regardless of the benchmark outcome

## Implementation Guide

### Suggested Approach

1. **Create the benchmark script** at `plans/performance-overhaul/epic-04-parallelism-overhaul/benchmarks/bench_executor.py`:
   - Accept `--case-dir` (path to NEWAVE output) and `--variable` (default `GHID_UHE`) and `--workers` (default from `PROCESSADORES`)
   - Set up a minimal `FSUnitOfWork` pointing at the case directory
   - Time the full `__resolve_UHE` equivalent using `ProcessPoolExecutor`, `ThreadPoolExecutor`, and sequential modes
   - Print median wall-clock time over 3 runs for each mode
   - Print the speedup ratio `process_time / thread_time`

2. **Test thread safety** in the benchmark:
   - After each run, compare the resulting DataFrame (sorted) to the sequential baseline using `pd.testing.assert_frame_equal`
   - Any mismatch indicates a data race

3. **If threads win** (>10% faster with correct results):
   - In `operation.py`, change the executor type in each `__resolve_*` method from `ProcessPoolExecutor` to `ThreadPoolExecutor`
   - Add `from concurrent.futures import ThreadPoolExecutor` (may coexist with `ProcessPoolExecutor` if scenario.py still needs it)
   - Adapt `Log.configure_process_logger` calls in entity methods: since threads share the process's logger hierarchy, either (a) use `logging.getLogger(f"worker-{name}-{idx}")` directly without a queue handler, or (b) replace `uow.queue` (multiprocessing.Queue) with a `queue.Queue` for thread-safe operation
   - Verify `FSUnitOfWork.__enter__` and `FilesRepository` do not hold mutable state that would cause races

4. **If processes win** (threads <= 10% faster or produce incorrect results):
   - Write `BENCHMARKS.md` with the results table and analysis
   - No production code changes

### Key Files to Modify

- `plans/performance-overhaul/epic-04-parallelism-overhaul/benchmarks/bench_executor.py` (new file)
- `app/services/synthesis/operation.py` (conditional, only if threads win)
- `app/utils/log.py` (conditional, only if threads win and logging adaptation is needed)
- `plans/performance-overhaul/epic-04-parallelism-overhaul/BENCHMARKS.md` (conditional, only if processes win)

### Patterns to Follow

- Follow the existing `time_and_log` pattern for timing measurements
- Follow the Polars/pandas parity test pattern from Epic 03: compare with `np.testing.assert_allclose(atol=1e-6)` for float columns
- If modifying logging, follow the existing `Log` singleton pattern in `app/utils/log.py`

### Pitfalls to Avoid

- Do NOT assume `cfinterface` releases the GIL -- this MUST be measured empirically via the benchmark
- Do NOT use `ThreadPoolExecutor` for `ScenarioSynthetizer` -- its `_resolve_forward_energy_iteration` and similar methods have different I/O patterns (reading large binary files like `vazaof.dat`) with unknown GIL behavior
- Do NOT remove `ProcessPoolExecutor` from imports -- even if threads win for operation synthesis, scenario synthesis still needs it
- `multiprocessing.Queue` is NOT safe for use with `ThreadPoolExecutor` workers -- if threads are adopted, the logging queue must be adapted
- Do NOT modify entity resolver method signatures -- if threads win, only the executor type changes, not the function interfaces

## Testing Requirements

### Unit Tests

- Run the existing test suite to verify no regressions
- The benchmark script itself is the primary validation tool for this ticket

### Integration Tests

- The benchmark script serves as a targeted integration test, running actual file I/O against nwlistop files

### E2E Tests (if applicable)

- Not applicable

## Dependencies

- **Blocked By**: ticket-012-replace-multiprocessing-with-futures.md
- **Blocks**: None

## Effort Estimate

**Points**: 3
**Confidence**: Medium
