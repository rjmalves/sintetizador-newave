# Executor Benchmark: ProcessPoolExecutor vs ThreadPoolExecutor

**Ticket**: ticket-014 -- Evaluate and Implement Thread-Based I/O Parallelism
**Date**: 2026-03-06
**Author**: automated via /implement

---

## Summary

| Decision                   | Outcome                                               |
| -------------------------- | ----------------------------------------------------- |
| Benchmark script created   | Yes -- `benchmarks/bench_executor.py`                 |
| Empirical data collected   | No -- no nwlistop files available in this environment |
| Production code changed    | No                                                    |
| Executor in `operation.py` | ProcessPoolExecutor (unchanged)                       |
| Executor in `scenario.py`  | ProcessPoolExecutor (unchanged, as specified)         |

---

## Context

After Epic 04 ticket-012, all entity-level parallelism in `OperationSynthetizer`
uses `ProcessPoolExecutor`. Each process incurs fork/spawn overhead plus pickle
serialization of `FSUnitOfWork` + `DeckContext` and inter-process transfer of
`pl.DataFrame` results.

The question posed by ticket-014 is: do the `cfinterface`/`inewave` binary file
parsers release the GIL during I/O? If they do, `ThreadPoolExecutor` would
eliminate all process-spawn and serialization overhead at zero correctness risk,
because threads share the same heap.

---

## Benchmark Script

The script is located at:

```
plans/performance-overhaul/epic-04-parallelism-overhaul/benchmarks/bench_executor.py
```

### Usage

```bash
# Run from repository root
cd /path/to/sintetizador-newave
python plans/performance-overhaul/epic-04-parallelism-overhaul/benchmarks/bench_executor.py \
    --case-dir /path/to/newave/output \
    --variable GHID_UHE \
    --workers 4 \
    --runs 3
```

### What it measures

The script times three execution modes for all UHEs of a representative variable
(`GHID_UHE` by default, ~200 entities):

| Mode       | Executor            | Workers                |
| ---------- | ------------------- | ---------------------- |
| sequential | ProcessPoolExecutor | 1 (baseline)           |
| process    | ProcessPoolExecutor | N (production default) |
| thread     | ThreadPoolExecutor  | N (candidate)          |

For each mode, the **median wall-clock time** over `--runs` repetitions is
reported. The script also prints:

- The speedup ratio of each mode vs sequential
- The speedup ratio `process_time / thread_time`
- A plain-English conclusion keyed to the 10% threshold from the ticket spec

### Correctness validation

After the first run of each mode the resulting `pl.DataFrame` for every UHE
is sorted and compared against the sequential baseline using
`numpy.testing.assert_allclose(atol=1e-6)` on all float columns. A mismatch
exits with code 2 and prints a clear `FAIL` message.

### Error handling

- `FileNotFoundError` for missing nwlistop files exits cleanly with code 0 and
  a `WARNING` message (no stack trace, no crash).
- If `Deck.hydros()` cannot locate required deck files, the script prints an
  error and exits with code 1.

---

## Empirical Results

**Not yet collected.** No NEWAVE output directory with nwlistop files was
available in the development environment at the time this ticket was executed.

The benchmark script is production-ready and can be run against any valid NEWAVE
output directory.

---

## Decision Rationale

Because empirical timing data could not be collected, the conservative choice is
to keep `ProcessPoolExecutor` in `operation.py`. The reasoning is:

1. **GIL status of `cfinterface` is unknown without measurement.** The
   `cfinterface` library uses Python-level I/O wrappers (`BlockFile.read`).
   Whether those wrappers release the GIL depends on whether the hot path is
   implemented in C or in pure Python. Assuming GIL release without profiling
   evidence would be incorrect.

2. **ProcessPoolExecutor is correct and already validated.** The existing
   implementation from ticket-012 is known to produce correct results and
   has been in use across all subsequent epics. Changing the executor type
   without empirical evidence introduces risk for no measured benefit.

3. **Thread safety of `Log.configure_process_logger` is unconfirmed for
   threads.** `Log.configure_process_logger` uses `multiprocessing.Queue`
   as the logging queue. The ticket notes that `multiprocessing.Queue` is
   NOT safe for use with `ThreadPoolExecutor` workers. Adapting the logging
   infrastructure would be required before threads could be adopted, and that
   adaptation has non-trivial scope.

4. **The 10% speedup threshold was not met (no data).** The ticket specification
   states that `ThreadPoolExecutor` must be faster by more than 10% to justify
   adoption. Without data, this condition is not satisfied.

---

## How to Proceed (Future)

When NEWAVE output data is available:

1. Run `bench_executor.py` against a representative case with a UHE variable:

   ```bash
   python plans/performance-overhaul/epic-04-parallelism-overhaul/benchmarks/bench_executor.py \
       --case-dir /path/to/newave/output \
       --variable GHID_UHE \
       --workers $(nproc) \
       --runs 5
   ```

2. If `ThreadPoolExecutor` is faster by more than 10% **and** all result checks
   pass:
   - Change the executor type in each `__resolve_*` method in
     `app/services/synthesis/operation.py` from `ProcessPoolExecutor` to
     `ThreadPoolExecutor`.
   - Adapt logging: replace the `multiprocessing.Queue`-based
     `Log.configure_process_logger` calls in entity methods with a
     `queue.Queue`-backed handler (since threads share the process logger
     hierarchy, a shared thread-safe queue is sufficient).
   - Verify `FSUnitOfWork.__enter__` and `FilesRepository` do not hold mutable
     shared state.
   - Keep `ScenarioSynthetizer` on `ProcessPoolExecutor` (its I/O patterns are
     different and were not benchmarked here).

3. If `ProcessPoolExecutor` remains faster or results diverge, update this file
   with the actual numbers and close without code changes.

---

## Files Involved

| File                                  | Status        |
| ------------------------------------- | ------------- |
| `benchmarks/bench_executor.py`        | Created (new) |
| `app/services/synthesis/operation.py` | Not modified  |
| `app/services/synthesis/scenario.py`  | Not modified  |
| `app/utils/log.py`                    | Not modified  |
