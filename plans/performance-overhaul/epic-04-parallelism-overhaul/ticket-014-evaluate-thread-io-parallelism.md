# ticket-014 Evaluate and Implement Thread-Based I/O Parallelism

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Evaluate whether replacing `ProcessPoolExecutor` with `ThreadPoolExecutor` for the file reading portion of entity resolution improves performance. The nwlistop file reading via `BlockFile.read(path)` is I/O-bound at the OS level and the C-level parsing in cfinterface may release the GIL. If so, threads avoid the subprocess spawn and serialization overhead entirely. Implement the faster option based on benchmarking both approaches on a representative case.

## Anticipated Scope

- **Files likely to be modified**: `app/services/synthesis/operation.py` (executor selection in `__resolve_*` methods), possibly a new configuration option for executor type
- **Key decisions needed**: Whether `BlockFile.read()` releases the GIL during file parsing (depends on cfinterface implementation -- needs profiling). Whether to use a hybrid approach (ThreadPoolExecutor for I/O, then Polars for CPU-bound post-processing which is already multi-threaded). Whether the executor type should be configurable via CLI option or environment variable.
- **Open questions**: Does cfinterface/inewave release the GIL during file reads? What is the actual I/O vs CPU split in entity resolution? Is there contention on the OS file cache when reading thousands of files in parallel? What is the optimal thread count for I/O-bound file reading?

## Dependencies

- **Blocked By**: ticket-012-replace-multiprocessing-with-futures.md
- **Blocks**: None

## Effort Estimate

**Points**: 3
**Confidence**: Low (will be re-estimated during refinement)
