# ticket-012 Replace multiprocessing.Pool with concurrent.futures

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Replace all uses of `multiprocessing.Pool` with `apply_async` in the synthesis modules (`app/services/synthesis/operation.py`, `app/services/synthesis/scenario.py`) with `concurrent.futures.ProcessPoolExecutor` using `submit()`. This modernizes the parallelism API, provides better exception propagation, context manager support, and prepares for the variable-group parallelism in ticket-013.

## Anticipated Scope

- **Files likely to be modified**: `app/services/synthesis/operation.py` (all `__resolve_*` methods that create `Pool`), `app/services/synthesis/scenario.py` (equivalent methods)
- **Key decisions needed**: Whether to use a single `ProcessPoolExecutor` for the entire synthesis run (created in `synthetize()`) or one per `__resolve_*` call. Whether the executor should be configurable for testing (e.g., `ThreadPoolExecutor` in tests for determinism).
- **Open questions**: Does the DeckContext (from ticket-002) serialize efficiently enough for ProcessPoolExecutor? Are there Polars-specific serialization concerns for DataFrames sent to/from subprocesses?

## Dependencies

- **Blocked By**: ticket-011-migrate-parquet-export-polars.md
- **Blocks**: ticket-013-variable-group-parallelism.md, ticket-014-evaluate-thread-io-parallelism.md

## Effort Estimate

**Points**: 3
**Confidence**: Low (will be re-estimated during refinement)
