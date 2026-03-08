# Epic 04 Learnings — Parallelism Overhaul

**Epic**: epic-04-parallelism-overhaul
**Tickets**: ticket-012, ticket-013, ticket-014
**Date**: 2026-03-06

---

## Patterns Established

- **Shared-executor pattern for variable groups**: `synthetize()` in `app/services/synthesis/operation.py` now allocates one `ProcessPoolExecutor` per spatial-resolution group rather than one per variable. A `current_resolution` sentinel and `current_executor` variable track the active group; the executor is shut down and replaced only when the resolution changes. This is the canonical pattern to follow for any future loop that dispatches multiple same-resolution variables.

- **Executor threading through the call chain**: The `executor: Optional[ProcessPoolExecutor] = None` parameter is threaded through five method levels: `synthetize()` -> `_synthetize_single_variable()` -> `_resolve_synthesis()` -> `_resolve_spatial_resolution()` -> each `__resolve_*()` method. The `Optional[X] = None` parameter pattern (established for `DeckContext` in Epic 01) was reused exactly here, making the threading mechanical and consistent.

- **`assert executor is not None` at the pool call site**: Rather than a silent fallback that creates a local executor, the `__resolve_*` methods assert that an executor was provided. This converts a latent misuse (calling a resolve method outside `synthetize()`) into an immediate, readable error. The assertion message names the method and the correct entry point.

- **SIN special-casing at the dispatch layer**: `_resolve_spatial_resolution()` contains an explicit `if spatial_resolution == SpatialResolution.SISTEMA_INTERLIGADO` guard to skip passing the executor to `__resolve_SIN`, since that method reads a single file with no worker pool. This guard is visible at the dispatch layer rather than buried inside `__resolve_SIN`, making the asymmetry discoverable (`app/services/synthesis/operation.py` around line 2256).

- **`_resolve_SBM_MER_MERL` keeps its own local executor**: The nested function `_resolve_SBM_MER_MERL` (used only for the MER/MERL path, which is uncommon) continues to create its own `ProcessPoolExecutor(max_workers=n_procs)` locally rather than receiving the group executor. This was a deliberate choice to avoid complicating the nested function's already unusual call path.

- **Benchmark-first investigation tickets**: ticket-014 structured the thread-vs-process decision as an empirical investigation with a prescribed decision rule (>10% speedup required). The benchmark script at `plans/performance-overhaul/epic-04-parallelism-overhaul/benchmarks/bench_executor.py` is self-contained, accepts `--case-dir`, `--variable`, `--workers`, and `--runs` options, and includes correctness validation via `numpy.testing.assert_allclose(atol=1e-6)` on float columns.

---

## Architectural Decisions

- **Decision: Executor allocated at resolution-group granularity, not at variable granularity or globally.** Rejected: (a) one global executor for the entire `synthetize()` call — rejected because different resolutions have different entity counts and the executor size is resolution-dependent; (b) one executor per variable (prior state) — rejected as the motivating inefficiency of this epic. Rationale: resolution-group granularity captures all consecutive same-resolution variables in a single pool lifetime while still cleanly resetting when resolution changes.

- **Decision: `__resolve_*` methods receive `executor` as an injected parameter rather than reading from a class-level attribute.** Rejected: storing `current_executor` as a class variable on `OperationSynthetizer`. Rationale: `OperationSynthetizer` uses `@classmethod` throughout; a class-level mutable executor reference would be unsafe under concurrent calls and harder to test. Injection via parameter keeps the methods stateless.

- **Decision: Keep `ProcessPoolExecutor` for both `operation.py` and `scenario.py`.** `ThreadPoolExecutor` was evaluated (ticket-014) but not adopted because: (1) GIL behavior of `cfinterface` parsers was not measurable without NEWAVE output data; (2) `Log.configure_process_logger` uses `multiprocessing.Queue`, which is not safe for thread workers; (3) no empirical evidence met the 10% speedup threshold. The decision is documented in `plans/performance-overhaul/epic-04-parallelism-overhaul/BENCHMARKS.md` with a step-by-step guide for reopening when data is available.

- **Decision: `scenario.py` parallelism model left unchanged.** `ScenarioSynthetizer` parallelizes per-iteration (not per-entity), does not share entity lists across variables, and its I/O patterns differ from `operation.py`. The ticket scope explicitly excluded it from variable-group restructuring to avoid cross-cutting risk.

---

## Files & Structures Created

- `plans/performance-overhaul/epic-04-parallelism-overhaul/benchmarks/bench_executor.py` — Self-contained CLI benchmark using `typer` + `rich`. Times sequential / process / thread executor modes for a UHE variable over N runs, prints a median-time table, and validates result correctness against the sequential baseline.

- `plans/performance-overhaul/epic-04-parallelism-overhaul/BENCHMARKS.md` — Decision record for the thread-vs-process evaluation. Documents the outcome (no empirical data, `ProcessPoolExecutor` retained), the three reasons for the conservative choice, and a copy-paste procedure to re-run the evaluation when NEWAVE data is available.

---

## Conventions Adopted

- **`from concurrent.futures import ProcessPoolExecutor`** replaces `from multiprocessing import Pool` in both `app/services/synthesis/operation.py` and `app/services/synthesis/scenario.py`. No other `multiprocessing` imports were removed; `uow.queue` is a `multiprocessing.Queue` used for subprocess logging and is independent of the pool API.

- **`executor.submit(fn, arg1, arg2, ...)` unpacks args** (not `executor.submit(fn, (arg1, arg2, ...))`). The old `pool.apply_async(fn, (args_tuple,))` pattern wrapped arguments in a tuple; `concurrent.futures.submit` takes positional arguments directly. All 11 call sites across the two files follow this form.

- **`future.result(timeout=3600)` replaces `async_result.get(timeout=3600)`**. Exception semantics are now consistent: any subprocess exception is re-raised immediately at `.result()` rather than being stored silently until `.get()`. The existing `try/except Exception` handlers in `_synthetize_single_variable` continue to catch both `concurrent.futures.TimeoutError` and arbitrary subprocess exceptions.

- **`executor.shutdown(wait=True)` is called explicitly** at each resolution-group boundary in `synthetize()` rather than relying on the `with` context manager. This is because the executor lifetime spans multiple iterations of the loop and cannot be scoped to a single `with` block. A final shutdown is called unconditionally after the loop via `if current_executor is not None: current_executor.shutdown(wait=True)`.

- **Test pattern for executor reuse: `_TrackedExecutor` stub + `patch`**. The test added in `tests/app/services/synthesis/test_operation.py` (function `test_executor_criado_uma_vez_por_grupo_resolucao_uhe`) patches `app.services.synthesis.operation.ProcessPoolExecutor` with a minimal class that records constructor calls. `_synthetize_single_variable` is simultaneously patched to a no-op so that no real I/O occurs. This pattern can be reused for any future test that counts executor lifecycles.

---

## Surprises & Deviations

- **ticket-012 skipped per-method `n_procs` reads.** The original spec said each `__resolve_*` method should keep reading `int(Settings().processors)` locally. In practice, the implementation moved the single `n_procs = int(Settings().processors)` call up to `synthetize()` (where the group executor is created) and removed the redundant per-method reads. This is a sensible consolidation: the processor count is now read once per `synthetize()` call rather than once per variable, and the value is consistent across all variables in a run. The resulting code is in `app/services/synthesis/operation.py` around line 2887.

- **ticket-013 used `assert` instead of `Optional` fallback.** The ticket's implementation guide suggested `executor=None` with a local fallback for backward compatibility. The actual implementation uses `assert executor is not None` in each `__resolve_*` method, removing the fallback entirely. The rationale is that no production call path reaches these methods without `synthetize()` providing an executor. Tests that previously called `__resolve_*` directly were updated accordingly. The assert messages name the correct entry point.

- **`__resolve_PEE` received the executor signature addition.** The `__resolve_PEE` method (wind power plants, not yet implemented — raises `NotImplementedError`) was also updated to accept `executor: Optional[ProcessPoolExecutor] = None` for signature consistency with all other `__resolve_*` methods. This was not explicitly mentioned in the ticket but is correct. A large commented-out block inside `__resolve_PEE` was also removed, cleaning up dead code (`app/services/synthesis/operation.py`).

- **ticket-014 benchmark was not run empirically.** No NEWAVE output directory was available in the development environment, so the benchmark script was created and validated structurally but not executed against real data. The `BENCHMARKS.md` decision record explicitly documents this gap and provides the commands needed to complete the evaluation when data becomes available. The production executor choice (`ProcessPoolExecutor`) was unchanged.

- **`scenario.py` also lost the `TypeVar T` and a commented-out filter.** In addition to the mechanical `Pool` -> `ProcessPoolExecutor` replacement, the diff shows removal of `T = TypeVar("T")` (an unused class variable) and a commented-out line `# if "_BKW" not in a` in `_default_args()`. These are unrelated housekeeping removals that were bundled with ticket-012. This scope creep was minimal and benign.

---

## Recommendations for Future Epics

- **When epic-05 refactors `operation.py`, preserve the `current_executor` pattern.** The executor allocation in `synthetize()` is tightly coupled to the `synthesis_with_dependencies` loop and the `SpatialResolution` enum. If `synthetize()` is decomposed into smaller methods, the executor must still span the full resolution group — do not accidentally push executor creation back into individual `__resolve_*` methods.

- **Thread re-evaluation is deferred, not cancelled.** When NEWAVE output data becomes available, run `plans/performance-overhaul/epic-04-parallelism-overhaul/benchmarks/bench_executor.py` per the instructions in `BENCHMARKS.md`. If threads win, the logging adaptation (`multiprocessing.Queue` -> `queue.Queue` in `Log.configure_process_logger`) will need a dedicated sub-ticket before production adoption.

- **`pd_to_pl_lazy` from `app/utils/dataframe.py` remains unused.** This was flagged in epic-03 learnings and is still unreferenced in production. Epic 05 (code decomposition) should either adopt it in a lazy evaluation chain or remove it.

- **The `_resolve_SBM_MER_MERL` nested function remains an architectural anomaly.** It is defined inside `__resolve_UTE` and creates its own `ProcessPoolExecutor` independently. If epic-05 decomposes `operation.py`, this function should be extracted as a `@classmethod` (or `@staticmethod`) and receive the group executor via injection like all other `__resolve_*` methods.
