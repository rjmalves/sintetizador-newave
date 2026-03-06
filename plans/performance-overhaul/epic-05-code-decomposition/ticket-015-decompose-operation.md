# ticket-015 Decompose operation.py into Resolution Modules

## Context

### Background

`app/services/synthesis/operation.py` is a 2918-line monolith containing the `OperationSynthetizer` class with ~70 methods spanning spatial resolution handlers (SIN, SBM, SBP, REE, UHE, UTE, PEE), stub variable resolvers, cache management, export logic, temporal resolution, entity post-processing, and the main orchestrator. This makes the file difficult to navigate, test in isolation, and maintain. Epic 05 decomposes this and other large files into well-organized modules with clear responsibilities.

### Relation to Epic

This is the first and largest decomposition ticket in Epic 05. It establishes the package decomposition pattern (converting a single-file module into a package with `__init__.py` re-exports) that ticket-016 and ticket-017 will follow. The remaining tickets depend on the module structure created here.

### Current State

The `OperationSynthetizer` class at `app/services/synthesis/operation.py` (2918 lines) contains all methods as `@classmethod` on a single class. The class uses class-level dicts for caching (`CACHED_SYNTHESIS`, `ORDERED_SYNTHESIS_ENTITIES`, `SYNTHESIS_STATS`) and a `RESOLUTION_FUNCTION_MAP` dispatcher at line 2249 that maps `SpatialResolution` enums to `__resolve_*` methods. Methods fall into these logical groups:

1. **Orchestrator** (lines 2782-2918): `synthetize()`, `_synthetize_single_variable()`, `_preprocess_synthesis_variables()`, `enforce_version()`
2. **Resolution handlers** (lines 553-2236): `__resolve_SIN`, `__resolve_SBM`, `__resolve_SBP`, `__resolve_REE`, `__resolve_UHE`, `__resolve_UTE`, `__resolve_PEE` and their entity-level helpers
3. **Stub resolvers** (lines 1097-1900): `__stub_QDEF`, `__stub_VDEF`, `__stub_VEVAP`, `__stub_CTO`, `__stub_EVER`, `__stub_MER_MERL`, `__stub_GUNS`, `__stub_EVMIN`, `__stub_EARM_UHE`, `_stub_resolve_initial_stored_energy`, `__stub_resolve_initial_stored_volumes`, plus variable mapping helpers
4. **Shared pipeline** (lines 237-552): `_post_resolve_entity`, `_post_resolve`, `_resolve_temporal_resolution`, cache access, entity ordering
5. **Infrastructure** (lines 2582-2780): cache get/store, bounds resolution, export (metadata, stats, scenarios)

External importers of `OperationSynthetizer`:

- `app/services/handlers.py` (line 7)
- `tests/app/services/synthesis/test_operation.py` (line 107)
- `tests/app/services/synthesis/test_temporal_resolution.py` (line 33)
- `tests/app/services/synthesis/test_entity_pipeline.py` (line 33)

## Specification

### Requirements

1. Convert `app/services/synthesis/operation.py` into a package `app/services/synthesis/operation/` with an `__init__.py` that re-exports `OperationSynthetizer` so all existing `from app.services.synthesis.operation import OperationSynthetizer` statements continue to work without changes.
2. Split the methods into these modules (each under `app/services/synthesis/operation/`):
   - `orchestrator.py`: The `OperationSynthetizer` class definition with class-level attributes (`logger`, `DEFAULT_OPERATION_SYNTHESIS_ARGS`, `SYNTHESIS_TO_CACHE`, `CACHED_SYNTHESIS`, `ORDERED_SYNTHESIS_ENTITIES`, `SYNTHESIS_STATS`), `clear_cache()`, `synthetize()`, `enforce_version()`, `_synthetize_single_variable()`, `_preprocess_synthesis_variables()`, `_default_args()`, `_match_wildcards()`, `_process_variable_arguments()`, `_filter_valid_variables()`, `_add_synthesis_dependencies()`. Methods that dispatch to resolution modules call free functions imported from the resolution modules.
   - `pipeline.py`: `_post_resolve_entity()`, `_post_resolve()`, `_resolve_temporal_resolution()` (with all nested helpers), `_resolve_starting_stage()`, `_resolve_starting_stage_polars()`, `_initial_stored_energy_df()`, `_get_unique_column_values_in_order()`, `_set_ordered_entities()`, `_get_ordered_entities()`, `_generate_scenarios()`, `_resolve_temporal_resolution_GTER_UTE()`, `_post_resolve_GTER_UTE_entity()`.
   - `resolution_sin.py`: `__resolve_SIN()`.
   - `resolution_sbm.py`: `__resolve_SBM()`, `_resolve_SBM_entity()`.
   - `resolution_sbp.py`: `__resolve_SBP()`, `_resolve_SBP_entity()`.
   - `resolution_ree.py`: `__resolve_REE()`, `_resolve_REE_entity()`.
   - `resolution_uhe.py`: `__resolve_UHE()`, `_resolve_UHE_entity()`, `_calc_block_0_weighted_mean()` (currently nested), `_limit_stages_with_hydro()` (currently nested).
   - `resolution_ute.py`: `__resolve_UTE()`, `_resolve_GTER_UTE()`, `_resolve_GTER_UTE_entity()`.
   - `resolution_pee.py`: `__resolve_PEE()`.
   - `stubs.py`: All `__stub_*` methods, `_stub_mappings()`, `_resolve_stub()`, `_hydro_resolution_variable_map()`, `_flow_volume_hydro_variable_map()`, `_absolute_percent_volume_variable_map()`, `_stub_resolve_initial_stored_energy()`, `__stub_resolve_initial_stored_volumes()`, `_calc_accumulated_productivity()`, `_convert_volume_to_flow()`, `_convert_flow_to_volume()`.
   - `cache.py`: `_get_from_cache()`, `__get_from_cache_if_exists()`, `__store_in_cache_if_needed()`.
   - `export.py`: `_export_metadata()`, `_add_synthesis_stats()`, `_export_scenario_synthesis()`, `_export_stats()`.
   - `bounds.py`: `_resolve_bounds()`.
   - `spatial.py`: `_resolve_spatial_resolution()`, `_resolve_synthesis()` (the two dispatcher methods).

3. Extracted functions become module-level free functions that receive `cls` (the `OperationSynthetizer` class) as the first parameter, preserving the existing calling convention. Alternatively, they can remain as `@classmethod` on a mixin class that `OperationSynthetizer` inherits from -- choose free functions for simplicity.
4. Extract `_resolve_SBM_MER_MERL` (currently a nested function inside `__stub_MER_MERL` at line 1785 that creates its own local executor) as a top-level function in `stubs.py`. This addresses the architectural anomaly noted in epic-04 learnings.
5. No file in the resulting package exceeds 500 lines.
6. All existing tests pass without modification to test code (only import paths may change in `__init__.py` re-exports).

### Inputs/Props

- The single file `app/services/synthesis/operation.py` (2918 lines).

### Outputs/Behavior

- A package directory `app/services/synthesis/operation/` with 14 Python files (13 modules + `__init__.py`).
- Identical runtime behavior -- all synthesis operations produce the same results.
- All existing imports continue to resolve via `__init__.py` re-exports.

### Error Handling

No changes to error handling. All existing `try/except` blocks, `_log()` calls, and `print_exc()` invocations remain in their respective modules unchanged.

## Acceptance Criteria

- [ ] Given the directory `app/services/synthesis/operation/` exists, when listing its contents, then it contains `__init__.py`, `orchestrator.py`, `pipeline.py`, `resolution_sin.py`, `resolution_sbm.py`, `resolution_sbp.py`, `resolution_ree.py`, `resolution_uhe.py`, `resolution_ute.py`, `resolution_pee.py`, `stubs.py`, `cache.py`, `export.py`, `bounds.py`, and `spatial.py`
- [ ] Given any file in `app/services/synthesis/operation/`, when counting its lines with `wc -l`, then the count is at most 500
- [ ] Given the file `app/services/synthesis/operation/__init__.py`, when reading its contents, then it contains `from app.services.synthesis.operation.orchestrator import OperationSynthetizer`
- [ ] Given the test suite, when running `python -m pytest tests/app/services/synthesis/test_operation.py tests/app/services/synthesis/test_temporal_resolution.py tests/app/services/synthesis/test_entity_pipeline.py -x`, then all tests pass with exit code 0
- [ ] Given the old file `app/services/synthesis/operation.py`, when checking if it exists, then it does not exist (replaced by the package directory)

## Implementation Guide

### Suggested Approach

1. Create the package directory `app/services/synthesis/operation/`.
2. Create `__init__.py` with the re-export: `from app.services.synthesis.operation.orchestrator import OperationSynthetizer`.
3. Start with the leaf modules that have no intra-package dependencies: `cache.py`, `export.py`, `bounds.py`. Move the relevant free functions, adjusting imports.
4. Create `pipeline.py` with the shared pipeline functions.
5. Create each `resolution_*.py` file by extracting the corresponding `__resolve_*` and helper methods. Each resolution module imports from `pipeline.py` and `cache.py` as needed.
6. Create `stubs.py` with all stub functions. Extract `_resolve_SBM_MER_MERL` from its nested position.
7. Create `spatial.py` with the dispatcher functions `_resolve_spatial_resolution` and `_resolve_synthesis`.
8. Create `orchestrator.py` with the `OperationSynthetizer` class, importing from all other modules.
9. Delete the original `operation.py` file.
10. Run the full test suite to verify.

The key design choice is **free functions** rather than mixin inheritance. Each module defines functions that take `cls` (the OperationSynthetizer class) as their first argument. The `OperationSynthetizer` class in `orchestrator.py` delegates to these functions via thin `@classmethod` wrappers. This avoids complex MRO issues and keeps modules independently testable.

### Key Files to Modify

- `app/services/synthesis/operation.py` (delete after extraction)
- `app/services/synthesis/operation/__init__.py` (create)
- `app/services/synthesis/operation/orchestrator.py` (create)
- `app/services/synthesis/operation/pipeline.py` (create)
- `app/services/synthesis/operation/resolution_sin.py` (create)
- `app/services/synthesis/operation/resolution_sbm.py` (create)
- `app/services/synthesis/operation/resolution_sbp.py` (create)
- `app/services/synthesis/operation/resolution_ree.py` (create)
- `app/services/synthesis/operation/resolution_uhe.py` (create)
- `app/services/synthesis/operation/resolution_ute.py` (create)
- `app/services/synthesis/operation/resolution_pee.py` (create)
- `app/services/synthesis/operation/stubs.py` (create)
- `app/services/synthesis/operation/cache.py` (create)
- `app/services/synthesis/operation/export.py` (create)
- `app/services/synthesis/operation/bounds.py` (create)
- `app/services/synthesis/operation/spatial.py` (create)

### Patterns to Follow

- Use the same `from app.services.synthesis.operation.orchestrator import OperationSynthetizer` re-export pattern in `__init__.py` that Python packages conventionally use.
- Every extracted function preserves its original signature. If it was `@classmethod` and accessed class-level attributes (`cls.CACHED_SYNTHESIS`, `cls.logger`, etc.), the free function receives the class as the first parameter.
- Follow the `from app.utils.dataframe import pd_to_pl, pl_to_pd` import pattern already established in the codebase -- use absolute imports in all new modules.
- Preserve all existing docstrings verbatim.

### Pitfalls to Avoid

- Do NOT change the file `app/services/synthesis/operation.py` and the directory `app/services/synthesis/operation/` to coexist -- Python will prioritize the package over the file. Delete the file first or rename it before creating the directory.
- Do NOT introduce circular imports between modules. The dependency direction must be: `orchestrator` -> `spatial` -> `resolution_*` -> `pipeline`/`stubs` -> `cache`/`bounds`/`export`. Never import from `orchestrator` in leaf modules.
- Do NOT change any test file imports. The `__init__.py` re-export must make `from app.services.synthesis.operation import OperationSynthetizer` work exactly as before.
- The nested function `_resolve_SBM_MER_MERL` (line 1785) creates its own `ProcessPoolExecutor` -- when extracting, preserve this behavior but make it a top-level function, not a closure.

## Testing Requirements

### Unit Tests

No new unit tests required. All existing tests must pass unchanged:

- `tests/app/services/synthesis/test_operation.py`
- `tests/app/services/synthesis/test_temporal_resolution.py`
- `tests/app/services/synthesis/test_entity_pipeline.py`

### Integration Tests

Run `python -m pytest tests/ -x` to verify no import breakage across the entire test suite.

### E2E Tests

Not applicable -- this is a pure refactoring with no behavioral changes.

## Dependencies

- **Blocked By**: ticket-014-evaluate-thread-io-parallelism.md
- **Blocks**: ticket-016-decompose-deck.md

## Effort Estimate

**Points**: 5
**Confidence**: High
