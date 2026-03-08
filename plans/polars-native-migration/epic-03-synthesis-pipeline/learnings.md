# Epic-03 Learnings: Synthesis Pipeline

## Patterns Established

### Full-pipeline polars contract

After this epic, every function from NWLISTOP boundary through `post_resolve()` / `export_stats()` returns and accepts `pl.DataFrame`. The convert-at-boundary rule now reads: convert pandas (from `uow.files.get_nwlistop()`) to polars exactly once, at the entry point of `resolve_temporal_resolution()` or `resolve_temporal_resolution_GTER_UTE()`. Established in `app/services/synthesis/operation/pipeline.py` lines 99-102 and 226-237.

### Union-type boundary for NWLISTOP

`resolve_temporal_resolution()` accepts `Optional[pd.DataFrame | pl.DataFrame]` and dispatches on `isinstance(df, pl.DataFrame)`. This future-proofs the function for callers that already hold polars (e.g., stub resolvers in `_stubs_market.py`), while still converting pandas from NWLISTOP at the correct site. See `app/services/synthesis/operation/pipeline.py` lines 85-103.

### Dict-based lookup replaces per-row `.filter().item()`

Every resolution module that iterates entities now builds a `dict(zip(df[KEY].to_list(), df[VAL].to_list()))` before the executor loop, then uses `name_map[s]` inside each entity function. Avoids repeated scans of the same DataFrame per entity and mirrors the established epic-02 pattern from `app/services/deck/storage.py`. Canonical examples: `app/services/synthesis/operation/resolution_sbm.py` lines 68-74, `resolution_ute.py` lines 71-77.

### Extract-to-numpy for BFS/in-place mutation in stubs

The `calc_accumulated_productivity()` BFS accumulation and `fill_initial_storage_df()` shift both extract the target column to a numpy array, mutate in numpy, then write back once with `df.with_columns(pl.Series(name, arr))`. This is the canonical pattern for algorithms requiring random-access element mutation. See `app/services/synthesis/operation/_stubs_helpers.py` lines 41-44, 112-143.

### Immutable-fallback with polars empty DataFrame

Cache misses (`get_from_cache_if_exists()`) and spatial-resolution no-ops (`resolve_spatial_resolution()`) return `pl.DataFrame()` (empty, no schema). Callers uniformly check `df.is_empty()` (polars method, not `.empty` property). See `app/services/synthesis/operation/cache.py` line 35, `spatial.py` line 49, `orchestrator.py` line 373.

### `generate_scenarios` list-replication pattern

`generate_scenarios()` now expands rows using `pl.concat([df] * num_scenarios)` combined with `pl.Series("serie", np.repeat(np.arange(1, N+1), num_entries))`. This replaces `pd.concat([df] * N)` and does not require a cross-join. `app/services/synthesis/operation/pipeline.py` lines 208-213.

### Hook signature updated to polars throughout

`post_resolve()` early/late hooks now have the signature `(s: OperationSynthesis, df: pl.DataFrame, uow) -> pl.DataFrame`. All hook implementations in resolution modules (`_limit_stages_with_hydro` in `resolution_uhe.py`, `_sort_thermals` in `resolution_ute.py`) were updated accordingly. The hooks are closures that capture `deck_context` from the outer function scope, keeping their external signature compatible with `post_resolve()`.

## Architectural Decisions

### `export_metadata()` kept as pandas

Decision: `export_metadata()` in `export.py` retains `pd.DataFrame` and `pd.concat()` for the metadata builder, since `uow.export.synthetize_df()` expects pandas and metadata is tiny / non-hot-path.
Rejected: Porting `synthetize_df()` to accept polars, which would require touching the UoW export interface outside epic scope.
Rationale: Bounded technical debt with zero performance impact; the metadata write happens once per synthesis run.

### `Deck.initial_stored_volume()` still returns `pd.DataFrame`

Decision: `stubs.py` wraps `Deck.initial_stored_volume(uow)` with `pl.from_pandas()` at the call site (line 297), rather than changing `deck.py`.
Rejected: Adding a polars-returning path to `deck.py` for this method.
Rationale: `deck.py` has multiple `.to_pandas()` shims in its bounds-related methods that are not within epic-03 scope; changing one method would create an inconsistent mixed interface.

### Pandas mutation retained in `resolve_GTER_UTE_entity()`

Decision: `resolution_ute.py` line 49 still uses `df[SUBMARKET_CODE_COL] = sbm_index` on the pandas DataFrame returned from NWLISTOP, before calling `post_resolve_GTER_UTE_entity()` which converts internally.
Rejected: Converting to polars first and using `with_columns()`, or encoding `sbm_index` in the column inside `post_resolve_GTER_UTE_entity()`.
Rationale: The NWLISTOP result is already pandas at this point; mutating it before the single conversion boundary inside `post_resolve_GTER_UTE_entity()` is both correct and minimal-change. This is an undocumented residual that is logically acceptable but should be noted for epic-04 cleanup.

### `resolve_temporal_resolution()` does not remove `import pandas as pd`

Decision: `pipeline.py` retains `import pandas as pd` because function signatures for `resolve_temporal_resolution_GTER_UTE()`, `post_resolve_GTER_UTE_entity()`, and `post_resolve_entity()` accept `Optional[pd.DataFrame]` (NWLISTOP boundary).
Rationale: The NWLISTOP external dependency is permanent until either the inewave reader is migrated or the synthesis pipeline wraps all NWLISTOP reads internally. The pandas import is legitimate and required.

## Files and Structures Created/Modified

- `app/services/synthesis/operation/pipeline.py` -- dead pandas helpers removed; all pipeline functions return `pl.DataFrame`; `generate_scenarios`, `initial_stored_energy_df`, `resolve_temporal_resolution_GTER_UTE`, `post_resolve_GTER_UTE_entity` ported to polars
- `app/utils/operations.py` -- `calc_statistics()` and `_calc_statistics_polars()` now accept/return `pl.DataFrame` directly; `fast_group_df()` retained (still used by `scenario.py`)
- `app/services/synthesis/operation/bounds.py` -- thin wrapper, `pd_to_pl`/`pl_to_pd` pair removed; now pass-through to `OperationVariableBounds.resolve_bounds()`
- `app/services/synthesis/operation/cache.py` -- all `pd.DataFrame` replaced with `pl.DataFrame`; `pl.DataFrame()` for empty fallback
- `app/services/synthesis/operation/export.py` -- `export_scenario_synthesis()`, `add_synthesis_stats()`, `export_stats()` fully polars; `export_metadata()` kept pandas
- `app/services/synthesis/operation/spatial.py` -- return types changed to `pl.DataFrame`; empty fallback updated
- `app/services/synthesis/operation/orchestrator.py` -- `CACHED_SYNTHESIS`, `SYNTHESIS_STATS` typed with `pl.DataFrame`; `df.empty` replaced with `df.is_empty()`
- `app/services/synthesis/operation/resolution_sbm.py` -- `.to_pandas()` shim removed; polars filter + dict-lookup pattern
- `app/services/synthesis/operation/resolution_ree.py` -- `.to_pandas()` shims removed; `aux_df.filter().item()` for submarket lookup
- `app/services/synthesis/operation/resolution_uhe.py` -- `.to_pandas()` shims removed; `_limit_stages_with_hydro` hook updated to polars; `_calc_block_0_weighted_mean` retained as polars
- `app/services/synthesis/operation/resolution_ute.py` -- `.to_pandas()` shim removed; `_sort_thermals` hook inlined as closure returning `df.sort()`; `pd_to_pl()` boundary removed from `resolve_GTER_UTE()`
- `app/services/synthesis/operation/resolution_sbp.py` -- `.to_pandas()` shim removed; dict-lookup pattern for submarket pairs
- `app/services/synthesis/operation/stubs.py` -- all stub functions ported to polars; `pl.from_pandas()` retained for `Deck.initial_stored_volume()` (not yet polars)
- `app/services/synthesis/operation/_stubs_helpers.py` -- `fill_initial_storage_df()`, `two_cache_op()`, `calc_accumulated_productivity()` ported to polars; variable-set frozensets extracted here to stay under 500 lines
- `app/services/synthesis/operation/_stubs_market.py` -- `.to_pandas()` shims removed; `pl.from_pandas(df)` retained at NWLISTOP boundary before `generate_scenarios()`
- `tests/app/services/synthesis/test_temporal_resolution.py` -- pandas fallback tests removed; remaining tests pass unchanged
- `tests/app/services/synthesis/test_entity_pipeline.py` -- assertion updated to `isinstance(result, pl.DataFrame)`
- `tests/app/utils/test_operations.py` -- `calc_statistics()` test updated to pass `pl.DataFrame`

## Conventions Adopted

- `df.is_empty()` (call with parentheses) for polars empty check everywhere in the synthesis operation package; `df.empty` is the pandas attribute and will silently evaluate truthy as a method object if mistakenly used on polars
- `dict(zip(df[KEY].to_list(), df[VAL].to_list()))` as the standard pre-loop lookup table for per-entity resolution; never call `df.filter().item()` in a hot loop
- Hook closures capture `deck_context` from outer scope rather than adding it as a parameter; the hook protocol signature is fixed as `(s, df, uow) -> pl.DataFrame`
- `pl.concat([df] * N)` followed by a `pl.Series` scenario column is the canonical way to replicate rows for scenario expansion in this codebase (no cross-join needed when expanding a constant template)
- `_stubs_helpers.py` holds all variable-set frozensets (HYDRO_RESOLUTION_VARS, FLOW_VOLUME_VARS, etc.) so `stubs.py` stays below 500 lines; this split is intentional and should be maintained for future additions

## Surprises and Deviations

### `resolve_temporal_resolution()` kept its union-type signature

Plan specified that the input type remain `Optional[pd.DataFrame]` (unchanged). The implementation widened it to `Optional[pd.DataFrame | pl.DataFrame]` to support callers (`_stubs_market.py`) that already hold polars DataFrames. The function dispatches on `isinstance`. This was a pragmatic deviation that avoids double-conversion in `_stubs_market.py`; the ticket plan had not anticipated this call path. Resulting code: `app/services/synthesis/operation/pipeline.py` lines 85-103.

### `generate_scenarios` converts at its call site, not internally

The plan indicated stubs callers should convert to polars before calling `generate_scenarios()`. The implementation placed `pl.from_pandas(df)` at the call site in `_stubs_market.py` lines 53 and 104. This is functionally correct but left a visible `pl.from_pandas()` call in an otherwise pandas-free module. This is an acceptable residual since the NWLISTOP read returns pandas.

### `_stubs_helpers.py` was created as a new split module

The ticket plan described porting functions within the existing `stubs.py`. The actual implementation extracted helpers and frozensets into a new private module `_stubs_helpers.py` to stay within the 500-line module limit. This decomposition was not in the plan but is a clean structural improvement.

### `Deck.initial_stored_volume()` was not polars despite `deck.py` having been nominally migrated

`deck.py` still exposes `initial_stored_volume()` returning `pd.DataFrame` (confirmed at `app/services/deck/deck.py` line 370). This was not flagged as a remaining shim in the epic-02 learnings. The `stubs.py` caller wraps it with `pl.from_pandas()`. This is a known residual; it and many other `deck.py` `.to_pandas()` shims will be addressed when the Deck facade bounds layer is migrated.

### Quality scores were below target due to scope adherence penalty

Tickets 010-012 all received `scope_adherence: 0.0` in quality scoring because extra files (e.g., `orchestrator.py`, `scenario.py`, `operations.py`, test files) were modified beyond the declared "Key Files". This is a scoring artifact: those extra files are natural ripple-effect changes required by the type contract shift, not genuine scope creep. Future tickets should explicitly declare all ripple-effect files in the "Key Files" section.

## Recommendations for Future Epics

- `scenario.py` still uses `pd_to_pl`, `pl_to_pd`, `import pandas`, and wide-format vazoes (integer column names). The integer-column-name problem documented in epic-01 summary must be resolved before migrating `scenario.py` (ticket-013). See `app/services/synthesis/scenario.py` lines 49, 134-136.
- `deck.py` has ~20 `.to_pandas()` calls in its bounds-related methods (lines 269-371). These are the last major pandas shims in the Deck layer. Migrating them is out-of-scope for epic-04 but should be a dedicated epic-05 or post-migration cleanup task.
- The `export_metadata()` pandas path in `app/services/synthesis/operation/export.py` lines 34-65 is the last pandas usage in an otherwise fully-polars module. It will remain until `uow.export.synthetize_df()` accepts polars natively.
- `fast_group_df()` in `app/utils/operations.py` is dead in the synthesis pipeline but is still called by `scenario.py`. It should be removed when `scenario.py` is migrated (ticket-013/014).
- The `resolve_GTER_UTE_entity()` pandas mutation at `app/services/synthesis/operation/resolution_ute.py` line 49 should be refactored to `with_columns()` when a convenient opportunity arises; it is the only remaining direct pandas in-place mutation in the operation synthesis modules.
- When expanding "Key Files to Modify" in future tickets, explicitly list every ripple-effect file (tests, orchestrator, operations.py) to avoid `scope_adherence: 0.0` in quality scoring.
