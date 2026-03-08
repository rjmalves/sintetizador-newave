# Accumulated Learnings After Epic 03

## Conversion Patterns

- Convert-at-boundary (NWLISTOP): call `pl.from_pandas(df.rename(columns={...}))` exactly once at the entry of `resolve_temporal_resolution()` or `resolve_temporal_resolution_GTER_UTE()`; all downstream functions receive `pl.DataFrame`
- Convert-at-cache-insertion: call `pl.from_pandas(val)` before `cache[key] = val`; return the cached reference directly (no `.copy()` needed -- polars is immutable)
- `pl.from_pandas(df, include_index=True)` when the source DataFrame has meaningful data in its index (e.g., `hidr.cadastro` -- `app/services/deck/accessors.py` line 208)
- One-shot series accessors: chain inline as `pl.from_pandas(arq.series).rename({...})`
- Polars rename syntax: `.rename({"old": "new"})` -- no `columns=` keyword, unlike pandas
- Empty fallback with schema: `pl.DataFrame(schema={"col": pl.Int64, ...})` not `pl.DataFrame(columns=[...])`
- Cross-join for stage expansion: `df.join(pl.DataFrame({START_DATE_COL: dates}), how="cross").sort(...)` replaces `pd.concat([df] * N)` + `np.repeat()` -- used in `app/services/deck/hydro.py` lines 505-513
- Scenario-expansion without cross-join: `pl.concat([df] * N)` + `pl.Series("serie", np.repeat(np.arange(...), num_entries))` -- used in `app/services/synthesis/operation/pipeline.py` lines 208-213
- `pl.when().then().otherwise()` replaces all `.loc[condition, col] = value` mutations -- canonical example at `app/services/deck/thermal.py` lines 22-39
- Extract-to-numpy for per-row/BFS math: `df[c].to_numpy()`, compute, `df.with_columns(pl.Series(name, result))` -- used in `app/services/deck/storage.py` lines 44-103 and `app/services/synthesis/operation/_stubs_helpers.py` lines 41-44, 112-143
- Resample-to-daily-then-monthly: `pl.date_range() + left-join + forward_fill() + dt.truncate("1mo") + group_by().agg(.mean())` replaces pandas `resample` -- `app/services/deck/thermal.py` lines 75-171

## Critical Gotcha: Pandas Index

- `pl.from_pandas(df)` silently drops the pandas index
- Use `pl.from_pandas(df, include_index=True)` when the source DataFrame has meaningful data in its index
- After epic-02, domain modules (entities, hydro, storage) no longer use `set_index()`; all joins are explicit `on=col` polars joins

## Critical Gotcha: Integer Column Names

- `pl.from_pandas()` converts integer column names to strings
- `Deck.vazoes()` has flow-station-code integers as column names; after `.to_pandas()` the consumer must cast back: `[int(c) for c in df.columns]`
- SHIM at `app/services/synthesis/scenario.py` lines 134-136 marks this
- Resolution: switch to long-form (tidy) table before migrating `scenario.py` in epic-04

## Critical Gotcha: `df.is_empty()` vs `df.empty`

- Polars uses `df.is_empty()` (method call with parentheses); pandas uses `df.empty` (property)
- Using `df.empty` on a polars DataFrame silently evaluates to a truthy method object, never triggering the empty branch
- All cache-miss fallbacks and spatial-resolution no-ops in `app/services/synthesis/operation/` use `df.is_empty()` -- `cache.py` line 35, `spatial.py` line 49, `orchestrator.py` line 373

## Lookup Pattern for Per-Entity Resolution

- Build `dict(zip(df[KEY].to_list(), df[VAL].to_list()))` before the executor loop, then use `lookup[key]` inside each per-entity function
- Never call `df.filter(pl.col(KEY) == idx).item(0, COL)` in a hot loop; use the pre-built dict instead
- Canonical examples: `app/services/synthesis/operation/resolution_sbm.py` lines 68-74, `resolution_ute.py` lines 71-77, `resolution_sbp.py` lines 76-83

## Hook Signature Convention

- `post_resolve()` hooks have signature `(s: OperationSynthesis, df: pl.DataFrame, uow: AbstractUnitOfWork) -> pl.DataFrame`
- Hook implementations are closures that capture `deck_context` from the outer resolution function scope
- Examples: `_limit_stages_with_hydro` closure in `resolution_uhe.py` line 165, `_sort_thermals` closure in `resolution_ute.py` line 60

## Union-type Boundary for Mixed Callers

- `resolve_temporal_resolution()` accepts `Optional[pd.DataFrame | pl.DataFrame]` and dispatches on `isinstance(df, pl.DataFrame)` -- `app/services/synthesis/operation/pipeline.py` lines 85-103
- This pattern is appropriate when a function sits at a boundary where callers may be pre-converted or not; avoids double-conversion

## Shim Strategy

- Every `.to_pandas()` call added to a not-yet-migrated consumer was annotated `# SHIM: remove after polars migration of this module`
- After epic-03, the only remaining `.to_pandas()` shims are in `deck.py` facade bounds methods (~20 calls, lines 269-371) and `scenario.py` (epic-04 scope)
- `Deck.initial_stored_volume()` still returns `pd.DataFrame` (`deck.py` line 370); callers use `pl.from_pandas()` at the call site (`stubs.py` line 297)
- One residual pandas mutation: `resolution_ute.py` line 49 uses `df[SUBMARKET_CODE_COL] = sbm_index` on NWLISTOP pandas result before the conversion boundary

## Permanent Pandas Boundaries

- `uow.files.get_nwlistop()` returns `pd.DataFrame` -- permanent external dependency from the inewave reader
- `export_metadata()` in `export.py` lines 34-65 retains pandas because `uow.export.synthetize_df()` expects pandas; tiny non-hot-path, acceptable as permanent debt
- `fast_group_df()` in `app/utils/operations.py` is dead in the synthesis pipeline but used by `scenario.py`; remove when `scenario.py` is migrated

## Readers Boundary Pattern

- `readers.apply_modif_changes_to_hydros()` and `apply_modif_changes_to_hydros_in_stages()` accept `pd.DataFrame` indexed by `HYDRO_CODE_COL`
- Boundary: `df_pd = df.to_pandas().set_index(HYDRO_CODE_COL)` / call reader / `df = pl.from_pandas(df_pd.reset_index())`
- Sites: `app/services/deck/hydro.py` lines 97-118, 171-191, 306-309, 401-411, 426-436, 481-500

## Temporary Column Naming Convention

- Join-introduced lookup columns use `_` prefix (e.g., `_avg_value`, `_hm3_lower`) and are explicitly `.drop()`ped after use
- `app/services/deck/exchange.py` lines 43-83, `app/services/deck/hydro.py` lines 120-150

## Quality Scoring Artifact

- Tickets 010-012 all scored `scope_adherence: 0.0` because ripple-effect files (orchestrator, operations.py, test files) were not listed in "Key Files to Modify"
- These are not genuine scope violations; the type-contract shift propagates to callers/tests by design
- Mitigation: explicitly list every ripple-effect file in the ticket's "Key Files" section before dispatch to avoid this scoring penalty

## Epic-04 Priorities (scenario.py and cleanup)

- `scenario.py` uses `pd_to_pl`, `pl_to_pd`, wide-format vazoes with integer column names -- the integer-column-name problem must be resolved first (`scenario.py` lines 134-136)
- `deck.py` bounds methods (~20 `.to_pandas()` calls, lines 269-371) are the last major deck-layer shims; plan a dedicated ticket or epic for them
- `fast_group_df()` in `app/utils/operations.py` can be removed once `scenario.py` is migrated
- `resolve_GTER_UTE_entity()` line 49 in `resolution_ute.py` should be refactored to `with_columns()` for consistency
