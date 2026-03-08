# Epic 01 Learnings: Conversion Boundary

**Epic**: epic-01-conversion-boundary
**Tickets**: ticket-001, ticket-002, ticket-003
**Quality scores**: 0.88, 0.83, 0.85 (all ACCEPTABLE)

---

## Patterns Established

- **Convert-at-cache-insertion pattern**: In `accessors.py`, the conversion from pandas to polars happens once, at the point where the result is stored in the cache (`val = pl.from_pandas(val); cache[key] = val`). The cached reference is returned directly on subsequent calls. This is the authoritative pattern for all cached accessors and eliminates the need for defensive `.copy()` calls.
  - File: `app/services/deck/accessors.py`, functions `confhd`, `clast`, `term`, `manutt`, `expt`, `hidr`, `engnat`, `vazoes`

- **Convert-and-chain pattern for uncached series accessors**: For one-shot (uncached) accessors, conversion and column rename are chained inline: `pl.from_pandas(arq.series).rename({"ree": EER_CODE_COL, "serie": SCENARIO_COL})`. This is compact and avoids intermediate variables.
  - File: `app/services/deck/accessors.py`, functions `energiaf`, `enavazf`, `vazaof`, `energiab`, `enavazb`, `vazaob`, `energias`, `enavazs`, `vazaos`

- **SHIM comment convention**: Every `.to_pandas()` call added to a consumer that is not yet migrated is annotated with `# SHIM: remove after polars migration of this module`. This makes future removal unambiguous -- grep for `SHIM` to find all pending removal sites.
  - Files: `app/services/deck/hydro.py`, `app/services/deck/storage.py`, `app/services/deck/thermal.py`, `app/services/synthesis/operation/stubs.py`, `app/services/synthesis/scenario.py`

- **Empty polars DataFrame with schema**: For optional-data accessors that may find no data (e.g., `manutt`), the empty fallback is `pl.DataFrame(schema={...})` with explicit column types rather than `pl.DataFrame()`. This ensures downstream code can introspect schema even on empty returns.
  - File: `app/services/deck/accessors.py`, function `manutt`

---

## Architectural Decisions

- **inewave boundary stays pandas**: `readers.py` and all direct calls to inewave file objects (`.usinas`, `.manutencoes`, `.series`, `.cadastro`, etc.) are left as-is returning pandas. The conversion boundary is always in `accessors.py`, never in `readers.py`. Rationale: inewave is an external library that returns pandas; wrapping at the reader level would make testing and future library upgrades harder.

- **`entities.py` and `misc.py` were not shimmed**: These modules call inewave readers directly (not through the polars-returning cached accessors) or call each other through pandas. They do not consume the 8 newly-polars cached accessors and needed no shims. This confirms the scope of epic 01 was correctly drawn: only modules that directly call the accessor functions listed in ticket-001/002 needed shims.

- **`block_lengths` in `misc.py` stays pandas**: `Deck.block_lengths()` returns `pd.DataFrame` because `misc.block_lengths()` builds the result from a pandas reader directly (not from any of the converted accessors). `DeckContext.block_lengths` is `pl.DataFrame` because it is populated from `Deck.block_lengths(uow)` at context construction time -- except `pipeline.py` calls `pd_to_pl()` on it again when needed. This dual-representation exists as a side effect of the partial migration and will be resolved when misc.py is migrated in epic 02.

---

## Files and Structures Created / Modified

- `app/services/deck/accessors.py` -- conversion boundary established; all 8 cached DataFrame accessors and 9 uncached series accessors now return `pl.DataFrame`
- `app/services/deck/deck.py` -- return type annotations updated for all 17 converted methods
- `app/services/deck/context.py` -- `DeckContext` fields `block_lengths`, `eer_submarket_map`, `hydro_eer_submarket_map` changed to `pl.DataFrame`; `import pandas` removed, replaced with `import polars`
- `app/services/deck/hydro.py` -- 5 SHIM sites on `accessors.hidr().to_pandas().reset_index()`
- `app/services/deck/storage.py` -- 2 SHIM sites on `accessors.hidr().to_pandas().set_index(HYDRO_CODE_COL)`
- `app/services/deck/thermal.py` -- 6 SHIM sites on `accessors.expt().to_pandas()` and `accessors.manutt().to_pandas()`
- `app/services/synthesis/operation/stubs.py` -- 2 SHIM sites on `Deck.hidr(uow).to_pandas().set_index("codigo_usina")`
- `app/services/synthesis/scenario.py` -- 10 SHIM sites across multiple functions calling `Deck.vazoes(uow).to_pandas()` and other DataFrame accessors

---

## Conventions Adopted

- **Polars rename syntax in accessors**: `pl.from_pandas(df).rename({"old_name": "new_name"})` -- the dict is passed directly, not as `columns=`. This differs from pandas `.rename(columns={...})`.

- **`pl.from_pandas(val, include_index=True)` for indexed DataFrames**: When the source pandas DataFrame has a meaningful index (e.g., `hidr.cadastro` uses the plant code as index), pass `include_index=True` so the index becomes a column in the polars result. Consumers then call `.to_pandas().reset_index()` to restore the indexed form. Without `include_index=True`, the index is silently dropped.
  - File: `app/services/deck/accessors.py` line 208, function `hidr`

- **SHIM placement**: `.to_pandas()` is placed inline at the point of consumption, not at the Deck facade method boundary. This preserves the polars return type from `Deck.*()` for consumers that do not need pandas.

- **No shims in `entities.py`**: The `entities.py` module reads from inewave directly (via `readers.get_sistema`, `readers.get_ree`, `readers.get_confhd`, `readers.get_conft`) and performs all processing in pandas. It is not a consumer of the polars-returning cached accessors and will be fully migrated in epic 02 (ticket-004).

---

## Surprises and Deviations

- **`hidr` required `include_index=True` (not in ticket spec)**: The ticket spec said to use `pl.from_pandas(val)` uniformly for all cached accessors. For `hidr`, the underlying pandas DataFrame (`readers.get_hidr(...).cadastro`) has the plant code (`codigo_usina`) as its pandas index rather than a column. A plain `pl.from_pandas(val)` drops the index silently, and all downstream consumers (hydro.py, storage.py, stubs.py) call `.reset_index()` expecting the plant code to be a column. The fix was `pl.from_pandas(val, include_index=True)`, which materializes the index as a column named `codigo_usina` in the polars result. This is an important gotcha for any pandas DataFrame whose meaningful data lives in the index.
  - File: `app/services/deck/accessors.py` line 208

- **`vazoes` has integer column names that become strings in polars**: When `Deck.vazoes(uow).to_pandas()` is called in `scenario.py`, the column names (flow station codes, originally integers in pandas) are converted to strings by `pl.from_pandas`. The consumer code then has to re-cast them back: `vazoes.columns = [int(c) for c in vazoes.columns]`. This is annotated as a SHIM. When `scenario.py` is migrated in epic 04, column names should be kept as integers from the outset, or the access pattern should switch to polars integer-indexed column selection.
  - File: `app/services/synthesis/scenario.py` line 134-136

- **`DeckContext` type inconsistency**: After ticket-002, `DeckContext.block_lengths` is annotated as `pl.DataFrame`, but `Deck.block_lengths()` (the method in `deck.py` that delegates to `misc.block_lengths()`) still returns `pd.DataFrame`. The `from_deck()` classmethod happens to work because Python does not enforce type annotations at runtime. However, `pipeline.py` calls `pd_to_pl(df_block_lengths)` on the value from `DeckContext.block_lengths`, effectively converting it twice when it comes from `DeckContext` (it was never polars to begin with). This inconsistency will be resolved when `misc.py` is migrated in epic 02.

- **Shim count was higher than the ticket predicted**: The ticket estimated "approximately 40-50 call sites across ~15 files". The actual shim count was 25 annotated sites across 5 files. Several files listed in the ticket's "Key Files to Modify" section (`exchange.py`, `misc.py`, `policy.py`, `energy.py`, `pipeline.py`, `resolution_*.py`) required no shims because they do not consume the polars-returning accessors from ticket-001/002 directly, or because they were already using polars in other paths.

---

## Recommendations for Future Epics

- **Epic 02 priority: `entities.py` first (ticket-004)**: `entities.py` functions (`hydros`, `eers`, `submarkets`, `thermals`, `hydro_eer_submarket_map`, `eer_submarket_map`) return `pd.DataFrame` with pandas `.set_index()`. Many SHIM sites in hydro.py and storage.py call `.to_pandas().reset_index()` on `accessors.hidr()` precisely because entity functions still expect pandas-indexed inputs. Migrating `entities.py` to return polars (with `EER_CODE_COL` etc. as regular columns rather than index) will also collapse the dual-index vs. non-index representation. See `app/services/deck/entities.py`.

- **Handle integer column names before migration of `scenario.py`**: The `vazoes` DataFrame has integer column names representing flow station codes. Before migrating `scenario.py` (epic 04, ticket-013), decide the canonical representation: either store them as integers in a polars struct/map or switch to a long-form tidy table. The current SHIM in `app/services/synthesis/scenario.py` lines 134-136 highlights the brittleness.

- **Check for pandas index before calling `pl.from_pandas`**: Any pandas DataFrame whose meaningful data is in the index (as opposed to a regular column) requires `pl.from_pandas(df, include_index=True)`. During epic 02 and beyond, verify `.index.name` is not `None` or a meaningful domain identifier before converting. The `hidr.cadastro` case (line 208 of `app/services/deck/accessors.py`) is the template.

- **`DeckContext.block_lengths` type should be corrected in epic 02**: When `misc.block_lengths()` is migrated to return `pl.DataFrame` in ticket-005, `DeckContext.block_lengths` will be genuinely polars and the `pd_to_pl()` double-conversion in `pipeline.py` line 216 should be removed.

- **SHIM removal checklist**: At the start of each epic 02-04 ticket, run `grep -rn "SHIM" app/` and confirm the shims in the module being migrated are removed as part of that ticket. Leaving shims after migration creates silent performance overhead and ambiguous code ownership.
