# ticket-017 Decompose files.py into Variable Mapping Modules

## Context

### Background

`app/adapters/repository/files.py` is 1912 lines and contains the `AbstractFilesRepository` abstract base class, the `RawFilesRepository` concrete implementation, and the `factory()` function. The bulk of the file (lines 343-1131, ~790 lines) is the `__regras` dictionary inside `RawFilesRepository.__init__()`, which maps `(Variable, SpatialResolution)` tuples to lambda functions that read NWLISTOP output files. This dictionary has ~80 entries spanning 47 distinct `Variable` values and 7 `SpatialResolution` values. The remaining ~1000 lines contain the abstract interface, concrete `get_*` methods, and helper functions.

### Relation to Epic

This is the third decomposition ticket, following the patterns established in ticket-015 and ticket-016. Unlike the previous tickets which decomposed classes, this ticket decomposes a large dictionary literal into separate registry modules while keeping the `RawFilesRepository` class intact.

### Current State

The file structure:

- Lines 1-138: Imports (95 nwlistop reader classes, 20+ constants, abc, pandas, numpy)
- Lines 139-304: `AbstractFilesRepository` ABC with ~30 abstract methods
- Lines 305-342: `RawFilesRepository.__init__()` with 27 instance attributes
- Lines 343-1131: `__regras` dict literal (~790 lines, 80 entries)
- Lines 1134-1207: Helper methods (`__read_nwlistop_setting_version`, `__fix_indices_cenarios`, `__agg_cmo_dfs`, `__add_block_column`, `__replace_block_column`, `__eval_block_0_sum`, `__eval_block_0_sum_gter_ute`)
- Lines 1209-1908: Concrete `get_*` method implementations, `caso`, `arquivos`, `indices` properties
- Lines 1908-1912: `factory()` function

The `__regras` entries follow a few patterns:

1. **Simple block column**: `lambda dir, entity=1: self.__add_block_column(self.__read_nwlistop_setting_version(ReaderClass, join(dir, f"filename{entity}.out")))`
2. **Replace block column**: `lambda dir, entity=1: self.__replace_block_column(self.__read_nwlistop_setting_version(...))`
3. **Block 0 sum**: `lambda dir, entity=1: self.__eval_block_0_sum(self.__add_block_column(self.__read_nwlistop_setting_version(...)))`
4. **Custom aggregation**: `lambda dir, entity=1: self.__agg_cmo_dfs(dir, entity)` (CMO only)

Importers:

- `app/services/unitofwork.py` imports `AbstractFilesRepository`, `RawFilesRepository`, and `factory`
- `tests/app/adapters/repository/test_files.py` imports `factory`

## Specification

### Requirements

1. Extract the `__regras` dictionary entries from `RawFilesRepository.__init__()` into separate mapping modules organized by variable category under `app/adapters/repository/mappings/`.
2. Create these mapping modules:
   - `energy.py`: Variables related to energy (ENERGIA*NATURAL_AFLUENTE*_, ENERGIA*ARMAZENADA*_, ENERGIA*VERTIDA*_, ENERGIA*DESVIO*_, ENERGIA_EVAPORACAO, ENERGIA_VOLUME_MORTO)
   - `generation.py`: Variables related to generation (GERACAO_HIDRAULICA\*, GERACAO_TERMICA, GERACAO_EOLICA, CORTE_GERACAO_EOLICA, GERACAO_USINAS_NAO_SIMULADAS via GUNS)
   - `flow.py`: Variables related to flow/volume (VAZAO*\*, VOLUME*_, VIOLACAO*FPHA, VIOLACAO*_\_EVAPORACAO)
   - `cost.py`: Variables related to costs (CUSTO_MARGINAL_OPERACAO, CUSTO_GERACAO_TERMICA, CUSTO_OPERACAO, CUSTO_FUTURO, CUSTO_DEFICIT, VALOR_AGUA, VALOR_AGUA_INCREMENTAL)
   - `exchange.py`: Variables related to exchange (INTERCAMBIO, EXCESSO)
   - `hydraulic.py`: Variables related to hydraulic data (COTA_JUSANTE, COTA_MONTANTE, QUEDA_LIQUIDA, DEFICIT, META_ENERGIA_DEFLUENCIA_MINIMA, VIOLACAO_ENERGIA_DEFLUENCIA_MINIMA, VIOLACAO_GERACAO_HIDRAULICA_MINIMA, MERCADO_LIQUIDO)
   - `wind.py`: Variables related to wind (VELOCIDADE_VENTO, GERACAO_EOLICA at PEE resolution)
   - `__init__.py`: Exports a `build_regras(repo: "RawFilesRepository") -> Dict[Tuple[Variable, SpatialResolution], Callable]` function that merges all category dicts.
3. Each mapping module exports a function `get_rules(repo: "RawFilesRepository") -> Dict[Tuple[Variable, SpatialResolution], Callable]` that returns the subset of `__regras` entries for that category.
4. The lambdas currently reference `self.__add_block_column`, `self.__read_nwlistop_setting_version`, etc. When extracted, the `repo` parameter replaces `self`, so lambdas become: `lambda dir, entity=1: repo._RawFilesRepository__add_block_column(...)` or (preferred) the helper methods are made non-private (single underscore) to avoid name mangling.
5. In `RawFilesRepository.__init__()`, replace the inline `__regras` dict with a call to `build_regras(self)`.
6. Make the helper methods used by lambdas (`__read_nwlistop_setting_version`, `__add_block_column`, `__replace_block_column`, `__eval_block_0_sum`, `__eval_block_0_sum_gter_ute`, `__agg_cmo_dfs`) single-underscore private (`_read_nwlistop_setting_version`, etc.) so they are accessible from the mapping modules without name mangling.
7. All existing imports of `AbstractFilesRepository`, `RawFilesRepository`, and `factory` continue to work.
8. No file exceeds 500 lines.

### Inputs/Props

- The single file `app/adapters/repository/files.py` (1912 lines).

### Outputs/Behavior

- `app/adapters/repository/files.py` reduced to ~1100 lines (ABC + concrete methods + helpers, minus the 790-line dict).
- A new directory `app/adapters/repository/mappings/` with 8 Python files (7 category modules + `__init__.py`).
- Identical runtime behavior -- all file reading operations produce the same results.

### Error Handling

No changes to error handling. The `__fix_indices_cenarios` and `_validate_data` patterns remain unchanged.

## Acceptance Criteria

- [ ] Given the file `app/adapters/repository/files.py`, when counting its lines with `wc -l`, then the count is at most 1200
- [ ] Given the directory `app/adapters/repository/mappings/`, when listing its contents, then it contains `__init__.py`, `energy.py`, `generation.py`, `flow.py`, `cost.py`, `exchange.py`, `hydraulic.py`, and `wind.py`
- [ ] Given any file under `app/adapters/repository/mappings/`, when counting its lines with `wc -l`, then the count is at most 500
- [ ] Given the test suite, when running `python -m pytest tests/app/adapters/repository/test_files.py -x`, then all tests pass with exit code 0
- [ ] Given the full test suite, when running `python -m pytest tests/ -x`, then all tests pass with exit code 0

## Implementation Guide

### Suggested Approach

1. First, rename the double-underscore helper methods to single-underscore:
   - `__read_nwlistop_setting_version` -> `_read_nwlistop_setting_version`
   - `__add_block_column` -> `_add_block_column`
   - `__replace_block_column` -> `_replace_block_column`
   - `__eval_block_0_sum` -> `_eval_block_0_sum`
   - `__eval_block_0_sum_gter_ute` -> `_eval_block_0_sum_gter_ute`
   - `__agg_cmo_dfs` -> `_agg_cmo_dfs`
   - `__fix_indices_cenarios` -> `_fix_indices_cenarios`
     Update all references within `files.py` (both in `__regras` lambdas and in `get_*` methods).
2. Run tests to verify the rename is correct.
3. Create `app/adapters/repository/mappings/__init__.py` with the `build_regras()` function.
4. Create each category module. Each exports a `get_rules(repo)` function returning a dict of `(Variable, SpatialResolution) -> Callable` entries. The lambdas use `repo._add_block_column(...)` etc.
5. In `RawFilesRepository.__init__()`, replace the inline dict with: `self._regras = build_regras(self)` (also rename `__regras` to `_regras`).
6. Update `get_nwlistop()` and any other method that references `self.__regras` to use `self._regras`.
7. Run the full test suite.

### Key Files to Modify

- `app/adapters/repository/files.py` (rename helpers, remove dict, add import)
- `app/adapters/repository/mappings/__init__.py` (create)
- `app/adapters/repository/mappings/energy.py` (create)
- `app/adapters/repository/mappings/generation.py` (create)
- `app/adapters/repository/mappings/flow.py` (create)
- `app/adapters/repository/mappings/cost.py` (create)
- `app/adapters/repository/mappings/exchange.py` (create)
- `app/adapters/repository/mappings/hydraulic.py` (create)
- `app/adapters/repository/mappings/wind.py` (create)

### Patterns to Follow

- Each mapping module imports only the inewave reader classes and constants it needs -- do not import all 95 reader classes in every module.
- The `get_rules(repo)` function pattern provides consistency across all mapping modules.
- Use `from __future__ import annotations` at the top of each mapping module so that `"RawFilesRepository"` can be used as a forward reference in type hints without importing the class.

### Pitfalls to Avoid

- Do NOT leave the double-underscore methods as-is and use `repo._RawFilesRepository__add_block_column()` name-mangled access -- this is fragile and breaks if the class is renamed or subclassed. Rename to single-underscore first.
- Do NOT try to refactor the lambda patterns into named functions in this ticket -- that is a larger change that risks behavioral differences. Keep the lambdas as-is, just relocate them.
- The `__regras` attribute is accessed via `self.__regras` in `get_nwlistop()` method (around line 1378). Ensure this reference is updated to `self._regras`.
- Some lambdas use `.fillna(0.0)` on the result (e.g., `ViolNegEvap` entry at line 1130) -- preserve these post-processing steps exactly.

## Testing Requirements

### Unit Tests

No new unit tests required. All existing tests must pass unchanged:

- `tests/app/adapters/repository/test_files.py`

### Integration Tests

Run `python -m pytest tests/ -x` to verify no breakage across the full test suite.

### E2E Tests

Not applicable -- this is a pure refactoring with no behavioral changes.

## Dependencies

- **Blocked By**: ticket-016-decompose-deck.md
- **Blocks**: ticket-018-add-types-remove-dead-code.md

## Effort Estimate

**Points**: 3
**Confidence**: High
