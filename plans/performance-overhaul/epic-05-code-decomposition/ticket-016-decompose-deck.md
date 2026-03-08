# ticket-016 Decompose deck.py into Domain Modules

## Context

### Background

`app/services/deck/deck.py` is the largest file in the codebase at 4283 lines, containing the `Deck` class with ~130 class methods. These methods span disparate domains: raw file readers (`_get_*`), public data accessors (e.g., `confhd()`, `hidr()`, `thermal_costs()`), temporal/date computations, hydro bounds calculations, energy storage computations, thermal bounds, exchange bounds, policy cut coefficients, and entity mapping helpers. The file was already partially decomposed in earlier epics when `bounds.py` and `context.py` were extracted, but the core `Deck` class remains monolithic.

### Relation to Epic

This is the second decomposition ticket. It follows the package conversion pattern established in ticket-015 (converting a single-file module into a package with `__init__.py` re-exports). The resulting module structure must be compatible with the existing `bounds.py` and `context.py` files that already live under `app/services/deck/`.

### Current State

The `Deck` class at `app/services/deck/deck.py` (4283 lines) uses a class-level `DECK_DATA_CACHING` dict and `logger` field. All methods are `@classmethod`. The file is imported by:

- `app/services/deck/context.py` (lazy import inside method)
- `app/services/deck/bounds.py` (top-level import)
- `app/services/synthesis/execution.py`
- `app/services/synthesis/policy.py`
- `app/services/synthesis/system.py`
- `app/services/synthesis/operation.py` (now a package after ticket-015)
- `app/services/synthesis/scenario.py`
- `tests/app/services/deck/test_deck.py`

Sibling files `bounds.py` and `context.py` already exist under `app/services/deck/` and will remain in place.

Methods group naturally into these domains:

1. **Raw file readers** (lines 131-455): `_get_dger()`, `_get_shist()`, ..., `_get_estados()`, `_validate_data()` -- 25 methods
2. **Public data accessors** (lines 456-760): `dger()`, `pmo()`, `curva()`, `modif()`, `confhd()`, `clast()`, `term()`, `manutt()`, `expt()`, `hidr()`, `newavetim()`, `engnat()`, `energiaf()`, ..., `vazoes()`, `study_title()`, `version()` -- 24 methods
3. **Temporal/configuration** (lines 760-1500): `pre_study_period_starting_month()`, ..., `configurations()` -- ~30 methods dealing with study period dates, stages, iterations, scenarios
4. **Energy storage bounds** (lines 1509-1785): `eer_stored_energy_lower_bounds()`, `stored_energy_upper_bounds()`, `convergence()` -- energy/storage
5. **Thermal bounds** (lines 1799-2090): `thermal_generation_bounds()`, `thermal_costs()`, `_apply_thermal_bounds_maintenance_and_changes()` etc.
6. **Exchange/cost/misc** (lines 2090-2500): `exchange_bounds()`, `costs()`, `num_iterations()`, `runtimes()`, `submarkets()`, `eers()`, `hydros()`, `flow_diversion()`, `non_simulated_generation()`, `thermals()`, `num_blocks()`, `block_lengths()`, `exchange_block_limits()`
7. **Hydro bounds** (lines 2500-3220): `_get_hydro_data_changes_from_modif()`, `hydro_volume_bounds()`, ..., `hydro_drops_in_stages()` -- ~25 methods
8. **Initial stored energy/volume** (lines 3339-3730): `_evaluate_productivity()`, `initial_stored_energy()`, `initial_stored_volume()` etc.
9. **Entity maps** (lines 3733-3807): `eer_code_order()`, `hydro_code_order()`, `hydro_eer_submarket_map()`, `eer_submarket_map()`, `thermal_submarket_map()`
10. **Policy data** (lines 3807-4283): `_policy_df_building_block()`, `common_policy_df()`, `policy_variable_units()`, and all `_*_cut_entities()` methods

## Specification

### Requirements

1. Create new module files under `app/services/deck/` -- NOT a nested package. The `Deck` class remains in `deck.py` as a thin facade, and domain logic moves to new modules in the same directory.
2. Split into these domain modules (all under `app/services/deck/`):
   - `readers.py`: All `_get_*` methods as free functions and `_validate_data()`. These return raw inewave objects.
   - `accessors.py`: All public data accessor methods (`dger()`, `pmo()`, `confhd()`, ... through `version()`) as free functions that call reader functions from `readers.py`.
   - `temporal.py`: All date/stage/iteration/scenario computation methods (`pre_study_period_starting_month()` through `configurations()`, `_consider_post_study_years()`).
   - `energy.py`: Energy storage bounds methods (`eer_stored_energy_lower_bounds()`, `stored_energy_upper_bounds()`, `convergence()`, `_stored_energy_upper_bounds_inputs()`, `_stored_energy_upper_bounds_pmo()`).
   - `thermal.py`: Thermal bounds methods (`thermal_generation_bounds()`, `thermal_costs()`, `_apply_thermal_bounds_maintenance_and_changes()`, `_thermal_generation_bounds_term_manutt_expt()`, `_thermal_generation_bounds_pmo()`).
   - `hydro.py`: Hydro bounds, drops, and volume methods (`hydro_volume_bounds()` through `hydro_drops_in_stages()`, `_get_hydro_data_changes_from_modif()`, `_get_hydro_data_changes_from_modif_to_stages()`, `_get_value_and_unit_from_modif_entry()`).
   - `storage.py`: Initial stored energy and volume computations (`_evaluate_productivity()`, `_accumulate_productivity()`, `_hydro_accumulated_productivity_at_volume()`, `_initial_stored_energy_from_pmo()`, `_initial_stored_energy_from_confhd_hidr()`, `initial_stored_energy()`, `_initial_stored_volume_from_pmo()`, `_initial_stored_volume_from_confhd_hidr()`, `_initial_stored_volume_pre_study_condition()`, `initial_stored_volume()`).
   - `entities.py`: Entity mapping and ordering methods (`eer_code_order()`, `hydro_code_order()`, `hydro_eer_submarket_map()`, `eer_submarket_map()`, `thermal_submarket_map()`, `submarkets()`, `eers()`, `hydros()`, `thermals()`, `flow_diversion()`, `non_simulated_generation()`, `hybrid_policy()`).
   - `exchange.py`: Exchange-related methods (`exchange_bounds()`, `exchange_block_limits()`).
   - `policy.py`: Policy cut coefficient methods (`_policy_df_building_block()`, `_rhs_entities()`, `_eer_hydro_cut_entities()`, ..., `common_policy_df()`, `policy_variable_units()`).
   - `misc.py`: Remaining methods (`costs()`, `num_iterations()`, `runtimes()`, `num_blocks()`, `block_lengths()`, `models_wind_generation()`, `scenario_generation_model_type()`, `scenario_generation_model_max_order()`, `num_forward_series()`).

3. The `Deck` class in `deck.py` retains:
   - Class-level attributes: `T`, `logger`, `DECK_DATA_CACHING`
   - `_log()` classmethod
   - `clear_cache()` if it exists (or add one following the same pattern as `OperationSynthetizer.clear_cache()`)
   - Thin `@classmethod` wrappers that delegate to the domain module functions, preserving the exact same public API signatures.

4. `DECK_DATA_CACHING` remains centralized in `deck.py` on the `Deck` class. Domain modules receive it as a parameter or access it via `Deck.DECK_DATA_CACHING` import.

5. All existing imports of `from app.services.deck.deck import Deck` continue to work without changes.

6. Existing `bounds.py` and `context.py` remain untouched.

7. No file exceeds 500 lines.

### Inputs/Props

- The single file `app/services/deck/deck.py` (4283 lines).

### Outputs/Behavior

- `app/services/deck/deck.py` reduced to a thin facade (under 500 lines).
- 11 new domain module files under `app/services/deck/`.
- Identical runtime behavior for all Deck operations.

### Error Handling

No changes to error handling. All existing validation, logging, and exception patterns remain in their respective domain modules.

## Acceptance Criteria

- [ ] Given the file `app/services/deck/deck.py`, when counting its lines with `wc -l`, then the count is at most 500
- [ ] Given the directory `app/services/deck/`, when listing Python files, then it contains `deck.py`, `bounds.py`, `context.py`, `__init__.py`, `readers.py`, `accessors.py`, `temporal.py`, `energy.py`, `thermal.py`, `hydro.py`, `storage.py`, `entities.py`, `exchange.py`, `policy.py`, and `misc.py`
- [ ] Given any new module file under `app/services/deck/`, when counting its lines with `wc -l`, then the count is at most 500
- [ ] Given the test suite, when running `python -m pytest tests/app/services/deck/test_deck.py -x`, then all tests pass with exit code 0
- [ ] Given the full test suite, when running `python -m pytest tests/ -x`, then all tests pass with exit code 0 (no import breakage from operation.py or other consumers of Deck)

## Implementation Guide

### Suggested Approach

1. Create the domain module files one at a time, starting with leaf modules (`readers.py`, `entities.py`) that have no dependencies on other new modules.
2. For each domain module, extract the relevant methods as module-level free functions. Each function takes the `Deck` class (or `uow: AbstractUnitOfWork`) as its first parameter.
3. In `deck.py`, replace the method body with a delegation call: `return domain_module.function_name(cls, uow)` or similar.
4. The `DECK_DATA_CACHING` dict stays on the `Deck` class. Domain module functions that need cache access receive it as a parameter or import `Deck` -- but be careful about circular imports. The safest approach is to pass `DECK_DATA_CACHING` as a parameter to functions that need it.
5. After all extractions, verify that `deck.py` is under 500 lines.
6. Run tests after each module extraction to catch issues early.

### Key Files to Modify

- `app/services/deck/deck.py` (reduce to facade)
- `app/services/deck/readers.py` (create)
- `app/services/deck/accessors.py` (create)
- `app/services/deck/temporal.py` (create)
- `app/services/deck/energy.py` (create)
- `app/services/deck/thermal.py` (create)
- `app/services/deck/hydro.py` (create)
- `app/services/deck/storage.py` (create)
- `app/services/deck/entities.py` (create)
- `app/services/deck/exchange.py` (create)
- `app/services/deck/policy.py` (create)
- `app/services/deck/misc.py` (create)

### Patterns to Follow

- Follow the same free-function extraction pattern established in ticket-015 for `operation.py`.
- Preserve all existing docstrings verbatim.
- Use absolute imports throughout (e.g., `from app.services.deck.readers import _get_dger`).
- The `DECK_DATA_CACHING` caching pattern (check dict, call reader, store, return) should remain consistent across all accessor functions.

### Pitfalls to Avoid

- Do NOT create a nested package (`app/services/deck/deck/`) -- `deck.py` stays as a file alongside the new domain modules in `app/services/deck/`.
- Do NOT move `bounds.py` or `context.py` -- they are already decomposed and stable from earlier epics.
- Avoid circular imports: domain modules should NOT import `Deck` from `deck.py`. If they need `DECK_DATA_CACHING`, pass it as a parameter. If they need `_log()`, accept a logger parameter.
- The `_get_energiaf()`, `_get_enavazf()`, `_get_vazaof()` and backward variants accept an `iteracao` parameter -- ensure this is preserved in the free function signatures.

## Testing Requirements

### Unit Tests

No new unit tests required. All existing tests must pass unchanged:

- `tests/app/services/deck/test_deck.py`
- `tests/app/services/deck/test_context.py`
- `tests/app/services/deck/test_bounds_polars.py`

### Integration Tests

Run `python -m pytest tests/ -x` to verify no import breakage from any consumer of the `Deck` class (operation, scenario, execution, policy, system synthesizers).

### E2E Tests

Not applicable -- this is a pure refactoring with no behavioral changes.

## Dependencies

- **Blocked By**: ticket-015-decompose-operation.md
- **Blocks**: ticket-017-decompose-files.md

## Effort Estimate

**Points**: 5
**Confidence**: High
