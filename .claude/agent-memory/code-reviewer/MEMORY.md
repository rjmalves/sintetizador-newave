# Code Reviewer Memory - sintetizador-newave

## Project Profile

- **Stack**: Python (scientific computing / data pipeline)
- **Key libs**: pandas, polars, numpy, pyarrow, inewave
- **Python**: 3.10+ (project); currently running 3.14 in .venv
- **Standards**: ruff (format + lint E/F/W/I), mypy --strict, pytest --cov
- **Formatting**: line-length 80, E501 ignored

## Architecture Patterns (stable)

- `Deck` facade class: delegates to domain modules (`temporal.py`, `entities.py`, etc.) with shared `DECK_DATA_CACHING: Dict[str, Any]` dict
- `OperationSynthetizer` class: classmethods only; delegates to `pipeline.py`, `bounds.py`, `cache.py`, `export.py`, `stubs.py`, `spatial.py`, `resolution_*.py`
- `_pkg()` pattern in `pipeline.py` and `orchestrator.py`: returns `sys.modules[__package__]` to avoid circular imports; the package `__init__.py` re-exports `Deck`, `ProcessPoolExecutor` (pd_to_pl/pl_to_pd removed in polars-native-migration)
- Type hints use `TYPE_CHECKING` guard for `OperationSynthetizer` forward reference (circular import avoidance)
- After polars-native-migration: all internal Deck domain module returns are `pl.DataFrame`; the Deck facade converts to pandas at the boundary for legacy callers (bounds.py etc. still on pandas). Deck methods that return `pd.DataFrame` in their type annotation call `.to_pandas()` directly in deck.py.

## Known Pre-existing Issues (do NOT flag)

- `print_exc()` in orchestrator instead of `logger.exception()` (pre-existed in original 2538-line operation.py)
- Mutable default arguments `early_hooks: List[Callable] = []`, `late_hooks: List[Callable] = []`, `internal_stubs: Dict = {}` in pipeline.py (pre-existed in original)
- `os.path.join` in files.py (pre-existed, not newly introduced)
- `except Exception:` bare clauses in files.py file accessors (pre-existed)
- `assert executor is not None` in resolution\_\*.py files (introduced in epic-04 parallelism, intentional design guard)
- `vol_df[LOWER_BOUND_COL] = 0.0` before `UPPER_BOUND_COL` subtraction in stubs.py stub_EARM_UHE (pre-existing, lines 1404-1408 of original)
- `type: ignore` without code on pyarrow imports in export.py (pre-existed)
- `TestExportRepository.synthetize_df` returns `df` instead of `bool` (pre-existed)

## Standards: What Is NOT Enforced in This Project

- `from __future__ import annotations` — only used in `mappings/` package, not project-wide
- `List`/`Dict` from typing (not `list`/`dict`) — used throughout without issue

## Performance Overhaul Review Notes

- epic-01: chdir eliminated from FSUnitOfWork — clean
- epic-02: Polars-based `_calc_statistics_polars` in `operations.py` — single-pass group_by + unpivot pattern
- epic-03: Polars hot-path in `resolve_temporal_resolution` with pandas fallback on exception
- epic-04: `ProcessPoolExecutor` shared per spatial-resolution group in `synthetize()`; SIN uses no pool
- epic-05: `operation.py` (2538 lines) → 17-file package; `deck.py` → facade + 11 modules; `files.py` → unchanged + `mappings/` package

## Polars-Native Migration Notes (feat/polars-native-migration)

- All `pd_to_pl`/`pl_to_pd` conversion utilities removed; `app/utils/dataframe.py` deleted
- entities.py: all maps (`eers`, `hydros`, `submarkets`, etc.) now `pl.DataFrame`; no longer indexed by primary key; callers must use `.filter()/.join()` not `.at[]/.loc[index]`
- `eer_submarket_map` now returns `[EER_CODE_COL, EER_NAME_COL, SUBMARKET_CODE_COL, SUBMARKET_NAME_COL]` — if caller's df already has `EER_NAME_COL`, polars join creates `EER_NAME_COL_right` duplicate (minor schema issue in pipeline.py `initial_stored_energy_df` SUBMERCADO path)
- `scenario.py` retains `pd.date_range` for history date sequences (intentional, polars has no equivalent)
- `_resolve_forward_energy_iteration` and `_resolve_backward_energy_iteration` still type-annotated as `-> pd.DataFrame` and call `.to_pandas()` — `_post_resolve` handles mixed dict via `isinstance` check
- `storage.py` `hydro_volume_bounds_with_changes` and `_hydro_volume_bounds_in_stages` have pandas shim boundaries for `readers.apply_modif_changes_to_hydros[_in_stages]` — these convert to pandas, call readers, convert back
- `temporal.py` no longer imports pandas; `_month_range` replaced `pd.date_range` with manual `relativedelta` loop

## Double Conversion in synthetize_pl (export.py line 84)

`pa.Table.from_pandas(df.to_arrow().to_pandas())` — intentional to embed pandas metadata in Arrow table for UTC round-trip. Not a bug but has memory overhead. Below threshold for flagging since it's documented in the docstring.
