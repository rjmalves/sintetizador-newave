# Epic 02: Deck Domain Modules

## Goal

Port all deck domain modules (`entities.py`, `temporal.py`, `misc.py`, `exchange.py`, `energy.py`, `storage.py`, `hydro.py`, `thermal.py`, `policy.py`) from pandas to polars internally. After this epic, all deck domain computation is polars-native, consuming polars from accessors and returning polars through the Deck facade.

## Scope

- Port `entities.py` (13 pandas ops, config/metadata, <100 rows): rename, drop_duplicates, set_index, join, apply(lambda), groupby, copy
- Port `temporal.py` (1 pandas op, `configurations` function): mostly scalar returns, one DataFrame function
- Port `misc.py` (3 pandas ops): block_lengths groupby/concat, costs
- Port `exchange.py` (4 pandas ops): exchange bounds with apply(lambda), sort, concat
- Port `energy.py` (16 pandas ops): stored energy bounds with iterrows, groupby, concat, join, apply
- Port `storage.py` (18 pandas ops): productivity computation with apply(lambda), join, groupby, graph traversal
- Port `hydro.py` (19 pandas ops): volume/flow bounds expansion with concat, sort, apply, modif changes
- Port `thermal.py` (7 pandas ops): includes `resample().ffill()` pattern, date arithmetic, iterrows
- Port `policy.py` (9 pandas ops): policy coefficient expansion

## Tickets

| ID         | Title                                    | Effort |
| ---------- | ---------------------------------------- | ------ |
| ticket-004 | Port entities.py to polars               | 3      |
| ticket-005 | Port temporal.py and misc.py to polars   | 2      |
| ticket-006 | Port exchange.py and energy.py to polars | 3      |
| ticket-007 | Port hydro.py to polars                  | 5      |
| ticket-008 | Port storage.py and thermal.py to polars | 5      |
| ticket-009 | Port policy.py to polars                 | 3      |

## Dependencies

- Depends on Epic 1 (accessors return polars)
- Epic 3 depends on this epic

## Success Criteria

- All deck domain modules accept and return `pl.DataFrame` where applicable
- Zero pandas operations in domain module functions (except readers.py boundary)
- Zero `pd.DataFrame` imports in domain modules (except where needed for type checking against inewave returns)
- All 349+ existing tests pass
