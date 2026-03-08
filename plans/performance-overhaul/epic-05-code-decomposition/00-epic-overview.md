# Epic 05: Code Decomposition & Cleanup

## Goal

Decompose the four monolithic files (operation.py 2538 lines, deck.py 4283 lines, files.py 1912 lines, scenario.py 1663 lines) into well-organized modules with clear single responsibilities. Add type annotations, remove dead code, and improve overall code maintainability. This epic is primarily about code quality and maintainability -- it does not change behavior or performance significantly.

## Scope

1. **Decompose operation.py**: Split into modules by spatial resolution (SIN, SBM, REE, UHE, UTE, PEE, SBP) and by concern (stubs, cache, bounds, export).

2. **Decompose deck.py**: Split into modules by data domain (general config, hydro data, thermal data, temporal data, energy data, policy data).

3. **Decompose files.py**: Split the `__regras` dict into separate mapping modules by variable category.

4. **Add type annotations**: Replace `# type: ignore` comments with proper type annotations throughout.

5. **Remove dead code and unused imports**: Clean up imports, remove commented-out code, remove unused variables.

## Dependencies

- Epic 04 must be complete (all parallelism changes stabilized)

## Success Criteria

- No file exceeds 500 lines
- All public functions have type annotations
- `ruff check` passes clean (no linting errors)
- `mypy` runs with minimal ignores
- All existing tests pass
- Zero behavioral changes

## Tickets

| Ticket     | Title                                            | Points | Depends On |
| ---------- | ------------------------------------------------ | ------ | ---------- |
| ticket-015 | Decompose operation.py into resolution modules   | 5      | ticket-014 |
| ticket-016 | Decompose deck.py into domain modules            | 5      | ticket-015 |
| ticket-017 | Decompose files.py into variable mapping modules | 3      | ticket-016 |
| ticket-018 | Add type annotations and remove dead code        | 3      | ticket-017 |
