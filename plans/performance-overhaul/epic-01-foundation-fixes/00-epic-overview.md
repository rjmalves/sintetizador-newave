# Epic 01: Foundation Fixes

## Goal

Eliminate the three highest-impact, lowest-risk performance and correctness issues that form the foundation for all subsequent optimization work. These are changes that can be made incrementally without disrupting the overall architecture, and each delivers measurable improvement independently.

## Scope

1. **Remove `chdir()` from UnitOfWork**: Replace the `os.chdir()` calls in `FSUnitOfWork.__enter__`/`__exit__` with explicit absolute path passing. This is a correctness fix (race condition in multiprocessing) and an enabler for all parallelism improvements.

2. **Eliminate redundant deck file re-reads in subprocesses**: The `Deck.DECK_DATA_CACHING` class-level dict is not shared across `multiprocessing.Pool` subprocesses. Each subprocess re-reads all deck files (dger, confhd, patamar, etc.). Pre-read deck data in the main process and pass it to subprocesses.

3. **Move statistics computation after concatenation**: Currently `calc_statistics()` is called per entity in `_post_resolve_entity()`, computing 21 quantiles + mean + std for each individual entity. Then entities are concatenated. Move this to a single call after concatenation in `_post_resolve()` or `_export_scenario_synthesis()`.

## Dependencies

- No dependencies on other epics
- All tickets in this epic are ordered by dependency (chdir fix first, as it enables the other changes)

## Success Criteria

- All existing tests pass
- `chdir()` no longer appears in production code
- Deck files are read exactly once per synthesis run regardless of number of subprocesses
- `calc_statistics()` is called once per variable, not once per entity
- Measured runtime improvement of 2-3x on a representative case

## Tickets

| Ticket     | Title                                                  | Points | Depends On |
| ---------- | ------------------------------------------------------ | ------ | ---------- |
| ticket-001 | Eliminate chdir() from FSUnitOfWork                    | 3      | --         |
| ticket-002 | Pre-compute deck data and pass to subprocesses         | 5      | ticket-001 |
| ticket-003 | Move statistics computation after entity concatenation | 3      | --         |
