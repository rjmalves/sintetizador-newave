# Epic 01 — Foundation Fixes: Learnings

## What Worked Well

1. **chdir() removal was clean** — The `FSUnitOfWork` already used absolute paths internally via `Path(directory).resolve()`. Removing `chdir()` required only deleting 4 lines with no downstream breakage.

2. **DeckContext dataclass pattern** — Pre-computing deck data in the main process and passing it to subprocesses via pickle worked well. The dataclass is small (~KB), pickle round-trips cleanly, and the `Optional[DeckContext] = None` parameter approach allows gradual adoption.

3. **Statistics relocation was high-impact** — Moving `calc_statistics()` from per-entity to post-concatenation reduced the number of groupby operations from ~200 to 1 for UHE variables.

## What Caused Issues

1. **Numpy read-only arrays** — After removing per-entity statistics and caching only the scenario DataFrame (post sort+reset_index), the `.to_numpy()` call returned read-only arrays. Fix: always use `.to_numpy().copy()` when the array will be mutated in-place.

2. **Pre-existing test failures** — 136 tests failed before this epic due to `Settings().installdir` being `None` in the test environment. This caused noise in all guardian verifications. After ticket-003, this dropped to 15 failures (the statistics removal fixed many test paths that previously hit the installdir issue later).

3. **Type annotations in DeckContext** — The `from_deck(cls, uow: object)` annotation was too broad. Should use `from __future__ import annotations` and annotate as `AbstractUnitOfWork` to avoid circular imports.

## Recommendations for Future Epics

- **Always `.copy()` numpy arrays extracted from cached DataFrames** — pandas may return views that are read-only
- **Thread `deck_context` to scenario.py as well** — ticket-002 only threaded it through operation.py; scenario.py still re-reads deck files in subprocesses
- **Fix the `Settings().installdir` test issue** — either mock it properly or provide a fixture that sets the env var
- **Add unit tests for new behavior** — ticket-003 didn't add tests for the relocated statistics computation
