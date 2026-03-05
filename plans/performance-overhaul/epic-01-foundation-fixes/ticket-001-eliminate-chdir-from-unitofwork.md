# ticket-001 Eliminate chdir() from FSUnitOfWork

## Context

### Background

The `FSUnitOfWork` class at `app/services/unitofwork.py` calls `os.chdir(self._path)` in `__enter__` and `os.chdir(self._current_path)` in `__exit__`. This mutates global process state and is fundamentally unsafe for multiprocessing -- when multiple subprocesses each create their own `FSUnitOfWork` and call `__enter__`, they race on the global working directory. This is both a correctness bug and a blocker for any parallelism improvements.

The `chdir()` exists because downstream code (particularly `RawFilesRepository` in `app/adapters/repository/files.py`) uses relative paths to read files. The fix is to ensure all file operations use absolute paths, then remove the `chdir()` calls.

### Relation to Epic

This is the first ticket in Epic 01 (Foundation Fixes) and a prerequisite for ticket-002 (deck caching), since the deck caching fix requires reliable concurrent access to the UnitOfWork.

### Current State

- `FSUnitOfWork.__enter__` (line 93-96 of `app/services/unitofwork.py`): calls `chdir(self._path)` then creates the repository
- `FSUnitOfWork.__exit__` (line 98-102): calls `chdir(self._current_path)`, sets `_files` and `_exporter` to None
- `RawFilesRepository.__init__` (line 306-342 of `app/adapters/repository/files.py`): stores `self.__tmppath` as the base path, uses `join(str(self.__tmppath), ...)` for most file reads
- The `__regras` dict (lines 343-1131 of files.py): all lambda readers receive `dir` as the first argument and construct paths with `join(dir, filename)`
- `Deck._get_*` methods (app/services/deck/deck.py): each calls `with uow:` which triggers `__enter__`/`__exit__`, causing `chdir()` on every deck access

## Specification

### Requirements

1. Remove `os.chdir()` calls from `FSUnitOfWork.__enter__` and `FSUnitOfWork.__exit__`
2. Ensure `self._path` in `FSUnitOfWork` is always an absolute path (it already is via `Path(directory).resolve()`)
3. The `RawFilesRepository` must receive and use the absolute path for all file reads -- verify this is already the case since `self.__tmppath` is set from the UoW path
4. The export repository must receive an absolute path for the synthesis output directory -- verify this is already the case
5. `Deck._get_*` methods that wrap calls in `with uow:` must continue to work without `chdir()`, meaning the repository must resolve all file paths from the stored absolute path
6. All tests must pass after the change

### Inputs/Props

- `FSUnitOfWork.__init__` receives `directory: str` and a `Queue`
- `RawFilesRepository.__init__` receives `tmppath: str` (the working directory path)

### Outputs/Behavior

- After this change, `os.getcwd()` is never modified by the application
- All file operations use `self.__tmppath` (which is the absolute path to the NEWAVE output directory) as the base for constructing file paths
- The `with uow:` context manager still creates the repository and export objects on `__enter__` and cleans them up on `__exit__`, but no longer changes the working directory

### Error Handling

- If a file is not found because a path was incorrectly relative, the existing `try/except` in `get_nwlistop` (line 1385-1392) returns `None`, preserving current behavior
- If the repository creation fails, the existing `RuntimeError` in the `files` property (line 106-108) is raised

## Acceptance Criteria

- [ ] Given `FSUnitOfWork` is instantiated with a directory path, when `__enter__` is called, then `os.getcwd()` remains unchanged from before the call
- [ ] Given `FSUnitOfWork.__exit__` is called, when the context is exited, then `os.getcwd()` remains unchanged from before `__enter__` was called
- [ ] Given `grep -rn "chdir" app/` is run, when examining results, then no `chdir` calls appear in any production code file under `app/`
- [ ] Given the existing test suite is run with `pytest tests/`, when all tests execute, then all tests pass with zero failures
- [ ] Given `RawFilesRepository` is instantiated with an absolute path `/abs/path`, when `get_nwlistop` is called for any variable, then the file read uses a path rooted at `/abs/path` (not relative to cwd)

## Implementation Guide

### Suggested Approach

1. In `app/services/unitofwork.py`, modify `FSUnitOfWork.__enter__`:
   - Remove the `chdir(self._path)` call (line 94)
   - Keep `self.__create_repository()` (line 95)
   - Keep `return super().__enter__()` (line 96)

2. In `app/services/unitofwork.py`, modify `FSUnitOfWork.__exit__`:
   - Remove the `chdir(self._current_path)` call (line 99)
   - Keep `self._files = None` and `self._exporter = None` (lines 100-101)
   - Keep `super().__exit__(*args)` (line 102)

3. Remove the `self._current_path` assignment in `__init__` (line 72) since it is no longer needed.

4. Remove the `from os import chdir, curdir` import or adjust it (line 3).

5. Verify that `RawFilesRepository.__init__` stores `self.__tmppath` as an absolute path. It receives it from `FSUnitOfWork._path` which is constructed via `str(Path(directory).resolve())` -- this is already absolute.

6. Search for any code path that uses relative file paths or depends on `os.getcwd()` being the NEWAVE directory. Specifically check:
   - `Caso.read(join(str(self.__tmppath), "caso.dat"))` at line 309 of files.py -- uses absolute path, safe
   - All `__regras` lambdas receive `dir` (which is `self.__tmppath`) -- safe
   - The `__agg_cmo_dfs` method at line 1154 -- uses `join(dir, ...)` -- safe

7. Run the full test suite to verify nothing breaks.

### Key Files to Modify

- `app/services/unitofwork.py` (primary change -- remove chdir calls)

### Patterns to Follow

- The existing pattern of `join(self.__tmppath, filename)` in `RawFilesRepository` is the correct approach -- all paths are constructed from the stored absolute base path. No new pattern is needed.

### Pitfalls to Avoid

- Do NOT change the `__create_repository` method -- it already uses `self._path` which is absolute
- Do NOT change how `ParquetExportRepository` receives its path -- it is already constructed from `Path(self._path).joinpath(Settings().synthesis_dir)` in `FSUnitOfWork.__create_repository`
- Do NOT remove the `_files = None` / `_exporter = None` cleanup in `__exit__` -- this is unrelated to chdir and serves a resource cleanup purpose
- Watch for any test fixtures that depend on `os.getcwd()` being changed -- if any exist, they need to be updated to use explicit paths

## Testing Requirements

### Unit Tests

- Add a test that verifies `os.getcwd()` is unchanged after `FSUnitOfWork.__enter__` and `__exit__`
- Add a test that verifies `FSUnitOfWork` can be used concurrently from multiple threads without race conditions on cwd

### Integration Tests

- Run the existing full test suite (`pytest tests/`) -- all must pass

### E2E Tests (if applicable)

- Not required for this ticket

## Dependencies

- **Blocked By**: None
- **Blocks**: ticket-002-precompute-deck-data.md

## Effort Estimate

**Points**: 3
**Confidence**: High

## Out of Scope

- Changing how `RawFilesRepository` internally constructs file paths (beyond verifying they are absolute)
- Modifying the `Deck` class caching mechanism (that is ticket-002)
- Any changes to the Click CLI or export format
