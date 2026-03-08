# Epic 01 — Packaging & CI: Learnings

## Patterns Established

1. **uv-first CI pattern** — All GitHub Actions jobs use `astral-sh/setup-uv@v7` with `enable-cache: true` and `uv python install <version>` instead of `actions/setup-python`. No separate Python setup action is needed. Canonical form visible in `.github/workflows/main.yml`.

2. **4-job parallel CI structure** — `lint`, `typecheck`, `test`, `docs` run fully in parallel with no `needs` dependencies between them. `lint` and `typecheck` pin to a single Python version (3.12); `test` uses the full matrix [3.10–3.14]; `docs` uses a single version. Pattern in `.github/workflows/main.yml`.

3. **mypy strict with module-level overrides** — `[tool.mypy]` sets `strict = true` with `ignore_missing_imports = true` at the top level. For libraries that generate too many false positives (inewave, dateutil), use `[[tool.mypy.overrides]] module = ["inewave.*"] ignore_errors = true` rather than scattering `# type: ignore` at call sites. Pattern in `pyproject.toml`.

4. **ruff-native pre-commit hook** — Use `repo: https://github.com/astral-sh/ruff-pre-commit` (not `repo: local`) for ruff hooks; this runs a pre-compiled binary that is significantly faster than invoking ruff through uv. The mypy hook must be `repo: local` with `pass_filenames: false` so it checks the whole project (not individual staged files). Pattern in `.pre-commit-config.yaml`.

5. **Official GitHub Pages deployment** — Two-job split: `build` runs sphinx and `actions/upload-pages-artifact@v4`; `deploy` uses `actions/deploy-pages@v4` with `needs: build`, `environment: github-pages`, and permissions `pages: write` + `id-token: write`. This replaces the third-party `peaceiris/actions-gh-pages` action. Pattern in `.github/workflows/docs.yml`.

6. **PyPI trusted publishing** — Release workflow uses OIDC instead of stored API keys: `pypa/gh-action-pypi-publish@release/v1` with `id-token: write` permission scoped only to the `publish` job, protected by `environment: pypi`. The workflow chain is `test (matrix 3.10/3.12/3.14) -> build -> publish`. Pattern in `.github/workflows/release.yml`.

## Architectural Decisions

1. **`ignore_errors = true` per module over `# type: ignore` at call sites** — Rejected option was suppressing errors per-line in every accessor file. The `[[tool.mypy.overrides]]` approach in `pyproject.toml` eliminates ~60 suppressions per inewave-importing file, keeps import lines clean, and centralizes the suppression decision in one place. Chosen because inewave's API is stable and unlikely to develop stubs soon.

2. **`types-python-dateutil` as dev dependency** — dateutil is used at runtime but has no bundled stubs. Adding `types-python-dateutil` to `[project.optional-dependencies] dev` in `pyproject.toml` allows mypy strict mode to type-check dateutil call sites correctly rather than falling back to `Any`. A `[[tool.mypy.overrides]]` block for `dateutil.*` was kept as a safety net.

3. **Broad `Any` annotation for `deck_cls` parameter** — The `deck_cls` parameter in `app/services/deck/accessors.py` functions is typed as `Any` rather than a Protocol or ABC. Rejected option was defining a Protocol with all accessor method signatures (would require upfront design of ~60 methods). The `Any` annotation is acceptable because the parameter is always the `Deck` class object; this boundary will be revisited when inewave types stabilize.

4. **sphinx-build without `-W` flag in CI** — The `main.yml` docs job uses `-W` (treat warnings as errors). If pre-existing sphinx warnings prevent this, the `-W` flag should be removed temporarily and a tracking issue opened. This was documented in ticket-002 pitfalls; the final implementation includes `-W`, indicating no pre-existing sphinx warnings existed at the time of implementation.

## Files & Structures Created

- `pyproject.toml` — Modernized: removed `numba`, added classifiers for Python 3.10–3.14, `[tool.mypy]` section, `types-python-dateutil` and `pre-commit` in dev deps, `furo` alongside `sphinx-rtd-theme`
- `app/py.typed` — Empty PEP 561 marker file that declares this package as typed
- `.github/workflows/main.yml` — Rewrote from single sequential job to 4 parallel jobs
- `.github/workflows/docs.yml` — Rewrote from peaceiris-based to official GitHub Pages deployment
- `.github/workflows/release.yml` — New file; test/build/publish chain with OIDC trusted publishing
- `.pre-commit-config.yaml` — New file; ruff-precommit + local mypy hooks

## Conventions Adopted

1. **mypy suppression discipline** — Only use `# type: ignore[<code>]` (with error code) for isolated call sites that cannot be otherwise resolved. Bare `# type: ignore` without an error code is discouraged. Currently 21 suppressions in `app/` with the following distribution: `attr-defined` (6), `return-value` (5), `union-attr` (2), `assignment` (2), `arg-type` (2), `no-any-return` (1), bare (3 legacy). Do not add bare suppressions.

2. **Callable type annotation style** — When a Dict maps to callables with unknown signatures, use `Callable[..., Any]` not bare `Callable`. Observed in `app/services/deck/bounds.py` (`MAPPINGS` dict).

3. **Return type annotation on every method** — mypy strict requires explicit return types. Methods that return `None` must declare `-> None`. Methods previously written without return types had them added uniformly across ~50 files in this epic.

4. **`MPQueue` alias for `multiprocessing.queues.Queue`** — To avoid shadowing the builtin and to carry the generic type parameter, the import is aliased: `from multiprocessing.queues import Queue as MPQueue`. Used in `app/utils/log.py` as `MPQueue[logging.LogRecord]`.

## Surprises & Deviations

1. **Scope expansion for ticket-005** — The ticket specified creating `.pre-commit-config.yaml` and adding `pre-commit` to `pyproject.toml`. Actual implementation required fixing all pre-existing mypy and ruff errors across approximately 50 Python source files before `pre-commit run --all-files` could pass. This touched `app/` and `tests/` directories extensively. The ticket scored `scope_adherence: 0.0` (quality score 0.65) because of this undeclared expansion. In future epics, if a pre-commit or lint ticket is planned, a prerequisite ticket should explicitly scope the "fix all existing violations" work first.

2. **`[[tool.mypy.overrides]]` was not in the original ticket specification** — ticket-001 specified a `[tool.mypy]` section with `strict = true`, `warn_return_any = true`, `warn_unused_configs = true`, `ignore_missing_imports = true`. The implementation added two `[[tool.mypy.overrides]]` blocks (for `inewave.*` and `dateutil.*`) beyond what was specified. This was necessary to get mypy to pass; the ticket's suggested approach was insufficient for a codebase that heavily uses inewave internals.

3. **`types-python-dateutil` not anticipated** — The ticket did not mention this dev dependency. It was discovered when mypy strict reported missing stubs for `dateutil.relativedelta` imports. This is a pattern to anticipate for any project using libraries without bundled stubs.

4. **ruff-pre-commit version pinned to `v0.15.5` not `v0.15.0`** — The ticket suggested `v0.15.0` as the example version. The implementation used `v0.15.5` to match the actual installed ruff version from `uv.lock`. This is the correct practice (pin to the version in the lockfile).

## Recommendations for Future Epics

1. **Decouple "fix violations" from "add tooling" tickets** — When a linting or type-checking tool is being enabled for the first time on a large codebase, create a separate ticket scoped explicitly to clearing all pre-existing violations (estimated count, affected files) before the ticket that installs the hook/gate. See the scope expansion in ticket-005.

2. **Audit inewave `type: ignore[attr-defined]` regularly** — Currently 6 call sites in `app/services/deck/accessors.py` suppress `attr-defined` errors for inewave attributes. If inewave adds stubs in a future version, these suppressions will become redundant and mypy will warn about unnecessary ignores (unless `[[tool.mypy.overrides]] ignore_errors = true` is removed first).

3. **The `sphinx-build -W` flag in CI is now active** — Any future documentation change that introduces a sphinx warning will break the CI `docs` job in `.github/workflows/main.yml`. This is intentional, but contributors must be aware. Document this in CONTRIBUTING.md when epic-04 creates that file.

4. **PyPI trusted publishing requires manual one-time setup** — Before the first release, the repository owner must configure a trusted publisher on PyPI (Settings > Publishing > Add publisher) with the exact environment name `pypi` and workflow name `release.yml`. This step cannot be automated. Document this in the release runbook when created.
