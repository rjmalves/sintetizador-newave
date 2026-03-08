# Accumulated Learnings: infra-docs-overhaul (through Epic-04)

## Project Structure

- 92 Python source files in `./app`, 222 tests in `./tests`; Polars DataFrames used throughout
- Build backend: hatchling via uv; CI matrix covers Python 3.10-3.14
- `app/` has `py.typed` marker — downstream consumers can use strict typing
- Sphinx docs live in `docs/source/`; `geral/` subdir holds instalacao.rst, contribuicao.rst, tutorial.rst
- sphinx-gallery examples live in `examples/` (outside `./app` scope, so mypy/ruff app checks don't cover them)

## Documentation Architecture

- All documentation is in pt-BR with accented characters (RST, Sphinx pages, README quickstart text)
- Exception: CHANGELOG category headers (Added/Changed/Fixed/Removed) stay in English per Keep a Changelog standard
- README.md is the primary discovery document; Sphinx site at rjmalves.github.io/sintetizador-newave is the full reference
- CONTRIBUTING.md at root is the authoritative contributor guide; docs/source/geral/contribuicao.rst contains a `.. note::` redirect and preserves older content
- RST code blocks use the `::` inline shorthand, not `.. code-block:: bash` (see `instalacao.rst`, `contribuicao.rst`)

## Key Infrastructure Decisions

- mypy strict mode requires `ignore_missing_imports = true` because `inewave` and `cfinterface` lack type stubs
- mypy pre-commit hook runs on `./app` as a whole (`pass_filenames: false`) — per-file mode causes false positives
- ruff-pre-commit uses official astral-sh repo; version pinned at v0.15.5 to match lockfile
- Sphinx build has 1 pre-existing harmless warning: `html_static_path entry '_static' does not exist`
- Docs badge in README points to GitHub Actions `docs.yml` badge URL, not an external docs service
- `id-token: write` is scoped at job level in release.yml, at workflow level in docs.yml

## Patterns Confirmed Across Epics

- Pure RST/Markdown tickets score quality 1.0 (EXCELLENT) consistently due to non-code detection in the quality scorer
- Example `.py` scripts in `examples/` are penalized on lint/type dimensions since they count as code but aren't in `./app` scope
- Cross-references in RST use `:doc:` for same-directory links and `:ref:` for labeled targets
- Text-based diagrams use `.. code-block:: none` rather than Mermaid (no external extension added)
- Autosummary sub-pages produce `toc.not_included` warnings during Sphinx build — this is normal, not an error

## README Conventions (established ticket-015)

- Badge order: tests > codecov > PyPI version > Python versions > docs > license; each badge on its own line
- README CLI output block uses authentic log lines from the original README — do not fabricate
- Funcionalidades section must be updated when new CLI flags or major features are added (`/README.md`)

## CHANGELOG Conventions (established ticket-017)

- Format: Keep a Changelog with `## [X.Y.Z]` headers, categorized bullet points, empty `## [Unreleased]` section
- Footer comparison links ordered newest-first: `[Unreleased]` > `[2.3.0]` > ... > `[1.0.0]` using tags with `v` prefix
- Categorization heuristics: Implementada/Adicionado/Habilitado -> Added; Refatoracao/Substituida -> Changed; Correcao/Fix -> Fixed; Removidas/Descontinuado -> Removed

## Scope Surprises Worth Remembering

- ticket-005 (pre-commit hooks) expanded from 2 config files to ~65 source files due to pre-existing mypy/ruff violations across `./app` — fixing strict typing was the largest effort in the whole plan
- ticket-016 (CONTRIBUTING.md) implementation added two sections beyond the ticket spec ("Dependencias do Modulo inewave", "Observacao sobre Documentacao"), sourced from the existing RST; the guardian did not penalize because criteria were all met
- ticket-018 (instalacao.rst) merged the ticket's 4-section structure into 3 sections with a top-level `.. note::` for Python version; all acceptance criteria still satisfied

## Agent Selection Patterns

- Config/CI tickets (YAML, TOML, workflows): `python-task-automation-developer`
- Sphinx RST pages (new content): `open-source-documentation-writer`
- Sphinx conf.py + theme migration: `python-task-automation-developer`
- Root Markdown (README, CONTRIBUTING, CHANGELOG): `open-source-documentation-writer`

## Quality Score Summary by Epic

- Epic-01 (packaging/CI): mean quality ~0.92, one outlier ticket-005 at 0.65 (scope explosion)
- Epic-02 (Sphinx theme): mean quality ~0.88, ticket-007 (gallery examples) at 0.75 due to lint/type penalties on `.py` files outside `./app`
- Epic-03 (doc content): mean quality ~0.99, all RST-only tickets, highest-scoring epic
- Epic-04 (repo polish): mean quality ~0.99, all Markdown/RST tickets, consistently EXCELLENT
