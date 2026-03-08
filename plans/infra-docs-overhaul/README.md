# Infrastructure & Documentation Overhaul

Modernizacao da infraestrutura de empacotamento, CI/CD e documentacao do `sintetizador-newave`.

## Tech Stack

- Python >= 3.10 (testado ate 3.14)
- Build: hatchling + uv
- CI: GitHub Actions
- Docs: Sphinx + Furo
- Lint/Format: ruff
- Type Check: mypy (strict)
- Pre-commit: ruff + mypy hooks

## Epics

| Epic | Nome                               | Tickets | Detail Level | Phase     |
| ---- | ---------------------------------- | ------- | ------------ | --------- |
| 01   | Packaging & CI Modernization       | 5       | Detailed     | Completed |
| 02   | Sphinx Documentation Modernization | 3       | Detailed     | Completed |
| 03   | Documentation Content Expansion    | 6       | Refined      | Executing |
| 04   | Repository Polish                  | 4       | Outline      | Outline   |

## Progress

| Ticket     | Title                                                    | Epic    | Status    | Detail Level | Readiness | Quality | Badge      |
| ---------- | -------------------------------------------------------- | ------- | --------- | ------------ | --------- | ------- | ---------- |
| ticket-001 | Modernize pyproject.toml and package metadata            | epic-01 | completed | Detailed     | 0.97      | 0.94    | EXCELLENT  |
| ticket-002 | Restructure CI workflow into parallel jobs               | epic-01 | completed | Detailed     | 0.97      | 1.00    | EXCELLENT  |
| ticket-003 | Migrate docs deployment to official GitHub Pages actions | epic-01 | completed | Detailed     | 0.97      | 1.00    | EXCELLENT  |
| ticket-004 | Create PyPI release workflow with trusted publishing     | epic-01 | completed | Detailed     | 0.96      | 1.00    | EXCELLENT  |
| ticket-005 | Add pre-commit hooks configuration                       | epic-01 | completed | Detailed     | 0.96      | 0.65    | BELOW GATE |
| ticket-006 | Migrate Sphinx theme from RTD to Furo                    | epic-02 | completed | Detailed     | 0.96      | 0.88    | ACCEPTABLE |
| ticket-007 | Update sphinx-gallery examples to Polars                 | epic-02 | completed | Detailed     | 0.95      | 0.75    | ACCEPTABLE |
| ticket-008 | Update existing documentation text for Polars migration  | epic-02 | completed | Detailed     | 0.95      | 0.93    | EXCELLENT  |
| ticket-009 | Create architecture overview documentation page          | epic-03 | completed | Refined      | 0.95      | 0.95    | EXCELLENT  |
| ticket-010 | Create FAQ and troubleshooting page                      | epic-03 | completed | Refined      | 0.94      | 1.00    | EXCELLENT  |
| ticket-011 | Create performance tuning guide                          | epic-03 | completed | Refined      | 0.94      | 1.00    | EXCELLENT  |
| ticket-012 | Create API reference with autodoc                        | epic-03 | completed | Refined      | 0.94      | 0.96    | EXCELLENT  |
| ticket-013 | Create v1.x to v2.x migration guide                      | epic-03 | completed | Refined      | 0.94      | 1.00    | EXCELLENT  |
| ticket-014 | Update index.rst with new documentation structure        | epic-03 | completed | Refined      | 0.97      | 1.00    | EXCELLENT  |
| ticket-015 | Expand README with badges and quickstart                 | epic-04 | pending   | Outline      | --        | --      | --         |
| ticket-016 | Create CONTRIBUTING.md at repository root                | epic-04 | pending   | Outline      | --        | --      | --         |
| ticket-017 | Reformat CHANGELOG to Keep a Changelog standard          | epic-04 | pending   | Outline      | --        | --      | --         |
| ticket-018 | Update installation docs for PyPI and uv                 | epic-04 | pending   | Outline      | --        | --      | --         |

## Dependency Graph

```
ticket-001 (pyproject.toml)
  |-> ticket-002 (CI restructure)
  |-> ticket-003 (docs deployment)
  |-> ticket-004 (release workflow)
  |-> ticket-005 (pre-commit)
  |-> ticket-006 (Furo theme)
        |-> ticket-007 (examples Polars)
        |-> ticket-008 (docs text update)
        |-> ticket-009..013 (new content pages)
              |-> ticket-014 (index.rst update)

ticket-004 -> ticket-015 (README badges)
ticket-005 -> ticket-016 (CONTRIBUTING.md)
```
