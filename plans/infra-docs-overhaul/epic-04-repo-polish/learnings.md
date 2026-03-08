# Epic-04 Learnings: Repository Polish

## Patterns Established

- **Docs badge naming**: The `docs` badge in README links to the GitHub Actions workflow badge URL (not a readthedocs or similar), pointing at `docs.yml` for status display. Observed in `/README.md` lines 7-8.
- **Badge ordering convention**: Badges are ordered tests > codecov > PyPI version > Python versions > docs > license. Each badge on its own line. Observed in `/README.md` lines 3-8.
- **Keep a Changelog footnote links**: Comparison links go in the footer of `CHANGELOG.md` as bare reference definitions (`[X.Y.Z]: https://...`), ordered newest-first with `[Unreleased]` at the top. Observed in `/CHANGELOG.md` lines 141-153.
- **CONTRIBUTING.md authority pattern**: The root `CONTRIBUTING.md` is the authoritative source; the Sphinx RST counterpart (`docs/source/geral/contribuicao.rst`) adds a `.. note::` redirect at the top but retains its original content for historical context. Observed in `/docs/source/geral/contribuicao.rst` lines 4-8.
- **RST installation structure**: PyPI section comes before git-from-source section in `instalacao.rst`. The `uv` alternative lives in its own dedicated subsection rather than being appended inside the pip section. Observed in `/docs/source/geral/instalacao.rst`.

## Architectural Decisions

- **pt-BR body text, English category headers in CHANGELOG**: Keep a Changelog category names (`Added`, `Changed`, `Fixed`, `Removed`) remain in English as the standard specifies, while all bullet-point content stays in Portuguese. Rejected: translating category headers to Portuguese, which would break tooling that parses standard category names.
- **No dates in CHANGELOG version headers**: Version headers use `## [X.Y.Z]` without dates because the original changelog had no dates and fabricating them would be inaccurate. Rejected: adding placeholder or approximate dates.
- **CONTRIBUTING.md uses Markdown tables for hook/commit type reference**: Tabular format chosen over bullet lists for the pre-commit hook summary and conventional commit types, improving scanability. Observed in `/CONTRIBUTING.md` lines 31-36 and 114-122.
- **README quickstart shows three CLI invocation patterns**: Full synthesis, selective variables, and parallelization flags are all demonstrated. Rejected: showing only the simplest invocation, which would leave users unaware of the `--processadores` flag.

## Files & Structures Created

- `/README.md` — Rewritten from 47 lines to ~85 lines. Sections: 6 badges, Sobre (3 paragraphs), Funcionalidades (bulleted), Inicio Rapido (installation + 3 CLI examples with output), Documentacao, Licenca.
- `/CONTRIBUTING.md` — New file, ~147 lines. Sections: Configuracao do Ambiente, Pre-commit Hooks (with table), Executando Testes, Verificacao de Tipos, Estilo de Codigo, Fluxo de Contribuicao (7-step numbered list), Convencoes de Commit (with table), Dependencias do Modulo inewave, Observacao sobre Documentacao.
- `/CHANGELOG.md` — Reformatted from 79 lines to ~153 lines. Follows Keep a Changelog with header, Unreleased section, 11 categorized version entries, and comparison links in the footer.
- `/docs/source/geral/instalacao.rst` — Rewritten from 20 lines to 37 lines. New structure: PyPI section first, uv section second, git-from-source section third.
- `/docs/source/geral/contribuicao.rst` — Added `.. note::` redirect block at top (4 lines prepended). Existing content preserved unchanged.

## Conventions Adopted

- **README CLI output block uses authentic log output**: The terminal output block in Inicio Rapido reuses the exact log lines from the pre-existing README rather than fabricated examples. Do not replace with synthetic output. Located in `/README.md` lines 66-76.
- **CONTRIBUTING.md omits accented characters**: The file uses unaccented Portuguese (e.g., "Configuracao" not "Configuração") consistently throughout. This matches the style the specialist agent adopted; it is intentional to avoid encoding issues in plain-text tooling. Observed throughout `/CONTRIBUTING.md`.
- **RST code blocks use `::` inline syntax** (not `.. code-block:: bash`): The project's RST files use the `section heading::` shorthand followed by indented lines, not the explicit `.. code-block::` directive. This matches the pre-existing style in `contribuicao.rst` and `instalacao.rst`.
- **CHANGELOG categorization heuristics**: "Implementada/Adicionado/Habilitado/Suporte" maps to Added; "Refatoracao/Substituida/Compatibilizacao/Otimizado" maps to Changed; "Correcao/Fix/Concatenacao" maps to Fixed; "Removidas/Descontinuado" maps to Removed. Observed in `/CHANGELOG.md`.

## Surprises & Deviations

- **CONTRIBUTING.md added two extra sections beyond ticket spec**: The implemented file includes "Dependencias do Modulo inewave" and "Observacao sobre Documentacao" sections that were not listed in the ticket's required sections. These were sourced from the pre-existing `contribuicao.rst` content and considered additive rather than out-of-scope. Located at `/CONTRIBUTING.md` lines 133-147. The guardian scored this at quality 1.0 — no penalty.
- **instalacao.rst structure differs slightly from ticket spec**: The ticket suggested a 4-section structure (PyPI, uv, git, requisitos), but the implementation merged the uv and pip sections into a sequential flow: PyPI pip first, then "Instalando com uv" as a parallel alternative, then git. The note about Python >= 3.10 moved to a `.. note::` at the top. The acceptance criteria were still fully satisfied.
- **All 4 tickets scored quality 1.0 except ticket-017 (0.96)**: ticket-017 lost 0.04 from an unknown minor scope deviation detected by the guardian — likely related to how the `[Unreleased]` section appears (it is empty as required). All scores are EXCELLENT badge.

## Recommendations for Future Epics

- When a ticket modifies a Markdown file that has a Sphinx RST counterpart (like CONTRIBUTING.md and contribuicao.rst), always update both files in the same ticket to keep them synchronized. The pattern established in ticket-016 (`/CONTRIBUTING.md` + `/docs/source/geral/contribuicao.rst`) should be the template.
- Keep a Changelog reformatting is a one-time effort. Future release notes should be written directly in Keep a Changelog format; the pattern is now established in `/CHANGELOG.md`. Use the footer comparison links section as the reference for constructing new version links.
- The README `/README.md` now serves as the project's primary discovery document. Any new major feature or CLI flag added to the project should also update the Funcionalidades bullet list in the README.
- Pure documentation tickets (Markdown, RST) consistently achieve quality 1.0 and score EXCELLENT due to non-code detection. Prefer batching related doc changes into a single ticket to reduce overhead without risk of quality degradation.
