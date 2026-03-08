# ticket-010 Create FAQ and Troubleshooting Page

## Context

### Background

Users of sintetizador-newave encounter recurring issues related to file reading errors, NEWAVE version incompatibilities, output format questions, parallelism configuration, and installation problems. There is no centralized FAQ or troubleshooting page in the documentation. This ticket creates a new RST page in pt-BR with a Q&A-style FAQ covering the most common issues, organized by thematic sections.

### Relation to Epic

This is the second content page in epic-03 (Documentation Content Expansion). It complements the tutorial and architecture pages by addressing common pain points that are not covered in the step-by-step tutorial.

### Current State

The `docs/source/geral/` directory contains `instalacao.rst`, `tutorial.rst`, and `contribuicao.rst`. There is no FAQ or troubleshooting page. The tutorial covers basic CLI usage but does not address error scenarios or common configuration issues. The Furo theme is active with its built-in search functionality.

## Specification

### Requirements

1. Create a new file `docs/source/geral/faq.rst` with FAQ content in pt-BR
2. Organize the FAQ into thematic sections (not a flat list): Instalacao, Uso Basico, Formato de Saida, Paralelismo, Erros Comuns
3. Each Q&A item must have a clear question as a subsection header and a concise answer
4. Include at least 3 Q&A items per section (minimum 15 total)
5. Include code examples (CLI commands, error messages) where relevant using `.. code-block::` directives
6. Cross-reference existing pages (tutorial, instalacao) with `:doc:` or `:ref:` links where answers overlap

### Inputs/Props

- CLI help output from `sintetizador-newave --help` and subcommand help
- CHANGELOG.md for known issues and breaking changes
- Existing tutorial.rst for topics that need supplemental troubleshooting

### Outputs/Behavior

- A new RST file at `docs/source/geral/faq.rst` that renders in the Sphinx build
- The page is structured with clear section headers that work with Furo's sidebar navigation

### Error Handling

- Not applicable (documentation-only ticket)

## Acceptance Criteria

- [ ] Given the file `docs/source/geral/faq.rst` does not exist, when the ticket is implemented, then the file exists and contains at least 100 lines of RST content in pt-BR
- [ ] Given the new RST file, when `uv run sphinx-build -b html docs/source docs/build` is run from the repo root, then the build completes with no new warnings referencing `faq.rst`
- [ ] Given the RST content, when rendered, then it contains at least 5 thematic sections with at least 3 Q&A items each
- [ ] Given the RST content, when rendered, then it contains at least 2 cross-references to other documentation pages using `:doc:` or `:ref:` directives

## Implementation Guide

### Suggested Approach

1. Create `docs/source/geral/faq.rst` with the following structure:
   - Title: "FAQ e Troubleshooting" with `=` underline
   - Section "Instalacao": Q&A on Python version requirements (>= 3.10), pip vs uv installation, inewave version compatibility
   - Section "Uso Basico": Q&A on running synthesis, available commands (completa, operacao, cenarios, etc.), wildcard usage (`*`), running from a different directory (APP_BASEDIR)
   - Section "Formato de Saida": Q&A on parquet vs CSV (`--formato`), column naming (snake_case since v2.0), entity indexing by codes, reading parquet files with Polars/pandas
   - Section "Paralelismo": Q&A on `--processadores` flag, which commands support it (operacao, cenarios, completa), recommended number of processors
   - Section "Erros Comuns": Q&A on "arquivo nao encontrado" errors (wrong working directory), inewave version mismatch, NEWAVE version incompatibilities (>= 29.4 file renaming)
2. Use question-as-subsection pattern: `Como instalar o sintetizador-newave?` as a subsection with `-` underline, followed by the answer paragraph and code blocks
3. Cross-reference the tutorial page for basic usage: `:doc:`tutorial`` and installation page: `:doc:`instalacao``
4. Do NOT modify `docs/source/index.rst` -- that is ticket-014's scope

### Key Files to Modify

- `docs/source/geral/faq.rst` (new file)

### Patterns to Follow

- Use `=` underlines for page title, `-` for section headers, `~` for Q&A subsection headers (matches existing RST style)
- Use `.. code-block:: bash` for CLI commands, `.. code-block:: none` for output (matches tutorial.rst style using `::` blocks)
- Use `.. warning::` and `.. note::` admonitions for important caveats (matches contribuicao.rst)
- Write in pt-BR with accented characters

### Pitfalls to Avoid

- Do NOT modify `index.rst` -- toctree integration is ticket-014
- Do NOT duplicate content from tutorial.rst verbatim -- cross-reference instead
- Do NOT include speculative answers -- only document known behaviors from the codebase and CHANGELOG

## Testing Requirements

### Unit Tests

- Not applicable (RST documentation file)

### Integration Tests

- Verify `uv run sphinx-build -b html docs/source docs/build` succeeds without new warnings

### E2E Tests

- Not applicable

## Dependencies

- **Blocked By**: ticket-006-migrate-sphinx-theme.md
- **Blocks**: ticket-014-update-index-rst.md

## Effort Estimate

**Points**: 2
**Confidence**: High
