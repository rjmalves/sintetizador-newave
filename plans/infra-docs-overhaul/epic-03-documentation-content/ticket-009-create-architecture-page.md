# ticket-009 Create Architecture Overview Documentation Page

## Context

### Background

The sintetizador-newave project currently has user-facing documentation (tutorial, installation, contribution guide) and reference documentation (output format, data model), but lacks a page explaining the internal architecture of the application. This makes onboarding for new contributors difficult, as they must read the source code to understand how the application layers interact. This ticket creates a new RST page in pt-BR describing the internal architecture: the layered structure (CLI, domain, services/deck, model, adapters, utils), the data flow from NEWAVE file reading through to parquet synthesis output, and the role of each major module.

### Relation to Epic

This is the first content page in epic-03 (Documentation Content Expansion). It establishes the architectural narrative that other pages (FAQ, performance guide) can reference. The epic's goal is to add 5 new pt-BR pages covering architecture, FAQ, performance, API reference, and migration guide.

### Current State

The `docs/source/` directory has three toctree sections: Apresentacao, Geral (instalacao, tutorial, examples, contribuicao), and Referencia (saidas, modelo). There is no architecture documentation page. The Furo theme is active (epic-02 completed). The app has the following top-level packages: `adapters/` (repository pattern for file I/O and export), `domain/` (command dataclasses), `internal/` (constants), `model/` (data model enums and dataclasses for operation, scenario, execution, policy, system), `services/` (deck readers, synthesis orchestrators, unit of work), `utils/` (logging, timing, encoding, graph, singleton, regex, terminal), and `static/` (shell scripts).

## Specification

### Requirements

1. Create a new file `docs/source/arquitetura/arquitetura.rst` with an architecture overview in pt-BR
2. The page must describe each layer of the application with its purpose and key files
3. The page must describe the data flow: CLI entry point -> command dispatch -> Deck (inewave file reading) -> Synthetizer pipeline -> export (parquet/CSV)
4. Include a text-based data flow diagram using a `.. code-block:: none` block (no Mermaid or image dependencies -- keeps the build simple and avoids adding sphinx extensions)
5. The page must follow existing RST conventions: section headers with underlines (`=` for title, `-` for sections, `~` for subsections), `.. code-block:: python` for code, `.. list-table::` for tables
6. Write all content in pt-BR, consistent with the existing documentation language

### Inputs/Props

- Source code in `app/` for accurate module descriptions
- Existing RST style from `docs/source/geral/tutorial.rst` and `docs/source/apresentacao/apresentacao.rst`

### Outputs/Behavior

- A new RST file at `docs/source/arquitetura/arquitetura.rst` that renders in the Sphinx build
- The page appears in the Furo sidebar under a new toctree section (actual toctree integration is in ticket-014)

### Error Handling

- Not applicable (documentation-only ticket)

## Acceptance Criteria

- [ ] Given the file `docs/source/arquitetura/arquitetura.rst` does not exist, when the ticket is implemented, then the file exists and contains at least 80 lines of RST content in pt-BR
- [ ] Given the new RST file, when `uv run sphinx-build -b html docs/source docs/build` is run from the repo root, then the build completes with no new warnings referencing `arquitetura.rst`
- [ ] Given the RST content, when rendered, then it contains sections describing at least 5 of the 6 app packages: `adapters`, `domain`, `model`, `services`, `utils`, `internal`
- [ ] Given the RST content, when rendered, then it contains a data flow diagram in a `.. code-block:: none` block showing the pipeline from CLI to parquet output

## Implementation Guide

### Suggested Approach

1. Create the directory `docs/source/arquitetura/` if it does not exist
2. Create `docs/source/arquitetura/arquitetura.rst` with the following structure:
   - Title: "Arquitetura Interna" with `=` underline
   - Section "Visao Geral": 2-3 paragraph overview of the layered architecture
   - Section "Fluxo de Dados": text-based flow diagram in `.. code-block:: none` showing: `CLI (click) -> Command (domain) -> Handler (services.handlers) -> Synthetizer -> Deck (services.deck) -> inewave -> DataFrames -> Export (adapters.repository.export) -> parquet/CSV`
   - Section "Modulos Principais": subsections for each package with a `.. list-table::` listing key files and their purpose
   - Section "Modelo de Dados": brief description of the model/ package structure (operation, scenario, execution, policy, system) with references to the existing `referencia/modelo.rst` page using `:ref:`modelo``
3. Follow the RST conventions established in epic-02: use `.. code-block:: python` for code snippets, `.. code-block:: none` for output/diagrams
4. Do NOT modify `docs/source/index.rst` -- that is ticket-014's scope

### Key Files to Modify

- `docs/source/arquitetura/arquitetura.rst` (new file)

### Patterns to Follow

- Use `=` underlines for page title, `-` for sections, `~` for subsections (matches `tutorial.rst`, `contribuicao.rst`)
- Use `.. list-table::` with `:widths:` and `:header-rows:` for tables (matches `referencia/modelo.rst`)
- Use `.. code-block:: python` for code examples, `.. code-block:: none` for output blocks (established in epic-02)
- Write in pt-BR with accented characters (matches all existing pages)
- Reference other pages with `:ref:` cross-references where applicable

### Pitfalls to Avoid

- Do NOT add sphinx-mermaid or any new Sphinx extension -- use text-based diagrams only
- Do NOT modify `index.rst` -- toctree integration is ticket-014
- Do NOT add the page to any toctree in this ticket
- Do NOT document private/internal implementation details of individual functions -- keep it at the module/class level

## Testing Requirements

### Unit Tests

- Not applicable (RST documentation file)

### Integration Tests

- Verify `uv run sphinx-build -b html docs/source docs/build` succeeds without new warnings

### E2E Tests

- Not applicable

## Dependencies

- **Blocked By**: ticket-006-migrate-sphinx-theme.md (Furo theme must be active)
- **Blocks**: ticket-014-update-index-rst.md

## Effort Estimate

**Points**: 3
**Confidence**: High
