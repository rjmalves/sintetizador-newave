# ticket-013 Create v1.x to v2.x Migration Guide

## Context

### Background

The sintetizador-newave v2.0.0 introduced extensive breaking changes: column renaming to snake_case, entity indexing by codes instead of names, removal of violation-specific syntheses, new statistics and metadata output files, parquet compression change (gzip to snappy), patamar column restructuring, and CLI wildcard support. These changes are documented in CHANGELOG.md but in a dense, changelog format that is hard for users to follow as a migration guide. This ticket creates a dedicated migration guide page in pt-BR with before/after examples for each breaking change.

### Relation to Epic

This is the fifth content page in epic-03 (Documentation Content Expansion). It serves users upgrading from v1.x to v2.x by providing structured guidance that the raw CHANGELOG cannot offer.

### Current State

The `CHANGELOG.md` file contains detailed entries for v2.0.0, v2.0.1, v2.1.0, v2.1.1, v2.1.2, v2.2.0, v2.2.1, and v2.3.0. The v2.0.0 section has approximately 15 breaking changes with GitHub issue references. There is no migration guide page in the documentation. The `docs/source/geral/` directory has tutorial, instalacao, and contribuicao pages but no migration guide.

## Specification

### Requirements

1. Create a new file `docs/source/geral/migracao.rst` with migration guide content in pt-BR
2. Organize the guide by category of breaking change, not chronologically
3. For each breaking change category, provide:
   - Description of what changed and why
   - Before (v1.x) vs After (v2.x) examples using `.. code-block::` directives
4. Cover at minimum these breaking change categories from v2.0.0:
   - Python version: dropped 3.8, requires >= 3.10
   - Column naming: renamed to snake_case
   - Entity indexing: codes instead of names (e.g., `usina` -> `codigo_usina`)
   - Parquet format: snappy compression (was gzip), `.parquet` extension (was `.parquet.gz`)
   - Patamar restructuring: `patamar = 0` for stage average, duration column added
   - Statistics files: new `ESTATISTICAS_*.parquet` output files
   - Metadata files: new `METADADOS_*.parquet` output files
   - Removed violation syntheses: replaced by `limite_inferior`/`limite_superior` columns
5. Include a summary table at the top listing all breaking changes with their category
6. There are no automated migration scripts -- state this explicitly

### Inputs/Props

- CHANGELOG.md v2.0.0 section (primary source for breaking changes)
- `docs/source/referencia/saidas.rst` for current output format reference

### Outputs/Behavior

- A new RST file at `docs/source/geral/migracao.rst` that renders in the Sphinx build

### Error Handling

- Not applicable (documentation-only ticket)

## Acceptance Criteria

- [ ] Given the file `docs/source/geral/migracao.rst` does not exist, when the ticket is implemented, then the file exists and contains at least 80 lines of RST content in pt-BR
- [ ] Given the new RST file, when `uv run sphinx-build -b html docs/source docs/build` is run from the repo root, then the build completes with no new warnings referencing `migracao.rst`
- [ ] Given the RST content, when rendered, then it contains a `.. list-table::` summary of breaking changes near the top of the page
- [ ] Given the RST content, when rendered, then it contains at least 4 before/after comparison pairs using `.. code-block::` directives showing v1.x vs v2.x differences

## Implementation Guide

### Suggested Approach

1. Create `docs/source/geral/migracao.rst` with the following structure:
   - Title: "Guia de Migracao v1.x para v2.x" with `=` underline
   - Section "Resumo das Mudancas": `.. list-table::` with columns (Categoria, Descricao, Impacto) listing all breaking changes
   - Section "Requisitos de Python": explain drop of 3.8 support, need for >= 3.10
   - Section "Formato das Colunas": show before (`Usina`, `Submercado`) vs after (`codigo_usina`, `codigo_submercado`) with code blocks showing DataFrame column examples
   - Section "Formato dos Arquivos Parquet": explain snappy vs gzip, extension change
   - Section "Patamares": explain new `patamar = 0` convention and `duracao_patamar` column
   - Section "Estatisticas e Metadados": explain new output files (`ESTATISTICAS_*.parquet`, `METADADOS_*.parquet`)
   - Section "Sinteses de Violacao Removidas": explain replacement by `limite_inferior`/`limite_superior` columns
   - Section "Scripts de Migracao Automatica": explicitly state that there are no automated scripts; users must update their downstream code manually
2. Use before/after pattern:

   ```rst
   **v1.x (antes)**:

   .. code-block:: none

       >>> df.columns
       ['Usina', 'Submercado', 'Estagio', 'Valor']

   **v2.x (depois)**:

   .. code-block:: none

       >>> df.columns
       ['codigo_usina', 'codigo_submercado', 'estagio', 'valor']
   ```

3. Cross-reference `referencia/saidas.rst` for the current output format: `:ref:`comandos``
4. Do NOT modify `docs/source/index.rst` -- that is ticket-014's scope

### Key Files to Modify

- `docs/source/geral/migracao.rst` (new file)

### Patterns to Follow

- Use `=` underlines for title, `-` for sections (matches existing RST style)
- Use `.. list-table::` for the summary table (matches `referencia/modelo.rst`)
- Use `.. code-block:: none` for DataFrame output examples (established in epic-02)
- Use `.. warning::` admonitions for high-impact changes
- Write in pt-BR

### Pitfalls to Avoid

- Do NOT modify `index.rst` -- toctree integration is ticket-014
- Do NOT invent migration scripts that do not exist -- explicitly state there are none
- Do NOT cover v2.1.x or v2.2.x changes in the migration guide -- those are incremental, not breaking. Only cover v1.x to v2.0.0 breaking changes

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
