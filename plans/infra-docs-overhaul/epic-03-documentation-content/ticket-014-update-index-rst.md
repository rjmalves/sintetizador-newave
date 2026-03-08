# ticket-014 Update index.rst with New Documentation Structure

## Context

### Background

Tickets 009-013 create 5 new documentation pages (architecture, FAQ, performance, API reference, migration guide) that exist as RST files but are not linked from the main `index.rst` toctree. Without toctree entries, these pages are unreachable from the sidebar navigation and generate orphan document warnings during the Sphinx build. This ticket updates `docs/source/index.rst` to incorporate all new pages into a logically organized toctree structure.

### Relation to Epic

This is the final ticket in epic-03 (Documentation Content Expansion). It integrates all content pages created by tickets 009-013 into the site navigation. It must be implemented last because it depends on all content pages existing.

### Current State

The current `docs/source/index.rst` has three toctree sections:

1. **Apresentacao** (maxdepth 3): `apresentacao/apresentacao.rst`
2. **Geral** (maxdepth 3): `geral/instalacao`, `geral/tutorial`, `examples/index.rst`, `geral/contribuicao`
3. **Referencia** (maxdepth 2): `referencia/saidas`, `referencia/modelo`

The new pages to integrate are:

- `arquitetura/arquitetura` (from ticket-009)
- `geral/faq` (from ticket-010)
- `geral/performance` (from ticket-011)
- `referencia/api` (from ticket-012)
- `geral/migracao` (from ticket-013)

## Specification

### Requirements

1. Modify `docs/source/index.rst` to add all 5 new pages to the toctree
2. Organize pages logically within the existing toctree sections:
   - Add `arquitetura/arquitetura` as a new toctree section "Arquitetura" between Apresentacao and Geral
   - Add `geral/faq`, `geral/performance`, and `geral/migracao` to the existing "Geral" toctree section
   - Add `referencia/api` to the existing "Referencia" toctree section
3. Preserve all existing toctree entries and their order
4. Keep `maxdepth` settings consistent with existing sections

### Inputs/Props

- Current `docs/source/index.rst` content (33 lines)
- The 5 new RST files created by tickets 009-013

### Outputs/Behavior

- Updated `docs/source/index.rst` with all new pages linked in the toctree
- All pages appear in the Furo sidebar navigation in their respective sections
- No orphan document warnings during Sphinx build

### Error Handling

- Not applicable (documentation-only ticket)

## Acceptance Criteria

- [ ] Given the current `docs/source/index.rst` with 3 toctree sections, when the ticket is implemented, then the file contains 4 toctree sections: Apresentacao, Arquitetura, Geral, Referencia
- [ ] Given the updated `index.rst`, when `uv run sphinx-build -b html docs/source docs/build` is run from the repo root, then the build completes with no orphan document warnings for any of the 5 new pages
- [ ] Given the updated `index.rst`, when the HTML output is inspected, then the Furo sidebar shows all 5 new pages organized under their respective sections
- [ ] Given the updated `index.rst`, when compared to the original, then all pre-existing toctree entries (apresentacao, instalacao, tutorial, examples, contribuicao, saidas, modelo) are preserved in their original order

## Implementation Guide

### Suggested Approach

1. Open `docs/source/index.rst` and add the new toctree sections and entries. The target structure should be:

```rst
.. toctree::
   :caption: Apresentacao
   :maxdepth: 3

   apresentacao/apresentacao.rst

.. toctree::
   :caption: Arquitetura
   :maxdepth: 3

   arquitetura/arquitetura

.. toctree::
   :caption: Geral
   :maxdepth: 3

   geral/instalacao
   geral/tutorial
   examples/index.rst
   geral/performance
   geral/faq
   geral/migracao
   geral/contribuicao

.. toctree::
   :caption: Referencia
   :maxdepth: 2

   referencia/saidas
   referencia/modelo
   referencia/api
```

2. Place `performance` and `faq` after the examples and before `migracao` and `contribuicao` in the Geral section -- this groups user-facing guides together before the developer-facing contribution guide and migration guide
3. Place `api` after `modelo` in the Referencia section -- the hand-written data model reference comes first, then the auto-generated API reference

### Key Files to Modify

- `docs/source/index.rst`

### Patterns to Follow

- Keep the same toctree directive format with `:caption:` and `:maxdepth:` options
- Use relative paths without `.rst` extension for entries (except `apresentacao.rst` and `examples/index.rst` which already use the extension in the current file)
- Maintain blank lines between toctree entries and directives

### Pitfalls to Avoid

- Do NOT remove or reorder existing toctree entries
- Do NOT change `maxdepth` values for existing sections
- Do NOT add pages that were not created by tickets 009-013
- Do NOT create a separate toctree section for every new page -- group them logically into existing sections where possible

## Testing Requirements

### Unit Tests

- Not applicable (RST documentation file)

### Integration Tests

- Verify `uv run sphinx-build -b html docs/source docs/build` succeeds with no orphan warnings
- Verify all 5 new pages are reachable from the sidebar in the built HTML

### E2E Tests

- Not applicable

## Dependencies

- **Blocked By**: ticket-009-create-architecture-page.md, ticket-010-create-faq-page.md, ticket-011-create-performance-guide.md, ticket-012-create-api-reference.md, ticket-013-create-migration-guide.md
- **Blocks**: None

## Effort Estimate

**Points**: 1
**Confidence**: High
