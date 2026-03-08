# ticket-013 Create v1.x to v2.x Migration Guide

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Criar uma pagina de guia de migracao em pt-BR documentando as breaking changes entre v1.x e v2.x do sintetizador-newave. O guia deve cobrir: mudancas no formato de saida (colunas snake_case, indexacao por codigos), remocao de sinteses especificas de violacao, novo formato de estatisticas, metadados, e mudancas no CLI. O conteudo deve ser derivado do CHANGELOG.md existente.

## Anticipated Scope

- **Files likely to be modified**: `docs/source/geral/migracao.rst` (novo), `docs/source/index.rst`
- **Key decisions needed**: Nivel de detalhe do guia (lista de mudancas vs. exemplos antes/depois para cada mudanca)
- **Open questions**: Existem scripts de migracao automatica que devam ser documentados? O guia deve cobrir tambem a transicao de pandas para Polars nos parquet outputs?

## Dependencies

- **Blocked By**: ticket-006-migrate-sphinx-theme.md
- **Blocks**: ticket-014-update-index-rst.md

## Effort Estimate

**Points**: 2
**Confidence**: Low (will be re-estimated during refinement)
