# ticket-011 Create Performance Tuning Guide

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Criar uma pagina de guia de performance em pt-BR documentando como otimizar o uso do sintetizador-newave: uso correto do argumento `--processadores`, impacto do formato de saida (parquet vs CSV), recomendacoes de hardware, e melhores praticas para sinteses de grandes casos. O conteudo deve refletir os ganhos da recente migracao para Polars e do overhaul de performance.

## Anticipated Scope

- **Files likely to be modified**: `docs/source/geral/performance.rst` (novo), `docs/source/index.rst`
- **Key decisions needed**: Incluir benchmarks comparativos (antes/depois do Polars)? Incluir recomendacoes de memoria/CPU?
- **Open questions**: Existem dados de benchmark disponíveis do performance-overhaul que possam ser referenciados?

## Dependencies

- **Blocked By**: ticket-006-migrate-sphinx-theme.md
- **Blocks**: ticket-014-update-index-rst.md

## Effort Estimate

**Points**: 2
**Confidence**: Low (will be re-estimated during refinement)
