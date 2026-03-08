# ticket-016 Create CONTRIBUTING.md at Repository Root

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Criar um arquivo CONTRIBUTING.md na raiz do repositorio em pt-BR, consolidando e expandindo o conteudo da pagina `contribuicao.rst` existente no Sphinx. O arquivo deve cobrir: como configurar o ambiente de desenvolvimento (com uv), como rodar testes, como usar pre-commit, convencoes de codigo, e fluxo de contribuicao via PR.

## Anticipated Scope

- **Files likely to be modified**: `/home/rogerio/git/sintetizador-newave/CONTRIBUTING.md` (novo), possivelmente `docs/source/geral/contribuicao.rst` (para referenciar o arquivo raiz)
- **Key decisions needed**: Duplicar conteudo entre CONTRIBUTING.md e contribuicao.rst, ou CONTRIBUTING.md ser a fonte unica e contribuicao.rst referenciar? Pre-commit hooks estarao configurados (depende do ticket-005)
- **Open questions**: O CONTRIBUTING.md deve incluir instrucoes para pre-commit hooks instalados no ticket-005?

## Dependencies

- **Blocked By**: ticket-005-add-pre-commit-hooks.md
- **Blocks**: Nenhum

## Effort Estimate

**Points**: 2
**Confidence**: Low (will be re-estimated during refinement)
