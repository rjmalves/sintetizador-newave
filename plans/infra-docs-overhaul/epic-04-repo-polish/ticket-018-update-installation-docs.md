# ticket-018 Update Installation Docs for PyPI and uv

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Atualizar a pagina de instalacao (`instalacao.rst`) para incluir instrucoes de instalacao via PyPI (`pip install sintetizador-newave`), instrucoes usando `uv` (`uv pip install sintetizador-newave`), e manter as instrucoes existentes de instalacao via git. Reorganizar a pagina para que o metodo mais simples (PyPI) seja apresentado primeiro.

## Anticipated Scope

- **Files likely to be modified**: `/home/rogerio/git/sintetizador-newave/docs/source/geral/instalacao.rst`
- **Key decisions needed**: Incluir instrucoes para Docker? Incluir requisitos de sistema (Python >= 3.10)?
- **Open questions**: O PyPI publish estara ativo quando este ticket for implementado? Se nao, documentar como "em breve"?

## Dependencies

- **Blocked By**: ticket-004-create-release-workflow.md
- **Blocks**: Nenhum

## Effort Estimate

**Points**: 1
**Confidence**: Low (will be re-estimated during refinement)
