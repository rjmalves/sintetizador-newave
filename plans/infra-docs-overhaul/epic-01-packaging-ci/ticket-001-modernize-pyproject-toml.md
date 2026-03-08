# ticket-001 Modernize pyproject.toml and Package Metadata

## Context

### Background

O `pyproject.toml` atual do `sintetizador-newave` contem dependencias obsoletas (`numba`, que nao e mais importado em nenhum modulo), classifiers desatualizados (listam apenas Python 3.10), nao possui configuracao de mypy, e o marker `py.typed` esta ausente. O lockfile `uv.lock` esta no `.gitignore`, impedindo builds reprodutiveis em CI.

### Relation to Epic

Este e o primeiro ticket do Epic 01 (Packaging & CI Modernization). Todas as alteracoes subsequentes em CI e docs dependem de um `pyproject.toml` limpo e correto.

### Current State

O arquivo `/home/rogerio/git/sintetizador-newave/pyproject.toml` possui:

- `numba>=0.60.0` como dependencia obrigatoria (nao importado em nenhum arquivo .py)
- Classifiers listam apenas `Python :: 3.10`
- Nenhuma secao `[tool.mypy]`
- `description` minimalista: apenas `"sintetizador-newave"`
- Dev dependencies incluem `sphinx-rtd-theme` (sera substituido por `furo` no epic 02, mas aqui adicionamos `furo` ao lado para permitir build durante a transicao)
- `uv.lock` esta em `.gitignore` na linha 139

O marker file `app/py.typed` nao existe.

## Specification

### Requirements

1. Remover `numba>=0.60.0` da lista `dependencies`
2. Adicionar classifiers para Python 3.11, 3.12, 3.13, 3.14
3. Atualizar `description` para frase descritiva em pt-BR
4. Adicionar secao `[tool.mypy]` com configuracao strict
5. Criar arquivo vazio `app/py.typed`
6. Remover a linha `uv.lock` do `.gitignore`
7. Adicionar `furo` ao `[project.optional-dependencies] dev` (ao lado de `sphinx-rtd-theme`, que sera removido no epic 02)
8. Atualizar `Development Status` classifier de `4 - Beta` para `5 - Production/Stable` (projeto esta na v2.3.0)

### Inputs/Props

- Arquivo fonte: `/home/rogerio/git/sintetizador-newave/pyproject.toml`
- Arquivo fonte: `/home/rogerio/git/sintetizador-newave/.gitignore`
- Novo arquivo: `/home/rogerio/git/sintetizador-newave/app/py.typed`

### Outputs/Behavior

- `pyproject.toml` atualizado com todas as mudancas acima
- `.gitignore` sem a linha `uv.lock`
- `app/py.typed` existente (arquivo vazio)
- `uv sync` deve funcionar sem erros apos as mudancas

### Error Handling

- Se `uv sync` falhar apos remocao de numba, verificar se alguma dependencia transitiva requer numba (nao deve, pois numba nao e importado)

## Acceptance Criteria

- [ ] Given o arquivo `pyproject.toml`, when inspecionado, then a string `numba` nao aparece em nenhuma linha
- [ ] Given o arquivo `pyproject.toml`, when a secao `classifiers` e lida, then contem entradas para Python 3.10, 3.11, 3.12, 3.13 e 3.14
- [ ] Given o arquivo `pyproject.toml`, when a secao `[tool.mypy]` e lida, then `strict = true` esta presente
- [ ] Given o arquivo `app/py.typed`, when verificada sua existencia com `test -f app/py.typed`, then o comando retorna exit code 0
- [ ] Given o arquivo `.gitignore`, when inspecionado, then a string `uv.lock` nao aparece em nenhuma linha

## Implementation Guide

### Suggested Approach

1. Editar `pyproject.toml`:
   - Remover `"numba>=0.60.0",` da lista `dependencies`
   - Substituir `description = "sintetizador-newave"` por `description = "Aplicacao CLI para sintese de dados do modelo NEWAVE"`
   - Substituir `"Development Status :: 4 - Beta"` por `"Development Status :: 5 - Production/Stable"`
   - Adicionar classifiers: `"Programming Language :: Python :: 3.11"`, `"Programming Language :: Python :: 3.12"`, `"Programming Language :: Python :: 3.13"`, `"Programming Language :: Python :: 3.14"`
   - Adicionar `"furo"` a lista `dev` em `[project.optional-dependencies]`
   - Adicionar secao ao final do arquivo:
     ```toml
     [tool.mypy]
     strict = true
     warn_return_any = true
     warn_unused_configs = true
     ignore_missing_imports = true
     ```
2. Criar arquivo vazio `app/py.typed`
3. Editar `.gitignore`: remover a linha `uv.lock`
4. Executar `uv sync --all-extras --dev` para validar
5. Executar `uv run mypy ./app` para verificar que a config mypy carrega (pode ter erros pre-existentes; isso e esperado e nao bloqueia este ticket)

### Key Files to Modify

- `/home/rogerio/git/sintetizador-newave/pyproject.toml`
- `/home/rogerio/git/sintetizador-newave/.gitignore`
- `/home/rogerio/git/sintetizador-newave/app/py.typed` (novo)

### Patterns to Follow

- Manter formatacao TOML existente (indentacao com 4 espacos, strings entre aspas duplas)
- Classifiers seguem o padrao PyPI: `"Programming Language :: Python :: X.Y"`

### Pitfalls to Avoid

- Nao remover `pandas` das dependencias (ainda usado na fronteira com inewave)
- Nao remover `sphinx-rtd-theme` do dev dependencies agora (sera feito no epic 02 quando furo estiver configurado)
- Nao definir `mypy strict` sem `ignore_missing_imports = true`, pois dependencias como `inewave` e `cfinterface` nao tem stubs

## Testing Requirements

### Unit Tests

- Nenhum teste unitario novo necessario

### Integration Tests

- Executar `uv sync --all-extras --dev` e verificar exit code 0
- Executar `uv build` e verificar que sdist e wheel sao gerados sem erro
- Verificar que `py.typed` esta incluido no wheel: `unzip -l dist/*.whl | grep py.typed`

### E2E Tests

- N/A

## Dependencies

- **Blocked By**: Nenhum
- **Blocks**: ticket-002-restructure-ci-workflow.md, ticket-003-migrate-docs-deployment.md, ticket-004-create-release-workflow.md, ticket-005-add-pre-commit-hooks.md, ticket-006-migrate-sphinx-theme.md

## Effort Estimate

**Points**: 3
**Confidence**: High
