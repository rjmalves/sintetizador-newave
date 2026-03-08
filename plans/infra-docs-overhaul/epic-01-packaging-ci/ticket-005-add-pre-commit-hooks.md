# ticket-005 Add Pre-commit Hooks Configuration

## Context

### Background

O projeto nao tem pre-commit hooks configurados. Desenvolvedores dependem de executar `ruff check`, `ruff format` e `mypy` manualmente antes de fazer push, ou descobrem problemas somente quando o CI falha. Pre-commit hooks automatizam estas verificacoes antes de cada commit.

### Relation to Epic

Quinto e ultimo ticket do Epic 01. Depende do ticket-001 para mypy config no pyproject.toml.

### Current State

- Nao existe `.pre-commit-config.yaml` no repositorio
- Verificacoes de qualidade sao feitas apenas no CI
- Ferramentas ja configuradas no pyproject.toml: ruff (line-length 80, select E/F/W/I)

## Specification

### Requirements

1. Criar `.pre-commit-config.yaml` na raiz do repositorio
2. Incluir hook para `ruff check` (lint)
3. Incluir hook para `ruff format` (formatacao)
4. Incluir hook para `mypy` (type checking)
5. Usar ruff como hook nativo (repo: https://github.com/astral-sh/ruff-pre-commit) com versao fixada
6. Usar mypy como hook local (via `uv run mypy`) para respeitar a configuracao do projeto
7. Adicionar `pre-commit` ao `[project.optional-dependencies] dev` no `pyproject.toml`

### Inputs/Props

- Novo arquivo: `/home/rogerio/git/sintetizador-newave/.pre-commit-config.yaml`
- Arquivo a editar: `/home/rogerio/git/sintetizador-newave/pyproject.toml` (adicionar pre-commit ao dev deps)

### Outputs/Behavior

- `pre-commit install` configura os hooks no repositorio local
- `pre-commit run --all-files` executa todos os hooks sem erros no estado atual do codigo
- Cada `git commit` executa automaticamente ruff check, ruff format e mypy nos arquivos staged

### Error Handling

- Se mypy falhar em arquivos que nao foram modificados, o hook so deve verificar arquivos staged (configuracao padrao do pre-commit)

## Acceptance Criteria

- [ ] Given o arquivo `.pre-commit-config.yaml`, when inspecionado, then contem pelo menos 3 hooks: ruff-check, ruff-format, mypy
- [ ] Given o comando `uv run pre-commit run --all-files`, when executado na raiz do projeto, then o exit code e 0
- [ ] Given o arquivo `pyproject.toml`, when a secao `dev` de optional-dependencies e inspecionada, then contem `"pre-commit"`
- [ ] Given o arquivo `.pre-commit-config.yaml`, when o hook de ruff e inspecionado, then o repo e `https://github.com/astral-sh/ruff-pre-commit` com versao fixada

## Implementation Guide

### Suggested Approach

1. Adicionar `"pre-commit"` a lista `dev` em `[project.optional-dependencies]` no `pyproject.toml`

2. Criar `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.15.0
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format

  - repo: local
    hooks:
      - id: mypy
        name: mypy
        entry: uv run mypy
        language: system
        types: [python]
        pass_filenames: false
        args: [./app]
```

3. Executar `uv sync --all-extras --dev` para instalar pre-commit
4. Executar `uv run pre-commit run --all-files` para validar

Nota: A versao do ruff-pre-commit (`v0.15.0`) deve corresponder a versao do ruff instalada no projeto. Verificar a versao atual com `uv run ruff --version` e ajustar o `rev` de acordo.

### Key Files to Modify

- `/home/rogerio/git/sintetizador-newave/.pre-commit-config.yaml` (novo)
- `/home/rogerio/git/sintetizador-newave/pyproject.toml` (adicionar pre-commit ao dev deps)

### Patterns to Follow

- Usar ruff-pre-commit do repo oficial do astral-sh (mais rapido que executar via local hook)
- Usar hook local para mypy (necessario para que use a configuracao e venv do projeto)
- Fixar versoes nos hooks para reprodutibilidade

### Pitfalls to Avoid

- Nao usar `repo: local` para ruff — o hook oficial do astral-sh e significativamente mais rapido
- Nao usar `pass_filenames: true` no hook mypy — mypy precisa verificar o projeto inteiro para type inference funcionar; passar arquivos individuais causa falsos positivos
- Verificar que a versao do ruff no pre-commit config corresponde a versao no lockfile

## Testing Requirements

### Unit Tests

- N/A

### Integration Tests

- `uv run pre-commit run --all-files` deve retornar exit code 0
- `uv run pre-commit run ruff --all-files` deve retornar exit code 0
- `uv run pre-commit run ruff-format --all-files` deve retornar exit code 0

### E2E Tests

- N/A

## Dependencies

- **Blocked By**: ticket-001-modernize-pyproject-toml.md
- **Blocks**: Nenhum

## Effort Estimate

**Points**: 2
**Confidence**: High
