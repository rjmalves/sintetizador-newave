# ticket-002 Restructure CI Workflow into Parallel Jobs

## Context

### Background

O workflow atual `.github/workflows/main.yml` executa lint, type-check, testes, cobertura e sphinx-build todos sequencialmente em cada entry da matrix Python. Isso significa que um erro de formatacao ruff so e detectado apos os testes (potencialmente longos) completarem. Separar em jobs paralelos da feedback mais rapido e aproveita melhor os runners do GitHub Actions.

### Relation to Epic

Segundo ticket do Epic 01. Depende do ticket-001 para pyproject.toml atualizado (setup-uv@v7, dependencias corretas).

### Current State

O arquivo `.github/workflows/main.yml` tem:

- Um unico job `test` com matrix Python [3.10, 3.11, 3.12, 3.13, 3.14]
- Steps sequenciais: checkout -> setup-uv@v3 -> uv sync -> pytest+cov -> codecov -> mypy -> ruff -> sphinx-build
- Todos os steps rodam em cada versao Python da matrix, mesmo que lint/typecheck so precisem de uma

## Specification

### Requirements

1. Upgrade `astral-sh/setup-uv` de `@v3` para `@v7` em todos os workflows
2. Criar job `lint` que executa `ruff check` e `ruff format --check` em uma unica versao Python (3.12)
3. Criar job `typecheck` que executa `mypy ./app` em uma unica versao Python (3.12)
4. Manter job `test` com matrix Python [3.10, 3.11, 3.12, 3.13, 3.14] executando apenas `pytest --cov`
5. Criar job `docs` que executa `sphinx-build` em uma unica versao Python (3.12)
6. Jobs `lint`, `typecheck`, `test` e `docs` devem executar em paralelo (sem `needs` entre eles)
7. Codecov upload deve permanecer no job `test`
8. Habilitar cache do uv com `enable-cache: true` no setup-uv

### Inputs/Props

- Arquivo fonte: `/home/rogerio/git/sintetizador-newave/.github/workflows/main.yml`

### Outputs/Behavior

- Workflow `main.yml` com 4 jobs paralelos
- Cada job instala dependencias independentemente com `uv sync`
- Todos os 4 jobs devem passar para o PR ser verde

### Error Handling

- Se um job falha, os demais continuam executando (comportamento padrao do GitHub Actions para jobs paralelos)

## Acceptance Criteria

- [ ] Given o arquivo `.github/workflows/main.yml`, when o campo `jobs` e inspecionado, then existem exatamente 4 jobs: `lint`, `typecheck`, `test`, `docs`
- [ ] Given o job `lint`, when inspecionado, then nao possui `strategy.matrix` e usa Python 3.12
- [ ] Given o job `test`, when inspecionado, then possui `strategy.matrix.python-version` com 5 versoes: 3.10, 3.11, 3.12, 3.13, 3.14
- [ ] Given qualquer job, when o step de setup-uv e inspecionado, then usa `astral-sh/setup-uv@v7` com `enable-cache: true`
- [ ] Given o job `lint`, when os steps sao inspecionados, then executa tanto `ruff check ./app` quanto `ruff format --check ./app`

## Implementation Guide

### Suggested Approach

Reescrever `.github/workflows/main.yml` com a seguinte estrutura:

```yaml
name: tests

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v7
        with:
          enable-cache: true
      - run: uv python install 3.12
      - run: uv sync --all-extras --dev
      - run: uv run ruff check ./app
      - run: uv run ruff format --check ./app

  typecheck:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v7
        with:
          enable-cache: true
      - run: uv python install 3.12
      - run: uv sync --all-extras --dev
      - run: uv run mypy ./app

  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.10", "3.11", "3.12", "3.13", "3.14"]
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v7
        with:
          enable-cache: true
      - run: uv python install ${{ matrix.python-version }}
      - run: uv sync --all-extras --dev
      - run: uv run pytest --cov-report=xml --cov=app ./tests
      - uses: codecov/codecov-action@v4
        with:
          token: ${{ secrets.CODECOV_TOKEN }}
          files: ./coverage.xml
          flags: unittests
          env_vars: OS,PYTHON
          name: codecov-sintetizador
          fail_ci_if_error: true
          verbose: true

  docs:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v7
        with:
          enable-cache: true
      - run: uv python install 3.12
      - run: uv sync --all-extras --dev
      - run: uv run sphinx-build -W -M html docs/source docs/build
```

Notas:

- O flag `-W` no sphinx-build trata warnings como erros
- Nao adicionar `needs` entre os jobs para que executem em paralelo

### Key Files to Modify

- `/home/rogerio/git/sintetizador-newave/.github/workflows/main.yml`

### Patterns to Follow

- Usar `uv python install` ao inves de `actions/setup-python` (consistente com abordagem uv-first)
- Manter codecov apenas no job `test`

### Pitfalls to Avoid

- Nao colocar `needs: lint` no job test — queremos paralelismo
- Nao esquecer `enable-cache: true` em todos os jobs
- O flag `-W` no sphinx-build pode falhar se houver warnings pre-existentes; se isso acontecer, remover o flag temporariamente e criar issue para corrigir os warnings

## Testing Requirements

### Unit Tests

- N/A

### Integration Tests

- Fazer push para um branch e verificar que os 4 jobs aparecem no GitHub Actions e executam em paralelo
- Verificar que todos os 4 jobs passam

### E2E Tests

- N/A

## Dependencies

- **Blocked By**: ticket-001-modernize-pyproject-toml.md
- **Blocks**: Nenhum

## Effort Estimate

**Points**: 3
**Confidence**: High
