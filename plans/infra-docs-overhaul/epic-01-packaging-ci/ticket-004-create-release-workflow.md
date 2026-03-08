# ticket-004 Create PyPI Release Workflow with Trusted Publishing

## Context

### Background

O `sintetizador-newave` nao tem publicacao automatizada no PyPI. A instalacao e feita via `pip install git+https://github.com/...`. Trusted publishing e o metodo recomendado pelo PyPI para publicar pacotes via GitHub Actions, usando OIDC tokens sem necessidade de API keys armazenadas como secrets.

### Relation to Epic

Quarto ticket do Epic 01. Pode ser executado em paralelo com tickets 002 e 003 (depende apenas do ticket-001 para pyproject.toml correto).

### Current State

- Nao existe workflow de release em `.github/workflows/`
- O pacote nao esta publicado no PyPI (instalacao apenas via git)
- O build backend e `hatchling` (ja configurado em `pyproject.toml`)

## Specification

### Requirements

1. Criar arquivo `.github/workflows/release.yml`
2. Trigger: criacao de release no GitHub (evento `release`, tipo `published`)
3. Job `build`: gerar sdist e wheel usando `uv build`
4. Job `publish`: publicar no PyPI usando `pypa/gh-action-pypi-publish@release/v1`
5. Job `publish` deve ter permission `id-token: write` para trusted publishing
6. Job `publish` deve usar `environment: pypi` para controle de aprovacao
7. Incluir job `test` que executa testes antes do build (gate de qualidade)
8. O workflow deve tambem disparar o deploy de docs (ou o workflow de docs ja cobre isso via push para main)

### Inputs/Props

- Novo arquivo: `/home/rogerio/git/sintetizador-newave/.github/workflows/release.yml`

### Outputs/Behavior

- Ao criar uma release no GitHub, o workflow: executa testes, builda o pacote, publica no PyPI
- Pacote disponivel via `pip install sintetizador-newave` apos primeira publicacao

### Error Handling

- Se testes falham, o build nao acontece e o publish nao acontece
- Se o publish falha (ex: versao ja existe no PyPI), o erro e reportado claramente no Actions log
- Se trusted publishing nao esta configurado no PyPI, o job falha com mensagem sobre OIDC

## Acceptance Criteria

- [ ] Given o arquivo `.github/workflows/release.yml`, when inspecionado, then o trigger e `on: release: types: [published]`
- [ ] Given o job `publish`, when as permissions sao inspecionadas, then contem `id-token: write`
- [ ] Given o job `publish`, when o step de publicacao e inspecionado, then usa `pypa/gh-action-pypi-publish@release/v1`
- [ ] Given o job `build`, when os steps sao inspecionados, then executa `uv build` e faz upload do artefato
- [ ] Given o job `test`, when inspecionado, then o job `build` tem `needs: test`

## Implementation Guide

### Suggested Approach

Criar `.github/workflows/release.yml`:

```yaml
name: Release

on:
  release:
    types: [published]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.10", "3.12", "3.14"]
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v7
        with:
          enable-cache: true
      - run: uv python install ${{ matrix.python-version }}
      - run: uv sync --all-extras --dev
      - run: uv run pytest ./tests

  build:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v7
        with:
          enable-cache: true
      - run: uv python install 3.12
      - run: uv build
      - uses: actions/upload-artifact@v4
        with:
          name: dist
          path: dist/

  publish:
    needs: build
    runs-on: ubuntu-latest
    environment: pypi
    permissions:
      id-token: write
    steps:
      - uses: actions/download-artifact@v4
        with:
          name: dist
          path: dist/
      - uses: pypa/gh-action-pypi-publish@release/v1
```

**IMPORTANTE**: Antes da primeira release, o owner deve:

1. Criar o projeto no PyPI (pode ser via primeiro upload manual ou pendente)
2. Configurar trusted publisher no PyPI: Settings > Publishing > Add a new publisher
   - PyPI project name: `sintetizador-newave`
   - Owner: `rjmalves`
   - Repository: `sintetizador-newave`
   - Workflow name: `release.yml`
   - Environment name: `pypi`
3. Criar environment `pypi` no GitHub repo: Settings > Environments > New environment

Documentar estes passos no PR description.

### Key Files to Modify

- `/home/rogerio/git/sintetizador-newave/.github/workflows/release.yml` (novo)

### Patterns to Follow

- Separar test, build e publish em jobs distintos com `needs` chain
- Usar matrix reduzida para testes no release (3 versoes ao inves de 5, para velocidade)
- Usar `actions/upload-artifact` / `actions/download-artifact` para passar artefatos entre jobs

### Pitfalls to Avoid

- Nao colocar `id-token: write` no nivel do workflow; manter no nivel do job `publish` (principio de menor privilegio)
- Nao esquecer o environment `pypi` no job publish; sem ele o trusted publishing nao funciona
- A matrix de teste no release e reduzida (3.10, 3.12, 3.14) para nao atrasar o release; testes completos ja rodam no main.yml

## Testing Requirements

### Unit Tests

- N/A

### Integration Tests

- Criar uma release de teste (ex: `v2.3.1-rc.1`) e verificar que o workflow e disparado
- Verificar que os artefatos (sdist + wheel) sao gerados corretamente

### E2E Tests

- Apos configuracao do trusted publishing no PyPI, verificar que o pacote aparece em https://pypi.org/project/sintetizador-newave/

## Dependencies

- **Blocked By**: ticket-001-modernize-pyproject-toml.md
- **Blocks**: Nenhum

## Effort Estimate

**Points**: 3
**Confidence**: High
