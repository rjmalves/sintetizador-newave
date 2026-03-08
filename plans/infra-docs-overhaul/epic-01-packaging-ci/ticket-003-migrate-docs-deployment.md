# ticket-003 Migrate Docs Deployment to Official GitHub Pages Actions

## Context

### Background

O workflow de deploy de documentacao (`docs.yml`) usa `peaceiris/actions-gh-pages@v3`, uma action de terceiros que publica para um branch `gh-pages`. A abordagem oficial e moderna do GitHub usa `actions/upload-pages-artifact@v4` + `actions/deploy-pages@v4`, que publica diretamente via GitHub Pages sem necessidade de branch intermediario.

### Relation to Epic

Terceiro ticket do Epic 01. Depende do ticket-001 para dependencias atualizadas. Pode ser executado em paralelo com ticket-002.

### Current State

O arquivo `/home/rogerio/git/sintetizador-newave/.github/workflows/docs.yml`:

- Usa `peaceiris/actions-gh-pages@v3` para deploy
- Publica para branch `gh-pages` com `force_orphan: true`
- Roda testes como parte do workflow de docs (redundante com `main.yml`)
- Usa `setup-uv@v3`
- Fixado em Python 3.12

## Specification

### Requirements

1. Substituir `peaceiris/actions-gh-pages@v3` por `actions/upload-pages-artifact@v4` + `actions/deploy-pages@v4`
2. Upgrade `astral-sh/setup-uv` de `@v3` para `@v7`
3. Remover o step de testes do workflow de docs (ja coberto pelo `main.yml`)
4. Adicionar permissions `pages: write` e `id-token: write` no job de deploy
5. Separar em dois jobs: `build` (gera HTML) e `deploy` (publica no Pages)
6. Manter trigger em push para `main` e `workflow_dispatch`
7. Adicionar `enable-cache: true` no setup-uv
8. Configurar `environment: github-pages` no job deploy

### Inputs/Props

- Arquivo fonte: `/home/rogerio/git/sintetizador-newave/.github/workflows/docs.yml`

### Outputs/Behavior

- Workflow `docs.yml` com 2 jobs: `build` e `deploy`
- Deploy usa mecanismo oficial do GitHub Pages
- Documentacao acessivel em https://rjmalves.github.io/sintetizador-newave/

### Error Handling

- Job deploy requer que GitHub Pages esteja configurado para "GitHub Actions" como source no Settings do repositorio (ao inves de branch `gh-pages`). Se nao estiver configurado, o deploy falha com mensagem clara.

## Acceptance Criteria

- [ ] Given o arquivo `.github/workflows/docs.yml`, when inspecionado, then a string `peaceiris` nao aparece em nenhuma linha
- [ ] Given o arquivo `docs.yml`, when os jobs sao inspecionados, then existem exatamente 2 jobs: `build` e `deploy`
- [ ] Given o job `deploy`, when as permissions sao inspecionadas, then contem `pages: write` e `id-token: write`
- [ ] Given o job `deploy`, when inspecionado, then usa `actions/deploy-pages@v4`
- [ ] Given o job `build`, when os steps sao inspecionados, then nao contem nenhum step executando `pytest`

## Implementation Guide

### Suggested Approach

Reescrever `.github/workflows/docs.yml`:

```yaml
name: Docs

on:
  push:
    branches: [main]
  workflow_dispatch:

permissions:
  contents: read
  pages: write
  id-token: write

concurrency:
  group: "pages"
  cancel-in-progress: false

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v7
        with:
          enable-cache: true
      - run: uv python install 3.12
      - run: uv sync --all-extras --dev
      - run: uv run sphinx-build -M html docs/source docs/build
      - uses: actions/upload-pages-artifact@v4
        with:
          path: docs/build/html

  deploy:
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - name: Deploy to GitHub Pages
        id: deployment
        uses: actions/deploy-pages@v4
```

**IMPORTANTE**: Apos o merge, o owner do repositorio precisa ir em Settings > Pages e mudar a source de "Deploy from a branch" para "GitHub Actions". Documentar este passo no PR description.

### Key Files to Modify

- `/home/rogerio/git/sintetizador-newave/.github/workflows/docs.yml`

### Patterns to Follow

- Separar build e deploy em jobs distintos (padrao oficial do GitHub)
- Usar `concurrency` para evitar deploys simultaneos

### Pitfalls to Avoid

- Nao esquecer de documentar a mudanca necessaria em Settings > Pages
- O `upload-pages-artifact` espera o path direto para o diretorio HTML, nao o diretorio pai
- Remover o step de testes; manter o workflow focado apenas em build + deploy de docs

## Testing Requirements

### Unit Tests

- N/A

### Integration Tests

- Fazer push para branch `main` e verificar que o workflow executa com sucesso
- Verificar que o site de docs esta acessivel apos deploy

### E2E Tests

- N/A

## Dependencies

- **Blocked By**: ticket-001-modernize-pyproject-toml.md
- **Blocks**: Nenhum

## Effort Estimate

**Points**: 2
**Confidence**: High
