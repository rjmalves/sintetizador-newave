# Master Plan: Infrastructure & Documentation Overhaul

## Executive Summary

Modernizar a infraestrutura de empacotamento, CI/CD e documentacao do `sintetizador-newave`, alinhando o projeto com as praticas correntes (2025-2026) do ecossistema Python. O trabalho abrange: atualizacao do `pyproject.toml` e dependencias, reestruturacao dos workflows do GitHub Actions (CI paralelo, deploy Pages oficial, publicacao PyPI via trusted publishing), migracao do tema Sphinx para Furo com conteudo expandido em pt-BR, e polimento geral do repositorio (README, CONTRIBUTING, CHANGELOG, pre-commit).

## Goals & Non-Goals

### Goals

1. **Empacotamento moderno**: pyproject.toml limpo, dependencias corretas (remover numba), `py.typed`, lockfile commitado
2. **CI/CD robusto**: jobs paralelos, setup-uv@v7, deploy Pages oficial, release workflow com trusted publishing no PyPI
3. **Documentacao completa em pt-BR**: tema Furo, exemplos atualizados para Polars, paginas novas (arquitetura, FAQ, performance, API reference, guia de migracao)
4. **Repositorio polido**: README expandido, CONTRIBUTING.md, CHANGELOG no formato Keep a Changelog, pre-commit hooks

### Non-Goals

- Mudancas na logica de negocio ou nos modulos de sintese
- Migracao de pandas residual no codigo-fonte (fora do escopo; existente na fronteira com inewave)
- Testes novos de cobertura funcional (somente testes de infra: build, docs)
- Internacionalizacao (i18n) — toda documentacao permanece exclusivamente em pt-BR
- Refatoracao do CLI (Click) ou mudanca de framework CLI

## Architecture Overview

### Current State

| Area          | Estado atual                                                                                                                 |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Empacotamento | hatchling build, `pyproject.toml` com numba desnecessario, sem `py.typed`, uv.lock no .gitignore                             |
| CI            | Workflow unico `main.yml` com todos os steps sequenciais por entry na matrix; `setup-uv@v3`; `peaceiris/actions-gh-pages@v3` |
| Publicacao    | Nenhuma — instalacao apenas via git clone/pip install git+                                                                   |
| Documentacao  | sphinx-rtd-theme, exemplos com pandas, 6 paginas (apresentacao, instalacao, tutorial, contribuicao, saidas, modelo)          |
| Repositorio   | README minimo, sem CONTRIBUTING.md raiz, CHANGELOG simples, sem pre-commit                                                   |

### Target State

| Area          | Estado alvo                                                                                                                                      |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Empacotamento | numba removido, `py.typed` adicionado, mypy strict em pyproject.toml, uv.lock commitado, classifiers atualizados                                 |
| CI            | Jobs paralelos (lint, type-check, test matrix, docs build); `setup-uv@v7`; `actions/deploy-pages@v4`; workflow de release com trusted publishing |
| Publicacao    | PyPI via trusted publishing em GitHub Releases                                                                                                   |
| Documentacao  | Furo theme com dark mode, exemplos Polars, +5 paginas novas (arquitetura, FAQ, performance, API ref, migracao v1->v2)                            |
| Repositorio   | README completo com badges e quickstart, CONTRIBUTING.md, Keep a Changelog, pre-commit (ruff format + check, mypy)                               |

### Key Design Decisions

1. **Furo sobre sphinx-rtd-theme**: Melhor suporte a dark mode, mobile, acessibilidade e manutencao ativa
2. **Jobs CI paralelos**: lint/typecheck rapidos falham cedo sem bloquear testes longos
3. **Trusted publishing**: Sem secrets de API token; OIDC nativo do GitHub Actions
4. **pre-commit sobre scripts manuais**: Padronizacao automatica antes de cada commit, sem depender de CI

## Technical Approach

### Tech Stack

- Python >= 3.10 (testado ate 3.14)
- Build: hatchling
- Package manager: uv
- CI: GitHub Actions
- Docs: Sphinx + Furo + sphinx-gallery + numpydoc + autodoc
- Linting: ruff
- Type checking: mypy (strict)
- Pre-commit: pre-commit framework

### Component/Module Breakdown

1. **Packaging** — `pyproject.toml`, `app/py.typed`, `uv.lock`, `.gitignore`
2. **CI Workflows** — `.github/workflows/main.yml`, `.github/workflows/docs.yml`, `.github/workflows/release.yml`
3. **Sphinx Config** — `docs/source/conf.py`, templates, static
4. **Sphinx Content** — rst pages, exemplos, auto-generated API
5. **Repository Files** — `README.md`, `CONTRIBUTING.md`, `CHANGELOG.md`, `.pre-commit-config.yaml`

### Data Flow

```
git push/PR -> CI (lint | typecheck | test matrix | docs build)
                                                        |
git tag/release -> release workflow -> build sdist+wheel -> PyPI trusted publish
                                    -> docs build -> GitHub Pages deploy
```

### Testing Strategy

- Validacao de build: `uv build` deve produzir sdist e wheel sem erros
- Validacao de docs: `sphinx-build -W` (warnings as errors) deve passar
- Validacao de CI: todos os jobs paralelos passam
- Pre-commit: `pre-commit run --all-files` sem falhas

## Phases & Milestones

| Epic | Nome                  | Duracao estimada | Milestone                                                                         |
| ---- | --------------------- | ---------------- | --------------------------------------------------------------------------------- |
| 1    | Packaging & CI        | 1-2 semanas      | CI verde com jobs paralelos, release workflow funcional, PyPI publish configurado |
| 2    | Sphinx Modernization  | 1-2 semanas      | Furo theme ativo, exemplos Polars, docs build limpo                               |
| 3    | Documentation Content | 2-3 semanas      | 5 paginas novas em pt-BR, conteudo existente atualizado                           |
| 4    | Repository Polish     | 1 semana         | README, CONTRIBUTING, CHANGELOG, pre-commit configurados                          |

## Risk Analysis

| Risco                                                          | Probabilidade | Impacto | Mitigacao                                                                           |
| -------------------------------------------------------------- | ------------- | ------- | ----------------------------------------------------------------------------------- |
| sphinx-gallery incompativel com Furo                           | Media         | Alto    | Testar combinacao cedo; fallback para exemplos rst manuais se necessario            |
| Trusted publishing requer config no PyPI antes de primeiro uso | Baixa         | Medio   | Documentar passos manuais no PyPI; workflow falha graciosamente                     |
| Exemplos Polars nao funcionam com sphinx-gallery               | Media         | Medio   | sphinx-gallery executa scripts; validar que Polars + plotly funciona no ambiente CI |
| Remocao de numba quebra install em ambientes existentes        | Baixa         | Baixo   | Major version bump se necessario; numba ja nao e importado                          |

## Success Metrics

1. `pip install sintetizador-newave` funciona a partir do PyPI
2. CI completa em menos tempo que o workflow monolitico atual (paralelismo)
3. Docs site com Furo renderiza corretamente com dark mode
4. Todas as 5 paginas novas acessiveis e navegaveis
5. `pre-commit run --all-files` passa sem modificacoes no codigo atual
6. README tem badges funcionais (tests, codecov, PyPI version, docs)
