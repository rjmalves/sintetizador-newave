# ticket-015 Expand README with Badges and Quickstart

## Context

### Background

O README.md atual do sintetizador-newave e minimalista: contem apenas dois badges (tests e codecov), uma descricao de uma linha, instrucoes basicas de instalacao via git, e um exemplo de comando CLI. Com a conclusao dos epics 01-03 -- que modernizaram o empacotamento PyPI, CI/CD, documentacao Sphinx com Furo, e adicionaram paginas de arquitetura, FAQ, performance, API reference e migration guide -- o README precisa ser expandido para refletir a maturidade atual do projeto e servir como porta de entrada para novos usuarios.

### Relation to Epic

Este ticket e o primeiro do epic-04 (Repository Polish), que finaliza a modernizacao do repositorio. O README expandido e o artefato mais visivel do projeto e estabelece a primeira impressao para visitantes do GitHub.

### Current State

O arquivo `README.md` na raiz do repositorio contem:

- Titulo `# sintetizador-newave`
- 2 badges: tests (workflow `main.yml`) e codecov
- 1 linha de descricao
- Secao "Instalacao" com `git clone` e `pip install git+...`
- Secao "Modelo Unificado de Dados" (1 paragrafo)
- Secao "Comandos" com exemplo CLI
- Secao "Documentacao" com link para o site

Workflows disponiveis para badges:

- `.github/workflows/main.yml` (name: `tests`) -- badge ja existe
- `.github/workflows/docs.yml` (name: `Docs`)
- `.github/workflows/release.yml` (name: `Release`)

Projeto publicado no PyPI como `sintetizador-newave`. Licenca MIT. Python >= 3.10.

## Specification

### Requirements

1. Manter o conteudo em pt-BR, consistente com o restante da documentacao
2. Adicionar badges na seguinte ordem: tests (manter existente), codecov (manter existente), PyPI version, Python versions, docs, license
3. Adicionar secao "Sobre" com descricao expandida (2-3 paragrafos) sobre o que o projeto faz, para quem e destinado, e quais modelos suporta
4. Adicionar secao "Funcionalidades" com lista de features principais (formato bullet list)
5. Adicionar secao "Inicio Rapido" com exemplo de instalacao via pip/uv e exemplo de uso CLI com output
6. Manter secao "Documentacao" com link para o site Sphinx
7. Adicionar secao "Licenca" referenciando MIT
8. Remover secoes "Modelo Unificado de Dados" e "Comandos" (conteudo sera absorvido nas novas secoes "Sobre" e "Inicio Rapido")
9. Nao incluir GIFs ou screenshots -- apenas texto e badges

### Inputs/Props

- Arquivo existente: `README.md` (47 linhas)
- Badge URLs derivadas dos workflows existentes e do PyPI

### Outputs/Behavior

Um arquivo `README.md` reescrito com todas as secoes listadas acima, mantendo Markdown valido e badges funcionais.

### Error Handling

N/A -- este ticket modifica apenas um arquivo Markdown estatico.

## Acceptance Criteria

- [ ] Given o arquivo `README.md` na raiz do repositorio, when o conteudo e inspecionado, then ele contem exatamente 6 badges na ordem: tests, codecov, PyPI version, Python versions, docs, license
- [ ] Given o badge do PyPI, when a URL e verificada, then ela aponta para `https://pypi.org/project/sintetizador-newave/` e usa o shield `https://img.shields.io/pypi/v/sintetizador-newave`
- [ ] Given o badge de Python versions, when a URL e verificada, then ela usa o shield `https://img.shields.io/pypi/pyversions/sintetizador-newave`
- [ ] Given o badge de docs, when a URL e verificada, then o link aponta para `https://rjmalves.github.io/sintetizador-newave/`
- [ ] Given o badge de license, when a URL e verificada, then ela usa o shield `https://img.shields.io/github/license/rjmalves/sintetizador-newave`
- [ ] Given a secao "Sobre", when o conteudo e lido, then ela contem pelo menos 2 paragrafos descrevendo o proposito do projeto e o modelo NEWAVE
- [ ] Given a secao "Inicio Rapido", when o conteudo e lido, then ela contem um bloco de codigo com `pip install sintetizador-newave` e outro com `uv pip install sintetizador-newave`, seguidos de um exemplo de uso CLI com output
- [ ] Given o arquivo completo, when buscadas as secoes "Modelo Unificado de Dados" e "Comandos", then elas nao existem (conteudo absorvido em "Sobre" e "Inicio Rapido")
- [ ] Given o arquivo completo, when o conteudo e lido, then todo o texto esta em pt-BR

## Implementation Guide

### Suggested Approach

1. Reescrever o `README.md` do zero, preservando o titulo `# sintetizador-newave`
2. Montar os 6 badges usando shields.io e GitHub Actions badge URLs:
   - Tests: `[![tests](https://github.com/rjmalves/sintetizador-newave/actions/workflows/main.yml/badge.svg)](https://github.com/rjmalves/sintetizador-newave/actions/workflows/main.yml)`
   - Codecov: manter o badge existente com token
   - PyPI: `[![PyPI](https://img.shields.io/pypi/v/sintetizador-newave)](https://pypi.org/project/sintetizador-newave/)`
   - Python: `[![Python](https://img.shields.io/pypi/pyversions/sintetizador-newave)](https://pypi.org/project/sintetizador-newave/)`
   - Docs: `[![docs](https://github.com/rjmalves/sintetizador-newave/actions/workflows/docs.yml/badge.svg)](https://rjmalves.github.io/sintetizador-newave/)`
   - License: `[![license](https://img.shields.io/github/license/rjmalves/sintetizador-newave)](https://github.com/rjmalves/sintetizador-newave/blob/main/LICENSE)`
3. Escrever secao "Sobre" baseada na descricao do `pyproject.toml` e no conteudo existente
4. Escrever secao "Funcionalidades" listando: sintese de operacao, cenarios, politica, sistema, execucao; saida em Parquet; CLI com wildcards; suporte a Polars
5. Escrever secao "Inicio Rapido" com instalacao pip/uv e exemplo CLI (reusar o exemplo existente da secao "Comandos")
6. Adicionar secao "Documentacao" com link
7. Adicionar secao "Licenca" com referencia a MIT

### Key Files to Modify

- `README.md` (reescrita completa)

### Patterns to Follow

- Todos os docs do projeto sao em pt-BR (padrao estabelecido nos epics 02 e 03)
- Badges em linha separados por newline (padrao existente no README atual)
- Blocos de codigo com linguagem especificada (` ```bash `, ` ```python `)

### Pitfalls to Avoid

- Nao usar o badge URL antigo do codecov sem o token -- manter o token `9AJRL5L21W` existente
- Nao adicionar badges para workflows que nao existem (ex: nao ha workflow de "build")
- Nao incluir instrucoes de instalacao via `git clone` na secao principal -- isso ja esta na documentacao Sphinx em `instalacao.rst`
- Nao fabricar output de CLI -- reusar o output existente do README atual

## Testing Requirements

### Unit Tests

N/A -- arquivo Markdown, sem codigo executavel.

### Integration Tests

N/A

### E2E Tests

N/A

## Dependencies

- **Blocked By**: ticket-004-create-release-workflow.md (PyPI badges dependem do workflow de release estar ativo)
- **Blocks**: Nenhum

## Effort Estimate

**Points**: 2
**Confidence**: High
