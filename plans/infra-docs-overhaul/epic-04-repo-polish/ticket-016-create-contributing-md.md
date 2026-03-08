# ticket-016 Create CONTRIBUTING.md at Repository Root

## Context

### Background

O repositorio atualmente nao possui um arquivo `CONTRIBUTING.md` na raiz. A unica orientacao para contribuidores esta em `docs/source/geral/contribuicao.rst`, que cobre dependencias do inewave, convencoes PEP8, tipagem estatica, gerenciamento com pyproject.toml/uv, e procedimentos de teste (pytest, mypy, ruff). Com a conclusao do epic-01 -- que adicionou pre-commit hooks (ruff + mypy), CI modernizado, e release workflow -- o fluxo de contribuicao mudou significativamente e precisa ser documentado em um arquivo padrao que o GitHub reconhece automaticamente.

### Relation to Epic

Este ticket cria o CONTRIBUTING.md como parte do epic-04 (Repository Polish). E o segundo ticket do epic e complementa o README expandido (ticket-015) ao fornecer orientacoes detalhadas para contribuidores.

### Current State

- `CONTRIBUTING.md` nao existe na raiz do repositorio
- `docs/source/geral/contribuicao.rst` existe com conteudo parcialmente desatualizado (menciona `pip install .[dev]` ao inves de `uv`, nao menciona pre-commit hooks)
- Pre-commit hooks configurados em `.pre-commit-config.yaml`: ruff (lint + format) e mypy (strict, `./app`)
- Dev dependencies em `pyproject.toml` `[project.optional-dependencies] dev`: pytest, pytest-cov, pre-commit, ruff, mypy, types-python-dateutil, furo, sphinx-gallery, sphinx, numpydoc, plotly, matplotlib
- CI matrix: Python 3.10-3.14
- Build backend: hatchling via uv

## Specification

### Requirements

1. Criar `CONTRIBUTING.md` na raiz do repositorio em pt-BR
2. O arquivo deve ser a fonte unica de verdade para contribuicao -- `contribuicao.rst` sera atualizado para redirecionar ao arquivo raiz com um `.. note::` e link
3. Secoes obrigatorias:
   - **Configuracao do Ambiente**: clone, `uv sync`, instalacao de dev deps com `uv pip install -e ".[dev]"`
   - **Pre-commit Hooks**: `pre-commit install`, o que os hooks fazem (ruff lint+format, mypy strict)
   - **Executando Testes**: `uv run pytest ./tests`, `uv run pytest ./tests --cov=app`
   - **Verificacao de Tipos**: `uv run mypy ./app`
   - **Estilo de Codigo**: PEP8 via ruff, tipagem estatica obrigatoria, `line-length = 80`
   - **Fluxo de Contribuicao**: fork, branch, commit, PR, review
   - **Convencoes de Commit**: conventional commits (`feat:`, `fix:`, `refactor:`, etc.)
4. Atualizar `docs/source/geral/contribuicao.rst` para adicionar um `.. note::` no topo redirecionando ao `CONTRIBUTING.md` na raiz, mantendo o conteudo existente como referencia secundaria

### Inputs/Props

- Conteudo existente de `docs/source/geral/contribuicao.rst` (75 linhas)
- Configuracao de `.pre-commit-config.yaml`
- Dev dependencies de `pyproject.toml`

### Outputs/Behavior

- Novo arquivo `CONTRIBUTING.md` na raiz do repositorio
- Arquivo `docs/source/geral/contribuicao.rst` atualizado com nota de redirecionamento

### Error Handling

N/A -- arquivos Markdown e RST estaticos.

## Acceptance Criteria

- [ ] Given o repositorio, when `ls CONTRIBUTING.md` e executado na raiz, then o arquivo existe
- [ ] Given o arquivo `CONTRIBUTING.md`, when o conteudo e lido, then ele contem as secoes: "Configuracao do Ambiente", "Pre-commit Hooks", "Executando Testes", "Verificacao de Tipos", "Estilo de Codigo", "Fluxo de Contribuicao"
- [ ] Given a secao "Configuracao do Ambiente", when o conteudo e lido, then ela contem o comando `uv pip install -e ".[dev]"` em um bloco de codigo
- [ ] Given a secao "Pre-commit Hooks", when o conteudo e lido, then ela menciona `pre-commit install` e descreve os hooks ruff e mypy
- [ ] Given a secao "Executando Testes", when o conteudo e lido, then ela contem `uv run pytest ./tests` em um bloco de codigo
- [ ] Given o arquivo `docs/source/geral/contribuicao.rst`, when o conteudo e lido, then ele contem uma diretiva `.. note::` com texto referenciando `CONTRIBUTING.md` na raiz do repositorio
- [ ] Given o arquivo `CONTRIBUTING.md`, when o conteudo e lido, then todo o texto esta em pt-BR

## Implementation Guide

### Suggested Approach

1. Criar `CONTRIBUTING.md` na raiz com as secoes especificadas
2. Basear o conteudo tecnico no `contribuicao.rst` existente, atualizando:
   - Substituir `pip install .[dev]` por `uv pip install -e ".[dev]"`
   - Adicionar secao sobre pre-commit hooks (nao existe no RST atual)
   - Adicionar secao sobre convencoes de commit
   - Adicionar fluxo de PR (fork -> branch -> commit -> push -> PR)
3. Atualizar `contribuicao.rst` adicionando no topo (apos o titulo):

   ```rst
   .. note::

      As instrucoes mais atualizadas para contribuicao estao no arquivo
      `CONTRIBUTING.md <https://github.com/rjmalves/sintetizador-newave/blob/main/CONTRIBUTING.md>`_
      na raiz do repositorio.
   ```

4. Manter o conteudo existente do RST intacto abaixo da nota

### Key Files to Modify

- `CONTRIBUTING.md` (novo, criar na raiz)
- `docs/source/geral/contribuicao.rst` (adicionar nota de redirecionamento)

### Patterns to Follow

- Linguagem pt-BR consistente com toda a documentacao do projeto (epics 02 e 03)
- Blocos de codigo com linguagem especificada (` ```bash `)
- Estrutura de secoes com headers Markdown `##`

### Pitfalls to Avoid

- Nao remover conteudo existente do `contribuicao.rst` -- apenas adicionar a nota de redirecionamento
- Nao referenciar ferramentas que nao estao no projeto (ex: nao mencionar black, flake8, pylama -- o projeto usa ruff)
- Nao mencionar `setup.py` -- o projeto usa `pyproject.toml` com hatchling
- Nao incluir instrucoes para Docker -- o projeto nao tem Dockerfile

## Testing Requirements

### Unit Tests

N/A -- arquivos Markdown e RST estaticos.

### Integration Tests

N/A

### E2E Tests

N/A

## Dependencies

- **Blocked By**: ticket-005-add-pre-commit-hooks.md (instrucoes dependem dos hooks estarem configurados)
- **Blocks**: Nenhum

## Effort Estimate

**Points**: 2
**Confidence**: High
