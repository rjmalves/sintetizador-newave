# ticket-018 Update Installation Docs for PyPI and uv

## Context

### Background

A pagina de instalacao (`instalacao.rst`) na documentacao Sphinx atualmente so cobre instalacao via git (clone ou `pip install git+...`). Com a conclusao do epic-01 -- que criou o workflow de release para PyPI com trusted publishing -- o pacote `sintetizador-newave` agora pode ser instalado diretamente via `pip install sintetizador-newave` ou `uv pip install sintetizador-newave`. A pagina precisa ser atualizada para refletir este novo metodo de instalacao (o mais simples) e incluir instrucoes para o gerenciador de pacotes `uv`.

### Relation to Epic

Este ticket e o ultimo do epic-04 (Repository Polish) e fecha o ciclo de modernizacao garantindo que a documentacao Sphinx de instalacao esta alinhada com a infraestrutura criada nos epics anteriores.

### Current State

O arquivo `docs/source/geral/instalacao.rst` contem:

- Titulo "Instalacao"
- Nota sobre compatibilidade com Python >= 3.10
- Secao "Instalando a partir do repositorio oficial" com:
  - Comando `pip uninstall sintetizador-newave`
  - Comando `pip install git+https://github.com/rjmalves/sintetizador-newave`
  - Comando para instalar branch/release especifico
- Total: 20 linhas
- Nao menciona PyPI nem uv

## Specification

### Requirements

1. Reorganizar a pagina para que o metodo mais simples (PyPI) seja apresentado primeiro
2. Adicionar secao "Instalando via PyPI" com:
   - `pip install sintetizador-newave`
   - `uv pip install sintetizador-newave`
   - Instalacao com extras opcionais se aplicavel
3. Manter a secao "Instalando a partir do repositorio oficial" como segunda opcao (para quem precisa da versao de desenvolvimento)
4. Adicionar nota sobre requisitos de sistema: Python >= 3.10
5. Adicionar secao "Instalando com uv" como alternativa moderna ao pip em ambos os cenarios
6. Manter o conteudo em pt-BR, consistente com o restante da documentacao Sphinx
7. Nao incluir instrucoes para Docker (nao ha Dockerfile no projeto)

### Inputs/Props

- Arquivo existente: `docs/source/geral/instalacao.rst` (20 linhas)
- Informacoes de `pyproject.toml`: `requires-python = ">= 3.10"`, nome do pacote `sintetizador-newave`

### Outputs/Behavior

Arquivo `docs/source/geral/instalacao.rst` reescrito com secoes para PyPI (primeiro), repositorio git (segundo), e notas sobre requisitos.

### Error Handling

N/A -- arquivo RST estatico.

## Acceptance Criteria

- [ ] Given o arquivo `docs/source/geral/instalacao.rst`, when as secoes sao listadas, then a primeira secao apos o titulo e "Instalando via PyPI" (antes de "Instalando a partir do repositorio oficial")
- [ ] Given a secao "Instalando via PyPI", when o conteudo e lido, then ela contem os comandos `pip install sintetizador-newave` e `uv pip install sintetizador-newave` em blocos de codigo separados
- [ ] Given a secao "Instalando a partir do repositorio oficial", when o conteudo e lido, then ela mantem o comando `pip install git+https://github.com/rjmalves/sintetizador-newave` existente
- [ ] Given a pagina inteira, when buscada a string "Python >= 3.10", then ela aparece na pagina (nota de requisitos)
- [ ] Given a pagina inteira, when o conteudo e lido, then todo o texto esta em pt-BR e usa a sintaxe RST correta (blocos `::` para codigo, headers com underlines)

## Implementation Guide

### Suggested Approach

1. Reescrever `docs/source/geral/instalacao.rst` preservando o titulo e a nota de compatibilidade
2. Estruturar as secoes na seguinte ordem:
   - Nota de pre-requisitos: Python >= 3.10
   - "Instalando via PyPI" (nova secao, metodo mais simples)
   - "Instalando com uv" (nova secao, alternativa moderna)
   - "Instalando a partir do repositorio" (secao existente, reorganizada)
3. Usar a sintaxe RST padrao do projeto:
   - Headers com underlines (`---` para subsecoes)
   - Blocos de codigo com `::` e indentacao de 4 espacos
   - Notas com `.. note::`
4. Nao duplicar instrucoes excessivamente entre pip e uv -- cada secao deve ser concisa

### Key Files to Modify

- `docs/source/geral/instalacao.rst` (reescrita)

### Patterns to Follow

- Sintaxe RST padrao do projeto: headers com underlines de `=` (titulo) e `-` (subsecoes)
- Blocos de codigo usando `::` seguido de linha em branco e indentacao de 4 espacos (padrao usado em `contribuicao.rst`)
- Cross-references com `:doc:` para links internos (padrao dos epics 02 e 03)
- Texto em pt-BR com acentos

### Pitfalls to Avoid

- Nao usar `.. code-block:: bash` se o padrao do arquivo existente e `::` -- manter consistencia com o estilo ja usado em `instalacao.rst` e `contribuicao.rst`
- Nao remover a opcao de instalar um branch/release especifico via git -- usuarios avancados precisam disso
- Nao mencionar `pip install .` (instalacao local apos clone) -- isso esta no CONTRIBUTING.md, nao na pagina de instalacao para usuarios finais
- Nao incluir instrucoes de `uv add` ou `uv sync` -- essas sao para contribuidores, cobertas no CONTRIBUTING.md (ticket-016)

## Testing Requirements

### Unit Tests

N/A -- arquivo RST estatico.

### Integration Tests

N/A

### E2E Tests

N/A

## Dependencies

- **Blocked By**: ticket-004-create-release-workflow.md (instrucoes de PyPI dependem do workflow de release estar ativo)
- **Blocks**: Nenhum

## Effort Estimate

**Points**: 1
**Confidence**: High
