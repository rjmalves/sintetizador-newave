# ticket-017 Reformat CHANGELOG to Keep a Changelog Standard

## Context

### Background

O CHANGELOG.md atual usa um formato simples: headers `# vX.Y.Z` seguidos de bullet points descrevendo mudancas sem categorizacao. O padrao Keep a Changelog (https://keepachangelog.com/) organiza entradas em categorias (Added, Changed, Fixed, Removed, Deprecated, Security) e inclui um header padrao, links de comparacao entre versoes, e uma secao Unreleased. A adocao deste padrao melhora a legibilidade e facilita a geracao automatica de notas de release.

### Relation to Epic

Este ticket e o terceiro do epic-04 (Repository Polish). Reformata o CHANGELOG existente para um padrao reconhecido internacionalmente, complementando o README expandido (ticket-015) e o CONTRIBUTING.md (ticket-016).

### Current State

O arquivo `CHANGELOG.md` contem 13 versoes documentadas (v1.0.0 ate v2.3.0):

- Headers no formato `# vX.Y.Z` (sem data)
- Bullet points sem categorizacao
- Algumas entradas referenciam issues do GitHub (`[#NN](url)`)
- Versao v1.2.0 tem subtitulo `(v1-compat)`
- Conteudo em pt-BR
- Total de ~79 linhas

## Specification

### Requirements

1. Reformatar o CHANGELOG.md para seguir o padrao Keep a Changelog
2. Usar categorias em ingles conforme o padrao internacional: `Added`, `Changed`, `Fixed`, `Removed` -- o conteudo dos bullet points permanece em pt-BR
3. Adicionar header padrao do Keep a Changelog no topo do arquivo:

   ```
   # Changelog

   Todas as mudancas notaveis neste projeto serao documentadas neste arquivo.

   O formato e baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
   e este projeto adere ao [Versionamento Semantico](https://semver.org/lang/pt-BR/).
   ```

4. Adicionar secao `## [Unreleased]` vazia no topo (abaixo do header)
5. Reformatar cada versao para `## [X.Y.Z]` (sem o prefixo `v`)
6. Categorizar cada bullet point existente na categoria apropriada (Added, Changed, Fixed, Removed)
7. Adicionar links de comparacao entre versoes no rodape do arquivo usando o formato `[X.Y.Z]: https://github.com/rjmalves/sintetizador-newave/compare/vX.Y.Z-1...vX.Y.Z`
8. Preservar todas as referencias a issues existentes (`[#NN](url)`)

### Inputs/Props

- Arquivo existente: `CHANGELOG.md` (~79 linhas, 13 versoes)

### Outputs/Behavior

Um arquivo `CHANGELOG.md` reformatado com header padrao, secao Unreleased, versoes categorizadas, e links de comparacao no rodape.

### Error Handling

N/A -- arquivo Markdown estatico.

## Acceptance Criteria

- [ ] Given o arquivo `CHANGELOG.md`, when as primeiras 5 linhas sao lidas, then o arquivo comeca com `# Changelog` seguido do paragrafo descritivo com links para Keep a Changelog e Versionamento Semantico
- [ ] Given o arquivo `CHANGELOG.md`, when buscada a secao `## [Unreleased]`, then ela existe imediatamente apos o header e esta vazia
- [ ] Given a secao da versao 2.3.0, when o conteudo e lido, then o header e `## [2.3.0]` (sem prefixo `v`) e os bullet points estao categorizados sob `### Added` e/ou `### Changed`
- [ ] Given a secao da versao 2.0.0, when o conteudo e lido, then ela contem subcategorias `### Added`, `### Changed`, `### Fixed`, e `### Removed` com os bullet points distribuidos corretamente
- [ ] Given o rodape do arquivo, when os links de comparacao sao lidos, then existe um link para cada versao no formato `[X.Y.Z]: https://github.com/rjmalves/sintetizador-newave/compare/vPREV...vX.Y.Z`
- [ ] Given todas as referencias a issues no CHANGELOG original (ex: `[#55]`, `[#50]`, `[#51]`), when o CHANGELOG reformatado e inspecionado, then todas as referencias estao preservadas com suas URLs

## Implementation Guide

### Suggested Approach

1. Ler o CHANGELOG.md atual integralmente
2. Criar o novo header padrao Keep a Changelog em pt-BR
3. Adicionar secao `## [Unreleased]` vazia
4. Para cada versao existente:
   - Converter `# vX.Y.Z` para `## [X.Y.Z]`
   - Classificar cada bullet point:
     - **Added**: "Implementada", "Adicionado", "Criacao", "Habilitado", "Implementado", "Suporte a"
     - **Changed**: "Refatoracao", "Substituida", "Uso do", "Otimizado", "Logging", "Entidades passam", "Colunas", "Opcao de exportacao", "Compatibilizacao"
     - **Fixed**: "Correcao", "Fix", "Concatenacao"
     - **Removed**: "Removidas", "Descontinuado", "Suporte a Python 3.8 descontinuado"
   - Agrupar bullet points sob suas categorias
5. Adicionar links de comparacao no rodape, na ordem reversa (mais recente primeiro):
   - `[Unreleased]: https://github.com/rjmalves/sintetizador-newave/compare/v2.3.0...HEAD`
   - `[2.3.0]: https://github.com/rjmalves/sintetizador-newave/compare/v2.2.1...v2.3.0`
   - ... ate ...
   - `[1.0.0]: https://github.com/rjmalves/sintetizador-newave/releases/tag/v1.0.0`

### Key Files to Modify

- `CHANGELOG.md` (reescrita completa mantendo todo o conteudo)

### Patterns to Follow

- Categorias em ingles (padrao Keep a Changelog): Added, Changed, Fixed, Removed
- Conteudo dos bullet points em pt-BR (idioma do projeto)
- Links de comparacao no rodape usando tags git existentes (`vX.Y.Z`)

### Pitfalls to Avoid

- Nao perder nenhum bullet point durante a recategorizacao -- o numero total de items deve ser identico
- Nao alterar o texto dos bullet points -- apenas mover sob a categoria correta
- Nao remover as referencias a issues (`[#NN](url)`) -- preservar intactas
- Nao adicionar datas as versoes se elas nao existem no original -- o CHANGELOG atual nao tem datas
- Usar tags com prefixo `v` nos links de comparacao (`v2.3.0`, nao `2.3.0`) pois as tags git usam esse formato

## Testing Requirements

### Unit Tests

N/A -- arquivo Markdown estatico.

### Integration Tests

N/A

### E2E Tests

N/A

## Dependencies

- **Blocked By**: Nenhum
- **Blocks**: Nenhum

## Effort Estimate

**Points**: 1
**Confidence**: High
