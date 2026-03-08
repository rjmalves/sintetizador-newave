# ticket-008 Update Existing Documentation Text for Polars Migration

## Context

### Background

A documentacao existente referencia pandas em varios locais como a biblioteca utilizada para DataFrames, tanto no texto descritivo quanto nos code blocks de exemplo inline. Desde a migracao para Polars, estes textos estao desatualizados. Os code blocks nas paginas de referencia (`saidas.rst`, `modelo.rst`) mostram `pd.read_parquet` e output formatado como pandas DataFrames.

### Relation to Epic

Terceiro e ultimo ticket do Epic 02. Depende do ticket-006 para tema Furo ativo. Pode ser executado em paralelo com ticket-007.

### Current State

Paginas que referenciam pandas:

- `docs/source/apresentacao/apresentacao.rst`: "DataFrames do pandas" (linha 8)
- `docs/source/referencia/saidas.rst`: code blocks com `import pandas as pd` e `pd.read_parquet` (linhas 117-119, 150-152, 182-185)
- `docs/source/geral/tutorial.rst`: nao referencia pandas diretamente, mas mostra output desatualizado do `--help`
- `docs/source/geral/contribuicao.rst`: menciona ferramentas de dev mas nao pandas

## Specification

### Requirements

1. Em `apresentacao.rst`: substituir "DataFrames do pandas" por "DataFrames" (sem mencionar biblioteca especifica, ja que o formato de saida e parquet e agnistico)
2. Em `saidas.rst`: atualizar code blocks de `pd.read_parquet` para `pl.read_parquet` com `import polars as pl`
3. Em `saidas.rst`: atualizar o output representativo dos DataFrames para formato Polars (Polars usa formatacao diferente de pandas no repr)
4. Manter linguagem pt-BR e estilo de escrita existente
5. Nao alterar as tabelas de referencia (list-table) — apenas os code blocks e texto descritivo

### Inputs/Props

- `/home/rogerio/git/sintetizador-newave/docs/source/apresentacao/apresentacao.rst`
- `/home/rogerio/git/sintetizador-newave/docs/source/referencia/saidas.rst`

### Outputs/Behavior

- Texto da documentacao reflete que o projeto usa Polars
- Code blocks mostram sintaxe Polars
- Build Sphinx completa sem warnings

### Error Handling

- Se o output representativo dos DataFrames Polars nao for facilmente reproduzivel, manter o formato tabular generico sem assumir formato especifico de biblioteca

## Acceptance Criteria

- [ ] Given o arquivo `apresentacao.rst`, when inspecionado, then a string `pandas` nao aparece em nenhuma linha
- [ ] Given o arquivo `saidas.rst`, when os code blocks sao inspecionados, then usam `import polars as pl` e `pl.read_parquet` ao inves de pandas
- [ ] Given o comando `uv run sphinx-build -M html docs/source docs/build`, when executado, then o exit code e 0

## Implementation Guide

### Suggested Approach

1. Editar `docs/source/apresentacao/apresentacao.rst`:
   - Linha 8: substituir "em tabelas normalizadas e estruturadas em DataFrames do `pandas <https://pandas.pydata.org/pandas-docs/stable/index.html>`_" por "em tabelas normalizadas e estruturadas em DataFrames do `Polars <https://docs.pola.rs/>`_"

2. Editar `docs/source/referencia/saidas.rst`:
   - Substituir todos os `import pandas as pd` por `import polars as pl` nos code blocks
   - Substituir todos os `pd.read_parquet(...)` por `pl.read_parquet(...)` nos code blocks
   - Atualizar o output representativo para formato Polars. Exemplo:

     ```
     # Antes (pandas repr)
     import pandas as pd
     meta_df = pd.read_parquet("sintese/METADADOS_OPERACAO.parquet")
     meta_df

     # Depois (polars repr)
     import polars as pl
     meta_df = pl.read_parquet("sintese/METADADOS_OPERACAO.parquet")
     print(meta_df)
     ```

   - O repr do Polars mostra o shape e dtypes no header; recriar o output representativo executando os comandos localmente com os parquet de exemplo em `examples/sintese/`

3. Validar build: `uv run sphinx-build -M html docs/source docs/build`

### Key Files to Modify

- `/home/rogerio/git/sintetizador-newave/docs/source/apresentacao/apresentacao.rst`
- `/home/rogerio/git/sintetizador-newave/docs/source/referencia/saidas.rst`

### Patterns to Follow

- Manter estilo rst existente (indentacao, uso de `.. code-block:: python`)
- Manter linguagem pt-BR
- Nao alterar estrutura das paginas, apenas conteudo de texto e code blocks

### Pitfalls to Avoid

- Nao alterar as tabelas list-table de referencia de variaveis — sao independentes de biblioteca
- Os outputs representativos nao precisam ser pixel-perfect — basta que sejam representativos do formato Polars
- Nao remover a mencao a pandas completamente da documentacao se for relevante para o usuario (ex: "os arquivos parquet podem ser lidos com qualquer biblioteca como Polars, pandas ou DuckDB")

## Testing Requirements

### Unit Tests

- N/A

### Integration Tests

- `uv run sphinx-build -M html docs/source docs/build` deve retornar exit code 0
- Verificar visualmente que as paginas renderizadas mostram code blocks corretos

### E2E Tests

- N/A

## Dependencies

- **Blocked By**: ticket-006-migrate-sphinx-theme.md
- **Blocks**: Nenhum

## Effort Estimate

**Points**: 2
**Confidence**: High
