# ticket-007 Update Sphinx-Gallery Examples to Polars

## Context

### Background

Os 3 exemplos da sphinx-gallery (`plot_sintese_operacao.py`, `plot_sintese_execucao.py`, `plot_sintese_cenarios.py`) usam pandas para ler e manipular os parquet de saida. Como o projeto migrou para Polars, os exemplos devem demonstrar o uso com Polars, que e a biblioteca nativa do projeto.

### Relation to Epic

Segundo ticket do Epic 02. Depende do ticket-006 para tema Furo ativo e build funcional.

### Current State

Os 3 arquivos em `/home/rogerio/git/sintetizador-newave/examples/`:

- `plot_sintese_operacao.py` (172 linhas): usa `pd.read_parquet`, `pd.DataFrame` operations, plotly
- `plot_sintese_execucao.py` (89 linhas): usa `pd.read_parquet`, `pd.to_timedelta`, plotly
- `plot_sintese_cenarios.py` (133 linhas): usa `pd.read_parquet`, plotly

Todos importam `pandas as pd` e usam metodos pandas como `.loc[]`, `.isin()`, `.unique()`, `.tolist()`, `.dtypes`.

Os exemplos sao executados pela sphinx-gallery durante o build da documentacao. Cada exemplo le arquivos `.parquet` do diretorio `examples/sintese/`.

## Specification

### Requirements

1. Substituir `import pandas as pd` por `import polars as pl` em todos os 3 exemplos
2. Substituir `pd.read_parquet(...)` por `pl.read_parquet(...)` em todos os exemplos
3. Adaptar operacoes de filtragem: `.loc[condition]` -> `.filter(condition)` com sintaxe Polars
4. Adaptar `.unique().tolist()` -> `.unique().to_list()` (Polars syntax)
5. Adaptar `.dtypes` -> `.schema` ou `.dtypes` (ambos existem em Polars)
6. Adaptar `.head(N)` -> `.head(N)` (mesma API)
7. Adaptar `pd.to_timedelta` -> logica equivalente em Polars
8. Manter todos os graficos plotly funcionais (plotly aceita Polars DataFrames diretamente desde plotly >= 5.16)
9. Manter texto dos comentarios em pt-BR, mesmo estilo atual
10. Atualizar o texto dos comentarios onde necessario para refletir que os dados sao Polars DataFrames

### Inputs/Props

- `/home/rogerio/git/sintetizador-newave/examples/plot_sintese_operacao.py`
- `/home/rogerio/git/sintetizador-newave/examples/plot_sintese_execucao.py`
- `/home/rogerio/git/sintetizador-newave/examples/plot_sintese_cenarios.py`

### Outputs/Behavior

- 3 exemplos reescritos com Polars
- sphinx-gallery gera as paginas de exemplos sem erros
- Graficos plotly renderizados corretamente

### Error Handling

- Se plotly nao aceitar Polars DataFrame diretamente em alguma funcao, converter com `.to_pandas()` apenas nesse ponto especifico (e documentar no comentario)

## Acceptance Criteria

- [ ] Given o arquivo `plot_sintese_operacao.py`, when inspecionado, then a string `import pandas` nao aparece e `import polars as pl` aparece
- [ ] Given o arquivo `plot_sintese_execucao.py`, when inspecionado, then a string `import pandas` nao aparece e `import polars as pl` aparece
- [ ] Given o arquivo `plot_sintese_cenarios.py`, when inspecionado, then a string `import pandas` nao aparece e `import polars as pl` aparece
- [ ] Given o comando `uv run sphinx-build -M html docs/source docs/build`, when executado, then o exit code e 0 e o diretorio `docs/build/html/examples/` contem arquivos HTML gerados
- [ ] Given qualquer dos 3 exemplos, when o texto dos comentarios e inspecionado, then esta em pt-BR

## Implementation Guide

### Suggested Approach

Para cada exemplo, aplicar as seguintes transformacoes sistematicas:

**Imports:**

```python
# Antes
import pandas as pd
# Depois
import polars as pl
```

**Leitura de parquet:**

```python
# Antes
df = pd.read_parquet("sintese/ARQUIVO.parquet")
# Depois
df = pl.read_parquet("sintese/ARQUIVO.parquet")
```

**Filtragem:**

```python
# Antes
filtered = df.loc[df["coluna"] == valor]
filtered = df.loc[(df["col1"] <= 12) & df["col2"].isin([6, 169])]
# Depois
filtered = df.filter(pl.col("coluna") == valor)
filtered = df.filter((pl.col("col1") <= 12) & pl.col("col2").is_in([6, 169]))
```

**Valores unicos:**

```python
# Antes
vals = df["coluna"].unique().tolist()
# Depois
vals = df["coluna"].unique().to_list()
```

**Print:**

```python
# Antes
print(df.head(10))
# Depois
print(df.head(10))  # mesma API
```

**Dtypes:**

```python
# Antes
df.dtypes
# Depois
df.schema  # retorna dict {name: dtype}
```

**Timedelta (plot_sintese_execucao.py):**

```python
# Antes
tempo["tempo"] = pd.to_timedelta(tempo["tempo"], unit="s") / timedelta(hours=1)
tempo["label"] = [str(timedelta(hours=d)) for d in tempo["tempo"].tolist()]
# Depois
tempo = tempo.with_columns(
    (pl.col("tempo") / 3600).alias("tempo_horas"),
    pl.col("tempo").map_elements(
        lambda s: str(timedelta(seconds=int(s))), return_dtype=pl.String
    ).alias("label"),
)
```

**Plotly:**
Plotly >= 5.16 aceita Polars DataFrames nativamente em `px.box`, `px.line`, `px.violin`, `px.pie`, `px.bar`. Verificar que a versao instalada e >= 5.16 (atualmente `plotly` esta no dev deps sem versao fixada; manter assim).

### Key Files to Modify

- `/home/rogerio/git/sintetizador-newave/examples/plot_sintese_operacao.py`
- `/home/rogerio/git/sintetizador-newave/examples/plot_sintese_execucao.py`
- `/home/rogerio/git/sintetizador-newave/examples/plot_sintese_cenarios.py`

### Patterns to Follow

- Manter a mesma estrutura de celulas (`# %%` separators) dos exemplos atuais
- Manter comentarios em pt-BR no mesmo estilo
- Usar `pl.col()` para expressoes (idiomatico Polars)

### Pitfalls to Avoid

- Nao assumir que `plotly.express` aceita todos os metodos com Polars — testar cada grafico
- Cuidado com `.loc[]` que nao existe em Polars — sempre usar `.filter()`
- A coluna `cenario` nos parquets pode ser int ou str dependendo do arquivo; Polars e mais strict com tipos que pandas
- Nao alterar os arquivos parquet em `examples/sintese/` — eles sao dados de teste fixos

## Testing Requirements

### Unit Tests

- N/A

### Integration Tests

- `uv run sphinx-build -M html docs/source docs/build` deve completar sem erros
- Verificar que `docs/build/html/examples/plot_sintese_operacao.html` existe e contem conteudo renderizado
- Verificar que `docs/build/html/examples/plot_sintese_execucao.html` existe
- Verificar que `docs/build/html/examples/plot_sintese_cenarios.html` existe

### E2E Tests

- N/A

## Dependencies

- **Blocked By**: ticket-006-migrate-sphinx-theme.md
- **Blocks**: Nenhum

## Effort Estimate

**Points**: 3
**Confidence**: Medium (depende de compatibilidade plotly+polars+sphinx-gallery)
