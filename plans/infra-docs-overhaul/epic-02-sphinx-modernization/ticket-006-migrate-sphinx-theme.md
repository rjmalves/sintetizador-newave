# ticket-006 Migrate Sphinx Theme from RTD to Furo

## Context

### Background

O `sintetizador-newave` usa `sphinx-rtd-theme` para a documentacao Sphinx. O Furo e um tema moderno com dark mode nativo, melhor responsividade mobile, e manutencao ativa. A migracao envolve substituir a configuracao do tema no `conf.py`, atualizar dependencias e ajustar estilos.

### Relation to Epic

Primeiro ticket do Epic 02 (Sphinx Modernization). Os demais tickets deste epic (exemplos Polars, texto atualizado) dependem do tema estar configurado para validar o build.

### Current State

O arquivo `docs/source/conf.py`:

- Usa `html_theme = "sphinx_rtd_theme"` (linha 88)
- Tem `html_theme_options` especificas do RTD (linhas 89-96)
- Inclui `"sphinx_rtd_theme"` na lista `extensions` (linha 49)
- Intersphinx mapping inclui `pandas` (linha 117) mas nao `polars`
- Usa `plotly.io` com renderer `sphinx_gallery` (linhas 17-19)

O `pyproject.toml` (apos ticket-001) tera `furo` e `sphinx-rtd-theme` ambos nos dev deps.

## Specification

### Requirements

1. Substituir `html_theme = "sphinx_rtd_theme"` por `html_theme = "furo"` em `conf.py`
2. Remover `"sphinx_rtd_theme"` da lista `extensions` (Furo nao precisa ser listado como extensao)
3. Substituir `html_theme_options` por opcoes compativeis com Furo (sidebar_hide_name, navigation_with_keys, top_of_page_buttons)
4. Adicionar intersphinx mapping para `polars`
5. Manter intersphinx mapping para `pandas` (ainda usado em exemplos de leitura de parquet pelo usuario)
6. Remover `sphinx-rtd-theme` do dev dependencies no `pyproject.toml`
7. Remover import `from typing import List` no conf.py (nao necessario em Python >= 3.10)
8. Validar build Sphinx sem warnings

### Inputs/Props

- Arquivo fonte: `/home/rogerio/git/sintetizador-newave/docs/source/conf.py`
- Arquivo fonte: `/home/rogerio/git/sintetizador-newave/pyproject.toml`

### Outputs/Behavior

- Documentacao renderizada com tema Furo (dark mode disponivel, layout responsivo)
- Build Sphinx completa sem erros
- Intersphinx links para polars funcionais

### Error Handling

- Se sphinx-gallery nao for compativel com Furo, verificar versao minima de sphinx-gallery que suporta temas modernos. Fallback: desabilitar temporariamente sphinx-gallery e abrir issue.

## Acceptance Criteria

- [ ] Given o arquivo `docs/source/conf.py`, when a variavel `html_theme` e inspecionada, then o valor e `"furo"`
- [ ] Given o arquivo `conf.py`, when a lista `extensions` e inspecionada, then a string `sphinx_rtd_theme` nao aparece
- [ ] Given o arquivo `conf.py`, when o dicionario `intersphinx_mapping` e inspecionado, then contem uma chave `"polars"`
- [ ] Given o comando `uv run sphinx-build -M html docs/source docs/build`, when executado, then o exit code e 0
- [ ] Given o arquivo `pyproject.toml`, when os dev dependencies sao inspecionados, then `sphinx-rtd-theme` nao aparece e `furo` aparece

## Implementation Guide

### Suggested Approach

1. Editar `docs/source/conf.py`:
   - Remover `from typing import List` (linha 6)
   - Substituir `exclude_patterns: List[str] = []` por `exclude_patterns: list[str] = []`
   - Na lista `extensions`, remover `"sphinx_rtd_theme"`
   - Substituir `html_theme = "sphinx_rtd_theme"` por `html_theme = "furo"`
   - Substituir o bloco `html_theme_options` por:
     ```python
     html_theme_options = {
         "sidebar_hide_name": False,
         "navigation_with_keys": True,
         "top_of_page_buttons": ["view", "edit"],
     }
     ```
   - Adicionar ao `intersphinx_mapping`:
     ```python
     "polars": ("https://docs.pola.rs/api/python/stable/", None),
     ```
   - Atualizar `intersphinx_mapping` de `pandas` para usar HTTPS:
     ```python
     "pandas": ("https://pandas.pydata.org/pandas-docs/stable/", None),
     ```

2. Editar `pyproject.toml`:
   - Remover `"sphinx-rtd-theme"` da lista `dev`

3. Executar `uv sync --all-extras --dev` para atualizar lockfile
4. Executar `uv run sphinx-build -M html docs/source docs/build`
5. Abrir `docs/build/html/index.html` no navegador para verificar renderizacao visual

### Key Files to Modify

- `/home/rogerio/git/sintetizador-newave/docs/source/conf.py`
- `/home/rogerio/git/sintetizador-newave/pyproject.toml`

### Patterns to Follow

- Furo nao precisa ser listado em `extensions` (e registrado automaticamente via entry points do Sphinx)
- Manter `language = "pt_BR"` para que strings do Sphinx geradas automaticamente (ex: "Indice", "Busca") fiquem em portugues

### Pitfalls to Avoid

- Nao remover `numpydoc` da lista de extensions — ainda necessario para autodoc de docstrings
- Nao remover `sphinx_gallery.gen_gallery` — os exemplos dependem dela
- Verificar que `sphinx_gallery_conf` continua funcional com Furo (o tema nao afeta a gallery em si, mas testar)
- Se `top_of_page_buttons` incluir `"edit"`, o Furo tenta construir link para editar no GitHub; configurar `html_context` com repo info se necessario

## Testing Requirements

### Unit Tests

- N/A

### Integration Tests

- `uv run sphinx-build -M html docs/source docs/build` deve retornar exit code 0
- Verificar que `docs/build/html/index.html` existe e contem referencia ao Furo (ex: `furo` no CSS)

### E2E Tests

- N/A

## Dependencies

- **Blocked By**: ticket-001-modernize-pyproject-toml.md
- **Blocks**: ticket-007-update-sphinx-gallery-examples.md, ticket-008-update-docs-text.md

## Effort Estimate

**Points**: 3
**Confidence**: High
