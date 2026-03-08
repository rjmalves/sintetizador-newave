# ticket-011 Create Performance Tuning Guide

## Context

### Background

The sintetizador-newave recently migrated from pandas to Polars for its data processing pipeline, and supports multiprocessing via the `--processadores` CLI flag. However, there is no documentation explaining how to optimize synthesis performance for large NEWAVE cases. Users need guidance on parallelism configuration, output format impact, and general best practices. This ticket creates a performance tuning guide in pt-BR.

### Relation to Epic

This is the third content page in epic-03 (Documentation Content Expansion). It addresses a gap identified during the Polars migration -- users need to understand the performance characteristics of the new pipeline and how to configure it for their workloads.

### Current State

The tutorial page mentions `--processadores` briefly but does not explain performance implications. The CLI supports `--processadores INTEGER` for `operacao`, `cenarios`, and `completa` commands. The `--formato` flag controls output format (PARQUET default, CSV alternative). The `Settings` class in `app/model/settings.py` reads `PROCESSADORES` from environment or defaults to 1. There is no performance documentation page.

## Specification

### Requirements

1. Create a new file `docs/source/geral/performance.rst` with performance tuning content in pt-BR
2. Document the `--processadores` flag: what it does, which commands support it (operacao, cenarios, completa), and recommended values
3. Document output format impact: PARQUET (snappy compression, default) vs CSV (larger files, slower I/O)
4. Document the Polars migration benefit: replaced pandas DataFrame operations with Polars lazy evaluation and native parallelism
5. Include practical recommendations: number of processors vs case size, memory considerations, disk I/O bottlenecks
6. Include example CLI invocations with timing comparisons (qualitative, not benchmark numbers -- e.g., "significantly faster" with Polars)

### Inputs/Props

- `app/model/settings.py` for configuration options (PROCESSADORES, FORMATO_SINTESE)
- Tutorial log output showing timing per synthesis variable
- CHANGELOG v2.0.0 entries about Polars migration and parquet format changes

### Outputs/Behavior

- A new RST file at `docs/source/geral/performance.rst` that renders in the Sphinx build

### Error Handling

- Not applicable (documentation-only ticket)

## Acceptance Criteria

- [ ] Given the file `docs/source/geral/performance.rst` does not exist, when the ticket is implemented, then the file exists and contains at least 60 lines of RST content in pt-BR
- [ ] Given the new RST file, when `uv run sphinx-build -b html docs/source docs/build` is run from the repo root, then the build completes with no new warnings referencing `performance.rst`
- [ ] Given the RST content, when rendered, then it contains sections covering at least: paralelismo (`--processadores`), formato de saida (PARQUET vs CSV), and recomendacoes gerais
- [ ] Given the RST content, when rendered, then it contains at least 2 CLI example invocations using `.. code-block:: bash` showing `--processadores` and `--formato` usage

## Implementation Guide

### Suggested Approach

1. Create `docs/source/geral/performance.rst` with the following structure:
   - Title: "Guia de Performance" with `=` underline
   - Section "Paralelismo": explain `--processadores N`, which commands support it, that it controls multiprocessing workers for reading NEWAVE output files in parallel. Recommend starting with the number of physical CPU cores and adjusting down if memory is constrained.
   - Section "Formato de Saida": explain PARQUET (default, snappy compression, efficient columnar storage) vs CSV (text-based, larger files, slower read/write). Note that PARQUET is recommended for all use cases; CSV is available for compatibility with tools that cannot read parquet.
   - Section "Migracao para Polars": briefly explain that v2.x migrated from pandas to Polars, resulting in lower memory usage and faster processing. Reference that Polars DataFrames are used natively throughout the pipeline.
   - Section "Recomendacoes para Casos Grandes": practical tips for large NEWAVE cases (many scenarios, many UHEs): use `--processadores` matching CPU cores, use PARQUET format, ensure sufficient disk space for synthesis output directory.
2. Include CLI examples:

   ```
   .. code-block:: bash

       $ sintetizador-newave operacao --processadores 8
       $ sintetizador-newave completa --processadores 16 --formato PARQUET
   ```

3. Do NOT include fabricated benchmark numbers -- keep performance claims qualitative
4. Do NOT modify `docs/source/index.rst` -- that is ticket-014's scope

### Key Files to Modify

- `docs/source/geral/performance.rst` (new file)

### Patterns to Follow

- Use `=` underlines for title, `-` for sections (matches existing RST style)
- Use `.. code-block:: bash` for CLI commands (matches tutorial.rst)
- Use `.. note::` admonitions for important tips
- Write in pt-BR

### Pitfalls to Avoid

- Do NOT modify `index.rst` -- toctree integration is ticket-014
- Do NOT fabricate specific benchmark numbers or timing comparisons -- only describe qualitative improvements
- Do NOT recommend specific hardware configurations -- keep recommendations generic (e.g., "number of physical CPU cores")

## Testing Requirements

### Unit Tests

- Not applicable (RST documentation file)

### Integration Tests

- Verify `uv run sphinx-build -b html docs/source docs/build` succeeds without new warnings

### E2E Tests

- Not applicable

## Dependencies

- **Blocked By**: ticket-006-migrate-sphinx-theme.md
- **Blocks**: ticket-014-update-index-rst.md

## Effort Estimate

**Points**: 2
**Confidence**: High
