# Epic 02: Sphinx Documentation Modernization

## Goal

Migrar a documentacao Sphinx do tema sphinx-rtd-theme para o Furo, atualizar os exemplos da sphinx-gallery de pandas para Polars, e garantir que o build de documentacao funciona corretamente com a nova infraestrutura de CI.

## Scope

- Substituir sphinx-rtd-theme por Furo no `conf.py` e `pyproject.toml`
- Atualizar configuracoes do Sphinx (intersphinx para polars, remover pandas mapping)
- Migrar os 3 exemplos da sphinx-gallery (`plot_sintese_*.py`) para Polars
- Atualizar texto em `apresentacao.rst` para refletir migracao Polars
- Ajustar templates e static files para Furo
- Validar build completo sem warnings

## Tickets

| ID         | Titulo                                                  | Pontos |
| ---------- | ------------------------------------------------------- | ------ |
| ticket-006 | Migrate Sphinx theme from RTD to Furo                   | 3      |
| ticket-007 | Update sphinx-gallery examples to Polars                | 3      |
| ticket-008 | Update existing documentation text for Polars migration | 2      |

## Dependencies

- Epic 01 (ticket-001) para dependencias atualizadas no pyproject.toml

## Deliverables

1. `docs/source/conf.py` configurado com Furo
2. 3 exemplos sphinx-gallery usando Polars
3. Texto da documentacao atualizado para refletir Polars
4. Build Sphinx sem warnings
