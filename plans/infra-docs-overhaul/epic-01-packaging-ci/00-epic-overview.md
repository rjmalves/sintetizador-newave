# Epic 01: Packaging & CI Modernization

## Goal

Modernizar o empacotamento Python e os workflows de CI/CD do GitHub Actions, estabelecendo a infraestrutura necessaria para publicacao no PyPI, builds reprodutiveis e feedback rapido em PRs.

## Scope

- Limpeza do `pyproject.toml` (remover numba, adicionar py.typed, mypy config, classifiers atualizados)
- Commitar `uv.lock` e atualizar `.gitignore`
- Reestruturar `main.yml` em jobs paralelos com `setup-uv@v7`
- Migrar `docs.yml` para `actions/deploy-pages@v4`
- Criar `release.yml` com trusted publishing no PyPI
- Adicionar `.pre-commit-config.yaml`

## Tickets

| ID         | Titulo                                                   | Pontos |
| ---------- | -------------------------------------------------------- | ------ |
| ticket-001 | Modernize pyproject.toml and package metadata            | 3      |
| ticket-002 | Restructure CI workflow into parallel jobs               | 3      |
| ticket-003 | Migrate docs deployment to official GitHub Pages actions | 2      |
| ticket-004 | Create PyPI release workflow with trusted publishing     | 3      |
| ticket-005 | Add pre-commit hooks configuration                       | 2      |

## Dependencies

- Nenhuma dependencia externa. Este epic e a base para os demais.

## Deliverables

1. `pyproject.toml` limpo e moderno
2. `app/py.typed` marker file
3. `uv.lock` commitado
4. `.github/workflows/main.yml` com jobs paralelos
5. `.github/workflows/docs.yml` usando actions/deploy-pages@v4
6. `.github/workflows/release.yml` com trusted publishing
7. `.pre-commit-config.yaml` com ruff + mypy
