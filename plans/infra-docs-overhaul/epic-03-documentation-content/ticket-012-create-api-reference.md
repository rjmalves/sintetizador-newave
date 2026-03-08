# ticket-012 Create API Reference with Autodoc

## Context

### Background

The sintetizador-newave codebase has `sphinx.ext.autosummary` and `sphinx.ext.autodoc` extensions already configured in `docs/source/conf.py` with `autosummary_generate = True` and the `numpydoc` extension for docstring parsing. However, there are no RST pages that invoke `.. autosummary::` or `.. automodule::` directives to generate API reference pages for the public modules. This ticket creates API reference RST pages that auto-generate documentation from the codebase's docstrings and class/enum definitions.

### Relation to Epic

This is the fourth content page in epic-03 (Documentation Content Expansion). It provides auto-generated API reference that complements the hand-written architecture page (ticket-009) and the existing data model reference (`referencia/modelo.rst`).

### Current State

The `docs/source/referencia/` directory has `saidas.rst` (output file documentation) and `modelo.rst` (data model variables/mnemonics reference). The `conf.py` already has: `sphinx.ext.autosummary` (with `autosummary_generate = True`), `sphinx.ext.autodoc`, `sphinx.ext.viewcode`, `numpydoc`, `add_module_names = False`, and `modindex_common_prefix = ["app."]`. The `_templates` directory does not exist yet -- it is referenced in `templates_path` but was never created. The key public modules for API reference are the `app.model` subpackages (enums for variables, spatial resolutions, synthesis dataclasses) and `app.services.unitofwork` (the UnitOfWork abstraction).

## Specification

### Requirements

1. Create `docs/source/referencia/api.rst` as the main API reference entry page
2. Use `.. autosummary::` with `:toctree: api/` to auto-generate individual module pages for the public model modules
3. Cover the following modules in the API reference:
   - `app.model.operation.variable` (Variable enum)
   - `app.model.operation.spatialresolution` (SpatialResolution enum)
   - `app.model.operation.operationsynthesis` (OperationSynthesis dataclass)
   - `app.model.scenario.variable` (Variable enum for scenarios)
   - `app.model.scenario.scenariosynthesis` (ScenarioSynthesis dataclass)
   - `app.model.settings` (Settings singleton)
4. Create the `docs/source/_templates/autosummary/` directory with a `module.rst` template that renders module members using `.. automodule::` with `:members:` and `:undoc-members:`
5. The generated API pages must render Enum members with their values

### Inputs/Props

- Existing `conf.py` configuration for autosummary and autodoc
- Public modules in `app/model/` with Enum classes and dataclasses

### Outputs/Behavior

- A new RST file at `docs/source/referencia/api.rst` with `.. autosummary::` directives
- A new template at `docs/source/_templates/autosummary/module.rst` for autosummary page generation
- Auto-generated API pages in `docs/source/referencia/api/` (generated at build time by autosummary)

### Error Handling

- If a module lacks docstrings, autosummary will still render class/function signatures with no description. This is acceptable -- docstring improvement is out of scope for this ticket.

## Acceptance Criteria

- [ ] Given the file `docs/source/referencia/api.rst` does not exist, when the ticket is implemented, then the file exists and contains `.. autosummary::` directives listing at least 6 modules from `app.model`
- [ ] Given the file `docs/source/_templates/autosummary/module.rst` does not exist, when the ticket is implemented, then the file exists and contains `.. automodule::` with `:members:` flag
- [ ] Given the new RST files, when `uv run sphinx-build -b html docs/source docs/build` is run from the repo root, then the build completes with no new errors (warnings about missing docstrings are acceptable)
- [ ] Given the build output, when inspecting `docs/build/referencia/api/`, then at least 6 HTML files exist corresponding to the listed modules

## Implementation Guide

### Suggested Approach

1. Create `docs/source/_templates/autosummary/module.rst` with content:

   ```rst
   {{ fullname | escape | underline}}

   .. automodule:: {{ fullname }}
      :members:
      :undoc-members:
      :show-inheritance:
   ```

2. Create `docs/source/referencia/api.rst` with:
   - Title: "Referencia da API" with `=` underline
   - Introductory paragraph explaining this is auto-generated from docstrings
   - `.. autosummary::` directive with `:toctree: api/` and `:template: autosummary/module.rst` options, listing the 6+ module paths
3. The `autosummary_generate = True` setting in `conf.py` will auto-generate the stub RST files in `referencia/api/` at build time -- no manual creation of individual module pages is needed
4. Do NOT modify `docs/source/index.rst` -- that is ticket-014's scope
5. Do NOT modify `docs/source/conf.py` -- autosummary is already configured

### Key Files to Modify

- `docs/source/referencia/api.rst` (new file)
- `docs/source/_templates/autosummary/module.rst` (new file)

### Patterns to Follow

- Use `=` underlines for title, consistent with other RST pages
- Reference `referencia/modelo.rst` with `:ref:` for cross-linking to the hand-written data model docs
- The autosummary template follows Sphinx autosummary conventions

### Pitfalls to Avoid

- Do NOT modify `index.rst` -- toctree integration is ticket-014
- Do NOT modify `conf.py` -- autosummary is already enabled
- Do NOT include private modules (those with `_` prefix like `_stubs_helpers.py`, `_stubs_market.py`) in the autosummary listing
- Do NOT attempt to improve docstrings in source code -- that is out of scope
- The `_templates` directory must match the `templates_path` in conf.py (it is `["_templates"]`)

## Testing Requirements

### Unit Tests

- Not applicable (RST documentation file)

### Integration Tests

- Verify `uv run sphinx-build -b html docs/source docs/build` succeeds
- Verify that HTML files are generated in `docs/build/referencia/api/`

### E2E Tests

- Not applicable

## Dependencies

- **Blocked By**: ticket-006-migrate-sphinx-theme.md
- **Blocks**: ticket-014-update-index-rst.md

## Effort Estimate

**Points**: 3
**Confidence**: High
