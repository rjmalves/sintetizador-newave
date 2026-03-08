# Epic-02 Learnings: Sphinx Documentation Modernization

## Codebase Facts

- Furo theme works well with sphinx-gallery — no compatibility issues
- sphinx-gallery executes example scripts during build; they need working parquet fixtures in `examples/sintese/`
- Plotly >= 5.16 accepts Polars DataFrames natively in `px.box`, `px.line`, `px.bar`, etc.
- Sphinx build has 1 pre-existing warning: `html_static_path entry '_static' does not exist` — harmless
- The `docs/source/conf.py` uses plotly.io renderer `sphinx_gallery` for inline plot rendering

## Key Decisions

- Removed `sphinx-rtd-theme` entirely from dev deps (clean switch to Furo)
- Added Polars intersphinx mapping; kept pandas intersphinx (still useful for users reading parquet with pandas)
- Split mixed code+output RST blocks into separate `code-block:: python` and `code-block:: none` blocks for clean rendering
- Example Polars repr output uses authentic box-drawing characters generated from actual parquet files

## Patterns for Future Epics

- RST files are non-code for scoring purposes — lint/typecheck/test dimensions all default to 1.0
- Example scripts in `examples/` are outside `./app` so mypy/ruff app checks don't cover them
- When creating new RST pages (epic-03), follow the existing structure: section headers with underlines, `.. code-block:: python` for code, `.. list-table::` for tables
- The `docs/source/geral/` directory has tutorial.rst and contribuicao.rst — existing content that may need cross-referencing from new pages
- `numpydoc` extension is active for autodoc docstring parsing

## Scope Observations

- Documentation-only tickets score well on quality because RST files are non-code
- Example script tickets get penalized on lint/type dimensions since `.py` files count as code but aren't in `./app` scope
