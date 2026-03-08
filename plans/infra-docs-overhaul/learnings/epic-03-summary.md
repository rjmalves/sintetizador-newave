# Epic-03 Learnings: Documentation Content Expansion

## Codebase Facts

- 5 new RST pages created: architecture, FAQ, performance, API reference, migration guide
- Architecture page covers all 6 app packages with accurate descriptions from source code inspection
- FAQ has 18 Q&A items across 5 thematic sections — sourced from CLI flags, CHANGELOG, and codebase
- Performance guide avoids fabricated benchmarks; qualitative guidance based on actual code patterns
- API reference uses autosummary with manual stubs (autosummary_generate doesn't work for orphan pages)
- Migration guide covers 8 breaking change categories from v1.x to v2.0.0 with before/after code pairs

## Key Decisions

- Text-based diagrams in `.. code-block:: none` instead of Mermaid (no new extensions)
- API reference scoped to `app.model` only — services/adapters are internal
- Manual autosummary stubs created because pages not in toctree can't auto-generate
- New "Arquitetura" toctree section between Apresentacao and Geral
- FAQ uses thematic sections (not flat Q&A) for better Furo sidebar navigation

## Patterns for Future Epics

- RST-only tickets score perfectly on quality (non-code detection)
- The `_static` warning in Sphinx build is pre-existing and harmless
- Autosummary-generated sub-pages produce toc.not_included warnings — this is normal
- Cross-references use `:doc:` for same-directory and `:ref:` for labeled targets
- All docs are in pt-BR with accented characters throughout

## What Went Well

- All 6 tickets completed with mean quality 0.99 — highest scoring epic
- No scope changes needed during refinement
- Specialist agents produced high-quality RST content on first pass
