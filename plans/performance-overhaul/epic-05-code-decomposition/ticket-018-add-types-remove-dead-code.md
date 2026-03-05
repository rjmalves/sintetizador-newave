# ticket-018 Add Type Annotations and Remove Dead Code

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Add comprehensive type annotations to all public functions across the decomposed modules (from tickets 015-017), remove `# type: ignore` comments where proper types can be inferred, remove unused imports, delete commented-out code, and ensure `ruff check` and `mypy` pass cleanly. This is the final cleanup ticket that brings the codebase to a consistent quality standard after all structural and performance changes.

## Anticipated Scope

- **Files likely to be modified**: All files under `app/` that were touched or created in epics 01-05, `pyproject.toml` (mypy/ruff configuration if needed)
- **Key decisions needed**: What mypy strictness level to target (strict vs basic). Whether to add `py.typed` marker for PEP 561 compliance. Which `# type: ignore` comments are removable vs necessary (e.g., inewave returns `Any` typed values).
- **Open questions**: How many `# type: ignore` comments currently exist? What fraction are due to inewave's untyped API vs internal issues? Is the ruff configuration in pyproject.toml sufficient or does it need updating for the new module structure?

## Dependencies

- **Blocked By**: ticket-017-decompose-files.md
- **Blocks**: None

## Effort Estimate

**Points**: 3
**Confidence**: Low (will be re-estimated during refinement)
