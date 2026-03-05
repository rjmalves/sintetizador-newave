# ticket-015 Decompose operation.py into Resolution Modules

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Split `app/services/synthesis/operation.py` (currently ~2538 lines) into a package `app/services/synthesis/operation/` with separate modules for each spatial resolution handler (SIN, SBM, REE, UHE, UTE, PEE, SBP), stub methods, cache management, export logic, and the main orchestrator. The `OperationSynthetizer` class becomes a thin orchestrator that delegates to resolution-specific modules. No behavioral changes.

## Anticipated Scope

- **Files likely to be modified**: `app/services/synthesis/operation.py` (split into package), `app/services/handlers.py` (update import), tests that import from operation.py
- **Key decisions needed**: Module organization within the package. Whether the resolution handlers are classes, functions, or a registry pattern. How the stub method dispatch works across modules. Whether the cache remains class-level or moves to a module-level singleton.
- **Open questions**: What is the cleanest way to split the class while preserving the dependency ordering logic? How to handle cross-resolution dependencies (e.g., UHE variables used to compute REE/SBM aggregates)? Will the test imports change significantly?

## Dependencies

- **Blocked By**: ticket-014-evaluate-thread-io-parallelism.md
- **Blocks**: ticket-016-decompose-deck.md

## Effort Estimate

**Points**: 5
**Confidence**: Low (will be re-estimated during refinement)
