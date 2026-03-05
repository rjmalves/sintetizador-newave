# ticket-013 Implement Variable-Group Parallelism by Spatial Resolution

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Restructure the main synthesis loop in `OperationSynthetizer.synthetize()` to group variables by spatial resolution and process each group in a single parallel batch, instead of the current pattern of iterating variables sequentially and parallelizing per-entity within each variable. Variables sharing the same spatial resolution (e.g., all UHE variables) share the same entity list (200 UHEs), the same DeckContext subset, and the same file naming pattern. Batching them together enables reading multiple variables for the same entity in a single subprocess invocation, reducing subprocess spawn overhead by the number of variables.

## Anticipated Scope

- **Files likely to be modified**: `app/services/synthesis/operation.py` (`synthetize()`, `_synthetize_single_variable`, `_resolve_spatial_resolution`, the `__resolve_*` methods), `app/services/synthesis/scenario.py` (equivalent restructuring)
- **Key decisions needed**: Whether to read all variables for one entity in a single subprocess call (reducing spawns but increasing per-call work) or to parallelize across entity x variable pairs. How to handle stub variables that depend on other variables' cache (these must be sequenced after their dependencies). Whether the dependency ordering from `_add_synthesis_dependencies` is compatible with group-level parallelism.
- **Open questions**: What is the actual distribution of variables across spatial resolutions? How many UHE variables exist vs SBM vs REE? Does the subprocess spawn overhead dominate over file I/O time? What is the optimal granularity -- one file per subprocess or multiple files per subprocess?

## Dependencies

- **Blocked By**: ticket-012-replace-multiprocessing-with-futures.md
- **Blocks**: None

## Effort Estimate

**Points**: 5
**Confidence**: Low (will be re-estimated during refinement)
