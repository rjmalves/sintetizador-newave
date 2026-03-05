# ticket-017 Decompose files.py into Variable Mapping Modules

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Split `app/adapters/repository/files.py` (currently ~1912 lines) by extracting the massive `__regras` dictionary (lines 343-1131, mapping `(Variable, SpatialResolution)` tuples to lambda file readers) into separate mapping modules organized by variable category or spatial resolution. The `RawFilesRepository` class retains core file reading infrastructure while the variable-specific mapping logic moves to dedicated modules.

## Anticipated Scope

- **Files likely to be modified**: `app/adapters/repository/files.py` (extract `__regras` dict), new files under `app/adapters/repository/mappings/` or similar
- **Key decisions needed**: Whether to organize mappings by variable category (energy, flow, cost, generation, etc.) or by spatial resolution. Whether to use a registry pattern where mapping modules register themselves. Whether the lambda-heavy pattern should be refactored to named functions.
- **Open questions**: How many distinct variable categories exist? Are there opportunities to reduce code duplication in the lambdas (many follow the pattern `lambda dir, entity=1: self.__add_block_column(self.__read_nwlistop_setting_version(ReaderClass, join(dir, f"filename{entity}.out")))`)?

## Dependencies

- **Blocked By**: ticket-016-decompose-deck.md
- **Blocks**: ticket-018-add-types-remove-dead-code.md

## Effort Estimate

**Points**: 3
**Confidence**: Low (will be re-estimated during refinement)
