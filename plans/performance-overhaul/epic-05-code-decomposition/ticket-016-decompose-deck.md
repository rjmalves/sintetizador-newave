# ticket-016 Decompose deck.py into Domain Modules

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Split `app/services/deck/deck.py` (currently ~4283 lines) into a package `app/services/deck/` with separate modules for each data domain: general configuration (dger, pmo, newavetim), hydro data (confhd, hidr, modif, vazoes), thermal data (conft, clast, term, manutt, expt), temporal data (patamar, stages, dates), energy data (energias, enavaz), and policy data (cortes, estados). The `Deck` class becomes a facade that delegates to domain-specific modules. No behavioral changes.

## Anticipated Scope

- **Files likely to be modified**: `app/services/deck/deck.py` (split into modules), `app/services/deck/context.py` (may need import updates), `app/services/deck/bounds.py` (import updates), all files that import from `app.services.deck.deck`
- **Key decisions needed**: Whether domain modules are classes or collections of functions. Whether the DECK_DATA_CACHING dict is centralized or distributed across domain modules. Whether the facade preserves the exact same public API.
- **Open questions**: How many distinct domains exist when analyzed by usage patterns? Are there circular dependencies between domain modules? Does the DeckContext from ticket-002 influence the module boundaries?

## Dependencies

- **Blocked By**: ticket-015-decompose-operation.md
- **Blocks**: ticket-017-decompose-files.md

## Effort Estimate

**Points**: 5
**Confidence**: Low (will be re-estimated during refinement)
