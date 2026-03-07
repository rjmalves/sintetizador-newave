# Polars-Native Migration Plan

## Overview

Migrate sintetizador-newave from a hybrid pandas/polars architecture to a polars-native architecture. Eliminates ~6 unnecessary DataFrame copies per variable, ~30 pd/pl conversion call sites, 7 defensive `.copy()` calls, and ~70 lines of dead fallback code.

## Tech Stack

- Python >= 3.10
- Polars >= 1.0.0 (primary DataFrame library, post-migration)
- pandas (inewave boundary only: readers.py, accessors.py pre-conversion)
- pyarrow >= 18 (Parquet export)
- inewave >= 1.9.2 (external, returns pandas)

## Epics

| Epic | Name                | Tickets      | Status    |
| ---- | ------------------- | ------------ | --------- |
| 01   | Conversion Boundary | 3 (detailed) | Completed |
| 02   | Deck Domain Modules | 6 (detailed) | Completed |
| 03   | Synthesis Pipeline  | 3 (outline)  | Pending   |
| 04   | Scenario & Cleanup  | 2 (outline)  | Pending   |

## Progress Tracking

| Ticket     | Title                                                       | Epic    | Status    | Detail Level | Readiness | Quality | Badge      |
| ---------- | ----------------------------------------------------------- | ------- | --------- | ------------ | --------- | ------- | ---------- |
| ticket-001 | Convert cached accessors to return polars DataFrames        | epic-01 | completed | Detailed     | 1.00      | 0.88    | ACCEPTABLE |
| ticket-002 | Convert uncached series accessors and DeckContext to polars | epic-01 | completed | Detailed     | 1.00      | 0.83    | ACCEPTABLE |
| ticket-003 | Add polars compatibility shims to downstream consumers      | epic-01 | completed | Detailed     | 0.91      | 0.85    | ACCEPTABLE |
| ticket-004 | Port entities.py to polars                                  | epic-02 | completed | Detailed     | 0.95      | 0.87    | ACCEPTABLE |
| ticket-005 | Port temporal.py and misc.py to polars                      | epic-02 | completed | Detailed     | 1.00      | 0.90    | EXCELLENT  |
| ticket-006 | Port exchange.py and energy.py to polars                    | epic-02 | completed | Detailed     | 1.00      | 0.78    | ACCEPTABLE |
| ticket-007 | Port hydro.py to polars                                     | epic-02 | completed | Detailed     | 0.97      | 0.65    | BELOW GATE |
| ticket-008 | Port storage.py and thermal.py to polars                    | epic-02 | completed | Detailed     | 0.97      | 0.78    | ACCEPTABLE |
| ticket-009 | Port policy.py to polars                                    | epic-02 | completed | Detailed     | 1.00      | 0.88    | ACCEPTABLE |
| ticket-010 | Port pipeline.py to native polars                           | epic-03 | pending   | Outline      | --        | --      | --         |
| ticket-011 | Port synthesis bounds, cache, and export to polars          | epic-03 | pending   | Outline      | --        | --      | --         |
| ticket-012 | Port resolution modules and spatial dispatch to polars      | epic-03 | pending   | Outline      | --        | --      | --         |
| ticket-013 | Port scenario.py to polars                                  | epic-04 | pending   | Outline      | --        | --      | --         |
| ticket-014 | Remove conversion utilities and dead pandas imports         | epic-04 | pending   | Outline      | --        | --      | --         |

## Dependency Graph

```
ticket-001 (cached accessors) --> ticket-002 (uncached + DeckContext)
                                        |
                                        v
                                  ticket-003 (shims)
                                        |
                                        v
                                  ticket-004 (entities)
                                   /    |    \
                                  v     v     v
                      ticket-005   ticket-006   ticket-007 (hydro)
                      (temporal+misc) (exchange+energy)   |
                                  \     |     /           v
                                   v    v    v      ticket-008 (storage+thermal)
                                  ticket-009 (policy)
                                        |
                                        v
                                  ticket-010 (pipeline) [OUTLINE]
                                        |
                                        v
                                  ticket-011 (bounds+cache+export) [OUTLINE]
                                        |
                                        v
                                  ticket-012 (resolution modules) [OUTLINE]
                                        |
                                        v
                                  ticket-013 (scenario) [OUTLINE]
                                        |
                                        v
                                  ticket-014 (cleanup) [OUTLINE]
```

## Readiness Scores

| Ticket     | Composite | Structure | Testability | Boundary | Dep Clarity | Atomicity |
| ---------- | --------- | --------- | ----------- | -------- | ----------- | --------- |
| ticket-001 | 1.00      | 1.00      | 1.00        | 1.00     | 1.00        | 1.00      |
| ticket-002 | 1.00      | 1.00      | 1.00        | 1.00     | 1.00        | 1.00      |
| ticket-003 | 0.91      | 1.00      | 0.90        | 0.88     | 1.00        | 0.60      |
| ticket-004 | 0.95      | 1.00      | 0.90        | 1.00     | 1.00        | 0.80      |
| ticket-005 | 1.00      | 1.00      | 1.00        | 1.00     | 1.00        | 1.00      |
| ticket-006 | 1.00      | 1.00      | 1.00        | 1.00     | 1.00        | 1.00      |
| ticket-007 | 0.97      | 1.00      | 0.90        | 1.00     | 1.00        | 1.00      |
| ticket-008 | 0.97      | 1.00      | 0.90        | 1.00     | 1.00        | 1.00      |
| ticket-009 | 1.00      | 1.00      | 1.00        | 1.00     | 1.00        | 1.00      |

Dimensions below 0.85: ticket-003:atomicity (0.60)

Note: ticket-003 (compatibility shims) inherently touches many files (~18) because it must add shims to every downstream consumer of Deck. This cannot be decomposed further without creating intermediate broken states. The composite score (0.91) is above the 0.85 gate.
