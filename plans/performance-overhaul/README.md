# Performance Overhaul Plan

## Overview

Comprehensive performance and architecture overhaul for sintetizador-newave, targeting an 8x runtime improvement (from ~40 minutes to under 5 minutes) for large-scale NEWAVE post-processing.

## Tech Stack

- Python >= 3.10
- Polars (new) - data manipulation hot path
- pandas - inewave boundary, legacy paths
- pyarrow >= 18 - Parquet export
- click >= 8.1.7 - CLI
- inewave >= 1.9.2 - NEWAVE file parsing
- concurrent.futures (stdlib) - parallelism

## Epics

| Epic | Name                                    | Tickets      | Status    |
| ---- | --------------------------------------- | ------------ | --------- |
| 01   | Foundation Fixes                        | 3 (detailed) | Completed |
| 02   | Statistics & Data Pipeline Optimization | 4 (detailed) | Completed |
| 03   | Full Hot-Path Polars Migration          | 4 (refined)  | Completed |
| 04   | Parallelism Overhaul                    | 3 (refined)  | Completed |
| 05   | Code Decomposition & Cleanup            | 4 (refined)  | Executing |

## Progress Tracking

| Ticket     | Title                                                  | Epic    | Status    | Detail Level | Readiness | Quality | Badge      |
| ---------- | ------------------------------------------------------ | ------- | --------- | ------------ | --------- | ------- | ---------- |
| ticket-001 | Eliminate chdir() from FSUnitOfWork                    | epic-01 | completed | Detailed     | 1.00      | 0.86    | ACCEPTABLE |
| ticket-002 | Pre-compute deck data and pass to subprocesses         | epic-01 | completed | Detailed     | 1.00      | 0.80    | ACCEPTABLE |
| ticket-003 | Move statistics computation after entity concatenation | epic-01 | completed | Detailed     | 1.00      | 0.83    | ACCEPTABLE |
| ticket-004 | Add Polars dependency and conversion utilities         | epic-02 | completed | Detailed     | 1.00      | 0.95    | EXCELLENT  |
| ticket-005 | Rewrite calc_statistics using Polars                   | epic-02 | completed | Detailed     | 0.97      | 0.86    | ACCEPTABLE |
| ticket-006 | Optimize DataFrame concatenation with Polars           | epic-02 | completed | Detailed     | 0.96      | 1.00    | EXCELLENT  |
| ticket-007 | Eliminate unnecessary DataFrame copies in cache        | epic-02 | completed | Detailed     | 1.00      | 0.90    | EXCELLENT  |
| ticket-008 | Migrate temporal resolution to Polars                  | epic-03 | completed | Refined      | 1.00      | 1.00    | EXCELLENT  |
| ticket-009 | Migrate entity post-processing pipeline to Polars      | epic-03 | completed | Refined      | 1.00      | 0.88    | ACCEPTABLE |
| ticket-010 | Migrate bounds computation to Polars                   | epic-03 | completed | Refined      | 1.00      | 0.90    | EXCELLENT  |
| ticket-011 | Migrate Parquet export to Polars native writer         | epic-03 | completed | Refined      | 1.00      | 1.00    | EXCELLENT  |
| ticket-012 | Replace multiprocessing.Pool with concurrent.futures   | epic-04 | completed | Refined      | 0.98      | 0.90    | EXCELLENT  |
| ticket-013 | Implement variable-group parallelism                   | epic-04 | completed | Refined      | 1.00      | 0.85    | ACCEPTABLE |
| ticket-014 | Evaluate thread-based I/O parallelism                  | epic-04 | completed | Refined      | 1.00      | 0.85    | ACCEPTABLE |
| ticket-015 | Decompose operation.py into resolution modules         | epic-05 | completed | Refined      | 0.96      | 0.63    | BELOW GATE |
| ticket-016 | Decompose deck.py into domain modules                  | epic-05 | completed | Refined      | 0.96      | 0.93    | EXCELLENT  |
| ticket-017 | Decompose files.py into variable mapping modules       | epic-05 | completed | Refined      | 0.96      | 0.83    | ACCEPTABLE |
| ticket-018 | Add type annotations and remove dead code              | epic-05 | completed | Refined      | 0.94      | 0.88    | ACCEPTABLE |

## Dependency Graph

```
ticket-001 (chdir) ──> ticket-002 (deck context)
                                    |
ticket-003 (stats) ──> ticket-004 (polars dep) ──> ticket-005 (polars stats)  ──> ticket-007 (copies)
                                    |               ticket-006 (polars concat) ──/
                                    v
                          ticket-008 (temporal) ──> ticket-009 (entity pipeline) ──> ticket-010 (bounds) ──> ticket-011 (export)
                                                                                                                     |
                                                                                                                     v
                                                                                                           ticket-012 (futures)
                                                                                                            /              \
                                                                                              ticket-013 (groups)    ticket-014 (threads)
                                                                                                            \              /
                                                                                                              v            v
                                                                                                    ticket-015 (decompose operation)
                                                                                                              |
                                                                                                    ticket-016 (decompose deck)
                                                                                                              |
                                                                                                    ticket-017 (decompose files)
                                                                                                              |
                                                                                                    ticket-018 (types & cleanup)
```

## Readiness Scores

| Ticket     | Composite | Structure | Testability | Boundary | Dep Clarity | Atomicity |
| ---------- | --------- | --------- | ----------- | -------- | ----------- | --------- |
| ticket-001 | 1.00      | 1.00      | 1.00        | 1.00     | 1.00        | 1.00      |
| ticket-002 | 1.00      | 1.00      | 1.00        | 1.00     | 1.00        | 1.00      |
| ticket-003 | 1.00      | 1.00      | 1.00        | 1.00     | 1.00        | 1.00      |
| ticket-004 | 1.00      | 1.00      | 1.00        | 1.00     | 1.00        | 1.00      |
| ticket-005 | 0.97      | 1.00      | 0.90        | 1.00     | 1.00        | 1.00      |
| ticket-006 | 0.96      | 1.00      | 0.88        | 1.00     | 1.00        | 1.00      |
| ticket-007 | 1.00      | 1.00      | 1.00        | 1.00     | 1.00        | 1.00      |
| ticket-008 | 1.00      | 1.00      | 1.00        | 1.00     | 1.00        | 1.00      |
| ticket-009 | 1.00      | 1.00      | 1.00        | 1.00     | 1.00        | 1.00      |
| ticket-010 | 1.00      | 1.00      | 1.00        | 1.00     | 1.00        | 1.00      |
| ticket-011 | 1.00      | 1.00      | 1.00        | 1.00     | 1.00        | 1.00      |
| ticket-012 | 0.98      | 1.00      | 1.00        | 1.00     | 1.00        | 0.80      |
| ticket-013 | 1.00      | 1.00      | 1.00        | 1.00     | 1.00        | 1.00      |
| ticket-014 | 1.00      | 1.00      | 1.00        | 1.00     | 1.00        | 1.00      |
| ticket-015 | 0.96      | 1.00      | 1.00        | 1.00     | 1.00        | 0.60      |
| ticket-016 | 0.96      | 1.00      | 1.00        | 1.00     | 1.00        | 0.60      |
| ticket-017 | 0.96      | 1.00      | 1.00        | 1.00     | 1.00        | 0.60      |
| ticket-018 | 0.94      | 1.00      | 1.00        | 1.00     | 1.00        | 0.40      |

Dimensions below 0.85: ticket-015:atomicity (0.60), ticket-016:atomicity (0.60), ticket-017:atomicity (0.60), ticket-018:atomicity (0.40)

Note: Low atomicity scores for decomposition tickets are inherent to their nature -- splitting a monolithic file into a package necessarily touches many files. These tickets cannot be further decomposed without creating artificial intermediate states. All composite scores are above the 0.85 gate.
