# Epic 04: Parallelism Overhaul

## Goal

Replace the current `multiprocessing.Pool` per-entity parallelism model with a modern `concurrent.futures`-based approach that eliminates redundant work, enables variable-group parallelism, and makes better use of available cores. With deck data pre-computed (Epic 01), statistics post-concatenation (Epic 01), and Polars handling multi-threaded operations internally (Epics 02-03), the parallelism model can be restructured for I/O-bound file reading and CPU-bound post-processing as separate concerns.

## Scope

1. **Replace multiprocessing.Pool with concurrent.futures**: Modernize the pool management API for better error handling and context management.

2. **Variable-group parallelism**: Instead of processing variables sequentially and parallelizing per-entity within each variable, group variables by spatial resolution and process the groups concurrently. Variables at the same resolution share entity lists and deck context.

3. **Optimize subprocess payload**: With DeckContext from Epic 01, subprocesses only need to read one nwlistop file each. Minimize the data sent to and from subprocesses.

4. **Evaluate thread-based I/O parallelism**: For the file reading portion (which is I/O-bound and GIL-free during the C-level read), evaluate whether `ThreadPoolExecutor` outperforms `ProcessPoolExecutor` by avoiding process spawn overhead.

## Dependencies

- Epic 03 must be complete (full Polars pipeline, no pandas in hot path)

## Success Criteria

- `multiprocessing.Pool` no longer appears in production code
- Variables at the same spatial resolution are processed in a single parallel batch
- Subprocess overhead (spawn, serialization) is minimized
- All existing tests pass
- Runtime improvement over Epic 03 baseline

## Tickets

| Ticket     | Title                                                      | Points | Depends On |
| ---------- | ---------------------------------------------------------- | ------ | ---------- |
| ticket-012 | Replace multiprocessing.Pool with concurrent.futures       | 3      | ticket-011 |
| ticket-013 | Implement variable-group parallelism by spatial resolution | 5      | ticket-012 |
| ticket-014 | Evaluate and implement thread-based I/O parallelism        | 3      | ticket-012 |
