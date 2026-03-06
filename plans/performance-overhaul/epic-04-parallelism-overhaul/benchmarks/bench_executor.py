"""
bench_executor.py -- Benchmark ProcessPoolExecutor vs ThreadPoolExecutor for
entity-level I/O parallelism in OperationSynthetizer.

Usage
-----
    python bench_executor.py --case-dir /path/to/newave/output --variable GHID_UHE

The script times three execution modes for a representative UHE variable:

    - sequential   : max_workers=1, ProcessPoolExecutor (baseline, no parallelism)
    - process      : max_workers=N, ProcessPoolExecutor (production default)
    - thread       : max_workers=N, ThreadPoolExecutor  (candidate replacement)

For each mode it takes the median wall-clock time over 3 runs, then prints a
summary table and the speedup ratio process_time / thread_time.

After each concurrent run the resulting DataFrame (sorted) is compared against
the sequential baseline using numpy.testing.assert_allclose to detect data
races or result divergence.

Requirements
------------
The script must be run from the repository root so that the ``app`` package is
importable, or the repo root must be on sys.path.  Example:

    cd /path/to/sintetizador-newave
    python plans/performance-overhaul/epic-04-parallelism-overhaul/benchmarks/bench_executor.py \
        --case-dir /path/to/newave/output \
        --variable GHID_UHE \
        --workers 4 \
        --runs 3
"""

from __future__ import annotations

import logging
import statistics
import sys
import time
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Queue
from pathlib import Path
from typing import Optional

import numpy as np
import typer
from rich.console import Console
from rich.table import Table

# ---------------------------------------------------------------------------
# Ensure repo root is importable when the script is run directly.
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parents[4]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from app.model.operation.operationsynthesis import OperationSynthesis  # noqa: E402
from app.model.operation.spatialresolution import SpatialResolution  # noqa: E402
from app.model.operation.variable import Variable  # noqa: E402
from app.services.deck.context import DeckContext  # noqa: E402
from app.services.deck.deck import Deck  # noqa: E402
from app.services.synthesis.operation import OperationSynthetizer  # noqa: E402
from app.services.unitofwork import FSUnitOfWork  # noqa: E402
from app.utils.log import Log  # noqa: E402

console = Console()
app = typer.Typer(
    name="bench_executor",
    help="Benchmark ProcessPoolExecutor vs ThreadPoolExecutor for UHE I/O parallelism.",
    no_args_is_help=True,
)


# ---------------------------------------------------------------------------
# Worker functions
# ---------------------------------------------------------------------------


def _resolve_uhe_entity_worker(
    case_dir: str,
    synthesis_repr: str,
    uhe_index: int,
    uhe_name: str,
) -> Optional[object]:
    """
    Standalone worker callable for both ProcessPoolExecutor and
    ThreadPoolExecutor.  Constructs its own FSUnitOfWork (safe because
    FSUnitOfWork is stateless beyond the resolved path) and calls
    OperationSynthetizer._resolve_UHE_entity directly.

    Parameters
    ----------
    case_dir:
        Absolute path to the NEWAVE output directory.
    synthesis_repr:
        String representation of the OperationSynthesis (e.g. "GHID_UHE").
    uhe_index:
        Numeric index of the UHE.
    uhe_name:
        Name of the UHE (used for logging).

    Returns
    -------
    pl.DataFrame or None as returned by _resolve_UHE_entity.
    """
    import queue as queue_mod  # stdlib; avoids multiprocessing.Queue in threads

    # Build a minimal queue for logging.  For processes a plain queue.Queue
    # is not shareable across process boundaries, but since each process
    # creates its own queue here the log records are simply discarded
    # (no listener is running in this benchmark).  For threads the same
    # queue.Queue instance is created per-call, which is safe.
    q: Queue = Queue()  # type: ignore[assignment]

    parsed = synthesis_repr.split("_")
    synthesis = OperationSynthesis(
        Variable.factory(parsed[0]),  # type: ignore[arg-type]
        SpatialResolution.factory(parsed[1]),
    )
    uow = FSUnitOfWork(case_dir, q)

    Log.configure_process_logger(q, synthesis.variable.value, uhe_index)
    return OperationSynthetizer._resolve_UHE_entity(  # type: ignore[attr-defined]
        uow,
        synthesis,
        uhe_index,
        uhe_name,
        deck_context=None,
    )


def _run_mode(
    mode: str,
    case_dir: str,
    synthesis_repr: str,
    hydros_idx: list[int],
    hydros_name: list[str],
    n_workers: int,
) -> tuple[dict[str, object], float]:
    """
    Execute entity resolution for all UHEs in the given mode and return
    (results_dict, elapsed_seconds).

    Parameters
    ----------
    mode:
        One of "sequential", "process", "thread".
    case_dir:
        Absolute path to the NEWAVE output directory.
    synthesis_repr:
        String representation of the OperationSynthesis.
    hydros_idx:
        List of UHE numeric indices.
    hydros_name:
        Parallel list of UHE names.
    n_workers:
        Number of worker threads/processes for concurrent modes.  Ignored for
        "sequential" (always uses 1 worker).

    Returns
    -------
    Tuple of (results mapping name -> pl.DataFrame|None, elapsed wall seconds).
    """
    effective_workers = 1 if mode == "sequential" else n_workers
    start = time.perf_counter()

    if mode == "thread":
        executor_cls = ThreadPoolExecutor
    else:
        executor_cls = ProcessPoolExecutor  # both "sequential" and "process"

    futures: dict[str, Future[object]] = {}
    with executor_cls(max_workers=effective_workers) as executor:
        for idx, name in zip(hydros_idx, hydros_name):
            futures[name] = executor.submit(
                _resolve_uhe_entity_worker,
                case_dir,
                synthesis_repr,
                idx,
                name,
            )
        results: dict[str, object] = {
            name: f.result(timeout=3600) for name, f in futures.items()
        }

    elapsed = time.perf_counter() - start
    return results, elapsed


def _assert_results_equal(
    baseline: dict[str, object],
    candidate: dict[str, object],
    mode_label: str,
) -> bool:
    """
    Compare float columns of two result dicts against the sequential baseline.
    Returns True if all results match within tolerance, False on mismatch.

    Comparison uses numpy.testing.assert_allclose with atol=1e-6 on all float
    columns, as specified in the ticket's testing requirements.
    """
    import polars as pl

    if set(baseline.keys()) != set(candidate.keys()):
        console.print(
            f"[red]MISMATCH[/red] ({mode_label}): key sets differ. "
            f"Baseline has {len(baseline)} keys, candidate has {len(candidate)}."
        )
        return False

    for name in sorted(baseline.keys()):
        base_df = baseline[name]
        cand_df = candidate[name]

        # Both None is fine.
        if base_df is None and cand_df is None:
            continue
        if base_df is None or cand_df is None:
            console.print(
                f"[red]MISMATCH[/red] ({mode_label}): UHE '{name}' -- one result is None."
            )
            return False

        assert isinstance(base_df, pl.DataFrame)
        assert isinstance(cand_df, pl.DataFrame)

        # Sort both on all columns to eliminate ordering differences.
        sort_cols = base_df.columns
        base_sorted = base_df.sort(sort_cols)
        cand_sorted = cand_df.sort(sort_cols)

        if base_sorted.shape != cand_sorted.shape:
            console.print(
                f"[red]MISMATCH[/red] ({mode_label}): UHE '{name}' shape "
                f"{base_sorted.shape} != {cand_sorted.shape}"
            )
            return False

        float_cols = [
            col
            for col in base_sorted.columns
            if base_sorted[col].dtype in (pl.Float32, pl.Float64)
        ]
        for col in float_cols:
            try:
                np.testing.assert_allclose(
                    base_sorted[col].to_numpy(),
                    cand_sorted[col].to_numpy(),
                    atol=1e-6,
                    err_msg=f"UHE '{name}', column '{col}'",
                )
            except AssertionError as exc:
                console.print(f"[red]MISMATCH[/red] ({mode_label}): {exc}")
                return False

    return True


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


@app.command()
def main(
    case_dir: Path = typer.Argument(
        ...,
        help="Path to the NEWAVE output directory containing nwlistop files.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
    ),
    variable: str = typer.Option(
        "GHID_UHE",
        "--variable",
        "-v",
        help="OperationSynthesis variable string (e.g. GHID_UHE).",
    ),
    workers: Optional[int] = typer.Option(
        None,
        "--workers",
        "-w",
        help=(
            "Number of parallel workers.  Defaults to PROCESSADORES env var "
            "or 1 if unset."
        ),
    ),
    runs: int = typer.Option(
        3,
        "--runs",
        "-r",
        help="Number of timed repetitions per mode (median is reported).",
        min=1,
    ),
) -> None:
    """
    Time entity resolution for a UHE variable using three execution modes:
    sequential (baseline), ProcessPoolExecutor, and ThreadPoolExecutor.

    Prints median wall-clock time over the requested number of runs for each
    mode, followed by the speedup ratio process_time / thread_time.

    After each concurrent run the resulting DataFrame is validated against the
    sequential baseline using numpy.testing.assert_allclose (atol=1e-6) to
    detect data races.
    """
    import os

    from app.model.settings import Settings

    # Resolve worker count from CLI > env > 1.
    if workers is None:
        workers = int(os.getenv("PROCESSADORES", Settings().processors or 1))

    console.rule("[bold cyan]bench_executor[/bold cyan]")
    console.print(f"  case_dir : {case_dir}")
    console.print(f"  variable : {variable}")
    console.print(f"  workers  : {workers}")
    console.print(f"  runs     : {runs}")
    console.print()

    # Parse the synthesis string.
    parsed = variable.split("_")
    if len(parsed) != 2:
        console.print(
            f"[red]ERROR[/red]: variable must be in 'VAR_RES' format, got '{variable}'"
        )
        raise typer.Exit(code=1)

    try:
        synthesis = OperationSynthesis(
            Variable.factory(parsed[0]),  # type: ignore[arg-type]
            SpatialResolution.factory(parsed[1]),
        )
    except Exception as exc:
        console.print(
            f"[red]ERROR[/red]: failed to parse variable '{variable}': {exc}"
        )
        raise typer.Exit(code=1)

    if synthesis.spatial_resolution != SpatialResolution.USINA_HIDROELETRICA:
        console.print(
            f"[yellow]WARNING[/yellow]: This benchmark is designed for UHE variables. "
            f"Got spatial resolution '{synthesis.spatial_resolution.value}'. Proceeding anyway."
        )

    # Set up a minimal UoW to enumerate UHEs.
    q: Queue = Queue()  # type: ignore[assignment]
    uow = FSUnitOfWork(str(case_dir), q)

    # Silence the main process logger to avoid noise during timing.
    logging.getLogger().setLevel(logging.ERROR)

    try:
        with uow:
            hydros = Deck.hydros(uow).reset_index().sort_values("codigo_usina")
    except FileNotFoundError as exc:
        console.print(
            f"[red]ERROR[/red]: required deck file not found: {exc}\n"
            "Ensure the case directory contains NEWAVE output files."
        )
        raise typer.Exit(code=1)
    except Exception as exc:
        console.print(f"[red]ERROR[/red]: failed to load UHE list: {exc}")
        raise typer.Exit(code=1)

    hydros_idx: list[int] = hydros["codigo_usina"].tolist()
    hydros_name: list[str] = hydros["nome_usina"].tolist()
    n_uhe = len(hydros_idx)
    console.print(f"  UHEs found: {n_uhe}")
    console.print()

    if n_uhe == 0:
        console.print("[red]ERROR[/red]: no UHEs found in case directory.")
        raise typer.Exit(code=1)

    # Check that nwlistop files are present for at least one UHE.
    # If the first file is missing, skip the benchmark gracefully.
    try:
        with uow:
            probe_df = uow.files.get_nwlistop(
                synthesis.variable,
                synthesis.spatial_resolution,
                uhe=hydros_idx[0],
            )
        if probe_df is None:
            console.print(
                f"[yellow]WARNING[/yellow]: no nwlistop file found for variable "
                f"'{variable}' / UHE index {hydros_idx[0]}. Skipping benchmark."
            )
            raise typer.Exit(code=0)
    except FileNotFoundError as exc:
        console.print(
            f"[yellow]WARNING[/yellow]: nwlistop file not found for UHE {hydros_idx[0]}: {exc}. "
            "Skipping benchmark."
        )
        raise typer.Exit(code=0)

    synthesis_repr = (
        f"{synthesis.variable.value}_{synthesis.spatial_resolution.value}"
    )
    modes = ["sequential", "process", "thread"]
    timings: dict[str, list[float]] = {m: [] for m in modes}
    results_per_mode: dict[str, dict[str, object]] = {}

    for run_idx in range(1, runs + 1):
        console.print(f"[bold]Run {run_idx}/{runs}[/bold]")
        for mode in modes:
            console.print(f"  [{mode}] ...", end="")
            try:
                results, elapsed = _run_mode(
                    mode,
                    str(case_dir),
                    synthesis_repr,
                    hydros_idx,
                    hydros_name,
                    workers,
                )
            except FileNotFoundError as exc:
                console.print(
                    f"\n[yellow]WARNING[/yellow]: nwlistop file not found during "
                    f"'{mode}' run: {exc}. Skipping remainder of benchmark."
                )
                raise typer.Exit(code=0)

            timings[mode].append(elapsed)
            if run_idx == 1:
                results_per_mode[mode] = results
            console.print(f" {elapsed:.2f}s")
        console.print()

    # Validate correctness of concurrent modes vs sequential baseline.
    console.rule("Result Validation")
    baseline = results_per_mode["sequential"]
    all_correct = True
    for mode in ["process", "thread"]:
        ok = _assert_results_equal(baseline, results_per_mode[mode], mode)
        label = (
            "[green]PASS[/green]"
            if ok
            else "[red]FAIL (data race or mismatch)[/red]"
        )
        console.print(f"  {mode:12s}: {label}")
        if not ok:
            all_correct = False
    console.print()

    # Compute median timings.
    med: dict[str, float] = {m: statistics.median(timings[m]) for m in modes}

    # Build results table.
    table = Table(title="Executor Benchmark Results (median wall time)")
    table.add_column("Mode", style="cyan")
    table.add_column("Workers", justify="right")
    table.add_column(f"Median ({runs} runs)", justify="right")
    table.add_column("vs Sequential", justify="right")
    table.add_column("Result Check", justify="center")

    seq_time = med["sequential"]
    check_labels = {
        "sequential": "[green]BASELINE[/green]",
        "process": "[green]PASS[/green]" if all_correct else "[red]FAIL[/red]",
        "thread": "[green]PASS[/green]" if all_correct else "[red]FAIL[/red]",
    }
    worker_labels = {
        "sequential": "1",
        "process": str(workers),
        "thread": str(workers),
    }

    for mode in modes:
        speedup = seq_time / med[mode] if med[mode] > 0 else float("inf")
        table.add_row(
            mode,
            worker_labels[mode],
            f"{med[mode]:.3f}s",
            f"{speedup:.2f}x",
            check_labels[mode],
        )

    console.print(table)
    console.print()

    # Print speedup ratio between process and thread.
    proc_time = med["process"]
    thread_time = med["thread"]
    if thread_time > 0:
        ratio = proc_time / thread_time
        console.print(
            f"Speedup ratio (process / thread): [bold]{ratio:.3f}x[/bold]"
        )
        if ratio > 1.10:
            console.print(
                "[green]CONCLUSION[/green]: ThreadPoolExecutor is >10% faster than "
                "ProcessPoolExecutor. Consider adopting ThreadPoolExecutor in "
                "app/services/synthesis/operation.py (see ticket-014 for details)."
            )
        elif ratio < (1 / 1.10):
            console.print(
                "[yellow]CONCLUSION[/yellow]: ProcessPoolExecutor is >10% faster than "
                "ThreadPoolExecutor (GIL contention likely). Keep ProcessPoolExecutor. "
                "Document findings in BENCHMARKS.md."
            )
        else:
            console.print(
                "[blue]CONCLUSION[/blue]: No significant difference (<10%) between "
                "ProcessPoolExecutor and ThreadPoolExecutor. Keep ProcessPoolExecutor "
                "(conservative choice). Document findings in BENCHMARKS.md."
            )
    else:
        console.print(
            "[yellow]WARNING[/yellow]: thread_time is zero; cannot compute ratio."
        )

    if not all_correct:
        console.print(
            "\n[red]IMPORTANT[/red]: Result mismatch detected -- do NOT adopt "
            "ThreadPoolExecutor. Data races indicate cfinterface parsers are not "
            "thread-safe. Keep ProcessPoolExecutor."
        )
        raise typer.Exit(code=2)


if __name__ == "__main__":
    app()
