import logging
import shlex
import subprocess
import sys
import time
from collections import deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import nullcontext, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, TypeVar, cast


################################################################################
# #region LoggerProtocol
class ProtocolRunLogger(Protocol):
    """Minimal logger protocol used by runner APIs."""

    def debug(self, message: str) -> Any: ...

    def info(self, message: str) -> Any: ...

    def warning(self, message: str) -> Any: ...

    def error(self, message: str) -> Any: ...


def _get_default_logger() -> ProtocolRunLogger:
    """Return default logger implementation (prefer loguru)."""
    try:
        from loguru import logger as loguru_logger

        return cast(ProtocolRunLogger, loguru_logger)
    except Exception:
        return cast(ProtocolRunLogger, logging.getLogger("axiomkit.runner"))


def _log_success(cls_logger: ProtocolRunLogger, message: str) -> None:
    """Use loguru success level when available, fallback to info."""
    fn_success = getattr(cls_logger, "success", None)
    if callable(fn_success):
        fn_success(message)
        return
    cls_logger.info(message)


# #endregion
################################################################################
# #region WorkerDistribution
@dataclass(frozen=True)
class ReportWorkerDistribution:
    workers: int
    threads_per_worker: int


def derive_worker_distribution(
    threads: int,
    *,
    per_threads_min: int = 2,
    per_threads_max: int = 16,
    desc: str = "",
    logger: ProtocolRunLogger | None = None,
) -> ReportWorkerDistribution:
    """Derive worker/thread split under a total thread budget.

    Contract:
        - ``workers >= 1`` and ``threads_per_worker >= 1``.
        - ``workers * threads_per_worker <= threads``.
        - Prefer larger ``threads_per_worker`` first (then derive workers).
        - When ``threads < per_threads_min``, uses one worker with all threads.

    Args:
        threads: Total available thread budget. Must be >= 1.
        per_threads_min: Preferred minimum threads per worker. Must be >= 1.
        per_threads_max: Preferred maximum threads per worker.
            Must satisfy ``per_threads_max >= per_threads_min``.
        desc: Optional label for debug logs.
        logger: Optional logger implementing ``ProtocolRunLogger``.
            If omitted, defaults to ``loguru.logger`` when available.
            If loguru is unavailable, falls back to stdlib logging.

    Returns:
        Worker/thread distribution satisfying the above contract.

    Raises:
        ValueError: If inputs are out of range.

    Examples:
        >>> derive_worker_distribution(threads=32, per_threads_min=2, per_threads_max=16)
        ReportWorkerDistribution(workers=2, threads_per_worker=16)
        >>> derive_worker_distribution(threads=1, per_threads_min=2, per_threads_max=16)
        ReportWorkerDistribution(workers=1, threads_per_worker=1)
    """
    if threads < 1:
        raise ValueError(f"threads must be >= 1, got {threads}")
    if per_threads_min < 1:
        raise ValueError(f"per_threads_min must be >= 1, got {per_threads_min}")
    if per_threads_max < per_threads_min:
        raise ValueError(
            "per_threads_max must be >= per_threads_min, "
            f"got {per_threads_max} < {per_threads_min}"
        )

    if threads < per_threads_min:
        n_threads = threads
        n_workers = 1
    else:
        n_threads = min(per_threads_max, threads)
        n_workers = max(1, threads // n_threads)

    cls_logger = logger or _get_default_logger()
    cls_logger.debug(
        f"Workers for `{desc}`: {n_workers}, threads per worker: {n_threads}"
    )
    return ReportWorkerDistribution(workers=n_workers, threads_per_worker=n_threads)


# #endregion
################################################################################
# #region StepRunner
@dataclass(frozen=True)
class ReportStep:
    return_code: int
    seconds: float
    hours: float
    cmd: list[str]
    file_log: Path | None
    tail: str


def run_step(
    cmd: Sequence[Any],
    title: str = "Step",
    *,
    file_log: Path | None = None,
    lines_tail: int = 100,
    timeout: float | None = None,
    logger: ProtocolRunLogger | None = None,
    if_write_tail_to_stderr: bool = True,
) -> ReportStep:
    """Run one shell command and return a structured execution report.

    Args:
        cmd: Command sequence to execute. Each item is converted to ``str``.
            Example: ``["python3", "-V"]`` or ``[Path("script.sh"), "--help"]``.
        title: Human-readable title for logs and diagnostics.
        file_log: Optional file path to stream full command output.
            If provided, parent directories are created automatically.
        lines_tail: Number of trailing output lines kept in ``ReportStep.tail``.
            Also controls how many lines are printed to ``stderr`` on failures.
            Set to ``0`` to disable tail capture.
        timeout: Optional timeout in seconds passed to subprocess wait.
        logger: Optional logger implementing ``ProtocolRunLogger``.
            If omitted, defaults to ``loguru.logger`` when available.
            If loguru is unavailable, falls back to stdlib logging.
        if_write_tail_to_stderr: Whether failure tail should be printed to
            ``stderr``. Disable in library-only integration scenarios.

    Returns:
        ReportStep: Structured execution report including return code, elapsed
        time, normalized command, log file path, and output tail.

    Raises:
        subprocess.CalledProcessError: The command exits with non-zero code.
            The exception ``output`` contains captured tail text.
        subprocess.TimeoutExpired: The command exceeds ``timeout``.
        KeyboardInterrupt: The caller interrupts execution (Ctrl+C).

    Examples:
        Basic command:

        >>> report = run_step(["python3", "-V"], title="Show Python version")
        >>> report.return_code
        0

        With persistent log and timeout:

        >>> report = run_step(
        ...     ["echo", "hello"],
        ...     title="Echo hello",
        ...     file_log=Path("logs/echo.log"),
        ...     timeout=30,
        ... )
        >>> report.file_log
        PosixPath('logs/echo.log')

        With custom logger adapter:

        >>> import logging
        >>> log = logging.getLogger("demo")
        >>> _ = run_step(["echo", "ok"], title="Echo", logger=log)

        Disable ``stderr`` tail output on failures:

        >>> try:
        ...     _ = run_step(
        ...         ["python3", "-c", "import sys; sys.exit(1)"],
        ...         title="Fail without stderr tail",
        ...         if_write_tail_to_stderr=False,
        ...     )
        ... except subprocess.CalledProcessError:
        ...     pass
    """
    cls_logger = logger or _get_default_logger()
    t0 = time.perf_counter()
    l_cmd = [str(i) for i in cmd]
    cls_logger.debug(f"RUN [{title}]:: {shlex.join(l_cmd)}")

    try:
        report_step = execute_step(
            cmd=cmd,
            title=title,
            file_log=file_log,
            lines_tail=lines_tail,
            timeout=timeout,
        )
    except subprocess.CalledProcessError as e:
        n_elapsed = time.perf_counter() - t0
        cls_logger.error(f"Fail: [{title}] (rc={e.returncode}) in {n_elapsed:.1f}s")
        c_tail = str(e.output or "")
        if if_write_tail_to_stderr and lines_tail > 0 and c_tail:
            sys.stderr.write(f"--- output tail (last {lines_tail} lines) ---\n{c_tail}")
            sys.stderr.flush()
        raise
    except KeyboardInterrupt:
        cls_logger.warning(
            f"KeyboardInterrupt received during [{title}], terminating..."
        )
        raise
    else:
        _log_success(cls_logger, f"Done: [{title}] in {report_step.seconds:.1f}s")
        return report_step


def execute_step(
    cmd: Sequence[Any],
    title: str | None = None,
    *,
    file_log: Path | None = None,
    lines_tail: int = 100,
    timeout: float | None = None,
) -> ReportStep:
    """Execute one command with minimal side effects (internal helper).

    Technical behavior:
        - streams merged ``stdout/stderr`` to ``file_log`` when provided,
        - captures only trailing lines (bounded by ``lines_tail``),
        - returns ``ReportStep`` on success,
        - raises subprocess exceptions on failure/timeout.

    This helper intentionally does not write status logs and does not print to
    ``stderr``; external-facing UX is handled by :func:`run_step`.

    Args:
        cmd: Command sequence to execute.
        title: Optional header label used in ``file_log``.
        file_log: Optional file path for full streamed output.
        lines_tail: Maximum trailing lines stored in result/exception output.
        timeout: Optional timeout in seconds.

    Returns:
        ReportStep: Structured execution report.

    Raises:
        subprocess.CalledProcessError: The command exits with non-zero code.
        subprocess.TimeoutExpired: Timeout reached while waiting for process.
    """
    t0 = time.perf_counter()
    l_cmd = [str(i) for i in cmd]

    q: deque[str] | None = (
        deque(maxlen=lines_tail) if lines_tail and lines_tail > 0 else None
    )
    p = None
    n_return_code: int | None = None

    if file_log and file_log.parent != Path("."):
        file_log.parent.mkdir(parents=True, exist_ok=True)

    file_ctx = open(file_log, "a", encoding="utf-8") if file_log else nullcontext()
    c_title = title or shlex.join(l_cmd)

    try:
        with file_ctx as fh:
            if fh:
                time_start = time.strftime("%Y-%m-%d %H:%M:%S")
                fh.write(f"--- [{c_title}] started at {time_start} ---\n")
                fh.write(f"cmd: {shlex.join(l_cmd)}\n\n")
                fh.flush()

            p = subprocess.Popen(
                l_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                errors="replace",
                close_fds=True,
                encoding="utf-8",
            )
            assert p.stdout is not None

            for line in p.stdout:
                fh.write(line) if fh else None
                q.append(line) if lines_tail > 0 and q else None

            try:
                n_return_code = p.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                with suppress(Exception):
                    p.terminate()
                with suppress(Exception):
                    p.wait(5)
                with suppress(Exception):
                    p.kill()
                raise

        c_tail = "".join(q) if q else ""
        t1 = time.perf_counter() - t0

        if file_log:
            with open(file_log, "a", encoding="utf-8") as fh:
                fh.write(
                    f"\n--- [{c_title}] finished rc={n_return_code} elapsed={t1:.1f}s ({t1 / 3600:.2f}h) ---\n"
                )

        if n_return_code != 0:
            raise subprocess.CalledProcessError(
                n_return_code,
                l_cmd,
                output=c_tail,
            )

        return ReportStep(
            return_code=n_return_code,
            seconds=t1,
            hours=t1 / 3600,
            cmd=l_cmd,
            file_log=file_log,
            tail=c_tail,
        )

    finally:
        if p and p.poll() is None:
            with suppress(Exception):
                p.kill()


# #endregion
################################################################################
# #region JobRunner
T = TypeVar("T")


@dataclass(frozen=True)
class ReportJobs:
    num_done: int
    num_failed: int
    jobs_done: list[tuple[str, Any]]
    jobs_failed: list[tuple[str, str]]


def run_jobs(
    jobs: Iterable[T],
    fn_worker: Callable[[T], Any],
    *,
    max_workers: int = 1,
    title: str = "Job",
    id_getter: Callable[[T], str] | None = None,
    if_raise_on_interrupt: bool = True,
    if_raise_on_failure: bool = True,
    file_failed_log: Path | None = None,
    logger: ProtocolRunLogger | None = None,
) -> ReportJobs:
    """Run jobs in a thread pool and collect failures.

    Args:
        jobs: Iterable of job payloads.
        fn_worker: Worker function that processes one job. Raise exceptions to
            mark job failure.
        max_workers: Thread pool size.
        title: Label used in logs.
        id_getter: Optional callable to derive job ID string. If omitted,
            built-in rules are used: ``job.id`` -> ``job.ID`` -> ``job["id"]`` ->
            ``job["ID"]`` -> ``str(job)``.
        if_raise_on_interrupt: Whether to re-raise ``KeyboardInterrupt`` after
            best-effort cancellation.
        if_raise_on_failure: Whether to raise when any job fails.
        file_failed_log: Optional path to write failed jobs as
            ``<job_id>\t<error_message>``.
        logger: Optional logger implementing ``ProtocolRunLogger``.
            If omitted, defaults to ``loguru.logger`` when available.
            If loguru is unavailable, falls back to stdlib logging.

    Returns:
        Aggregated jobs report including successful job results.

    Raises:
        RuntimeError: If failures exist and ``if_raise_on_failure`` is True.
        KeyboardInterrupt: If interrupted and ``if_raise_on_interrupt`` is True.

    Examples:
        Basic usage:

        >>> run_jobs(
        ...     jobs=[1, 2, 3],
        ...     fn_worker=lambda n: n * n,
        ...     max_workers=2,
        ... )
        ReportJobs(num_done=3, num_failed=0, jobs_done=[...], jobs_failed=[])

        Run external commands with ``run_step``:

        >>> jobs = [["echo", "A"], ["echo", "B"]]
        >>> run_jobs(
        ...     jobs=jobs,
        ...     fn_worker=lambda cmd: run_step(cmd, title=f"Run: {' '.join(cmd)}"),
        ...     id_getter=lambda cmd: " ".join(cmd),
        ...     max_workers=2,
        ...     if_raise_on_failure=True,
        ... )

        Structured jobs with composite IDs:

        >>> jobs = [
        ...     {"sample": "S1", "stage": "trim", "cmd": ["echo", "trim S1"]},
        ...     {"sample": "S2", "stage": "trim", "cmd": ["echo", "trim S2"]},
        ... ]
        >>> run_jobs(
        ...     jobs=jobs,
        ...     fn_worker=lambda j: run_step(
        ...         j["cmd"],
        ...         title=f"{j['sample']}::{j['stage']}",
        ...     ),
        ...     id_getter=lambda j: f"{j['sample']}/{j['stage']}",
        ...     max_workers=2,
        ... )

        Complex worker with per-job log files and richer IDs:

        >>> jobs = [
        ...     {"sample": "A01", "lane": "L001", "cmd": ["echo", "A01-L001"]},
        ...     {"sample": "A01", "lane": "L002", "cmd": ["echo", "A01-L002"]},
        ... ]
        >>> def _worker(job):
        ...     c_id = f"{job['sample']}_{job['lane']}"
        ...     return run_step(
        ...         job["cmd"],
        ...         title=f"Process {c_id}",
        ...         file_log=Path("logs") / f"{c_id}.log",
        ...         lines_tail=200,
        ...     )
        >>> run_jobs(
        ...     jobs=jobs,
        ...     fn_worker=_worker,
        ...     id_getter=lambda j: f"{j['sample']}::{j['lane']}::{j['cmd'][0]}",
        ...     max_workers=2,
        ...     if_raise_on_failure=True,
        ... )
    """
    cls_logger = logger or _get_default_logger()

    if max_workers < 1:
        raise ValueError(f"max_workers must be >= 1, got {max_workers}")

    l_jobs = list(jobs)
    if not l_jobs:
        return ReportJobs(num_done=0, num_failed=0, jobs_done=[], jobs_failed=[])

    l_failed: list[tuple[str, str]] = []
    l_done: list[tuple[str, Any]] = []
    n_done = 0
    n_total = len(l_jobs)
    dict_futs: dict[Future[Any], T] = {}
    cls_executor: ThreadPoolExecutor | None = None

    try:
        cls_executor = ThreadPoolExecutor(max_workers=max_workers)
        dict_futs = {cls_executor.submit(fn_worker, j): j for j in l_jobs}
        for fut in as_completed(dict_futs):
            j = dict_futs[fut]
            sid = _resolve_job_id(job=j, id_getter=id_getter)
            try:
                v_result = fut.result()
                n_done += 1
                l_done.append((sid, v_result))
                if n_done % 10 == 0 or n_done == n_total:
                    cls_logger.info(f"[{title}] Completed: {n_done}/{n_total}")
            except Exception as e:
                l_failed.append((sid, str(e)))
                cls_logger.error(f"[{title}] Failed {sid}: {e}")

    except KeyboardInterrupt:
        cls_logger.warning(
            f"[{title}] KeyboardInterrupt. Completed: {len(l_jobs) - len(l_failed)}/{len(l_jobs)}"
        )
        for fut in dict_futs.keys():
            with suppress(Exception):
                fut.cancel()
        if cls_executor is not None:
            try:
                cls_executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                cls_executor.shutdown(wait=False)
        if if_raise_on_interrupt:
            raise

    finally:
        if cls_executor is not None:
            try:
                cls_executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                cls_executor.shutdown(wait=False)

    cls_report = ReportJobs(
        num_done=n_done,
        num_failed=len(l_failed),
        jobs_done=l_done,
        jobs_failed=l_failed,
    )

    if file_failed_log and l_failed:
        file_failed_log.parent.mkdir(parents=True, exist_ok=True)
        file_failed_log.write_text(
            "\n".join(f"{sid}\t{msg}" for sid, msg in l_failed),
            encoding="utf-8",
        )
        cls_logger.error(
            f"[{title}] Fail: {cls_report.num_failed} tasks. See: {file_failed_log}"
        )

    if if_raise_on_failure and l_failed:
        c_info_log = f" See {file_failed_log} for details." if file_failed_log else ""
        raise RuntimeError(
            f"[{title}] {cls_report.num_failed}/{n_total} tasks failed.{c_info_log}"
        )

    return cls_report


def _resolve_job_id(
    job: T,
    *,
    id_getter: Callable[[T], str] | None,
) -> str:
    """Resolve a stable job ID for logs/errors."""
    if id_getter is not None:
        try:
            return str(id_getter(job))
        except Exception:
            return "Unknown"

    return _default_job_id(job)


def _default_job_id(job: object) -> str:
    """Built-in fallback rule for job IDs.

    Resolution order:
        1) ``job.id``
        2) ``job.ID``
        3) ``job['id']`` (if mapping-like)
        4) ``job['ID']`` (if mapping-like)
        5) ``str(job)``
    """
    c_attr_id = getattr(job, "id", None)
    if c_attr_id is not None:
        return str(c_attr_id)

    c_attr_ID = getattr(job, "ID", None)
    if c_attr_ID is not None:
        return str(c_attr_ID)

    if isinstance(job, Mapping):
        cls_job_map = cast(Mapping[str, object], job)
        if (v_id := cls_job_map.get("id")) is not None:
            return str(v_id)
        if (v_ID := cls_job_map.get("ID")) is not None:
            return str(v_ID)

    cls_job_obj = cast(object, job)
    return str(cls_job_obj)


# #endregion
################################################################################
