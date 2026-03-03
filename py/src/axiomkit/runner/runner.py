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
from threading import Thread
from typing import Any, Protocol, TypeVar, cast


################################################################################
# #region LoggerProtocol
class ProtocolRunLogger(Protocol):
    """Minimal logger protocol used by runner APIs."""

    def debug(self, message: str) -> Any: ...

    def info(self, message: str) -> Any: ...

    def warning(self, message: str) -> Any: ...

    def error(self, message: str) -> Any: ...


class ProtocolTimedReport(Protocol):
    @property
    def seconds(self) -> float: ...


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


R = TypeVar("R", bound=ProtocolTimedReport)


def _run_with_logging(
    *,
    title: str,
    lines_tail: int,
    if_write_tail_to_stderr: bool,
    logger: ProtocolRunLogger,
    fn_execute: Callable[[], R],
) -> R:
    t0 = time.perf_counter()
    try:
        cls_report = fn_execute()
    except subprocess.CalledProcessError as e:
        n_elapsed = time.perf_counter() - t0
        logger.error(f"Fail: [{title}] (rc={e.returncode}) in {n_elapsed:.1f}s")
        c_tail = str(e.output or "")
        if if_write_tail_to_stderr and lines_tail > 0 and c_tail:
            sys.stderr.write(f"--- output tail (last {lines_tail} lines) ---\n{c_tail}")
            sys.stderr.flush()
        raise
    except KeyboardInterrupt:
        logger.warning(f"KeyboardInterrupt received during [{title}], terminating...")
        raise
    else:
        _log_success(logger, f"Done: [{title}] in {cls_report.seconds:.1f}s")
        return cls_report


def _terminate_process(
    proc: subprocess.Popen[Any], *, wait_seconds: float = 5.0
) -> None:
    if proc.poll() is not None:
        return
    with suppress(Exception):
        proc.terminate()
    with suppress(Exception):
        proc.wait(wait_seconds)
    if proc.poll() is None:
        with suppress(Exception):
            proc.kill()


def _kill_process_if_running(proc: subprocess.Popen[Any]) -> None:
    if proc.poll() is None:
        with suppress(Exception):
            proc.kill()


# #endregion
################################################################################
# #region WorkerDistribution
@dataclass(frozen=True)
class ReportWorkerDistribution:
    workers: int
    threads_per_worker: int


def derive_worker_distribution(
    threads: int,
    title: str = "",
    *,
    per_threads_min: int = 2,
    per_threads_max: int = 16,
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
        title: Optional label for debug logs.
        per_threads_min: Preferred minimum threads per worker. Must be >= 1.
        per_threads_max: Preferred maximum threads per worker.
            Must satisfy ``per_threads_max >= per_threads_min``.
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
        f"Workers for `{title}`: {n_workers}, threads per worker: {n_threads}"
    )
    return ReportWorkerDistribution(workers=n_workers, threads_per_worker=n_threads)


# #endregion
################################################################################
# #region CommandRunner
@dataclass(frozen=True)
class ReportCmd:
    return_code: int
    seconds: float
    hours: float
    cmd: list[str]
    file_log: Path | None
    tail: str


def run_cmd(
    cmd: Sequence[Any],
    title: str = "Command",
    *,
    file_log: Path | None = None,
    lines_tail: int = 100,
    timeout: float | None = None,
    logger: ProtocolRunLogger | None = None,
    if_write_tail_to_stderr: bool = True,
) -> ReportCmd:
    """Run one shell command and return a structured execution report.

    Args:
        cmd: Command sequence to execute. Each item is converted to ``str``.
            Example: ``["python3", "-V"]`` or ``[Path("script.sh"), "--help"]``.
        title: Human-readable title for logs and diagnostics.
        file_log: Optional file path to stream full command output.
            If provided, parent directories are created automatically.
        lines_tail: Number of trailing output lines kept in ``ReportCmd.tail``.
            Also controls how many lines are printed to ``stderr`` on failures.
            Set to ``0`` to disable tail capture.
        timeout: Optional timeout in seconds passed to subprocess wait.
        logger: Optional logger implementing ``ProtocolRunLogger``.
            If omitted, defaults to ``loguru.logger`` when available.
            If loguru is unavailable, falls back to stdlib logging.
        if_write_tail_to_stderr: Whether failure tail should be printed to
            ``stderr``. Disable in library-only integration scenarios.

    Returns:
        ReportCmd: Structured execution report including return code, elapsed
        time, normalized command, log file path, and output tail.

    Raises:
        subprocess.CalledProcessError: The command exits with non-zero code.
            The exception ``output`` contains captured tail text.
        subprocess.TimeoutExpired: The command exceeds ``timeout``.
        KeyboardInterrupt: The caller interrupts execution (Ctrl+C).

    Examples:
        Basic command:

        >>> report = run_cmd(["python3", "-V"], title="Show Python version")
        >>> report.return_code
        0

        With persistent log and timeout:

        >>> report = run_cmd(
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
        >>> _ = run_cmd(["echo", "ok"], title="Echo", logger=log)

        Disable ``stderr`` tail output on failures:

        >>> try:
        ...     _ = run_cmd(
        ...         ["python3", "-c", "import sys; sys.exit(1)"],
        ...         title="Fail without stderr tail",
        ...         if_write_tail_to_stderr=False,
        ...     )
        ... except subprocess.CalledProcessError:
        ...     pass
    """
    cls_logger = logger or _get_default_logger()
    l_cmd = _normalize_cmd(cmd, label="cmd")
    cls_logger.debug(f"RUN [{title}]:: {shlex.join(l_cmd)}")

    return _run_with_logging(
        title=title,
        lines_tail=lines_tail,
        if_write_tail_to_stderr=if_write_tail_to_stderr,
        logger=cls_logger,
        fn_execute=lambda: execute_cmd(
            cmd=cmd,
            title=title,
            file_log=file_log,
            lines_tail=lines_tail,
            timeout=timeout,
        ),
    )


def execute_cmd(
    cmd: Sequence[Any],
    title: str | None = None,
    *,
    file_log: Path | None = None,
    lines_tail: int = 100,
    timeout: float | None = None,
) -> ReportCmd:
    """Execute one command with minimal side effects (internal helper).

    Technical behavior:
        - streams merged ``stdout/stderr`` to ``file_log`` when provided,
        - captures only trailing lines (bounded by ``lines_tail``),
        - returns ``ReportCmd`` on success,
        - raises subprocess exceptions on failure/timeout.

    This helper intentionally does not write status logs and does not print to
    ``stderr``; external-facing UX is handled by :func:`run_cmd`.

    Args:
        cmd: Command sequence to execute.
        title: Optional header label used in ``file_log``.
        file_log: Optional file path for full streamed output.
        lines_tail: Maximum trailing lines stored in result/exception output.
        timeout: Optional timeout in seconds.

    Returns:
        ReportCmd: Structured execution report.

    Raises:
        subprocess.CalledProcessError: The command exits with non-zero code.
        subprocess.TimeoutExpired: Timeout reached while waiting for process.
    """
    t0 = time.perf_counter()
    l_cmd = _normalize_cmd(cmd, label="cmd")

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
                _terminate_process(p)
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

        return ReportCmd(
            return_code=n_return_code,
            seconds=t1,
            hours=t1 / 3600,
            cmd=l_cmd,
            file_log=file_log,
            tail=c_tail,
        )

    finally:
        if p and p.poll() is None:
            _kill_process_if_running(p)


# #endregion
################################################################################
# #region PipeRunner
@dataclass(frozen=True)
class ReportPipeCmd:
    index: int
    cmd: list[str]
    return_code: int


@dataclass(frozen=True)
class ReportPipe:
    return_code: int
    failed_indices: list[int]
    steps: list[ReportPipeCmd]
    seconds: float
    hours: float
    file_log: Path | None
    tail: str


def run_pipe(
    *cmds: Sequence[Any],
    title: str = "Pipe",
    file_log: Path | None = None,
    lines_tail: int = 100,
    timeout: float | None = None,
    logger: ProtocolRunLogger | None = None,
    if_write_tail_to_stderr: bool = True,
) -> ReportPipe:
    """Run shell command pipeline(s): ``cmd1 | cmd2 | ...``.

    Supports one call shape:
        - ``run_pipe(cmd1, cmd2, cmd3, ...)``
    """
    cls_logger = logger or _get_default_logger()
    if not cmds:
        raise ValueError("At least one command is required.")
    l_cmds = [_normalize_cmd(cmd, label=f"cmds[{i}]") for i, cmd in enumerate(cmds)]
    c_show = " | ".join(shlex.join(cmd) for cmd in l_cmds)
    cls_logger.debug(f"RUN [{title}]:: {c_show}")

    return _run_with_logging(
        title=title,
        lines_tail=lines_tail,
        if_write_tail_to_stderr=if_write_tail_to_stderr,
        logger=cls_logger,
        fn_execute=lambda: execute_pipe(
            cmds=l_cmds,
            title=title,
            file_log=file_log,
            lines_tail=lines_tail,
            timeout=timeout,
        ),
    )


def execute_pipe(
    cmds: Sequence[Sequence[Any]],
    title: str | None = None,
    *,
    file_log: Path | None = None,
    lines_tail: int = 100,
    timeout: float | None = None,
) -> ReportPipe:
    """Execute one command pipeline with pipefail-like failure behavior."""
    l_cmds = [
        _normalize_cmd(
            cmd,
            label=f"cmds[{i}]",
        )
        for i, cmd in enumerate(cmds)
    ]
    if not l_cmds:
        raise ValueError("cmds must contain at least one command.")

    if file_log and file_log.parent != Path("."):
        file_log.parent.mkdir(parents=True, exist_ok=True)

    q: deque[str] | None = (
        deque(maxlen=lines_tail) if lines_tail and lines_tail > 0 else None
    )
    t0 = time.perf_counter()
    c_title = title or " | ".join(shlex.join(cmd) for cmd in l_cmds)
    l_proc: list[subprocess.Popen[str]] = []
    l_stderr_threads: list[Thread] = []

    file_ctx = open(file_log, "a", encoding="utf-8") if file_log else nullcontext()

    def _consume_stream(
        stream: Any,
        *,
        prefix: str = "",
        fh: Any = None,
    ) -> None:
        try:
            for line in stream:
                c_line = f"{prefix}{line}" if prefix else line
                if fh:
                    fh.write(c_line)
                if q is not None:
                    q.append(c_line)
        finally:
            with suppress(Exception):
                stream.close()

    try:
        with file_ctx as fh:
            if fh:
                c_started = time.strftime("%Y-%m-%d %H:%M:%S")
                fh.write(f"--- [{c_title}] started at {c_started} ---\n")
                for i, cmd in enumerate(l_cmds, start=1):
                    fh.write(f"cmd[{i}]: {shlex.join(cmd)}\n")
                fh.write("\n")
                fh.flush()

            stream_prev_out = None
            for i, cmd in enumerate(l_cmds):
                p = subprocess.Popen(
                    cmd,
                    stdin=stream_prev_out,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1,
                    errors="replace",
                    close_fds=True,
                    encoding="utf-8",
                )
                l_proc.append(p)

                if stream_prev_out is not None:
                    stream_prev_out.close()

                assert p.stderr is not None
                th_err = Thread(
                    target=_consume_stream,
                    args=(p.stderr,),
                    kwargs={"prefix": f"[stderr:{i + 1}] ", "fh": fh},
                    daemon=True,
                )
                th_err.start()
                l_stderr_threads.append(th_err)

                stream_prev_out = p.stdout

            assert stream_prev_out is not None
            _consume_stream(stream_prev_out, fh=fh)

            n_deadline = (time.monotonic() + timeout) if timeout is not None else None
            l_rc: list[int] = []
            for p in l_proc:
                if n_deadline is None:
                    l_rc.append(p.wait())
                    continue

                assert timeout is not None
                n_remain = n_deadline - time.monotonic()
                if n_remain <= 0:
                    raise subprocess.TimeoutExpired(cmd=l_cmds[0], timeout=timeout)
                l_rc.append(p.wait(timeout=n_remain))

            for th in l_stderr_threads:
                th.join(timeout=1.0)

            l_steps = [
                ReportPipeCmd(
                    index=i + 1,
                    cmd=cmd,
                    return_code=rc,
                )
                for i, (cmd, rc) in enumerate(zip(l_cmds, l_rc, strict=True))
            ]
            l_failed_indices = [i + 1 for i, rc in enumerate(l_rc) if rc != 0]
            n_pipeline_rc = l_rc[l_failed_indices[-1] - 1] if l_failed_indices else 0

            c_tail = "".join(q) if q else ""
            t1 = time.perf_counter() - t0

            if fh:
                fh.write(
                    f"\n--- [{c_title}] finished rc={n_pipeline_rc} elapsed={t1:.1f}s ({t1 / 3600:.2f}h) ---\n"
                )
                fh.flush()

            if n_pipeline_rc != 0:
                idx_failed = l_failed_indices[-1] - 1
                raise subprocess.CalledProcessError(
                    returncode=n_pipeline_rc,
                    cmd=l_cmds[idx_failed],
                    output=c_tail,
                )

            return ReportPipe(
                return_code=0,
                failed_indices=[],
                steps=l_steps,
                seconds=t1,
                hours=t1 / 3600,
                file_log=file_log,
                tail=c_tail,
            )
    except subprocess.TimeoutExpired:
        for p in l_proc:
            _terminate_process(p)
        raise
    finally:
        for p in l_proc:
            if p.poll() is None:
                _kill_process_if_running(p)


def _is_non_string_sequence(value: object) -> bool:
    return isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    )


def _normalize_cmd(cmd: Sequence[Any], *, label: str) -> list[str]:
    if not _is_non_string_sequence(cmd):
        raise TypeError(f"{label} must be a command sequence, got {type(cmd)!r}.")

    l_items = list(cmd)
    if not l_items:
        raise ValueError(f"{label} must not be empty.")

    if any(isinstance(item, (bytes, bytearray)) for item in l_items):
        raise TypeError(
            f"{label} contains bytes-like token; decode to str explicitly before calling."
        )

    if any(_is_non_string_sequence(item) for item in l_items):
        raise TypeError(
            f"{label} must be a flat command sequence; nested sequence token detected."
        )

    return [str(item) for item in l_items]


# #endregion
################################################################################
# #region JobRunner
TJob = TypeVar("TJob")
TResult = TypeVar("TResult")


@dataclass(frozen=True, slots=True)
class SpecReportJobDone[TResult]:
    id: str
    payload: TResult


@dataclass(frozen=True, slots=True)
class SpecReportJobFailed:
    id: str
    msg_error: str


@dataclass(frozen=True, slots=True)
class ReportJobs[TResult]:
    cnt_done: int
    cnt_failed: int
    jobs_done: list[SpecReportJobDone[TResult]]
    jobs_failed: list[SpecReportJobFailed]


def run_jobs(
    jobs: Iterable[TJob],
    fn_worker: Callable[[TJob], TResult],
    *,
    max_workers: int = 1,
    title: str = "Job",
    id_getter: Callable[[TJob], str] | None = None,
    if_raise_on_interrupt: bool = True,
    if_raise_on_failure: bool = True,
    file_failed_log: Path | None = None,
    logger: ProtocolRunLogger | None = None,
) -> ReportJobs[TResult]:
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
        ReportJobs(cnt_done=3, cnt_failed=0, jobs_done=[...], jobs_failed=[])

        Run external commands with ``run_cmd``:

        >>> jobs = [["echo", "A"], ["echo", "B"]]
        >>> run_jobs(
        ...     jobs=jobs,
        ...     fn_worker=lambda cmd: run_cmd(cmd, title=f"Run: {' '.join(cmd)}"),
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
        ...     fn_worker=lambda j: run_cmd(
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
        ...     return run_cmd(
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
        return ReportJobs(cnt_done=0, cnt_failed=0, jobs_done=[], jobs_failed=[])

    l_failed: list[SpecReportJobFailed] = []
    l_done: list[SpecReportJobDone[TResult]] = []
    n_done = 0
    n_total = len(l_jobs)
    dict_futs: dict[Future[TResult], TJob] = {}
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
                l_done.append(SpecReportJobDone(id=sid, payload=v_result))
                if n_done % 10 == 0 or n_done == n_total:
                    cls_logger.info(f"[{title}] Completed: {n_done}/{n_total}")
            except Exception as e:
                l_failed.append(SpecReportJobFailed(id=sid, msg_error=str(e)))
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
        cnt_done=n_done,
        cnt_failed=len(l_failed),
        jobs_done=l_done,
        jobs_failed=l_failed,
    )

    if file_failed_log and l_failed:
        file_failed_log.parent.mkdir(parents=True, exist_ok=True)
        file_failed_log.write_text(
            "\n".join(f"{job.id}\t{job.msg_error}" for job in l_failed),
            encoding="utf-8",
        )
        cls_logger.error(
            f"[{title}] Fail: {cls_report.cnt_failed} tasks. See: {file_failed_log}"
        )

    if if_raise_on_failure and l_failed:
        c_info_log = f" See {file_failed_log} for details." if file_failed_log else ""
        raise RuntimeError(
            f"[{title}] {cls_report.cnt_failed}/{n_total} tasks failed.{c_info_log}"
        )

    return cls_report


def _resolve_job_id(
    job: TJob,
    *,
    id_getter: Callable[[TJob], str] | None,
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
