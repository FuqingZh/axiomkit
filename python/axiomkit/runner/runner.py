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


def _log_success(logger: ProtocolRunLogger, message: str) -> None:
    """Use loguru success level when available, fallback to info."""
    success_logger = getattr(logger, "success", None)
    if callable(success_logger):
        success_logger(message)
        return
    logger.info(message)


R = TypeVar("R", bound=ProtocolTimedReport)


def _run_with_logging(
    *,
    title: str,
    lines_tail: int,
    should_write_tail_to_stderr: bool,
    logger: ProtocolRunLogger,
    fn_execute: Callable[[], R],
) -> R:
    t0 = time.perf_counter()
    try:
        report = fn_execute()
    except subprocess.CalledProcessError as e:
        elapsed_seconds = time.perf_counter() - t0
        logger.error(f"Fail: [{title}] (rc={e.returncode}) in {elapsed_seconds:.1f}s")
        tail_output = str(e.output or "")
        if should_write_tail_to_stderr and lines_tail > 0 and tail_output:
            sys.stderr.write(
                f"--- output tail (last {lines_tail} lines) ---\n{tail_output}"
            )
            sys.stderr.flush()
        raise
    except KeyboardInterrupt:
        logger.warning(f"KeyboardInterrupt received during [{title}], terminating...")
        raise
    else:
        _log_success(logger, f"Done: [{title}] in {report.seconds:.1f}s")
        return report


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
class WorkerDistributionReport:
    workers: int
    threads_per_worker: int


def derive_worker_distribution(
    threads: int,
    title: str = "",
    *,
    threads_per_worker_min: int = 2,
    threads_per_worker_max: int = 16,
    logger: ProtocolRunLogger | None = None,
) -> WorkerDistributionReport:
    """Derive worker/thread split under a total thread budget.

    Contract:
        - ``workers >= 1`` and ``threads_per_worker >= 1``.
        - ``workers * threads_per_worker <= threads``.
        - Prefer larger ``threads_per_worker`` first (then derive workers).
        - When ``threads < threads_per_worker_min``, uses one worker with all threads.

    Args:
        threads: Total available thread budget. Must be >= 1.
        title: Optional label for debug logs.
        threads_per_worker_min: Preferred minimum threads per worker. Must be >= 1.
        threads_per_worker_max: Preferred maximum threads per worker.
            Must satisfy ``threads_per_worker_max >= threads_per_worker_min``.
        logger: Optional logger implementing ``ProtocolRunLogger``.
            If omitted, defaults to ``loguru.logger`` when available.
            If loguru is unavailable, falls back to stdlib logging.

    Returns:
        WorkerDistributionReport: Worker/thread distribution satisfying the above contract.

    Raises:
        ValueError: If inputs are out of range.

    Examples:
        ```python
        # Derive distribution for 32 threads with default preferences
        worker_rpt = derive_worker_distribution(32)
        print(worker_rpt.workers)  # e.g., 4
        print(worker_rpt.threads_per_worker)  # e.g., 8
        ```
    """
    if threads < 1:
        raise ValueError(f"threads must be >= 1, got {threads}")
    if threads_per_worker_min < 1:
        raise ValueError(
            f"threads_per_worker_min must be >= 1, got {threads_per_worker_min}"
        )
    if threads_per_worker_max < threads_per_worker_min:
        raise ValueError(
            "threads_per_worker_max must be >= threads_per_worker_min, "
            f"got {threads_per_worker_max} < {threads_per_worker_min}"
        )

    if threads < threads_per_worker_min:
        threads_per_worker = threads
        workers = 1
    else:
        threads_per_worker = min(threads_per_worker_max, threads)
        workers = max(1, threads // threads_per_worker)

    logger = logger or _get_default_logger()
    logger.debug(
        f"Workers for `{title}`: {workers}, threads per worker: {threads_per_worker}"
    )
    return WorkerDistributionReport(
        workers=workers,
        threads_per_worker=threads_per_worker,
    )


# #endregion
################################################################################
# #region CommandRunner
@dataclass(frozen=True)
class CmdReport:
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
    should_write_tail_to_stderr: bool = True,
) -> CmdReport:
    """Run one shell command and return a structured execution report.

    Args:
        cmd: Command sequence to execute. Each item is converted to ``str``.
            Example: ``["python3", "-V"]`` or ``[Path("script.sh"), "--help"]``.
        title: Human-readable title for logs and diagnostics.
        file_log: Optional file path to stream full command output.
            If provided, parent directories are created automatically.
        lines_tail: Number of trailing output lines kept in ``CmdReport.tail``.
            Also controls how many lines are printed to ``stderr`` on failures.
            Set to ``0`` to disable tail capture.
        timeout: Optional timeout in seconds passed to subprocess wait.
        logger: Optional logger implementing ``ProtocolRunLogger``.
            If omitted, defaults to ``loguru.logger`` when available.
            If loguru is unavailable, falls back to stdlib logging.
        should_write_tail_to_stderr: Whether failure tail should be printed to
            ``stderr``. Disable in library-only integration scenarios.

    Returns:
        CmdReport: Structured execution report including return code, elapsed
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
        ...         should_write_tail_to_stderr=False,
        ...     )
        ... except subprocess.CalledProcessError:
        ...     pass
    """
    logger = logger or _get_default_logger()
    command = _normalize_cmd(cmd, label="cmd")
    logger.debug(f"RUN [{title}]:: {shlex.join(command)}")

    return _run_with_logging(
        title=title,
        lines_tail=lines_tail,
        should_write_tail_to_stderr=should_write_tail_to_stderr,
        logger=logger,
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
) -> CmdReport:
    """Execute one command with minimal side effects (internal helper).

    Technical behavior:
        - streams merged ``stdout/stderr`` to ``file_log`` when provided,
        - captures only trailing lines (bounded by ``lines_tail``),
        - returns ``CmdReport`` on success,
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
        CmdReport: Structured execution report.

    Raises:
        subprocess.CalledProcessError: The command exits with non-zero code.
        subprocess.TimeoutExpired: Timeout reached while waiting for process.
    """
    t0 = time.perf_counter()
    command = _normalize_cmd(cmd, label="cmd")

    q: deque[str] | None = (
        deque(maxlen=lines_tail) if lines_tail and lines_tail > 0 else None
    )
    p = None
    return_code: int | None = None

    if file_log and file_log.parent != Path("."):
        file_log.parent.mkdir(parents=True, exist_ok=True)

    file_ctx = open(file_log, "a", encoding="utf-8") if file_log else nullcontext()
    command_title = title or shlex.join(command)

    try:
        with file_ctx as fh:
            if fh:
                time_start = time.strftime("%Y-%m-%d %H:%M:%S")
                fh.write(f"--- [{command_title}] started at {time_start} ---\n")
                fh.write(f"cmd: {shlex.join(command)}\n\n")
                fh.flush()

            p = subprocess.Popen(
                command,
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
                return_code = p.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                _terminate_process(p)
                raise

        tail_output = "".join(q) if q else ""
        elapsed_seconds = time.perf_counter() - t0

        if file_log:
            with open(file_log, "a", encoding="utf-8") as fh:
                fh.write(
                    f"\n--- [{command_title}] finished rc={return_code} elapsed={elapsed_seconds:.1f}s ({elapsed_seconds / 3600:.2f}h) ---\n"
                )

        if return_code != 0:
            raise subprocess.CalledProcessError(
                return_code,
                command,
                output=tail_output,
            )

        return CmdReport(
            return_code=return_code,
            seconds=elapsed_seconds,
            hours=elapsed_seconds / 3600,
            cmd=command,
            file_log=file_log,
            tail=tail_output,
        )

    finally:
        if p and p.poll() is None:
            _kill_process_if_running(p)


# #endregion
################################################################################
# #region PipeRunner
@dataclass(frozen=True)
class PipeCmdReport:
    index: int
    cmd: list[str]
    return_code: int


@dataclass(frozen=True)
class PipeReport:
    return_code: int
    failed_indices: list[int]
    steps: list[PipeCmdReport]
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
    should_write_tail_to_stderr: bool = True,
) -> PipeReport:
    """Run shell command pipeline(s): ``cmd1 | cmd2 | ...``.

    Supports one call shape:
        - ``run_pipe(cmd1, cmd2, cmd3, ...)``
    """
    logger = logger or _get_default_logger()
    if not cmds:
        raise ValueError("At least one command is required.")
    commands = [_normalize_cmd(cmd, label=f"cmds[{i}]") for i, cmd in enumerate(cmds)]
    command_show = " | ".join(shlex.join(cmd) for cmd in commands)
    logger.debug(f"RUN [{title}]:: {command_show}")

    return _run_with_logging(
        title=title,
        lines_tail=lines_tail,
        should_write_tail_to_stderr=should_write_tail_to_stderr,
        logger=logger,
        fn_execute=lambda: execute_pipe(
            cmds=commands,
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
) -> PipeReport:
    """Execute one command pipeline with pipefail-like failure behavior."""
    commands = [
        _normalize_cmd(
            cmd,
            label=f"cmds[{i}]",
        )
        for i, cmd in enumerate(cmds)
    ]
    if not commands:
        raise ValueError("cmds must contain at least one command.")

    if file_log and file_log.parent != Path("."):
        file_log.parent.mkdir(parents=True, exist_ok=True)

    q: deque[str] | None = (
        deque(maxlen=lines_tail) if lines_tail and lines_tail > 0 else None
    )
    t0 = time.perf_counter()
    command_title = title or " | ".join(shlex.join(cmd) for cmd in commands)
    processes: list[subprocess.Popen[str]] = []
    stderr_threads: list[Thread] = []

    file_ctx = open(file_log, "a", encoding="utf-8") if file_log else nullcontext()

    def _consume_stream(
        stream: Any,
        *,
        prefix: str = "",
        fh: Any = None,
    ) -> None:
        try:
            for line in stream:
                line_with_prefix = f"{prefix}{line}" if prefix else line
                if fh:
                    fh.write(line_with_prefix)
                if q is not None:
                    q.append(line_with_prefix)
        finally:
            with suppress(Exception):
                stream.close()

    try:
        with file_ctx as fh:
            if fh:
                time_started = time.strftime("%Y-%m-%d %H:%M:%S")
                fh.write(f"--- [{command_title}] started at {time_started} ---\n")
                for i, cmd in enumerate(commands, start=1):
                    fh.write(f"cmd[{i}]: {shlex.join(cmd)}\n")
                fh.write("\n")
                fh.flush()

            stream_prev_out = None
            for i, cmd in enumerate(commands):
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
                processes.append(p)

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
                stderr_threads.append(th_err)

                stream_prev_out = p.stdout

            assert stream_prev_out is not None
            _consume_stream(stream_prev_out, fh=fh)

            deadline = (time.monotonic() + timeout) if timeout is not None else None
            return_codes: list[int] = []
            for p in processes:
                if deadline is None:
                    return_codes.append(p.wait())
                    continue

                assert timeout is not None
                remaining_seconds = deadline - time.monotonic()
                if remaining_seconds <= 0:
                    raise subprocess.TimeoutExpired(cmd=commands[0], timeout=timeout)
                return_codes.append(p.wait(timeout=remaining_seconds))

            for thread in stderr_threads:
                thread.join(timeout=1.0)

            steps = [
                PipeCmdReport(
                    index=i + 1,
                    cmd=cmd,
                    return_code=rc,
                )
                for i, (cmd, rc) in enumerate(zip(commands, return_codes, strict=True))
            ]
            failed_indices = [i + 1 for i, rc in enumerate(return_codes) if rc != 0]
            pipeline_return_code = (
                return_codes[failed_indices[-1] - 1] if failed_indices else 0
            )

            tail_output = "".join(q) if q else ""
            elapsed_seconds = time.perf_counter() - t0

            if fh:
                fh.write(
                    f"\n--- [{command_title}] finished rc={pipeline_return_code} elapsed={elapsed_seconds:.1f}s ({elapsed_seconds / 3600:.2f}h) ---\n"
                )
                fh.flush()

            if pipeline_return_code != 0:
                failed_index = failed_indices[-1] - 1
                raise subprocess.CalledProcessError(
                    returncode=pipeline_return_code,
                    cmd=commands[failed_index],
                    output=tail_output,
                )

            return PipeReport(
                return_code=0,
                failed_indices=[],
                steps=steps,
                seconds=elapsed_seconds,
                hours=elapsed_seconds / 3600,
                file_log=file_log,
                tail=tail_output,
            )
    except subprocess.TimeoutExpired:
        for p in processes:
            _terminate_process(p)
        raise
    finally:
        for p in processes:
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

    items = list(cmd)
    if not items:
        raise ValueError(f"{label} must not be empty.")

    if any(isinstance(item, (bytes, bytearray)) for item in items):
        raise TypeError(
            f"{label} contains bytes-like token; decode to str explicitly before calling."
        )

    if any(_is_non_string_sequence(item) for item in items):
        raise TypeError(
            f"{label} must be a flat command sequence; nested sequence token detected."
        )

    return [str(item) for item in items]


# #endregion
################################################################################
# #region JobRunner
TJob = TypeVar("TJob")
TResult = TypeVar("TResult")


@dataclass(frozen=True, slots=True)
class JobDoneRecord[TResult]:
    id: str
    payload: TResult


@dataclass(frozen=True, slots=True)
class JobFailedRecord:
    id: str
    msg_error: str


@dataclass(frozen=True, slots=True)
class JobsReport[TResult]:
    cnt_done: int
    cnt_failed: int
    jobs_done: list[JobDoneRecord[TResult]]
    jobs_failed: list[JobFailedRecord]


def run_jobs(
    jobs: Iterable[TJob],
    fn_worker: Callable[[TJob], TResult],
    *,
    workers_max: int = 1,
    title: str = "Job",
    id_getter: Callable[[TJob], str] | None = None,
    should_raise_on_interrupt: bool = True,
    should_raise_on_failure: bool = True,
    file_failed_log: Path | None = None,
    logger: ProtocolRunLogger | None = None,
) -> JobsReport[TResult]:
    """Run jobs in a thread pool and collect failures.

    Args:
        jobs: Iterable of job payloads.
        fn_worker: Worker function that processes one job. Raise exceptions to
            mark job failure.
        workers_max: Thread pool size.
        title: Label used in logs.
        id_getter: Optional callable to derive job ID string. If omitted,
            built-in rules are used: ``job.id`` -> ``job.ID`` -> ``job["id"]`` ->
            ``job["ID"]`` -> ``str(job)``.
        should_raise_on_interrupt: Whether to re-raise ``KeyboardInterrupt`` after
            best-effort cancellation.
        should_raise_on_failure: Whether to raise when any job fails.
        file_failed_log: Optional path to write failed jobs as
            ``<job_id>\t<error_message>``.
        logger: Optional logger implementing ``ProtocolRunLogger``.
            If omitted, defaults to ``loguru.logger`` when available.
            If loguru is unavailable, falls back to stdlib logging.

    Returns:
        Aggregated jobs report including successful job results.

    Raises:
        RuntimeError: If failures exist and ``should_raise_on_failure`` is True.
        KeyboardInterrupt: If interrupted and ``should_raise_on_interrupt`` is True.

    Examples:
        Basic usage:

        >>> run_jobs(
        ...     jobs=[1, 2, 3],
        ...     fn_worker=lambda n: n * n,
        ...     workers_max=2,
        ... )
        JobsReport(cnt_done=3, cnt_failed=0, jobs_done=[...], jobs_failed=[])

        Run external commands with ``run_cmd``:

        >>> jobs = [["echo", "A"], ["echo", "B"]]
        >>> run_jobs(
        ...     jobs=jobs,
        ...     fn_worker=lambda cmd: run_cmd(cmd, title=f"Run: {' '.join(cmd)}"),
        ...     id_getter=lambda cmd: " ".join(cmd),
        ...     workers_max=2,
        ...     should_raise_on_failure=True,
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
        ...     workers_max=2,
        ... )

        Complex worker with per-job log files and richer IDs:

        >>> jobs = [
        ...     {"sample": "A01", "lane": "L001", "cmd": ["echo", "A01-L001"]},
        ...     {"sample": "A01", "lane": "L002", "cmd": ["echo", "A01-L002"]},
        ... ]
        >>> def _worker(job):
        ...     job_id = f"{job['sample']}_{job['lane']}"
        ...     return run_cmd(
        ...         job["cmd"],
        ...         title=f"Process {job_id}",
        ...         file_log=Path("logs") / f"{job_id}.log",
        ...         lines_tail=200,
        ...     )
        >>> run_jobs(
        ...     jobs=jobs,
        ...     fn_worker=_worker,
        ...     id_getter=lambda j: f"{j['sample']}::{j['lane']}::{j['cmd'][0]}",
        ...     workers_max=2,
        ...     should_raise_on_failure=True,
        ... )
    """
    logger = logger or _get_default_logger()

    if workers_max < 1:
        raise ValueError(f"max_workers must be >= 1, got {workers_max}")

    jobs = list(jobs)
    if not jobs:
        return JobsReport(cnt_done=0, cnt_failed=0, jobs_done=[], jobs_failed=[])

    jobs_failed: list[JobFailedRecord] = []
    jobs_done: list[JobDoneRecord[TResult]] = []
    cnt_jobs_done = 0
    cnt_jobs_total = len(jobs)
    futures_by_job: dict[Future[TResult], TJob] = {}
    executor: ThreadPoolExecutor | None = None

    try:
        executor = ThreadPoolExecutor(max_workers=workers_max)
        futures_by_job = {executor.submit(fn_worker, j): j for j in jobs}
        for future in as_completed(futures_by_job):
            job = futures_by_job[future]
            job_id = _resolve_job_id(job=job, id_getter=id_getter)
            try:
                result = future.result()
                cnt_jobs_done += 1
                jobs_done.append(JobDoneRecord(id=job_id, payload=result))
                if cnt_jobs_done % 10 == 0 or cnt_jobs_done == cnt_jobs_total:
                    logger.info(f"[{title}] Completed: {cnt_jobs_done}/{cnt_jobs_total}")
            except Exception as e:
                jobs_failed.append(JobFailedRecord(id=job_id, msg_error=str(e)))
                logger.error(f"[{title}] Failed {job_id}: {e}")

    except KeyboardInterrupt:
        logger.warning(
            f"[{title}] KeyboardInterrupt. Completed: {len(jobs) - len(jobs_failed)}/{len(jobs)}"
        )
        for future in futures_by_job.keys():
            with suppress(Exception):
                future.cancel()
        if executor is not None:
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                executor.shutdown(wait=False)
        if should_raise_on_interrupt:
            raise

    finally:
        if executor is not None:
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                executor.shutdown(wait=False)

    report = JobsReport(
        cnt_done=cnt_jobs_done,
        cnt_failed=len(jobs_failed),
        jobs_done=jobs_done,
        jobs_failed=jobs_failed,
    )

    if file_failed_log and jobs_failed:
        file_failed_log.parent.mkdir(parents=True, exist_ok=True)
        file_failed_log.write_text(
            "\n".join(f"{job.id}\t{job.msg_error}" for job in jobs_failed),
            encoding="utf-8",
        )
        logger.error(
            f"[{title}] Fail: {report.cnt_failed} tasks. See: {file_failed_log}"
        )

    if should_raise_on_failure and jobs_failed:
        info_log = f" See {file_failed_log} for details." if file_failed_log else ""
        raise RuntimeError(
            f"[{title}] {report.cnt_failed}/{cnt_jobs_total} tasks failed.{info_log}"
        )

    return report


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
    attr_id = getattr(job, "id", None)
    if attr_id is not None:
        return str(attr_id)

    attr_id_upper = getattr(job, "ID", None)
    if attr_id_upper is not None:
        return str(attr_id_upper)

    if isinstance(job, Mapping):
        job_map = cast(Mapping[str, object], job)
        if (value_id := job_map.get("id")) is not None:
            return str(value_id)
        if (value_id_upper := job_map.get("ID")) is not None:
            return str(value_id_upper)

    job_object = cast(object, job)
    return str(job_object)


# #endregion
################################################################################
