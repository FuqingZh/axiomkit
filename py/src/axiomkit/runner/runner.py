import shlex
import subprocess
import sys
import time
from collections import deque
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import nullcontext, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NamedTuple, TypeVar

from loguru import logger


################################################################################
# #region WorkerDistribution
class SpecWorkerDistribution(NamedTuple):
    workers: int
    threads_per_worker: int


def derive_worker_distribution(
    threads: int, per_threads_min: int = 2, per_threads_max: int = 16, desc: str = ""
) -> SpecWorkerDistribution:
    """
    Splits the total number of threads into a reasonable number of workers and threads per worker for parallel processing.
    1. Each worker has at least per_threads_min threads and at most per_threads_max threads.
    2. Total threads = number of workers * threads per worker.
    3. Prioritize ensuring each worker has enough threads.

    切分线程数为合理的 worker 数和每个 worker 的线程数，用于并行处理。
    1. 每个 worker 至少 per_threads_min 个线程，最多 per_threads_max 个线程。
    2. 总线程数 = worker 数 * 每个 worker 线程数。
    3. 优先保证每个 worker 有足够的线程数。

    Args:
        threads (int): Total number of threads.
        per_threads_min (int, optional): Minimum threads per worker. Defaults to 2.
        per_threads_max (int, optional): Maximum threads per worker. Defaults to 16.

    Returns:
        SpecWorkerDistribution: A specification of the number of workers and threads per worker.
    """
    n_threads = max(per_threads_min, min(per_threads_max, threads)) or 1
    n_workers = max(1, threads // n_threads) or 1
    logger.debug(f"Workers for `{desc}`: {n_workers}, threads per worker: {n_threads}")
    return SpecWorkerDistribution(workers=n_workers, threads_per_worker=n_threads)

# #endregion
################################################################################
# #region StepRunner
@dataclass(frozen=True)
class SpecStepResult:
    return_code: int
    seconds: float
    hours: float
    cmd: list[str]
    file_log: Path | None
    tail: str


def run_step(
    cmd: Sequence[Any],
    title: str,
    file_log: Path | None = None,
    lines_tail: int = 100,
    timeout: float | None = None,
) -> SpecStepResult:
    """
    Run a shell command as a subprocess, stream its output to a log file, and return execution results.

    Args:
        cmd (Sequence[str | Path | int | float]): The command and its arguments to execute.
        title (str): The title or label for this step, used for logging.
        file_log (Path | None, optional): Path to a log file where the command output will be written. Defaults to None.
        lines_tail (int, optional): Number of lines from the end of the output to keep as the tail. Defaults to 100.
        timeout (float | None, optional): Maximum number of seconds to allow for command execution before timing out. Defaults to None.

    Raises:
        CalledProcessError: If the subprocess exits with a non-zero return code.

    Returns:
        SpecStepResult(return_code, seconds, hours, cmd, file_log, tail): An object containing the return code, elapsed time, command, log file path, and output tail of the executed step.
    """
    t0 = time.perf_counter()
    l_cmd = [str(i) for i in cmd]
    logger.debug(f"RUN [{title}]:: {shlex.join(l_cmd)}")

    # keep last N lines of output
    q: deque[str] | None = (
        deque(maxlen=lines_tail) if lines_tail and lines_tail > 0 else None
    )
    p = None  # subprocess.Popen instance
    rc: int | None = None  # return code

    if file_log and file_log.parent != Path("."):
        file_log.parent.mkdir(parents=True, exist_ok=True)
    # 可选的全量日志文件
    file_ctx = open(file_log, "a", encoding="utf-8") if file_log else nullcontext()
    try:
        with file_ctx as fh:
            if fh:
                time_start = time.strftime("%Y-%m-%d %H:%M:%S")
                fh.write(f"--- [{title}] started at {time_start} ---\n")
                fh.write(f"cmd: {shlex.join(l_cmd)}\n\n")
                fh.flush()

            # combine stdout and stderr to the same pipe, then write to one .log to avoid deadlock
            p = subprocess.Popen(
                l_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # 合并两路，避免任一路阻塞
                text=True,
                bufsize=1,  # line-buffered
                errors="replace",  # avoid decode error
                close_fds=True,  # avoid fd leak
                encoding="utf-8",
            )
            assert p.stdout is not None

            # streaming output
            for line in p.stdout:
                fh.write(line) if fh else None
                q.append(line) if lines_tail > 0 and q else None

            try:
                rc = p.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                # first try terminate gracefully, then kill
                with suppress(Exception):
                    p.terminate()
                with suppress(Exception):
                    p.wait(5)
                with suppress(Exception):
                    p.kill()
                raise

        tail = "".join(q) if q else ""
        t1 = time.perf_counter() - t0
        if file_log:
            with open(file_log, "a", encoding="utf-8") as fh:
                fh.write(
                    f"\n--- [{title}] finished rc={rc} elapsed={t1:.1f}s ({t1 / 3600:.2f}h) ---\n"
                )

        if rc != 0:
            logger.error(f"Fail: [{title}] (rc={rc}) in {t1:.1f}s")
            sys.stderr.write(f"--- output tail (last {lines_tail} lines) ---\n{tail}")
            sys.stderr.flush()

            # raise CalledProcessError with output, cmd, returncode, and title
            e = subprocess.CalledProcessError(rc, l_cmd, output=tail)
            setattr(e, "Title", title)
            raise e
        else:
            logger.success(f"Done: [{title}] in {t1:.1f}s")
            return SpecStepResult(
                return_code=rc,
                seconds=t1,
                hours=t1 / 3600,
                cmd=l_cmd,
                file_log=file_log,
                tail=tail,
            )

    except KeyboardInterrupt as ki:
        if p and p.poll() is None:
            with suppress(Exception):
                p.terminate()
            with suppress(Exception):
                p.wait(2)
            with suppress(Exception):
                p.kill()
        logger.warning(f"KeyboardInterrupt received during [{title}], terminating...")
        setattr(ki, "Title", title)
        raise

    except Exception as e:
        setattr(e, "Title", title)
        raise

    finally:
        # 防泄漏
        if p and p.poll() is None:
            with suppress(Exception):
                p.kill()

# #endregion
################################################################################
# #region JobRunner
T = TypeVar("T")  # Generic type for job items


class SpecJobResult(NamedTuple):
    num_done: int
    num_failed: int
    jobs_failed: list[tuple[str, str]]


def run_jobs(
    jobs: Iterable[T],
    func_worker: Callable[[T], Any],
    max_workers: int = 1,
    title: str = "Job",
    id_attr: str = "ID",
    if_raise_interrupt: bool = False,
    if_raise_failure: bool = True,
    file_failed_log: Path | None = None,
) -> SpecJobResult:
    """
    Run a collection of jobs using a ThreadPoolExecutor and collect failures.

    Args:
        jobs (Iterable[T]): An iterable of job items to process.
        func_worker (Callable[[T], None]): A function that processes a single job item.
        max_workers (int, optional): Maximum number of worker threads. Defaults to 1.
        title (str, optional): Title for logging purposes. Defaults to "Job".
        id_attr (str, optional): Attribute name to identify each job. Defaults to "ID".
        if_raise_interrupt (bool, optional): Whether to raise KeyboardInterrupt. Defaults to False.
        if_raise_failure (bool, optional): Whether to raise an error if any job fails. Defaults to True.
        file_failed_log (Path | None, optional): Path to log file for failed jobs. Defaults to None.

    Returns:
      JobResult(num_done, num_failed, jobs_failed)
    """
    if max_workers < 1:
        raise ValueError(f"max_workers must be >= 1, got {max_workers}")

    l_jobs = list(jobs)
    if not l_jobs:
        return SpecJobResult(num_done=0, num_failed=0, jobs_failed=[])

    l_failed: list[tuple[str, str]] = []
    n_done = 0
    n_total = len(l_jobs)
    dict_futs: dict[Future[Any], T] = {}
    cls_executor: ThreadPoolExecutor | None = None
    try:
        cls_executor = ThreadPoolExecutor(max_workers=max_workers)
        dict_futs = {cls_executor.submit(func_worker, j): j for j in l_jobs}
        for fut in as_completed(dict_futs):
            j = dict_futs[fut]
            sid = getattr(j, id_attr, "Unknown")
            try:
                fut.result()
                n_done += 1
                if n_done % 10 == 0 or n_done == n_total:
                    logger.info(f"[{title}] Completed: {n_done}/{n_total}")
            except Exception as e:
                # get job ID for logging
                c_id_ex = getattr(e, id_attr, sid)
                l_failed.append((c_id_ex, str(e)))
                logger.error(f"[{title}] Failed {c_id_ex}: {e}")

    except KeyboardInterrupt:
        # best-effort cancellation of pending futures
        logger.warning(
            f"[{title}] KeyboardInterrupt. Completed: {len(l_jobs) - len(l_failed)}/{len(l_jobs)}"
        )
        for fut in dict_futs.keys():
            try:
                fut.cancel()
            except Exception:
                pass
        if cls_executor is not None:
            # cancel_futures True requests cancellation of pending futures (Py3.9+)
            try:
                cls_executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                # older Python versions may not support cancel_futures
                cls_executor.shutdown(wait=False)
        if if_raise_interrupt:
            raise

    finally:
        # Ensure the executor is properly shut down even if an error occurs
        if cls_executor is not None:
            try:
                cls_executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                cls_executor.shutdown(wait=False)

    cls_job_result = SpecJobResult(
        num_done=n_done,
        num_failed=len(l_failed),
        jobs_failed=l_failed,
    )

    # write failed jobs to log file if specified
    if file_failed_log and l_failed:
        file_failed_log.parent.mkdir(parents=True, exist_ok=True)
        file_failed_log.write_text(
            "\n".join(f"{sid}\t{msg}" for sid, msg in l_failed),
            encoding="utf-8",
        )
        logger.error(
            f"[{title}] Fail: {cls_job_result.num_failed} tasks. See: {file_failed_log}"
        )
    if if_raise_failure and l_failed:
        raise RuntimeError(
            f"[{title}] {cls_job_result.num_failed}/{n_total} tasks failed. See {file_failed_log} for details."
        )
    return cls_job_result

# #endregion
################################################################################
