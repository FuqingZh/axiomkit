from __future__ import annotations

from pathlib import Path

from axiomkit.runner.runner import derive_worker_distribution, run_cmd, run_jobs


def test_derive_worker_distribution_prefers_larger_threads_per_worker() -> None:
    report = derive_worker_distribution(
        threads=32,
        threads_per_worker_min=2,
        threads_per_worker_max=16,
    )

    assert report.workers == 2
    assert report.threads_per_worker == 16


def test_run_cmd_executes_and_returns_tail(tmp_path: Path) -> None:
    path_file_log = tmp_path / "cmd.log"
    report = run_cmd(
        ["python3", "-c", "print('hello runner')"],
        title="runner-smoke",
        file_log=path_file_log,
    )

    assert report.return_code == 0
    assert path_file_log.exists()
    assert "hello runner" in path_file_log.read_text(encoding="utf-8")


def test_run_jobs_collects_failures_without_raising() -> None:
    def worker(job: int) -> int:
        if job == 2:
            raise ValueError("boom")
        return job * 10

    report = run_jobs(
        jobs=[1, 2, 3],
        fn_worker=worker,
        workers_max=2,
        should_raise_on_failure=False,
    )

    assert report.cnt_done == 2
    assert report.cnt_failed == 1
    assert sorted(job.payload for job in report.jobs_done) == [10, 30]
    assert report.jobs_failed[0].id == "2"
