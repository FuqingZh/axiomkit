from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
RS_TARGET_DIR = PROJECT_ROOT.parent / "rs" / "target"
PATH_SO_LINK = SRC_DIR / "axiomkit" / "io" / "fs" / "_axiomkit_io_fs_rs.so"


@dataclass(frozen=True)
class SpecFsBenchmarkScenario:
    name: str
    n_dirs: int
    n_files_per_dir: int
    n_file_size_bytes: int


@dataclass(frozen=True)
class SpecFsBenchmarkStats:
    backend_profile: str
    backend_binary: str
    scenario: SpecFsBenchmarkScenario
    repeats: int
    times_seconds: list[float]
    mean_seconds: float
    min_seconds: float
    max_seconds: float
    cnt_matched: int
    cnt_scanned: int
    cnt_copied: int
    cnt_skipped: int
    cnt_errors: int
    cnt_warnings: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark axiomkit.io.copy_tree on Rust debug/release backends.",
    )
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=PROJECT_ROOT / "benchmarks" / "fs_copy_tree" / "results",
    )
    parser.add_argument(
        "--scenario",
        choices=("default",),
        default="default",
    )
    return parser.parse_args()


def build_scenario(name: str) -> SpecFsBenchmarkScenario:
    if name == "default":
        return SpecFsBenchmarkScenario(
            name="small_files_10k",
            n_dirs=50,
            n_files_per_dir=200,
            n_file_size_bytes=256,
        )
    raise ValueError(f"Unsupported scenario: {name}")


def prepare_source_tree(path_src: Path, scenario: SpecFsBenchmarkScenario) -> None:
    import shutil

    if path_src.exists():
        shutil.rmtree(path_src)
    path_src.mkdir(parents=True, exist_ok=True)

    n_payload_len = max(1, scenario.n_file_size_bytes)
    for n_idx_dir in range(scenario.n_dirs):
        path_dir = path_src / f"d{n_idx_dir:03d}"
        path_dir.mkdir(parents=True, exist_ok=True)
        for n_idx_file in range(scenario.n_files_per_dir):
            payload = (f"{n_idx_dir}-{n_idx_file}-" + ("x" * n_payload_len)).encode("utf-8")[
                :n_payload_len
            ]
            (path_dir / f"f{n_idx_file:03d}.txt").write_bytes(payload)


def derive_backend_binary_path(profile: str) -> Path:
    if profile == "debug":
        return RS_TARGET_DIR / "debug" / "lib_axiomkit_io_fs_rs.so"
    if profile == "release":
        return RS_TARGET_DIR / "release" / "lib_axiomkit_io_fs_rs.so"
    raise ValueError(f"Unsupported backend profile: {profile}")


def point_fs_backend(profile: str) -> Path:
    path_backend = derive_backend_binary_path(profile)
    if not path_backend.exists():
        raise FileNotFoundError(
            f"Backend binary not found for profile={profile!r}: {path_backend}"
        )
    if PATH_SO_LINK.exists() or PATH_SO_LINK.is_symlink():
        PATH_SO_LINK.unlink()
    PATH_SO_LINK.symlink_to(path_backend)
    return path_backend.resolve()


def run_single_copy_tree(path_src: Path, path_dst: Path) -> dict[str, Any]:
    code = (
        "import json\n"
        "from pathlib import Path\n"
        "from time import perf_counter\n"
        "from axiomkit.io.fs import copy_tree\n"
        f"path_src=Path({str(path_src)!r})\n"
        f"path_dst=Path({str(path_dst)!r})\n"
        "import shutil\n"
        "if path_dst.exists(): shutil.rmtree(path_dst)\n"
        "n_t0=perf_counter()\n"
        "report=copy_tree(path_src, path_dst, "
        "rule_conflict_file='overwrite', rule_conflict_dir='merge', if_keep_tree=True)\n"
        "n_elapsed=perf_counter()-n_t0\n"
        "print(json.dumps({'elapsed': n_elapsed, 'report': report.to_dict()}))\n"
    )
    env = dict(**__import__("os").environ)
    env["PYTHONPATH"] = str(SRC_DIR)
    res = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    return json.loads(res.stdout.strip())


def benchmark_backend(
    *,
    profile: str,
    path_src: Path,
    path_dst: Path,
    scenario: SpecFsBenchmarkScenario,
    repeat: int,
) -> SpecFsBenchmarkStats:
    path_backend = point_fs_backend(profile)
    l_times: list[float] = []
    report_last: dict[str, int] | None = None

    for _ in range(repeat):
        result = run_single_copy_tree(path_src, path_dst)
        n_elapsed = float(result["elapsed"])
        report = result["report"]
        l_times.append(n_elapsed)
        report_last = {
            "cnt_matched": int(report["cnt_matched"]),
            "cnt_scanned": int(report["cnt_scanned"]),
            "cnt_copied": int(report["cnt_copied"]),
            "cnt_skipped": int(report["cnt_skipped"]),
            "cnt_errors": int(report["cnt_errors"]),
            "cnt_warnings": int(report["cnt_warnings"]),
        }

    assert report_last is not None
    n_expected = scenario.n_dirs + (scenario.n_dirs * scenario.n_files_per_dir)
    if report_last["cnt_errors"] != 0:
        raise RuntimeError(f"copy_tree produced errors: {report_last}")
    if report_last["cnt_copied"] != n_expected:
        raise RuntimeError(
            f"Unexpected cnt_copied={report_last['cnt_copied']} expected={n_expected}"
        )

    return SpecFsBenchmarkStats(
        backend_profile=profile,
        backend_binary=str(path_backend),
        scenario=scenario,
        repeats=repeat,
        times_seconds=l_times,
        mean_seconds=sum(l_times) / len(l_times),
        min_seconds=min(l_times),
        max_seconds=max(l_times),
        cnt_matched=report_last["cnt_matched"],
        cnt_scanned=report_last["cnt_scanned"],
        cnt_copied=report_last["cnt_copied"],
        cnt_skipped=report_last["cnt_skipped"],
        cnt_errors=report_last["cnt_errors"],
        cnt_warnings=report_last["cnt_warnings"],
    )


def render_md(payload: dict[str, Any]) -> str:
    l_lines = [
        "# FS copy_tree Benchmark Record",
        "",
        f"- Timestamp (UTC): `{payload['timestamp_utc']}`",
        f"- Command: `{payload['command']}`",
        f"- Platform: `{payload['platform']}`",
        f"- Python: `{payload['python_version']}`",
        "",
        "| backend | mean_s | min_s | max_s | copied | errors | backend_binary |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for item in payload["results"]:
        l_lines.append(
            f"| {item['backend_profile']} | {item['mean_seconds']:.6f} | "
            f"{item['min_seconds']:.6f} | {item['max_seconds']:.6f} | "
            f"{item['cnt_copied']} | {item['cnt_errors']} | `{item['backend_binary']}` |"
        )

    debug = next((x for x in payload["results"] if x["backend_profile"] == "debug"), None)
    release = next((x for x in payload["results"] if x["backend_profile"] == "release"), None)
    if debug and release:
        ratio = release["mean_seconds"] / debug["mean_seconds"]
        delta_pct = (ratio - 1.0) * 100.0
        l_lines.extend(
            [
                "",
                f"- Release vs Debug (mean): `{delta_pct:+.2f}%` "
                "(negative means release is faster).",
            ]
        )

    return "\n".join(l_lines) + "\n"


def update_index(path_file_json: Path, path_file_md: Path, cmd: str, timestamp: str) -> None:
    path_file_index = path_file_json.parent / "INDEX.md"
    if not path_file_index.exists():
        path_file_index.write_text(
            "# FS Benchmark Archive Index\n\n"
            "| timestamp_utc | command | json | markdown |\n"
            "| --- | --- | --- | --- |\n",
            encoding="utf-8",
        )

    line = (
        f"| {timestamp} | `{cmd}` | `{path_file_json.name}` | `{path_file_md.name}` |\n"
    )
    with path_file_index.open("a", encoding="utf-8") as f:
        f.write(line)


def main() -> None:
    args = parse_args()
    if args.repeat < 1:
        raise ValueError("--repeat must be >= 1")

    scenario = build_scenario(args.scenario)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    path_src = Path("/tmp/axiomkit_fs_bench_src")
    path_dst = Path("/tmp/axiomkit_fs_bench_dst")
    prepare_source_tree(path_src, scenario)

    l_results = []
    try:
        for profile in ("debug", "release"):
            l_results.append(
                benchmark_backend(
                    profile=profile,
                    path_src=path_src,
                    path_dst=path_dst,
                    scenario=scenario,
                    repeat=args.repeat,
                )
            )
    finally:
        # Keep workspace default on release backend after benchmark.
        point_fs_backend("release")

    timestamp = datetime.now(timezone.utc).isoformat()
    timestamp_compact = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    cmd = " ".join(["scripts/benchmark_fs_copy_tree.py", *sys.argv[1:]])

    payload = {
        "timestamp_utc": timestamp,
        "command": cmd,
        "platform": platform.platform(),
        "python_version": sys.version.split()[0],
        "scenario": asdict(scenario),
        "results": [asdict(item) for item in l_results],
    }

    path_file_json = args.out_dir / f"fs_copy_tree_{timestamp_compact}.json"
    path_file_md = args.out_dir / f"fs_copy_tree_{timestamp_compact}.md"
    path_file_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    path_file_md.write_text(render_md(payload), encoding="utf-8")
    update_index(path_file_json, path_file_md, cmd, timestamp)

    print(path_file_json)
    print(path_file_md)


if __name__ == "__main__":
    main()
