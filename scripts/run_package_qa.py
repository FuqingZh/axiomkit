from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run package-level QA against a built axiomkit distribution in an isolated venv."
        )
    )
    parser.add_argument(
        "--dist-dir",
        type=Path,
        default=Path("dist"),
        help="Directory containing built distribution artifacts.",
    )
    parser.add_argument(
        "--wheel",
        type=Path,
        default=None,
        help="Specific wheel path. If omitted, picks newest axiomkit wheel in --dist-dir.",
    )
    parser.add_argument(
        "--sdist",
        type=Path,
        default=None,
        help="Specific sdist path. If set, installs from the source distribution artifact.",
    )
    parser.add_argument(
        "--tests-dir",
        type=Path,
        default=Path("tests"),
        help="Pytest target directory.",
    )
    parser.add_argument(
        "--install-target",
        default="axiomkit",
        help="Package target to install from wheel QA environment.",
    )
    return parser.parse_args()


def resolve_dist_artifact(args: argparse.Namespace) -> Path:
    if args.wheel is not None and args.sdist is not None:
        raise ValueError("Only one of --wheel or --sdist may be provided.")

    if args.wheel is not None:
        path_wheel = args.wheel.resolve()
        if not path_wheel.exists():
            raise FileNotFoundError(f"Wheel file not found: {path_wheel}")
        return path_wheel

    if args.sdist is not None:
        path_sdist = args.sdist.resolve()
        if not path_sdist.exists():
            raise FileNotFoundError(f"sdist file not found: {path_sdist}")
        return path_sdist

    wheels = sorted(args.dist_dir.glob("axiomkit-*.whl"))
    if not wheels:
        raise FileNotFoundError(
            f"No wheel artifact found in dist dir: {args.dist_dir.resolve()}"
        )

    wheels_manylinux = [path for path in wheels if "manylinux" in path.name]
    if wheels_manylinux:
        return wheels_manylinux[-1].resolve()

    return wheels[-1].resolve()


def main() -> None:
    args = parse_args()
    path_project_root = Path(__file__).resolve().parents[1]
    path_artifact = resolve_dist_artifact(args)

    with tempfile.TemporaryDirectory(prefix="axiomkit-dist-qa-") as dir_temp:
        path_venv = Path(dir_temp) / "venv"
        subprocess.run([sys.executable, "-m", "venv", str(path_venv)], check=True)

        if os.name == "nt":
            path_python = path_venv / "Scripts" / "python.exe"
        else:
            path_python = path_venv / "bin" / "python"
        subprocess.run([str(path_python), "-m", "pip", "install", "-U", "pip"], check=True)
        subprocess.run(
            [
                str(path_python),
                "-m",
                "pip",
                "install",
                f"{args.install_target} @ {path_artifact.as_uri()}",
                "pytest",
            ],
            check=True,
        )

        env = dict(os.environ)
        env["AXIOMKIT_TEST_IMPORT_MODE"] = "dist"
        subprocess.run(
            [
                str(path_python),
                "-m",
                "pytest",
                "-q",
                str(args.tests_dir),
            ],
            check=True,
            cwd=str(path_project_root),
            env=env,
        )


if __name__ == "__main__":
    main()
