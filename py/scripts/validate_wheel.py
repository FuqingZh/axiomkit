from __future__ import annotations

import argparse
import re
import zipfile
from pathlib import Path


RE_WHEEL_VERSION = re.compile(r"^axiomkit-([^-]+)-")
REQUIRED_EXTENSIONS = (
    "axiomkit/io/fs/_axiomkit_io_fs_rs",
    "axiomkit/io/xlsx/_axiomkit_io_xlsx_rs",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate built axiomkit wheel artifacts."
    )
    parser.add_argument(
        "--dist-dir",
        type=Path,
        default=Path("dist"),
        help="Directory containing wheel artifacts.",
    )
    parser.add_argument(
        "--expected-version",
        default=None,
        help="Expected wheel version (for tagged release validation).",
    )
    parser.add_argument(
        "--print-version",
        action="store_true",
        help="Print resolved version only (for shell capture).",
    )
    return parser.parse_args()


def parse_version(path_wheel: Path) -> str:
    match = RE_WHEEL_VERSION.match(path_wheel.name)
    if match is None:
        raise RuntimeError(f"Cannot parse version from wheel name: {path_wheel.name}")
    return match.group(1)


def has_extension(names: list[str], prefix: str) -> bool:
    return any(
        name.startswith(prefix) and name.endswith((".so", ".pyd", ".dylib"))
        for name in names
    )


def validate_wheels(path_dist: Path, expected_version: str | None) -> str:
    wheels = sorted(path_dist.glob("axiomkit-*.whl"))
    if not wheels:
        raise RuntimeError(f"No wheel artifact found in {path_dist.resolve()}")

    versions = {parse_version(path_wheel) for path_wheel in wheels}
    if len(versions) != 1:
        raise RuntimeError(
            f"Mixed wheel versions found: {sorted(versions)} from {[w.name for w in wheels]}"
        )
    version = next(iter(versions))

    if expected_version is not None and version != expected_version:
        raise RuntimeError(
            f"Wheel version mismatch: expected {expected_version!r}, got {version!r}"
        )

    for path_wheel in wheels:
        if path_wheel.name.endswith("none-any.whl"):
            raise RuntimeError(f"Wheel must be platform-specific, got: {path_wheel.name}")

        with zipfile.ZipFile(path_wheel) as zip_file:
            names = zip_file.namelist()

        for prefix in REQUIRED_EXTENSIONS:
            if not has_extension(names, prefix):
                raise RuntimeError(f"{path_wheel.name} missing Rust extension: {prefix}")

    return version


def main() -> None:
    args = parse_args()
    version = validate_wheels(args.dist_dir, args.expected_version)
    if args.print_version:
        print(version)
    else:
        print(f"Validated wheel artifacts in {args.dist_dir.resolve()} (version={version})")


if __name__ == "__main__":
    main()
