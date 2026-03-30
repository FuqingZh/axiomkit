from __future__ import annotations

import argparse
import re
import tarfile
import zipfile
from pathlib import Path


RE_WHEEL_VERSION = re.compile(r"^axiomkit-([^-]+)-")
RE_SDIST_VERSION = re.compile(r"^axiomkit-([^-]+)\.(?:tar\.gz|zip)$")
REQUIRED_EXTENSIONS = (
    "axiomkit/_axiomkit_rs",
)
REQUIRED_SHIMS = (
    "axiomkit/io/fs/_axiomkit_io_fs_rs.py",
    "axiomkit/io/xlsx/_axiomkit_io_xlsx_rs.py",
)
REQUIRED_SDIST_SUFFIXES = (
    "LICENSE",
    "pyproject.toml",
    "Cargo.toml",
    "Cargo.lock",
    "crates/axiomkit_py/Cargo.toml",
    "crates/axiomkit_py/src/lib.rs",
    "python/axiomkit/__init__.py",
    "python/axiomkit/io/fs/_axiomkit_io_fs_rs.py",
    "python/axiomkit/io/xlsx/_axiomkit_io_xlsx_rs.py",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate built axiomkit distribution artifacts."
    )
    parser.add_argument(
        "--dist-dir",
        type=Path,
        default=Path("dist"),
        help="Directory containing built distribution artifacts.",
    )
    parser.add_argument(
        "--expected-version",
        default=None,
        help="Expected distribution version (for tagged release validation).",
    )
    parser.add_argument(
        "--print-version",
        action="store_true",
        help="Print resolved version only (for shell capture).",
    )
    parser.add_argument(
        "--require-sdist",
        action="store_true",
        help="Require a source distribution artifact alongside wheel artifacts.",
    )
    parser.add_argument(
        "--expected-manylinux-tag",
        default="manylinux_2_28",
        help=(
            "Expected manylinux tag for Linux wheels. "
            "Use 'any' to accept any repaired manylinux tag."
        ),
    )
    return parser.parse_args()


def parse_version(path_wheel: Path) -> str:
    match = RE_WHEEL_VERSION.match(path_wheel.name)
    if match is None:
        raise RuntimeError(f"Cannot parse version from wheel name: {path_wheel.name}")
    return match.group(1)


def parse_sdist_version(path_sdist: Path) -> str:
    match = RE_SDIST_VERSION.match(path_sdist.name)
    if match is None:
        raise RuntimeError(f"Cannot parse version from sdist name: {path_sdist.name}")
    return match.group(1)


def has_extension(names: list[str], prefix: str) -> bool:
    return any(
        name.startswith(prefix) and name.endswith((".so", ".pyd", ".dylib"))
        for name in names
    )


def validate_wheels(
    path_dist: Path,
    expected_version: str | None,
    expected_manylinux_tag: str,
) -> set[str]:
    wheels = sorted(path_dist.glob("axiomkit-*.whl"))
    if not wheels:
        raise RuntimeError(f"No wheel artifact found in {path_dist.resolve()}")

    versions = {parse_version(path_wheel) for path_wheel in wheels}
    if expected_version is not None and versions != {expected_version}:
        raise RuntimeError(
            f"Wheel version mismatch: expected {expected_version!r}, got {sorted(versions)!r}"
        )

    for path_wheel in wheels:
        if path_wheel.name.endswith("none-any.whl"):
            raise RuntimeError(f"Wheel must be platform-specific, got: {path_wheel.name}")
        if "-abi3-" not in path_wheel.name:
            raise RuntimeError(
                f"Wheel must be built as abi3, got: {path_wheel.name}. "
                "Check bdist_wheel.py_limited_api / RustExtension.py_limited_api settings."
            )
        if "-linux_" in path_wheel.name and "manylinux" not in path_wheel.name:
            raise RuntimeError(
                f"Unrepaired Linux wheel tag detected: {path_wheel.name}. "
                "Run auditwheel repair to produce a manylinux wheel."
            )
        if (
            expected_manylinux_tag != "any"
            and "manylinux" in path_wheel.name
            and expected_manylinux_tag not in path_wheel.name
        ):
            raise RuntimeError(
                f"Linux wheel must target {expected_manylinux_tag}, got: {path_wheel.name}"
            )

        with zipfile.ZipFile(path_wheel) as zip_file:
            names = zip_file.namelist()

        for prefix in REQUIRED_EXTENSIONS:
            if not has_extension(names, prefix):
                raise RuntimeError(f"{path_wheel.name} missing Rust extension: {prefix}")
        for shim in REQUIRED_SHIMS:
            if shim not in names:
                raise RuntimeError(f"{path_wheel.name} missing Python shim module: {shim}")

    return versions


def validate_sdist(path_dist: Path, expected_version: str | None) -> set[str]:
    sdists = sorted(path_dist.glob("axiomkit-*.tar.gz")) + sorted(
        path_dist.glob("axiomkit-*.zip")
    )
    if not sdists:
        return set()
    if len(sdists) != 1:
        raise RuntimeError(
            f"Expected exactly one sdist artifact, got {[path.name for path in sdists]}"
        )

    versions = {parse_sdist_version(path_sdist) for path_sdist in sdists}
    version = next(iter(versions))
    if expected_version is not None and version != expected_version:
        raise RuntimeError(
            f"sdist version mismatch: expected {expected_version!r}, got {version!r}"
        )

    path_sdist = sdists[0]
    if path_sdist.suffixes[-2:] == [".tar", ".gz"]:
        with tarfile.open(path_sdist, "r:gz") as tar_file:
            names = tar_file.getnames()
    elif path_sdist.suffix == ".zip":
        with zipfile.ZipFile(path_sdist) as zip_file:
            names = zip_file.namelist()
    else:
        raise RuntimeError(f"Unsupported sdist archive format: {path_sdist.name}")

    for suffix in REQUIRED_SDIST_SUFFIXES:
        if not any(name.endswith(suffix) for name in names):
            raise RuntimeError(
                f"{path_sdist.name} missing required source file: {suffix}"
            )
    return versions


def validate_dist_artifacts(
    path_dist: Path,
    expected_version: str | None,
    expected_manylinux_tag: str,
    require_sdist: bool,
) -> str:
    wheel_versions = validate_wheels(path_dist, expected_version, expected_manylinux_tag)
    sdist_versions = validate_sdist(path_dist, expected_version)

    if require_sdist and not sdist_versions:
        raise RuntimeError(
            f"No source distribution artifact found in {path_dist.resolve()}"
        )

    versions = wheel_versions | sdist_versions
    if len(versions) != 1:
        raise RuntimeError(
            f"Mixed distribution versions found: {sorted(versions)} in {path_dist.resolve()}"
        )
    return next(iter(versions))


def main() -> None:
    args = parse_args()
    version = validate_dist_artifacts(
        args.dist_dir,
        args.expected_version,
        args.expected_manylinux_tag,
        args.require_sdist,
    )
    if args.print_version:
        print(version)
    else:
        print(f"Validated dist artifacts in {args.dist_dir.resolve()} (version={version})")


if __name__ == "__main__":
    main()
