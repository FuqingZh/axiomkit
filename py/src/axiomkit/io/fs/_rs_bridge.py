from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from .report import ReportCopy
from .spec import (
    EnumCopyDepthLimitMode,
    EnumCopyDirectoryConflictStrategy,
    EnumCopyFileConflictStrategy,
    EnumCopyPatternMode,
    EnumCopySymlinkStrategy,
    SpecCopyError,
)

EXPECTED_BRIDGE_ABI = 1
EXPECTED_BRIDGE_CONTRACT = "axiomkit.fs.copy_tree.v1"
EXPECTED_BRIDGE_TRANSPORT = "rust_native"

_copy_tree_rs: Any | None = None
_mod_rs: Any | None = None
_error_import: Exception | None = None
_error_contract: Exception | None = None

try:
    from . import _axiomkit_io_fs_rs as _mod_rs

    _copy_tree_rs = _mod_rs.copy_tree
except Exception as exc:  # pragma: no cover
    _mod_rs = None
    _copy_tree_rs = None
    _error_import = exc


def _validate_bridge_contract() -> None:
    if _mod_rs is None:
        return

    abi = getattr(_mod_rs, "__bridge_abi__", None)
    contract = getattr(_mod_rs, "__bridge_contract__", None)
    transport = getattr(_mod_rs, "__bridge_transport__", None)

    if abi != EXPECTED_BRIDGE_ABI:
        raise RuntimeError(
            "Rust fs bridge ABI mismatch: "
            f"python expects {EXPECTED_BRIDGE_ABI}, rust exports {abi!r}."
        )
    if contract != EXPECTED_BRIDGE_CONTRACT:
        raise RuntimeError(
            "Rust fs bridge contract mismatch: "
            f"python expects {EXPECTED_BRIDGE_CONTRACT!r}, rust exports {contract!r}."
        )
    if transport != EXPECTED_BRIDGE_TRANSPORT:
        raise RuntimeError(
            "Rust fs bridge transport mismatch: "
            f"python expects {EXPECTED_BRIDGE_TRANSPORT!r}, rust exports {transport!r}."
        )


if _mod_rs is not None:
    try:
        _validate_bridge_contract()
    except Exception as exc:  # pragma: no cover
        _copy_tree_rs = None
        _error_contract = exc


def is_rs_backend_available() -> bool:
    return _copy_tree_rs is not None


def _raise_unavailable() -> None:
    if _error_contract is not None:
        raise RuntimeError(
            "Rust fs backend contract validation failed."
        ) from _error_contract
    if _error_import is not None:
        raise RuntimeError("Rust fs backend import failed.") from _error_import
    raise RuntimeError("Rust fs backend is unavailable")


def _ensure_sequence(value: Sequence[str] | str | None) -> Sequence[str] | None:
    return [value] if isinstance(value, str) else value


def copy_tree_via_rs(
    dir_source: Path,
    dir_destination: Path,
    *,
    patterns_include_files: Sequence[str] | str | None,
    patterns_exclude_files: Sequence[str] | str | None,
    patterns_include_dirs: Sequence[str] | str | None,
    patterns_exclude_dirs: Sequence[str] | str | None,
    rule_pattern: EnumCopyPatternMode,
    rule_conflict_file: EnumCopyFileConflictStrategy,
    rule_conflict_dir: EnumCopyDirectoryConflictStrategy,
    rule_symlink: EnumCopySymlinkStrategy,
    depth_limit: int | None,
    rule_depth_limit: EnumCopyDepthLimitMode,
    num_workers_max: int | None,
    if_keep_tree: bool,
    if_dry_run: bool,
) -> ReportCopy:
    if _copy_tree_rs is None:  # pragma: no cover
        _raise_unavailable()
    else:
        report_rs = _copy_tree_rs(
            str(dir_source),
            str(dir_destination),
            patterns_include_files=_ensure_sequence(patterns_include_files),
            patterns_exclude_files=_ensure_sequence(patterns_exclude_files),
            patterns_include_dirs=_ensure_sequence(patterns_include_dirs),
            patterns_exclude_dirs=_ensure_sequence(patterns_exclude_dirs),
            rule_pattern=rule_pattern.value,
            rule_conflict_file=rule_conflict_file.value,
            rule_conflict_dir=rule_conflict_dir.value,
            rule_symlink=rule_symlink.value,
            depth_limit=depth_limit,
            rule_depth_limit=rule_depth_limit.value,
            num_workers_max=num_workers_max,
            if_keep_tree=if_keep_tree,
            if_dry_run=if_dry_run,
        )

    errors = tuple(
        SpecCopyError(path=Path(e.path), exception=RuntimeError(e.exception))
        for e in report_rs.errors
    )
    warnings = tuple(report_rs.warnings)

    return ReportCopy(
        cnt_matched=report_rs.cnt_matched,
        cnt_scanned=report_rs.cnt_scanned,
        cnt_copied=report_rs.cnt_copied,
        cnt_skipped=report_rs.cnt_skipped,
        errors=errors,
        warnings=warnings,
    )
