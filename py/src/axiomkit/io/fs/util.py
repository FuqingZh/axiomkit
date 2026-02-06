import os
import re
from collections.abc import Callable, Sequence
from fnmatch import fnmatchcase
from pathlib import Path
from typing import cast

from .spec import (
    EnumCopyDepthLimitMode,
    EnumCopyDirectoryConflictStrategy,
    EnumCopyFileConflictStrategy,
    EnumCopyPatternMode,
    EnumCopySymlinkStrategy,
    TypeCopyPatternSeq,
)

################################################################################
# #region StrategyValidation


def validate_copy_depth_mode(
    value: EnumCopyDepthLimitMode | str,
) -> EnumCopyDepthLimitMode:
    """Validate and normalize a depth limit mode.

    Args:
        value: Enum value or its string representation.

    Returns:
        Normalized EnumCopyDepthLimitMode.

    Raises:
        ValueError: If ``value`` is invalid.
    """
    if isinstance(value, EnumCopyDepthLimitMode):
        return value
    try:
        return EnumCopyDepthLimitMode(value)
    except ValueError as e:
        raise ValueError(
            f"Invalid depth mode: `{value}`. "
            f"Expected one of: {[s.value for s in EnumCopyDepthLimitMode]}"
        ) from e


def validate_copy_dir_conflict_strategy(
    value: EnumCopyDirectoryConflictStrategy | str,
) -> EnumCopyDirectoryConflictStrategy:
    """Validate and normalize a directory conflict strategy.

    Args:
        value: Enum value or its string representation.

    Returns:
        Normalized EnumCopyDirectoryConflictStrategy.

    Raises:
        ValueError: If ``value`` is invalid.
    """
    if isinstance(value, EnumCopyDirectoryConflictStrategy):
        return value
    try:
        return EnumCopyDirectoryConflictStrategy(value)
    except ValueError as e:
        raise ValueError(
            f"Invalid directory conflict strategy: `{value}`. "
            f"Expected one of: {[s.value for s in EnumCopyDirectoryConflictStrategy]}"
        ) from e


def validate_copy_file_conflict_strategy(
    value: EnumCopyFileConflictStrategy | str,
) -> EnumCopyFileConflictStrategy:
    """Validate and normalize a file conflict strategy.

    Args:
        value: Enum value or its string representation.

    Returns:
        Normalized EnumCopyFileConflictStrategy.

    Raises:
        ValueError: If ``value`` is invalid.
    """
    if isinstance(value, EnumCopyFileConflictStrategy):
        return value
    try:
        return EnumCopyFileConflictStrategy(value)
    except ValueError as e:
        raise ValueError(
            f"Invalid file conflict strategy: `{value}`. "
            f"Expected one of: {[s.value for s in EnumCopyFileConflictStrategy]}"
        ) from e


def validate_copy_pattern_strategy(
    value: EnumCopyPatternMode | str,
) -> EnumCopyPatternMode:
    """Validate and normalize a pattern interpretation mode.

    Args:
        value: Enum value or its string representation.

    Returns:
        Normalized EnumCopyPatternMode.

    Raises:
        ValueError: If ``value`` is invalid.
    """
    if isinstance(value, EnumCopyPatternMode):
        return value
    try:
        return EnumCopyPatternMode(value)
    except ValueError as e:
        raise ValueError(
            f"Invalid pattern strategy: `{value}`. "
            f"Expected one of: {[s.value for s in EnumCopyPatternMode]}"
        ) from e


def validate_copy_symlink_strategy(
    value: EnumCopySymlinkStrategy | str,
) -> EnumCopySymlinkStrategy:
    """Validate and normalize a symlink handling strategy.

    Args:
        value: Enum value or its string representation.

    Returns:
        Normalized EnumCopySymlinkStrategy.

    Raises:
        ValueError: If ``value`` is invalid.
    """
    if isinstance(value, EnumCopySymlinkStrategy):
        return value
    try:
        return EnumCopySymlinkStrategy(value)
    except ValueError as e:
        raise ValueError(
            f"Invalid symlink strategy: `{value}`. "
            f"Expected one of: {[s.value for s in EnumCopySymlinkStrategy]}"
        ) from e


# #endregion
################################################################################
# #region WorkerCalculation


def calculate_worker_limit(num_workers_max: int | None) -> int:
    """Calculate a safe worker limit bounded by CPU count.

    Args:
        num_workers_max: User-provided maximum workers; ``None`` uses CPU count.

    Returns:
        A positive worker count.
    """
    n_cpu = os.cpu_count() or 1
    return (
        max(1, n_cpu)
        if num_workers_max is None
        else max(1, min(num_workers_max, n_cpu))
    )


# #endregion
################################################################################
# #region Path


def _is_relative_to_base(path: Path, base: Path) -> bool:
    """Check whether ``path`` is within ``base``.

    Args:
        path: Path to test.
        base: Base directory.

    Returns:
        True if ``path`` is relative to ``base``.
    """
    try:
        path.relative_to(base)
        return True
    except ValueError:
        return False


def _normalize_path(path: Path) -> Path:
    """Resolve a path, allowing non-existent targets.

    Args:
        path: Path to resolve.

    Returns:
        Resolved path (best-effort).
    """
    try:
        return path.resolve()
    except FileNotFoundError:
        return path.resolve(strict=False)


def is_overlap(src: Path, dst: Path) -> bool:
    """Check whether two paths overlap or contain each other.

    Args:
        src: Source path.
        dst: Destination path.

    Returns:
        True if either path is within the other.
    """
    src_resolved = _normalize_path(src)
    dst_resolved = _normalize_path(dst)
    return _is_relative_to_base(dst_resolved, src_resolved) or _is_relative_to_base(
        src_resolved, dst_resolved
    )


# #endregion
################################################################################
# #region PatternMatching


def _is_pattern_matching(
    value: str,
    patterns: TypeCopyPatternSeq | None,
    rule_pattern: EnumCopyPatternMode,
) -> bool:
    """Check whether a value matches any pattern under the given mode.

    Args:
        value: String to test.
        patterns: Pattern sequence or ``None``.
        rule_pattern: Pattern interpretation mode.

    Returns:
        True if any pattern matches.
    """
    if not patterns:
        return False
    match rule_pattern:
        case EnumCopyPatternMode.LITERAL:
            patterns = cast(Sequence[str], patterns)
            return any(p in value for p in patterns)
        case EnumCopyPatternMode.GLOB:
            patterns = cast(Sequence[str], patterns)
            return any(fnmatchcase(value, p) for p in patterns)
        case EnumCopyPatternMode.REGEX:
            patterns = cast(Sequence[re.Pattern[str]], patterns)
            return any(p.search(value) for p in patterns)


def _should_include(
    value: str,
    patterns: TypeCopyPatternSeq | None,
    rule_pattern: EnumCopyPatternMode,
) -> bool:
    """Decide whether a value should be included.

    Args:
        value: String to test.
        patterns: Include patterns or ``None``.
        rule_pattern: Pattern interpretation mode.

    Returns:
        True if inclusion applies.
    """
    return (
        True
        if patterns is None
        else _is_pattern_matching(value, patterns, rule_pattern)
    )


def _should_exclude(
    value: str,
    patterns: TypeCopyPatternSeq | None,
    rule_pattern: EnumCopyPatternMode,
) -> bool:
    """Decide whether a value should be excluded.

    Args:
        value: String to test.
        patterns: Exclude patterns or ``None``.
        rule_pattern: Pattern interpretation mode.

    Returns:
        True if exclusion applies.
    """
    return (
        False
        if patterns is None
        else _is_pattern_matching(value, patterns, rule_pattern)
    )


def should_exclude_by_patterns(
    value: str,
    patterns_include: TypeCopyPatternSeq | None,
    patterns_exclude: TypeCopyPatternSeq | None,
    rule_pattern: EnumCopyPatternMode,
) -> bool:
    """Apply include-then-exclude filtering to a value.

    Args:
        value: String to test.
        patterns_include: Include patterns or ``None``.
        patterns_exclude: Exclude patterns or ``None``.
        rule_pattern: Pattern interpretation mode.

    Returns:
        True if the value should be excluded.
    """
    return not _should_include(
        value, patterns_include, rule_pattern
    ) or _should_exclude(value, patterns_exclude, rule_pattern)


# #endregion
################################################################################
# #region CopyHelpers


def should_error_broken_symlink(
    path_symlink: Path,
    rule_symlink: EnumCopySymlinkStrategy,
) -> bool:
    """Check whether a broken symlink should be treated as an error.

    Args:
        path_symlink: Symlink path to evaluate.
        rule_symlink: Symlink handling strategy.

    Returns:
        True if the symlink is broken and the strategy is dereference.
    """
    return (
        rule_symlink is EnumCopySymlinkStrategy.DEREFERENCE
        and not path_symlink.exists()
    )


def should_skip_dir_conflict(
    path_dst: Path,
    rule_conflict: EnumCopyDirectoryConflictStrategy,
    *,
    on_skip: Callable[[], None],
    on_error: Callable[[Exception], None],
) -> bool:
    """Apply directory conflict rules and decide whether to skip.

    Args:
        path_dst: Destination path.
        rule_conflict: Conflict strategy to apply.
        on_skip: Callback when a skip decision is made.
        on_error: Callback when an error is recorded.

    Returns:
        True if the caller should skip further handling.
    """
    if not path_dst.exists():
        return False
    if path_dst.is_file():
        on_error(
            NotADirectoryError(f"Destination is a file, expected directory: {path_dst}")
        )
        return True
    if rule_conflict is EnumCopyDirectoryConflictStrategy.SKIP:
        on_skip()
        return True
    if rule_conflict is EnumCopyDirectoryConflictStrategy.ERROR:
        on_error(FileExistsError(f"Destination exists: {path_dst}"))
        return True
    return False


def should_skip_file_conflict(
    path_dst: Path,
    rule_conflict: EnumCopyFileConflictStrategy,
    *,
    on_skip: Callable[[], None],
    on_error: Callable[[Exception], None],
) -> bool:
    """Apply file conflict rules and decide whether to skip.

    Args:
        path_dst: Destination path.
        rule_conflict: Conflict strategy to apply.
        on_skip: Callback when a skip decision is made.
        on_error: Callback when an error is recorded.

    Returns:
        True if the caller should skip further handling.
    """
    if not path_dst.exists():
        return False
    if path_dst.is_dir():
        on_error(IsADirectoryError(f"Destination is a directory: {path_dst}"))
        return True
    if rule_conflict is EnumCopyFileConflictStrategy.SKIP:
        on_skip()
        return True
    if rule_conflict is EnumCopyFileConflictStrategy.ERROR:
        on_error(FileExistsError(f"Destination exists: {path_dst}"))
        return True
    return False


def create_symbolic_link(
    path_src: Path,
    path_dst: Path,
    *,
    on_copy: Callable[[], None],
    on_error: Callable[[Exception], None],
) -> None:
    """Create a symbolic link from source to destination.

    Args:
        path_src: Source path.
        path_dst: Destination path.
        on_copy: Callback when the link is created.
        on_error: Callback when an error occurs.
    """
    try:
        os.symlink(os.readlink(path_src), path_dst)
        on_copy()
    except Exception as e:
        on_error(e)


def is_depth_within_limit(
    depth_value: int,
    depth_limit: int | None,
    rule_depth_limit: EnumCopyDepthLimitMode,
) -> bool:
    """Check whether a depth satisfies the limit rule.

    Args:
        depth_value: Current depth.
        depth_limit: Depth limit or None.
        rule_depth_limit: Limit mode (at_most or exact).

    Returns:
        True if the depth is within the limit.
    """
    if depth_limit is None:
        return True
    if rule_depth_limit is EnumCopyDepthLimitMode.AT_MOST:
        return depth_value <= depth_limit
    return depth_value == depth_limit


# #endregion
################################################################################
