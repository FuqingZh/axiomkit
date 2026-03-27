import os
from collections.abc import Sequence
from pathlib import Path

from ._rs_bridge import copy_tree_via_rs, is_rs_backend_available
from .report import CopyReport
from .spec import (
    CopyDepthLimitMode,
    CopyDirectoryConflictStrategy,
    CopyFileConflictStrategy,
    CopyPatternMode,
    CopySymlinkStrategy,
)
from .util import (
    validate_copy_depth_mode,
    validate_copy_dir_conflict_strategy,
    validate_copy_file_conflict_strategy,
    validate_copy_pattern_strategy,
    validate_copy_symlink_strategy,
)

################################################################################


def copy_tree(
    dir_source: os.PathLike[str] | str,
    dir_destination: os.PathLike[str] | str,
    *,
    patterns_include_files: Sequence[str] | str | None = None,
    patterns_exclude_files: Sequence[str] | str | None = None,
    patterns_include_dirs: Sequence[str] | str | None = None,
    patterns_exclude_dirs: Sequence[str] | str | None = None,
    rule_pattern: CopyPatternMode | str = "glob",
    rule_conflict_file: CopyFileConflictStrategy | str = "skip",
    rule_conflict_dir: CopyDirectoryConflictStrategy | str = "skip",
    rule_symlink: CopySymlinkStrategy | str = "copy_symlinks",
    depth_limit: int | None = None,
    rule_depth_limit: CopyDepthLimitMode | str = "at_most",
    workers_max: int | None = None,
    should_keep_tree: bool = True,
    should_dry_run: bool = False,
) -> CopyReport:
    """Copy a directory tree with filtering, depth limits, and conflict handling.

    This function delegates filesystem traversal/copy execution to the Rust backend.
    The Python layer here keeps public API shape, argument normalization, and
    validation behavior.

    Args:
        dir_source: Source directory.
        dir_destination: Destination directory.

        patterns_include_files: File basename include patterns.
        patterns_exclude_files: File basename exclude patterns.
        patterns_include_dirs: Directory basename include patterns.
        patterns_exclude_dirs: Directory basename exclude patterns.

        rule_pattern:
            Pattern interpretation mode. See :class:`CopyPatternMode`.
            - ``glob``: (Default) Unix shell-style wildcards.
            - ``regex``: Regular expressions.
            - ``literal``: Exact string matches.
        rule_conflict_file:
            File conflict strategy. See :class:`CopyFileConflictStrategy`.
            - ``skip``: (Default) Skip existing files.
            - ``overwrite``: Overwrite existing files.
            - ``error``: Raise an error on conflict.
        rule_conflict_dir:
            Directory conflict strategy. See :class:`CopyDirectoryConflictStrategy`.
            - ``skip``: (Default) Skip existing directories.
            - ``merge``: Merge contents into existing directories.
            - ``error``: Raise an error on conflict.
        rule_symlink:
            Symlink handling strategy. See :class:`CopySymlinkStrategy`.
            - ``copy_symlinks``: (Default) Copy symlinks as symlinks.
            - ``dereference``: Follow symlinks and copy target files/directories.
            - ``skip_symlinks``: Skip symlinked files and directories.

        depth_limit:
            Depth limit used with ``rule_depth_limit`` (None means no limit).
        rule_depth_limit:
            Depth selection mode. See :class:`CopyDepthLimitMode`.
            - ``at_most``: (Default) Copy items at depth <= depth_limit.
            - ``exact``: Copy items at depth == depth_limit.
            It requires ``depth_limit`` to be set.

        workers_max: Maximum worker threads.
        should_keep_tree:
            - ``True``: (Default) Keep source directory structure in destination.
            - ``False``: Flatten structure; copy all matched files into destination root.
        should_dry_run:
            - ``False``: (Default) Perform actual copy.
            - ``True``: Simulate copy without making changes.

    Raises:
        ValueError:
            If ``depth_limit`` is invalid, ``rule_depth_limit`` is ``exact`` without
            ``depth_limit``, or any enum-like rule value is invalid.
        NotADirectoryError:
            If ``dir_source`` is not a directory.
        RuntimeError:
            If Rust backend is unavailable.

    Returns:
        CopyReport: Summary of the copy operation.

    Examples:
        >>> from pathlib import Path
        >>> report = copy_tree(
        ...     Path("data/raw"),
        ...     Path("data/processed"),
        ...     patterns_include_files=["*.csv"],
        ...     rule_pattern="glob",
        ...     rule_conflict_file="skip",
        ... )
        >>> report.error_count == 0
        True
        >>> report_flat = copy_tree(
        ...     Path("data/raw"),
        ...     Path("data/flat"),
        ...     should_keep_tree=False,
        ...     patterns_include_files=["*.txt"],
        ... )
        >>> report_flat.error_count == 0
        True
    """
    enum_rule_pattern = validate_copy_pattern_strategy(rule_pattern)
    enum_rule_conflict_file = validate_copy_file_conflict_strategy(rule_conflict_file)
    enum_rule_conflict_dir = validate_copy_dir_conflict_strategy(rule_conflict_dir)
    enum_rule_symlink = validate_copy_symlink_strategy(rule_symlink)
    enum_rule_depth_limit = validate_copy_depth_mode(rule_depth_limit)

    if depth_limit is None:
        if enum_rule_depth_limit is CopyDepthLimitMode.EXACT:
            raise ValueError("`depth_limit` is required when depth_mode='exact'.")
    elif depth_limit < 1:
        raise ValueError("Arg `depth_limit` must be >= 1 or None.")

    if not is_rs_backend_available():
        raise RuntimeError(
            "Rust fs backend is unavailable. Build/install `_axiomkit_io_fs_rs` first."
        )

    return copy_tree_via_rs(
        Path(dir_source),
        Path(dir_destination),
        patterns_include_files=patterns_include_files,
        patterns_exclude_files=patterns_exclude_files,
        patterns_include_dirs=patterns_include_dirs,
        patterns_exclude_dirs=patterns_exclude_dirs,
        rule_pattern=enum_rule_pattern,
        rule_conflict_file=enum_rule_conflict_file,
        rule_conflict_dir=enum_rule_conflict_dir,
        rule_symlink=enum_rule_symlink,
        depth_limit=depth_limit,
        rule_depth_limit=enum_rule_depth_limit,
        workers_max=workers_max,
        should_keep_tree=should_keep_tree,
        should_dry_run=should_dry_run,
    )
