import os
import re
import shutil
from collections.abc import Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from functools import partial
from pathlib import Path
from typing import Any

from .report import ReportCopy, ReportCopyBuilder
from .spec import (
    EnumCopyDepthLimitMode,
    EnumCopyDirectoryConflictStrategy,
    EnumCopyFileConflictStrategy,
    EnumCopyPatternMode,
    EnumCopySymlinkStrategy,
    SpecCopyPatterns,
)
from .util import (
    calculate_worker_limit,
    create_symbolic_link,
    is_depth_within_limit,
    is_overlap,
    should_error_broken_symlink,
    should_exclude_by_patterns,
    should_skip_dir_conflict,
    should_skip_file_conflict,
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
    rule_pattern: EnumCopyPatternMode | str = "glob",
    rule_conflict_file: EnumCopyFileConflictStrategy | str = "skip",
    rule_conflict_dir: EnumCopyDirectoryConflictStrategy | str = "skip",
    rule_symlink: EnumCopySymlinkStrategy | str = "copy_symlinks",
    depth_limit: int | None = None,
    rule_depth_limit: EnumCopyDepthLimitMode | str = "at_most",
    num_workers_max: int | None = None,
    if_keep_tree: bool = True,
    if_dry_run: bool = False,
) -> ReportCopy:
    """Copy a directory tree with filtering, depth limits, and conflict handling.

    Args:
        dir_source: Source directory.
        dir_destination: Destination directory.
        patterns_include_files: Include patterns for file basenames.
        patterns_exclude_files: Exclude patterns for file basenames.
        patterns_include_dirs: Include patterns for directory basenames.
        patterns_exclude_dirs: Exclude patterns for directory basenames.
        rule_pattern: Pattern interpretation mode.
        rule_conflict_file: Conflict strategy for files.
        rule_conflict_dir: Conflict strategy for directories.
        rule_symlink: Symlink handling strategy.
        depth_limit: Depth limit used with ``rule_depth_limit`` (None means no limit).
        rule_depth_limit: Depth selection mode (``exact`` requires ``depth_limit``).
        num_workers_max: Maximum worker threads.
        if_keep_tree: Preserve directory structure if True.
        if_dry_run: If True, no filesystem changes are made.

    Returns:
        ReportCopy: Summary of the copy operation.
    """
    ########################################
    # #region ParameterValidation

    enum_rule_pattern = validate_copy_pattern_strategy(rule_pattern)
    enum_rule_conflict_file = validate_copy_file_conflict_strategy(rule_conflict_file)
    enum_rule_conflict_dir = validate_copy_dir_conflict_strategy(rule_conflict_dir)
    enum_rule_symlink = validate_copy_symlink_strategy(rule_symlink)
    enum_rule_depth_limit = validate_copy_depth_mode(rule_depth_limit)

    if depth_limit is None:
        if enum_rule_depth_limit is EnumCopyDepthLimitMode.EXACT:
            raise ValueError("`depth_limit` is required when depth_mode='exact'.")
    elif depth_limit < 1:
        raise ValueError("Arg `depth_limit` must be >= 1 or None.")

    try:
        spec_cp_pats = SpecCopyPatterns.from_raw(
            patterns_include_files=patterns_include_files,
            patterns_exclude_files=patterns_exclude_files,
            patterns_include_dirs=patterns_include_dirs,
            patterns_exclude_dirs=patterns_exclude_dirs,
            rule_pattern=enum_rule_pattern,
        )
    except re.error as e:
        raise ValueError(f"Invalid pattern in include/exclude: {e}") from e

    path_dir_src = Path(dir_source)
    path_dir_dst = Path(dir_destination)

    if not path_dir_src.is_dir():
        raise NotADirectoryError(f"Source is not a directory: {path_dir_src}")

    if is_overlap(path_dir_src, path_dir_dst):
        raise ValueError(
            f"Source and destination directories overlap: "
            f"{path_dir_src} <-> {path_dir_dst}"
        )

    path_dir_dst.mkdir(parents=True, exist_ok=True)

    n_workers_max = calculate_worker_limit(num_workers_max)

    # #endregion
    ########################################
    # #region CopyProcess
    builder_cp_report = ReportCopyBuilder()
    set_visited_dirs: set[tuple[int, int]] = set()

    with ThreadPoolExecutor(max_workers=n_workers_max) as executor:
        dict_futures: dict[Future[Any], tuple[Path, Path]] = {}

        # #tag WalkTraversal
        for _root, _dirnames, _filenames in os.walk(
            path_dir_src,
            followlinks=enum_rule_symlink is EnumCopySymlinkStrategy.DEREFERENCE,
        ):
            # #tag RootMeta
            path_root = Path(_root)
            try:
                n_depth_relative = len(path_root.relative_to(path_dir_src).parts)
            except ValueError:
                builder_cp_report.add_warning(
                    f"Skipped path outside root: {path_root} (root={path_dir_src})"
                )
                _dirnames[:] = []
                continue

            if enum_rule_symlink is EnumCopySymlinkStrategy.DEREFERENCE:
                try:
                    stat_root = path_root.stat()
                except OSError as e:
                    builder_cp_report.add_warning(
                        f"Failed to stat directory: {path_root} ({e})"
                    )
                    _dirnames[:] = []
                    continue
                if (
                    tuple_dirs_identifier := (stat_root.st_dev, stat_root.st_ino)
                ) in set_visited_dirs:
                    builder_cp_report.add_warning(f"Symlink loop detected: {path_root}")
                    _dirnames[:] = []
                    continue
                set_visited_dirs.add(tuple_dirs_identifier)

            # #tag PruneDirs
            if spec_cp_pats.patterns_include_dirs or spec_cp_pats.patterns_exclude_dirs:
                _dirnames[:] = [
                    _d
                    for _d in _dirnames
                    if not should_exclude_by_patterns(
                        _d,
                        spec_cp_pats.patterns_include_dirs,
                        spec_cp_pats.patterns_exclude_dirs,
                        enum_rule_pattern,
                    )
                ]

            b_is_depth_limit_reached = (
                depth_limit is not None and n_depth_relative >= depth_limit
            )
            if b_is_depth_limit_reached:
                _dirnames[:] = []

            # #tag HandleDirs
            l_kept_dirnames: list[str] = []
            for _dirname in _dirnames:
                path_dir_src_sub = path_root / _dirname
                b_depth_within = is_depth_within_limit(
                    n_depth_relative + 1, depth_limit, enum_rule_depth_limit
                )

                if path_dir_src_sub.is_symlink():
                    if enum_rule_symlink is EnumCopySymlinkStrategy.SKIP_SYMLINKS:
                        if if_keep_tree and b_depth_within:
                            builder_cp_report.add_counts(
                                "cnt_scanned", "cnt_matched", "cnt_skipped"
                            )
                        continue
                    if should_error_broken_symlink(path_dir_src_sub, enum_rule_symlink):
                        builder_cp_report.add_error(
                            path_dir_src_sub,
                            FileNotFoundError(f"Broken symlink: {path_dir_src_sub}"),
                        )
                        if if_keep_tree and b_depth_within:
                            builder_cp_report.add_counts("cnt_scanned", "cnt_matched")
                        continue

                    if enum_rule_symlink is EnumCopySymlinkStrategy.COPY_SYMLINKS:
                        if not b_depth_within:
                            continue
                        builder_cp_report.add_counts("cnt_scanned", "cnt_matched")

                        if if_keep_tree:
                            path_dir_dst_sub = (
                                path_dir_dst
                                / path_dir_src_sub.relative_to(path_dir_src)
                            )
                            if should_skip_dir_conflict(
                                path_dir_dst_sub,
                                enum_rule_conflict_dir,
                                on_skip=builder_cp_report.add_skipped,
                                on_error=partial(
                                    builder_cp_report.add_error, path_dir_dst_sub
                                ),
                            ):
                                continue
                            if (
                                enum_rule_conflict_dir
                                is EnumCopyDirectoryConflictStrategy.MERGE
                            ):
                                builder_cp_report.add_warning(
                                    f"Merge not applicable to symlink: {path_dir_dst_sub}"
                                )
                                builder_cp_report.add_skipped()
                                continue
                            if if_dry_run:
                                builder_cp_report.add_skipped()
                                continue
                            create_symbolic_link(
                                path_dir_src_sub,
                                path_dir_dst_sub,
                                on_copy=builder_cp_report.add_copied,
                                on_error=partial(
                                    builder_cp_report.add_error, path_dir_dst_sub
                                ),
                            )
                            continue

                        path_file_dst = path_dir_dst / path_dir_src_sub.name
                        if should_skip_file_conflict(
                            path_file_dst,
                            enum_rule_conflict_file,
                            on_skip=builder_cp_report.add_skipped,
                            on_error=partial(
                                builder_cp_report.add_error, path_file_dst
                            ),
                        ):
                            continue
                        if if_dry_run:
                            builder_cp_report.add_skipped()
                            continue

                        create_symbolic_link(
                            path_dir_src_sub,
                            path_file_dst,
                            on_copy=builder_cp_report.add_copied,
                            on_error=partial(
                                builder_cp_report.add_error, path_file_dst
                            ),
                        )
                        continue

                if if_keep_tree and b_depth_within:
                    builder_cp_report.add_counts("cnt_scanned", "cnt_matched")
                    path_dir_dst_sub = path_dir_dst / path_dir_src_sub.relative_to(
                        path_dir_src
                    )
                    if should_skip_dir_conflict(
                        path_dir_dst_sub,
                        enum_rule_conflict_dir,
                        on_skip=builder_cp_report.add_skipped,
                        on_error=partial(builder_cp_report.add_error, path_dir_dst_sub),
                    ):
                        continue
                    if if_dry_run:
                        builder_cp_report.add_skipped()
                    else:
                        try:
                            path_dir_dst_sub.mkdir(parents=True, exist_ok=True)
                            builder_cp_report.add_copied()
                        except Exception as e:
                            builder_cp_report.add_error(path_dir_dst_sub, e)
                            continue

                l_kept_dirnames.append(_dirname)

            _dirnames[:] = l_kept_dirnames

            # #tag HandleFiles
            for _filename in _filenames:
                path_file_src = path_root / _filename
                n_depth_file = n_depth_relative + 1
                if depth_limit is not None and not is_depth_within_limit(
                    n_depth_file, depth_limit, enum_rule_depth_limit
                ):
                    continue

                builder_cp_report.add_scanned()

                if should_exclude_by_patterns(
                    _filename,
                    spec_cp_pats.patterns_include_files,
                    spec_cp_pats.patterns_exclude_files,
                    enum_rule_pattern,
                ):
                    continue
                builder_cp_report.add_matched()

                b_is_symlink = path_file_src.is_symlink()
                if b_is_symlink:
                    if enum_rule_symlink is EnumCopySymlinkStrategy.SKIP_SYMLINKS:
                        builder_cp_report.add_skipped()
                        continue
                    if should_error_broken_symlink(path_file_src, enum_rule_symlink):
                        builder_cp_report.add_error(
                            path_file_src,
                            FileNotFoundError(f"Broken symlink: {path_file_src}"),
                        )
                        continue

                try:
                    if path_file_src.is_file() and path_file_src.stat().st_nlink > 1:
                        builder_cp_report.add_warning(
                            f"Hard link detected: {path_file_src}"
                        )
                except Exception:
                    pass

                path_file_dst = path_dir_dst / (
                    path_file_src.relative_to(path_dir_src)
                    if if_keep_tree
                    else path_file_src.name
                )

                if if_keep_tree:
                    try:
                        path_file_dst.parent.mkdir(parents=True, exist_ok=True)
                    except Exception as e:
                        builder_cp_report.add_error(path_file_dst, e)
                        continue
                if should_skip_file_conflict(
                    path_file_dst,
                    enum_rule_conflict_file,
                    on_skip=builder_cp_report.add_skipped,
                    on_error=partial(builder_cp_report.add_error, path_file_dst),
                ):
                    continue
                if if_dry_run:
                    builder_cp_report.add_skipped()
                    continue

                dict_futures[
                    executor.submit(
                        shutil.copy2,
                        path_file_src,
                        path_file_dst,
                        follow_symlinks=not (
                            b_is_symlink
                            and enum_rule_symlink
                            is EnumCopySymlinkStrategy.COPY_SYMLINKS
                        ),
                    )
                ] = (path_file_src, path_file_dst)

        # #tag AwaitWorkers
        for _future in as_completed(dict_futures):
            _, dst_path = dict_futures[_future]
            try:
                _future.result()
                builder_cp_report.add_copied()
            except Exception as e:
                builder_cp_report.add_error(dst_path, e)

    
    # #endregion
    ########################################

    return builder_cp_report.build()
