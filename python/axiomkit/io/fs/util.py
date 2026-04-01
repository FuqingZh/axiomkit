from .spec import (
    CopyDepthLimitMode,
    CopyDirectoryConflictMode,
    CopyFileConflictMode,
    CopyPatternMode,
    CopySymlinkMode,
)

################################################################################
# #region StrategyValidation


def normalize_copy_depth_mode(
    value: CopyDepthLimitMode | str,
) -> CopyDepthLimitMode:
    """Validate and normalize a depth limit mode."""
    if isinstance(value, CopyDepthLimitMode):
        return value
    try:
        return CopyDepthLimitMode(value)
    except ValueError as e:
        raise ValueError(
            f"Invalid depth mode: `{value}`. "
            f"Expected one of: {[s.value for s in CopyDepthLimitMode]}"
        ) from e


def normalize_copy_dir_conflict_mode(
    value: CopyDirectoryConflictMode | str,
) -> CopyDirectoryConflictMode:
    """Validate and normalize a directory conflict strategy."""
    if isinstance(value, CopyDirectoryConflictMode):
        return value
    try:
        return CopyDirectoryConflictMode(value)
    except ValueError as e:
        raise ValueError(
            f"Invalid directory conflict strategy: `{value}`. "
            f"Expected one of: {[s.value for s in CopyDirectoryConflictMode]}"
        ) from e


def normalize_copy_file_conflict_mode(
    value: CopyFileConflictMode | str,
) -> CopyFileConflictMode:
    """Validate and normalize a file conflict strategy."""
    if isinstance(value, CopyFileConflictMode):
        return value
    try:
        return CopyFileConflictMode(value)
    except ValueError as e:
        raise ValueError(
            f"Invalid file conflict strategy: `{value}`. "
            f"Expected one of: {[s.value for s in CopyFileConflictMode]}"
        ) from e


def normalize_copy_pattern_mode(
    value: CopyPatternMode | str,
) -> CopyPatternMode:
    """Validate and normalize a pattern interpretation mode."""
    if isinstance(value, CopyPatternMode):
        return value
    try:
        return CopyPatternMode(value)
    except ValueError as e:
        raise ValueError(
            f"Invalid pattern strategy: `{value}`. "
            f"Expected one of: {[s.value for s in CopyPatternMode]}"
        ) from e


def normalize_copy_symlink_mode(
    value: CopySymlinkMode | str,
) -> CopySymlinkMode:
    """Validate and normalize a symlink handling strategy."""
    if isinstance(value, CopySymlinkMode):
        return value
    try:
        return CopySymlinkMode(value)
    except ValueError as e:
        raise ValueError(
            f"Invalid symlink strategy: `{value}`. "
            f"Expected one of: {[s.value for s in CopySymlinkMode]}"
        ) from e


# #endregion
################################################################################
