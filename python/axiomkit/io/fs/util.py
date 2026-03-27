from .spec import (
    CopyDepthLimitMode,
    CopyDirectoryConflictStrategy,
    CopyFileConflictStrategy,
    CopyPatternMode,
    CopySymlinkStrategy,
)

################################################################################
# #region StrategyValidation


def validate_copy_depth_mode(
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


def validate_copy_dir_conflict_strategy(
    value: CopyDirectoryConflictStrategy | str,
) -> CopyDirectoryConflictStrategy:
    """Validate and normalize a directory conflict strategy."""
    if isinstance(value, CopyDirectoryConflictStrategy):
        return value
    try:
        return CopyDirectoryConflictStrategy(value)
    except ValueError as e:
        raise ValueError(
            f"Invalid directory conflict strategy: `{value}`. "
            f"Expected one of: {[s.value for s in CopyDirectoryConflictStrategy]}"
        ) from e


def validate_copy_file_conflict_strategy(
    value: CopyFileConflictStrategy | str,
) -> CopyFileConflictStrategy:
    """Validate and normalize a file conflict strategy."""
    if isinstance(value, CopyFileConflictStrategy):
        return value
    try:
        return CopyFileConflictStrategy(value)
    except ValueError as e:
        raise ValueError(
            f"Invalid file conflict strategy: `{value}`. "
            f"Expected one of: {[s.value for s in CopyFileConflictStrategy]}"
        ) from e


def validate_copy_pattern_strategy(
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


def validate_copy_symlink_strategy(
    value: CopySymlinkStrategy | str,
) -> CopySymlinkStrategy:
    """Validate and normalize a symlink handling strategy."""
    if isinstance(value, CopySymlinkStrategy):
        return value
    try:
        return CopySymlinkStrategy(value)
    except ValueError as e:
        raise ValueError(
            f"Invalid symlink strategy: `{value}`. "
            f"Expected one of: {[s.value for s in CopySymlinkStrategy]}"
        ) from e


# #endregion
################################################################################
