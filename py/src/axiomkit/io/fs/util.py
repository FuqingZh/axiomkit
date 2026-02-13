from .spec import (
    EnumCopyDepthLimitMode,
    EnumCopyDirectoryConflictStrategy,
    EnumCopyFileConflictStrategy,
    EnumCopyPatternMode,
    EnumCopySymlinkStrategy,
)

################################################################################
# #region StrategyValidation


def validate_copy_depth_mode(
    value: EnumCopyDepthLimitMode | str,
) -> EnumCopyDepthLimitMode:
    """Validate and normalize a depth limit mode."""
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
    """Validate and normalize a directory conflict strategy."""
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
    """Validate and normalize a file conflict strategy."""
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
    """Validate and normalize a pattern interpretation mode."""
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
    """Validate and normalize a symlink handling strategy."""
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
