import re
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import TypeAlias

TypeCopyPatternSeq: TypeAlias = Sequence[str] | Sequence[re.Pattern[str]]


class EnumCopySymlinkStrategy(StrEnum):
    DEREFERENCE = "dereference"
    COPY_SYMLINKS = "copy_symlinks"
    SKIP_SYMLINKS = "skip_symlinks"


class EnumCopyFileConflictStrategy(StrEnum):
    SKIP = "skip"
    OVERWRITE = "overwrite"
    ERROR = "error"


class EnumCopyDirectoryConflictStrategy(StrEnum):
    SKIP = "skip"
    MERGE = "merge"
    ERROR = "error"


class EnumCopyPatternMode(StrEnum):
    GLOB = "glob"
    REGEX = "regex"
    LITERAL = "literal"


class EnumCopyDepthLimitMode(StrEnum):
    AT_MOST = "at_most"  # <=depth
    EXACT = "exact"  # =depth


@dataclass(frozen=True, slots=True)
class SpecCopyError:
    path: Path
    exception: Exception


@dataclass(frozen=True, slots=True)
class SpecCopyTreeResult:
    ok: bool
    errors: tuple[SpecCopyError, ...] = ()
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SpecCopyPatterns:
    """Compiled include/exclude pattern sets for files and directories."""

    patterns_include_files: TypeCopyPatternSeq | None
    patterns_exclude_files: TypeCopyPatternSeq | None
    patterns_include_dirs: TypeCopyPatternSeq | None
    patterns_exclude_dirs: TypeCopyPatternSeq | None

    @staticmethod
    def _ensure_sequence(value: Sequence[str] | str | None) -> Sequence[str] | None:
        return [value] if isinstance(value, str) else value

    @staticmethod
    def _compile(
        patterns: Sequence[str] | None,
        rule_pattern: EnumCopyPatternMode,
    ) -> TypeCopyPatternSeq | None:
        if not patterns:
            return None
        if rule_pattern is EnumCopyPatternMode.REGEX:
            return [re.compile(p) for p in patterns]
        return list(patterns)

    @classmethod
    def from_raw(
        cls,
        *,
        patterns_include_files: Sequence[str] | str | None,
        patterns_exclude_files: Sequence[str] | str | None,
        patterns_include_dirs: Sequence[str] | str | None,
        patterns_exclude_dirs: Sequence[str] | str | None,
        rule_pattern: EnumCopyPatternMode,
    ) -> "SpecCopyPatterns":
        """Build a compiled pattern set from raw inputs."""
        include_files = cls._ensure_sequence(patterns_include_files)
        exclude_files = cls._ensure_sequence(patterns_exclude_files)
        include_dirs = cls._ensure_sequence(patterns_include_dirs)
        exclude_dirs = cls._ensure_sequence(patterns_exclude_dirs)

        return cls(
            patterns_include_files=cls._compile(include_files, rule_pattern),
            patterns_exclude_files=cls._compile(exclude_files, rule_pattern),
            patterns_include_dirs=cls._compile(include_dirs, rule_pattern),
            patterns_exclude_dirs=cls._compile(exclude_dirs, rule_pattern),
        )
