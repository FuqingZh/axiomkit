from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

from .spec import SpecCopyError


@dataclass(frozen=True, slots=True)
class ReportCopy:
    """
    Summary of the outcome of a copy operation.

    This dataclass aggregates statistics and diagnostic information about a batch of filesystem copy actions,
    such as those performed when copying a directory tree.
    It records how many filesystem entries were considered,
    how many were actually copied or skipped, and any errors or warnings that occurred.

    Attributes:
        cnt_matched:
            Number of filesystem entries that matched the selection criteria for the copy operation
            (e.g. after applying include/exclude patterns).
        cnt_scanned:
            Total number of filesystem entries that were examined during the copy operation,
            regardless of whether they matched or were copied.
        cnt_copied:
            Number of entries that were successfully copied to the destination.
        cnt_skipped:
            Number of entries that were intentionally not copied
            (for example due to conflict resolution strategy, filters, or patterns).
        errors:
            Tuple of :class:`SpecCopyError` instances describing failures
            that occurred while attempting to copy specific paths.
            The length of this tuple corresponds to the total number of errors.
        warnings:
            Tuple of warning messages (as strings) produced during the copy operation.
            These typically indicate non-fatal issues or
            noteworthy conditions that did not prevent the operation from continuing.
    """

    cnt_matched: int
    cnt_scanned: int = 0
    cnt_copied: int = 0
    cnt_skipped: int = 0
    errors: tuple[SpecCopyError, ...] = ()
    warnings: tuple[str, ...] = ()

    @property
    def calculate_error_count(self) -> int:
        return len(self.errors)

    @property
    def calculate_warning_count(self) -> int:
        return len(self.warnings)

    def to_dict(self) -> dict[str, int]:
        # 结构化统计（机器用）
        return {
            "cnt_matched": self.cnt_matched,
            "cnt_scanned": self.cnt_scanned,
            "cnt_copied": self.cnt_copied,
            "cnt_skipped": self.cnt_skipped,
            "cnt_errors": self.calculate_error_count,
            "cnt_warnings": self.calculate_warning_count,
        }

    def format(self, *, prefix: str = "[COPY]") -> str:
        s = self.to_dict()
        return (
            f"{prefix} matched={s['cnt_matched']} "
            f"scanned={s['cnt_scanned']} "
            f"copied={s['cnt_copied']} skipped={s['cnt_skipped']} "
            f"errors={s['cnt_errors']} warnings={s['cnt_warnings']}"
        )

    def __str__(self) -> str:
        # “Display”：默认人类可读格式
        return self.format()

    def __repr__(self) -> str:
        # “Debug”：更信息密度的开发者输出
        return (
            f"{self.__class__.__name__}("
            f"matched_count={self.cnt_matched}, "
            f"scanned_count={self.cnt_scanned}, "
            f"copied_count={self.cnt_copied}, "
            f"skipped_count={self.cnt_skipped}, "
            f"errors_count={self.calculate_error_count}, "
            f"warnings_count={self.calculate_warning_count})"
        )


@dataclass(slots=True)
class ReportCopyBuilder:
    """Mutable accumulator for copy statistics."""

    COUNTER_FIELDS: ClassVar[frozenset[str]] = frozenset(
        {"cnt_matched", "cnt_scanned", "cnt_copied", "cnt_skipped"}
    )

    cnt_matched: int = 0
    cnt_scanned: int = 0
    cnt_copied: int = 0
    cnt_skipped: int = 0
    errors: list[SpecCopyError] = field(default_factory=lambda: [])
    warnings: list[str] = field(default_factory=lambda: [])

    def add_counts(self, *field_names: str, value: int = 1) -> None:
        if not field_names:
            raise ValueError("`field_names` is required.")
        for _name in field_names:
            if _name not in self.COUNTER_FIELDS:
                raise ValueError(f"Unsupported counter: {_name}")
            setattr(self, _name, getattr(self, _name) + value)

    def add_matched(self) -> None:
        self.cnt_matched += 1

    def add_scanned(self) -> None:
        self.cnt_scanned += 1

    def add_copied(self) -> None:
        self.cnt_copied += 1

    def add_skipped(self) -> None:
        self.cnt_skipped += 1

    def add_warning(self, warning: str) -> None:
        self.warnings.append(warning)

    def add_error(self, path: Path, error: Exception) -> None:
        self.errors.append(SpecCopyError(path, error))

    def build(self) -> ReportCopy:
        return ReportCopy(
            cnt_matched=self.cnt_matched,
            cnt_scanned=self.cnt_scanned,
            cnt_copied=self.cnt_copied,
            cnt_skipped=self.cnt_skipped,
            errors=tuple(self.errors),
            warnings=tuple(self.warnings),
        )
