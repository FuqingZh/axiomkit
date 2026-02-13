from dataclasses import dataclass

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
    def error_count(self) -> int:
        return len(self.errors)

    @property
    def warning_count(self) -> int:
        return len(self.warnings)

    def to_dict(self) -> dict[str, int]:
        # 结构化统计（机器用）
        return {
            "cnt_matched": self.cnt_matched,
            "cnt_scanned": self.cnt_scanned,
            "cnt_copied": self.cnt_copied,
            "cnt_skipped": self.cnt_skipped,
            "cnt_errors": self.error_count,
            "cnt_warnings": self.warning_count,
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
            f"errors_count={self.error_count}, "
            f"warnings_count={self.warning_count})"
        )
