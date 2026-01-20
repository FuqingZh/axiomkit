# "Facts/Results/Plans" generated from processing DataFrame to Excel XLSX files.

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SheetPart:
    sheet_name: str
    row_start_inclusive: int
    row_end_exclusive: int  # exclusive in source df rows
    col_start_inclusive: int
    col_end_exclusive: int  # exclusive in source df cols


@dataclass(slots=True)
class XlsxReport:
    sheets: list[SheetPart]
    warnings: list[str]

    def warn(self, msg: str) -> None:
        self.warnings.append(str(msg))


@dataclass(frozen=True, slots=True)
class HorizontalMerge:
    row_idx_start: int
    col_idx_start: int
    col_idx_end: int  # inclusive
    text: str


@dataclass(frozen=True, slots=True)
class BorderSpec:
    top: int
    bottom: int
    left: int
    right: int
