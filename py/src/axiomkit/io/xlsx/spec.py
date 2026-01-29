# "Facts/Results/Plans" generated from processing DataFrame to Excel XLSX files.

from dataclasses import dataclass, replace
from typing import Any

import xlsxwriter.format


################################################################################
# #region CellFormatSpecification
@dataclass(frozen=True, slots=True)
class SpecCellFormat:
    # 字段名严格对齐 XlsxWriter format properties keys
    font_name: str | None = None
    font_size: int | None = None
    bold: bool | None = None
    italic: bool | None = None

    align: str | None = None
    valign: str | None = None
    border: int | None = None
    text_wrap: bool | None = None

    top: int | None = None
    bottom: int | None = None
    left: int | None = None
    right: int | None = None

    num_format: str | None = None
    bg_color: str | None = None
    font_color: str | None = None

    def with_(self, **kwargs: Any) -> "SpecCellFormat":
        return replace(self, **kwargs)

    def merge(self, other: "SpecCellFormat") -> "SpecCellFormat":
        # 右侧非 None 覆盖左侧
        data = {
            k: (
                getattr(other, k) if getattr(other, k) is not None else getattr(self, k)
            )
            for k in self.__dataclass_fields__
        }
        return SpecCellFormat(**data)

    def to_xlsxwriter(self) -> dict[str, Any]:
        return {
            k: getattr(self, k)
            for k in self.__dataclass_fields__
            if getattr(self, k) is not None
        }


@dataclass(frozen=True, slots=True)
class SpecCellBorder:
    top: int
    bottom: int
    left: int
    right: int


# #endregion
################################################################################
# #region ColumnFormatSpecification


@dataclass(slots=True)
class SpecColumnFormatPlan:
    fmts_by_col: list[xlsxwriter.format.Format]
    cols_formatted: list["SpecColumnFormatRange"]
    rules_conditional_fmt: list["SpecConditionalFormatRule"]
    is_use_conditional: bool


@dataclass(slots=True)
class SpecColumnFormatRange:
    col_start: int
    col_end: int
    fmt: xlsxwriter.format.Format


@dataclass(slots=True)
class SpecConditionalFormatRule:
    row_start: int
    col_start: int
    row_end: int
    col_end: int
    fmt: xlsxwriter.format.Format


# #endregion
################################################################################
# #region SheetFormatSpecification
@dataclass(frozen=True, slots=True)
class SpecSheetSlice:
    sheet_name: str
    row_start_inclusive: int
    row_end_exclusive: int  # exclusive in source df rows
    col_start_inclusive: int
    col_end_exclusive: int  # exclusive in source df cols


@dataclass(frozen=True, slots=True)
class SpecSheetHorizontalMerge:
    row_idx_start: int
    col_idx_start: int
    col_idx_end: int  # inclusive
    text: str


# #endregion
################################################################################
# #region ReportSpecification
@dataclass(slots=True)
class SpecXlsxReport:
    sheets: list[SpecSheetSlice]
    warnings: list[str]

    def warn(self, msg: str) -> None:
        self.warnings.append(str(msg))


# #endregion
################################################################################
