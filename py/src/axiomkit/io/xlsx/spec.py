# "Facts/Results/Plans" generated from processing DataFrame to Excel XLSX files.

from dataclasses import dataclass, field, replace
from typing import Any, Literal


################################################################################
# #region CellFormatSpecification
@dataclass(frozen=True, slots=True)
class CellFormatSpec:
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

    def with_(self, **kwargs: Any) -> "CellFormatSpec":
        return replace(self, **kwargs)

    def merge(self, other: "CellFormatSpec") -> "CellFormatSpec":
        # 右侧非 None 覆盖左侧
        data = {
            k: (
                getattr(other, k) if getattr(other, k) is not None else getattr(self, k)
            )
            for k in self.__dataclass_fields__
        }
        return CellFormatSpec(**data)

    def to_xlsxwriter(self) -> dict[str, Any]:
        return {
            k: getattr(self, k)
            for k in self.__dataclass_fields__
            if getattr(self, k) is not None
        }


@dataclass(frozen=True, slots=True)
class CellBorderSpec:
    top: int
    bottom: int
    left: int
    right: int


# #endregion
################################################################################
# #region WriteOptions


@dataclass(frozen=True, slots=True)
class XlsxValuePolicySpec:
    missing_value_str: str = "NA"
    nan_str: str = "NaN"
    posinf_str: str = "Inf"
    neginf_str: str = "-Inf"
    integer_coerce: Literal["coerce", "strict"] = "strict"


@dataclass(frozen=True, slots=True)
class XlsxRowChunkPolicySpec:
    width_large: int = 8_000
    width_medium: int = 2_000
    size_large: int = 1_000
    size_medium: int = 2_000
    size_default: int = 10_000
    fixed_size: int | None = None


@dataclass(frozen=True, slots=True)
class XlsxWriteOptionsSpec:
    value_policy: XlsxValuePolicySpec = field(default_factory=XlsxValuePolicySpec)
    should_keep_missing_values: bool = False
    should_infer_numeric_cols: bool = True
    should_infer_integer_cols: bool = True
    row_chunk_policy: XlsxRowChunkPolicySpec = field(
        default_factory=XlsxRowChunkPolicySpec
    )
    base_format_patch: "CellFormatSpec" = field(
        default_factory=lambda: CellFormatSpec(
            border=0, top=0, bottom=0, left=0, right=0
        )
    )


@dataclass(frozen=True, slots=True)
class ScientificPolicySpec:
    rule_scope: Literal["none", "decimal", "integer", "all"] = "decimal"
    thr_min: float = 0.0001
    thr_max: float = 1_000_000_000_000.0
    height_body_inferred_max: int | None = 20_000


@dataclass(frozen=True, slots=True)
class AutofitCellsPolicySpec:
    rule_columns: Literal["none", "header", "body", "all"] = "header"
    height_body_inferred_max: int | None = 20_000
    width_cell_min: int = 8
    width_cell_max: int = 60
    width_cell_padding: int = 2


# #endregion
################################################################################
# #region SheetFormatSpecification
@dataclass(frozen=True, slots=True)
class SheetSliceSpec:
    sheet_name: str
    row_start_inclusive: int
    row_end_exclusive: int  # exclusive in source df rows
    col_start_inclusive: int
    col_end_exclusive: int  # exclusive in source df cols


@dataclass(frozen=True, slots=True)
class SheetHorizontalMergeSpec:
    row_idx_start: int
    col_idx_start: int
    col_idx_end: int  # inclusive
    text: str


# #endregion
################################################################################
# #region ReportSpecification
@dataclass(slots=True)
class XlsxReport:
    sheets: list[SheetSliceSpec]
    warnings: list[str]

    def warn(self, msg: str) -> None:
        self.warnings.append(str(msg))


# #endregion
################################################################################
