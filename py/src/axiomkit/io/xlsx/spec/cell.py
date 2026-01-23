# "Facts/Results/Plans" generated from processing DataFrame to Excel XLSX files.

from dataclasses import dataclass, replace
from typing import Any


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
