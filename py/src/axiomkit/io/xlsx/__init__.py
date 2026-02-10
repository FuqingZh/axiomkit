from __future__ import annotations

from typing import TYPE_CHECKING, Any

from axiomkit._optional_deps import import_optional_attr

__all__ = ["XlsxWriter", "SpecCellFormat"]

if TYPE_CHECKING:
    from .spec import SpecCellFormat
    from .writer import XlsxWriter


def __getattr__(name: str) -> Any:
    if name == "SpecCellFormat":
        return import_optional_attr(
            module_name=".spec",
            attr_name=name,
            package=__name__,
            feature="axiomkit.io.xlsx",
            extras=("xlsx",),
            required_modules=(),
        )
    if name == "XlsxWriter":
        return import_optional_attr(
            module_name=".writer",
            attr_name=name,
            package=__name__,
            feature="axiomkit.io.xlsx",
            extras=("xlsx",),
            required_modules=("polars", "xlsxwriter"),
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
