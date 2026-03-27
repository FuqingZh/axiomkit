from __future__ import annotations

from typing import TYPE_CHECKING, Any

from axiomkit._optional_deps import import_optional_attr

__all__ = [
    "XlsxWriter",
    "CellFormatSpec",
    "AutofitCellsPolicySpec",
    "ScientificPolicySpec",
]

if TYPE_CHECKING:
    from .spec import AutofitCellsPolicySpec, CellFormatSpec, ScientificPolicySpec
    from .writer import XlsxWriter


def __getattr__(name: str) -> Any:
    if name in {"CellFormatSpec", "AutofitCellsPolicySpec", "ScientificPolicySpec"}:
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
            required_modules=("polars",),
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
