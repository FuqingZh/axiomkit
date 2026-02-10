from __future__ import annotations

from typing import TYPE_CHECKING, Any

from axiomkit._optional_deps import import_optional_attr
from .fs import copy_tree

__all__ = [
    "copy_tree",
    "sink_parquet_dataset",
    "SpecCellFormat",
    "XlsxWriter",
    "SpecFastaHeader",
    "read_fasta",
]

if TYPE_CHECKING:
    from .fasta import SpecFastaHeader, read_fasta
    from .parquet import sink_parquet_dataset
    from .xlsx import SpecCellFormat, XlsxWriter


_OPTIONAL_EXPORTS: dict[str, tuple[str, str, tuple[str, ...], tuple[str, ...]]] = {
    "sink_parquet_dataset": (
        ".parquet",
        "axiomkit.io.parquet",
        ("parquet",),
        ("polars",),
    ),
    "SpecCellFormat": (
        ".xlsx",
        "axiomkit.io.xlsx",
        ("xlsx",),
        ("polars", "xlsxwriter"),
    ),
    "XlsxWriter": (
        ".xlsx",
        "axiomkit.io.xlsx",
        ("xlsx",),
        ("polars", "xlsxwriter"),
    ),
    "SpecFastaHeader": (
        ".fasta",
        "axiomkit.io.fasta",
        ("fasta",),
        ("Bio", "pyteomics", "polars"),
    ),
    "read_fasta": (
        ".fasta",
        "axiomkit.io.fasta",
        ("fasta",),
        ("Bio", "pyteomics", "polars"),
    ),
}


def __getattr__(name: str) -> Any:
    if name in _OPTIONAL_EXPORTS:
        module_name, feature, extras, required = _OPTIONAL_EXPORTS[name]
        return import_optional_attr(
            module_name=module_name,
            attr_name=name,
            package=__name__,
            feature=feature,
            extras=extras,
            required_modules=required,
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
