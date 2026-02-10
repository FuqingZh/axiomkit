from __future__ import annotations

from typing import TYPE_CHECKING, Any

from axiomkit._optional_deps import import_optional_attr

__all__ = ["SpecFastaHeader", "read_fasta", "calculate_mw_kda"]

if TYPE_CHECKING:
    from .reader import SpecFastaHeader, calculate_mw_kda, read_fasta


def __getattr__(name: str) -> Any:
    if name in __all__:
        return import_optional_attr(
            module_name=".reader",
            attr_name=name,
            package=__name__,
            feature="axiomkit.io.fasta",
            extras=("fasta",),
            required_modules=("Bio", "pyteomics", "polars"),
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
