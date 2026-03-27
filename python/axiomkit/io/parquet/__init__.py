from __future__ import annotations

from typing import TYPE_CHECKING, Any

from axiomkit._optional_deps import import_optional_attr

__all__ = ["sink_parquet_dataset"]

if TYPE_CHECKING:
    from .writer import sink_parquet_dataset


def __getattr__(name: str) -> Any:
    if name == "sink_parquet_dataset":
        return import_optional_attr(
            module_name=".writer",
            attr_name=name,
            package=__name__,
            feature="axiomkit.io.parquet",
            extras=("parquet",),
            required_modules=("polars",),
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
