from __future__ import annotations

from typing import TYPE_CHECKING, Any

from axiomkit._optional_deps import import_optional_attr

__all__ = ["calculate_ora"]

if TYPE_CHECKING:
    from .ora import calculate_ora


def __getattr__(name: str) -> Any:
    if name == "calculate_ora":
        return import_optional_attr(
            module_name=".ora",
            attr_name=name,
            package=__name__,
            feature="axiomkit.stats",
            extras=("stats",),
            required_modules=("numpy", "scipy", "polars"),
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
