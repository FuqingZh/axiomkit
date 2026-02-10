from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .parser import EnumGroupKey, SpecParam

__all__ = [
    "CliHeadings",
    "ParserBuilder",
    "SpecParam",
    "EnumGroupKey",
]

if TYPE_CHECKING:
    from .console import CliHeadings
    from .parser import ParserBuilder


def __getattr__(name: str) -> Any:
    if name == "ParserBuilder":
        from .parser import ParserBuilder

        return ParserBuilder
    if name == "CliHeadings":
        from .console import CliHeadings

        return CliHeadings
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
