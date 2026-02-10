from __future__ import annotations

from typing import TYPE_CHECKING, Any

from axiomkit._optional_deps import import_optional_attr

__all__ = ["CliHeadings"]

if TYPE_CHECKING:
    from .cli_headings import CliHeadings


def __getattr__(name: str) -> Any:
    if name == "CliHeadings":
        return import_optional_attr(
            module_name=".cli_headings",
            attr_name=name,
            package=__name__,
            feature="axiomkit.cli.console",
            extras=("cli",),
            required_modules=("rich",),
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
