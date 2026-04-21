from typing import TYPE_CHECKING, Any

from axiomkit._optional_deps import import_optional_attr

from .action import ActionCommandPrefix, ActionHexColor, ActionNumericRange, ActionPath
from .spec import GroupKey

__all__ = [
    "ActionCommandPrefix",
    "ActionHexColor",
    "ActionNumericRange",
    "ActionPath",
    "ParserBuilder",
    "GroupKey",
]

if TYPE_CHECKING:
    from .builder import ParserBuilder


def __getattr__(name: str) -> Any:
    if name in {"ParserBuilder"}:
        return import_optional_attr(
            module_name=".builder",
            attr_name=name,
            package=__name__,
            feature="axiomkit.cli.parser",
            extras=("cli",),
            required_modules=("rich_argparse", "rich"),
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
