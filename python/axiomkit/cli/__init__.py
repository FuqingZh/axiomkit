from __future__ import annotations

from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING, Any
import warnings

from axiomkit._optional_deps import import_optional_attr

__all__ = ["console", "parser"]

if TYPE_CHECKING:
    import axiomkit.cli.console as console
    import axiomkit.cli.parser as parser

_ALIAS_MODULES: dict[str, str] = {
    "console": "axiomkit.cli.console",
    "parser": "axiomkit.cli.parser",
}

_DEPRECATED_ATTRS: dict[str, tuple[str, str]] = {
    "CliHeadings": ("axiomkit.cli.console", "CliHeadings"),
    "ActionCommandPrefix": ("axiomkit.cli.parser", "ActionCommandPrefix"),
    "ActionHexColor": ("axiomkit.cli.parser", "ActionHexColor"),
    "ActionNumericRange": ("axiomkit.cli.parser", "ActionNumericRange"),
    "ActionPath": ("axiomkit.cli.parser", "ActionPath"),
    "GroupKey": ("axiomkit.cli.parser", "GroupKey"),
    "ParserBuilder": ("axiomkit.cli.parser", "ParserBuilder"),
}


def __getattr__(name: str) -> Any:
    module_name = _ALIAS_MODULES.get(name)
    if module_name is not None:
        module_loaded: ModuleType = import_module(module_name)
        globals()[name] = module_loaded
        return module_loaded

    deprecated_target = _DEPRECATED_ATTRS.get(name)
    if deprecated_target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attr_name = deprecated_target
    warnings.warn(
        f"`axiomkit.cli.{name}` is deprecated; import `{attr_name}` from `{module_name}` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    value = import_optional_attr(
        module_name=module_name.removeprefix("axiomkit.cli"),
        attr_name=attr_name,
        package=__name__,
        feature=module_name,
        extras=("cli",),
        required_modules=("rich_argparse", "rich"),
    )
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
