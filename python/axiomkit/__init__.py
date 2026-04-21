from __future__ import annotations

from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from types import ModuleType
from typing import TYPE_CHECKING, Any
import warnings

__all__ = [
    "__version__",
    "cli",
    "io",
    "runner",
    "stats",
    "workspace",
]

try:
    __version__ = version("axiomkit")
except PackageNotFoundError:
    __version__ = "0.0.0"

if TYPE_CHECKING:
    import axiomkit.cli as cli
    import axiomkit.io as io
    import axiomkit.runner as runner
    import axiomkit.stats as stats
    import axiomkit.workspace as workspace

_ALIAS_MODULES: dict[str, str] = {
    "cli": "axiomkit.cli",
    "io": "axiomkit.io",
    "runner": "axiomkit.runner",
    "stats": "axiomkit.stats",
    "workspace": "axiomkit.workspace",
}

_DEPRECATED_ALIAS_MODULES: dict[str, str] = {
    "cli_parser": "axiomkit.cli.parser",
    "cli_console": "axiomkit.cli.console",
    "io_xlsx": "axiomkit.io.xlsx",
    "io_fs": "axiomkit.io.fs",
    "io_fasta": "axiomkit.io.fasta",
    "io_parquet": "axiomkit.io.parquet",
}


def __getattr__(name: str) -> Any:
    module_name = _ALIAS_MODULES.get(name)
    if module_name is not None:
        module_loaded: ModuleType = import_module(module_name)
        globals()[name] = module_loaded
        return module_loaded

    module_name = _DEPRECATED_ALIAS_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    warnings.warn(
        f"`axiomkit.{name}` is deprecated; import `{module_name}` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    module_loaded = import_module(module_name)
    globals()[name] = module_loaded
    return module_loaded


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
