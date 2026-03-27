from __future__ import annotations

from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from types import ModuleType
from typing import TYPE_CHECKING, Any

__all__ = [
    "__version__",
    "io_xlsx",
    "io_fs",
    "io_fasta",
    "io_parquet",
    "cli_parser",
    "cli_console",
]

try:
    __version__ = version("axiomkit")
except PackageNotFoundError:
    __version__ = "0.0.0"

if TYPE_CHECKING:
    import axiomkit.cli.console as cli_console
    import axiomkit.cli.parser as cli_parser
    import axiomkit.io.fasta as io_fasta
    import axiomkit.io.fs as io_fs
    import axiomkit.io.parquet as io_parquet
    import axiomkit.io.xlsx as io_xlsx

_ALIAS_MODULES: dict[str, str] = {
    "cli_parser": "axiomkit.cli.parser",
    "cli_console": "axiomkit.cli.console",
    "io_xlsx": "axiomkit.io.xlsx",
    "io_fs": "axiomkit.io.fs",
    "io_fasta": "axiomkit.io.fasta",
    "io_parquet": "axiomkit.io.parquet",
}


def __getattr__(name: str) -> Any:
    module_name = _ALIAS_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_loaded: ModuleType = import_module(module_name)
    globals()[name] = module_loaded
    return module_loaded


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
