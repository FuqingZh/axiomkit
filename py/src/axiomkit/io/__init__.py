from __future__ import annotations

from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING, Any

__all__ = ["fs", "xlsx", "fasta", "parquet"]

if TYPE_CHECKING:
    import axiomkit.io.fasta as fasta
    import axiomkit.io.fs as fs
    import axiomkit.io.parquet as parquet
    import axiomkit.io.xlsx as xlsx

_ALIAS_MODULES: dict[str, str] = {
    "fs": "axiomkit.io.fs",
    "xlsx": "axiomkit.io.xlsx",
    "fasta": "axiomkit.io.fasta",
    "parquet": "axiomkit.io.parquet",
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
