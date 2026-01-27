import argparse
import keyword
import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .base import ArgAdder
from .enum import EnumGroupKey, EnumScope

_RE_DEST = re.compile(r"[^0-9A-Za-z_]+")


def _infer_dest_from_id(base_id: str) -> str:
    c_base = base_id.replace("-", "_")
    c_base = _RE_DEST.sub("_", c_base).strip("_")
    if not c_base:
        raise ValueError(f"Cannot infer dest from id: {base_id!r}")
    if keyword.iskeyword(c_base):
        c_base = f"{c_base}_"
    return c_base


@dataclass(frozen=True, slots=True)
class SpecParam:
    id: str
    dest: str | None = None  # canonical runtime field name
    flags: tuple[str, ...] | None = None  # e.g. ("--thr_pval",)
    help: str | None = None
    group: EnumGroupKey = EnumGroupKey.GENERAL  # e.g. "thresholds" / "plot" / "rules"
    scope: EnumScope = EnumScope.INTERNAL
    order: int = 0
    aliases: tuple[str, ...] = ()
    if_deprecated: bool = False
    replace_by: str | None = None

    # single source of truth: how to add this argument
    arg_builder: Callable[[ArgAdder, "SpecParam"], None] | None = None

    @property
    def base_id(self) -> str:
        return self.id.split(".")[-1]

    @property
    def resolved_dest(self) -> str:
        return self.dest or _infer_dest_from_id(self.base_id)

    @property
    def resolved_flags(self) -> tuple[str, ...]:
        if self.flags:
            return self.flags
        return (f"--{self.base_id}",)

    def add_argument(self, g: ArgAdder, /, **kwargs: Any) -> Any:
        kwargs.setdefault("dest", self.resolved_dest)
        if self.help is not None:
            kwargs.setdefault("help", self.help)
        return g.add_argument(*self.resolved_flags, **kwargs)


@dataclass(frozen=True, slots=True)
class SpecCommand:
    """
    Immutable specification for a CLI command registered in ``RegistryCommand``.

    Each instance describes a single command, including:

    - ``id``: canonical command identifier used for registration and lookup.
    - ``help``: short help string shown in command listings.
    - ``arg_builder``: callback that configures an ``argparse.ArgumentParser``
      with this command's arguments and options.
    - ``entry``: optional entry point (such as a module path or script path)
      associated with the command.
    - ``group``: logical group name used to organize commands in the registry.
    - ``order``: numeric sort key controlling display order within a group.
    - ``aliases``: additional names that may be resolved to the canonical ``id``.
    """

    id: str
    help: str
    arg_builder: Callable[[argparse.ArgumentParser], argparse.ArgumentParser | None]
    entry: str | Path | None = None
    group: str = "default"
    order: int = 0
    aliases: tuple[str, ...] = ()
    param_keys: tuple[str, ...] = ()
