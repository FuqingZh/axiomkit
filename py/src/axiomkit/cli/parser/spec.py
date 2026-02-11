"""Specification models for parser construction.

This module defines immutable descriptors used by parser registries:

- ``SpecParam``: reusable argument spec.
- ``SpecCommand``: subcommand spec.

These specs are pure data + callbacks. They do not mutate parser state by
themselves; mutation happens when registries/materializers apply them.
"""

import argparse
import keyword
import re
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from .base import ArgAdder


class EnumScope(StrEnum):
    """Parameter visibility scope.

    Attributes:
        FRONT: Exposed to front-facing users.
        INTERNAL: Internal-facing parameter.
    """

    FRONT = "front"
    INTERNAL = "internal"


class EnumGroupKey(StrEnum):
    """Logical parser group keys used for help and organization.

    The key controls where a parameter appears in help output and how specs
    are grouped when materialized.
    """

    CONTRACT = "contract"
    EXECUTABLES = "executables"
    INPUTS = "inputs"
    OUTPUTS = "outputs"
    RULES = "rules"
    THRESHOLDS = "thresholds"
    SWITCHES = "switches"
    PLOTS = "plots"
    PERFORMANCE = "performance"
    GENERAL = "general"


DICT_ARG_GROUP_META = {
    EnumGroupKey.CONTRACT: (
        "Contract",
        "Upstream run contract: meta entrypoint, validation, and provenance.",
    ),
    EnumGroupKey.EXECUTABLES: (
        "Executables",
        "Paths to external executables (optional). If omitted, commands are resolved via PATH.",
    ),
    EnumGroupKey.INPUTS: ("Inputs", "Input files and directories."),
    EnumGroupKey.OUTPUTS: ("Outputs", "Output files and directories."),
    EnumGroupKey.RULES: ("Rules", "Filtering and processing rules."),
    EnumGroupKey.THRESHOLDS: ("Thresholds", "Cutoffs and threshold parameters."),
    EnumGroupKey.SWITCHES: ("Switches", "Boolean flags and toggles."),
    EnumGroupKey.PLOTS: ("Plots", "Plotting and graphics settings."),
    EnumGroupKey.PERFORMANCE: (
        "Performance",
        "Parallelism, memory, and performance tuning.",
    ),
    EnumGroupKey.GENERAL: ("General", "General settings and defaults."),
}


_RE_DEST = re.compile(r"[^0-9A-Za-z_]+")


def _infer_dest_from_id(base_id: str) -> str:
    """Infer argparse ``dest`` name from a parameter base id.

    Args:
        base_id: Last segment of parameter id, for example ``"thr_p_value"``.

    Returns:
        str: Normalized destination field name.

    Raises:
        ValueError: If destination cannot be derived.

    Examples:
        >>> _infer_dest_from_id("thr-p.value")
        'thr_p_value'
    """
    c_base = base_id.replace("-", "_")
    c_base = _RE_DEST.sub("_", c_base).strip("_")
    if not c_base:
        raise ValueError(f"Cannot infer dest from id: {base_id!r}")
    if keyword.iskeyword(c_base):
        c_base = f"{c_base}_"
    return c_base


@dataclass(frozen=True, slots=True)
class SpecParam:
    """Immutable parameter specification for parser materialization.

    Attributes:
        id: Canonical parameter identifier.
        dest: Runtime namespace field name. Inferred when omitted.
        flags: CLI option flags. Inferred as ``("--<base_id>",)`` when omitted.
        help: Help message shown in argparse output.
        group: Logical argument group.
        scope: Visibility scope.
        order: Sorting key inside group.
        if_deprecated: Whether this parameter is deprecated.
        replace_by: Suggested replacement id when deprecated.
        arg_builder:
            Callback that writes this parameter into a target ``ArgAdder``.

    Examples:
        >>> spec = SpecParam(id="general.verbose")
        >>> spec.resolved_dest
        'verbose'
        >>> spec.resolved_flags
        ('--verbose',)
        >>> import argparse
        >>> parser = argparse.ArgumentParser(prog="demo")
        >>> _ = spec.add_argument(parser, action="store_true")
        >>> ns = parser.parse_args(["--verbose"])
        >>> ns.verbose
        True
    """

    id: str
    dest: str | None = None
    flags: tuple[str, ...] | None = None
    help: str | None = None
    group: EnumGroupKey = EnumGroupKey.GENERAL
    scope: EnumScope = EnumScope.INTERNAL
    order: int = 0
    if_deprecated: bool = False
    replace_by: str | None = None

    arg_builder: Callable[[ArgAdder, "SpecParam"], None] | None = None

    @property
    def base_id(self) -> str:
        """Return the last token of ``id`` after dot-split."""
        return self.id.split(".")[-1]

    @property
    def resolved_dest(self) -> str:
        """Return effective destination field name."""
        return self.dest or _infer_dest_from_id(self.base_id)

    @property
    def resolved_flags(self) -> tuple[str, ...]:
        """Return effective option flags."""
        if self.flags:
            return self.flags
        return (f"--{self.base_id}",)

    def add_argument(self, g: ArgAdder, /, **kwargs: Any) -> Any:
        """Add this parameter to an argument target.

        Args:
            g: Argument receiver implementing ``add_argument``.
            **kwargs: Extra argparse keyword arguments.

        Returns:
            Any: Created argparse action.
        """
        kwargs.setdefault("dest", self.resolved_dest)
        if self.help is not None:
            kwargs.setdefault("help", self.help)
        return g.add_argument(*self.resolved_flags, **kwargs)


@dataclass(frozen=True, slots=True)
class SpecCommand:
    """Immutable command specification for subparser generation.

    Attributes:
        id: Canonical command id.
        help: Short help message.
        arg_builder: Callback used to add command-specific arguments.
        entry: Optional command entry label/path for downstream dispatch.
        group: Logical command group.
        order: Sorting key inside group.
        param_keys:
            Parameter ids auto-applied during build.
            Supports ``str`` and ``StrEnum``.

    Examples:
        >>> spec = SpecCommand(id="run", help="Run", arg_builder=lambda p: p)
        >>> spec.id
        'run'
        >>> spec.group
        'default'
    """

    id: str
    help: str
    arg_builder: Callable[[argparse.ArgumentParser], argparse.ArgumentParser | None]
    entry: str | Path | None = None
    group: str = "default"
    order: int = 0
    param_keys: tuple[str | StrEnum, ...] = ()
