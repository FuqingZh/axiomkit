from functools import lru_cache

from .actions import NumericRangeAction, PathAction
from .params_defs import build_param_registry
from .present import CliHeadings
from .registry import (
    CommandRegistry,
    CommandSpec,
    GroupKey,
    GroupView,
    ParamKey,
    ParamRegistry,
    ParserRegistry,
    SmartFormatter,
)


@lru_cache(maxsize=1)
def default_param_registry() -> ParamRegistry:
    """Get the default parameter registry."""
    cls_registry = ParamRegistry()
    build_param_registry(cls_registry)
    return cls_registry


__all__ = [
    "CliHeadings",
    "CommandRegistry",
    "CommandSpec",
    "GroupKey",
    "GroupView",
    "NumericRangeAction",
    "ParamKey",
    "ParamRegistry",
    "ParserRegistry",
    "PathAction",
    "SmartFormatter",
]
