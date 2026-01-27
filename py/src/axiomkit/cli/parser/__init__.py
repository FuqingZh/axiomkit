from functools import lru_cache

from .core import build_param_registry, BuilderParser
from .spec import (
    ActionCommandPrefix,
    ActionHexColor,
    ActionNumericRange,
    ActionPath,
    EnumGroupKey,
    EnumParamKey,
    RegistryCommand,
    RegistryParam,
    SmartFormatter,
    SpecCommand,
    SpecParam,
)


@lru_cache(maxsize=1)
def default_param_registry() -> RegistryParam:
    """Get the default parameter registry."""
    cls_registry = RegistryParam()
    build_param_registry(cls_registry)
    return cls_registry


__all__ = [
    "BuilderParser",
    "SmartFormatter",
    # Actions
    "ActionCommandPrefix",
    "ActionHexColor",
    "ActionNumericRange",
    "ActionPath",
    # Specs
    "SpecCommand",
    "SpecParam",
    # Enums
    "EnumParamKey",
    "EnumGroupKey",
    # Registry
    "RegistryParam",
    "RegistryCommand",
]
