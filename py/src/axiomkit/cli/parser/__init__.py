from functools import lru_cache

from .service import build_param_registry
from .spec import (
    ActionCommandPrefix,
    ActionHexColor,
    ActionNumericRange,
    ActionPath,
    BuilderParser,
    EnumGroupKey,
    EnumParamKey,
    RegistryCommand,
    RegistryParam,
    SmartFormatter,
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
    "SpecParam",
    # Enums
    "EnumParamKey",
    "EnumGroupKey",
    # Registry
    "RegistryParam",
    "RegistryCommand",
]
