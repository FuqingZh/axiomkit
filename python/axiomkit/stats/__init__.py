from typing import TYPE_CHECKING, Any

from axiomkit._optional_deps import import_optional_attr

__all__ = [
    "ContrastSpec",
    "calculate_ora",
    "calculate_t_test_one_sample",
    "calculate_t_test_paired",
    "calculate_t_test_two_sample",
]

if TYPE_CHECKING:
    from .ora import calculate_ora
    from .t_test import (
        ContrastSpec,
        calculate_t_test_one_sample,
        calculate_t_test_paired,
        calculate_t_test_two_sample,
    )


def __getattr__(name: str) -> Any:
    if name == "calculate_ora":
        return import_optional_attr(
            module_name=".ora",
            attr_name=name,
            package=__name__,
            feature="axiomkit.stats",
            extras=("stats",),
            required_modules=("numpy", "scipy", "polars"),
        )
    if name == "ContrastSpec":
        return import_optional_attr(
            module_name=".t_test",
            attr_name=name,
            package=__name__,
            feature="axiomkit.stats",
            extras=("stats",),
            required_modules=("numpy", "scipy", "polars"),
        )
    if name == "calculate_t_test_one_sample":
        return import_optional_attr(
            module_name=".t_test",
            attr_name=name,
            package=__name__,
            feature="axiomkit.stats",
            extras=("stats",),
            required_modules=("numpy", "scipy", "polars"),
        )
    if name == "calculate_t_test_paired":
        return import_optional_attr(
            module_name=".t_test",
            attr_name=name,
            package=__name__,
            feature="axiomkit.stats",
            extras=("stats",),
            required_modules=("numpy", "scipy", "polars"),
        )
    if name == "calculate_t_test_two_sample":
        return import_optional_attr(
            module_name=".t_test",
            attr_name=name,
            package=__name__,
            feature="axiomkit.stats",
            extras=("stats",),
            required_modules=("numpy", "scipy", "polars"),
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
