from typing import TYPE_CHECKING, Any

from axiomkit._optional_deps import import_optional_attr

__all__ = [
    "ContrastSpec",
    "calculate_anova_one_way",
    "calculate_anova_one_way_welch",
    "calculate_anova_two_way",
    "calculate_ora",
    "calculate_t_test_one_sample",
    "calculate_t_test_paired",
    "calculate_t_test_two_sample",
    "calculate_adjusted_p_values",
]

if TYPE_CHECKING:
    from .ora import calculate_ora
    from .p_value import calculate_adjusted_p_values
    from .parametric import (
        ContrastSpec,
        calculate_anova_one_way,
        calculate_anova_one_way_welch,
        calculate_anova_two_way,
        calculate_t_test_one_sample,
        calculate_t_test_paired,
        calculate_t_test_two_sample,
    )


def __getattr__(name: str) -> Any:
    if name == "calculate_anova_one_way":
        return import_optional_attr(
            module_name=".parametric",
            attr_name=name,
            package=__name__,
            feature="axiomkit.stats",
            extras=("stats",),
            required_modules=("numpy", "scipy", "polars"),
        )
    if name == "calculate_anova_one_way_welch":
        return import_optional_attr(
            module_name=".parametric",
            attr_name=name,
            package=__name__,
            feature="axiomkit.stats",
            extras=("stats",),
            required_modules=("numpy", "scipy", "polars"),
        )
    if name == "calculate_anova_two_way":
        return import_optional_attr(
            module_name=".parametric",
            attr_name=name,
            package=__name__,
            feature="axiomkit.stats",
            extras=("stats",),
            required_modules=("numpy", "scipy", "polars"),
        )
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
            module_name=".parametric",
            attr_name=name,
            package=__name__,
            feature="axiomkit.stats",
            extras=("stats",),
            required_modules=("numpy", "scipy", "polars"),
        )
    if name == "calculate_t_test_one_sample":
        return import_optional_attr(
            module_name=".parametric",
            attr_name=name,
            package=__name__,
            feature="axiomkit.stats",
            extras=("stats",),
            required_modules=("numpy", "scipy", "polars"),
        )
    if name == "calculate_t_test_paired":
        return import_optional_attr(
            module_name=".parametric",
            attr_name=name,
            package=__name__,
            feature="axiomkit.stats",
            extras=("stats",),
            required_modules=("numpy", "scipy", "polars"),
        )
    if name == "calculate_t_test_two_sample":
        return import_optional_attr(
            module_name=".parametric",
            attr_name=name,
            package=__name__,
            feature="axiomkit.stats",
            extras=("stats",),
            required_modules=("numpy", "scipy", "polars"),
        )
    if name == "calculate_adjusted_p_values":
        return import_optional_attr(
            module_name=".p_value",
            attr_name="calculate_adjusted_p_values",
            package=__name__,
            feature="axiomkit.stats",
            extras=("stats",),
            required_modules=("numpy", "scipy"),
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
