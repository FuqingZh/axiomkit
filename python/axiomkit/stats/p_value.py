from enum import StrEnum
from typing import Literal

import numpy as np
import scipy.stats as sci_stats


class PValueAdjustmentMode(StrEnum):
    BONFERRONI = "bonferroni"
    BENJAMINI_HOCHBERG = "bh"
    BENJAMINI_YEKUTIELI = "by"


PValueAdjustmentType = Literal[
    "bonferroni",
    "bh",
    "by",
]


def normalize_p_value_adjustment_mode(
    value: PValueAdjustmentMode | PValueAdjustmentType | str | None,
) -> PValueAdjustmentMode | None:
    """Validate and normalize a p-value adjustment mode."""
    if value is None:
        return None
    if isinstance(value, PValueAdjustmentMode):
        return value
    try:
        return PValueAdjustmentMode(value)
    except ValueError as e:
        raise ValueError(
            f"Invalid p-value adjustment mode: `{value}`. "
            f"Expected one of: {[s.value for s in PValueAdjustmentMode]}"
        ) from e


def calculate_p_adjustment_array(
    p_values: np.ndarray,
    *,
    rule_p_adjust: PValueAdjustmentMode | PValueAdjustmentType | str | None = None,
) -> np.ndarray:
    if rule_p_adjust is None:
        return p_values.copy()
    rule_p_adjust = normalize_p_value_adjustment_mode(rule_p_adjust)

    p_adjust = np.full_like(p_values, np.nan, dtype=np.float64)
    mask_valid = np.isfinite(p_values)
    p_valid = p_values[mask_valid]
    if p_valid.size == 0:
        return p_adjust

    match rule_p_adjust:
        case PValueAdjustmentMode.BONFERRONI:
            p_adjust_valid = np.minimum(p_valid * p_valid.size, 1.0)
        case PValueAdjustmentMode.BENJAMINI_HOCHBERG:
            p_adjust_valid = sci_stats.false_discovery_control(p_valid, method="bh")
        case PValueAdjustmentMode.BENJAMINI_YEKUTIELI:
            p_adjust_valid = sci_stats.false_discovery_control(p_valid, method="by")
    p_adjust[mask_valid] = p_adjust_valid

    return p_adjust
