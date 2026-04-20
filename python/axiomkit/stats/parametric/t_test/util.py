import numpy as np
import polars as pl
from scipy import stats as sci_stats

from .spec import (
    AlternativeHypothesisMode,
    AlternativeHypothesisType,
    TStatisticsResult,
)


def normalize_alternative_hypothesis_mode(
    value: AlternativeHypothesisMode | AlternativeHypothesisType | str,
) -> AlternativeHypothesisMode:
    """Validate and normalize an alternative hypothesis mode."""
    if isinstance(value, AlternativeHypothesisMode):
        return value
    try:
        return AlternativeHypothesisMode(value)
    except ValueError as e:
        raise ValueError(
            f"Invalid alternative hypothesis mode: `{value}`. "
            f"Expected one of: {[s.value for s in AlternativeHypothesisMode]}"
        ) from e


def calculate_p_values(
    *,
    t_statistic: np.ndarray,
    degrees_of_freedom: np.ndarray,
    rule_alternative: AlternativeHypothesisMode,
) -> np.ndarray:
    p_value = np.full_like(t_statistic, np.nan, dtype=np.float64)
    mask_valid = np.isfinite(degrees_of_freedom) & np.isfinite(t_statistic)
    if not np.any(mask_valid):
        return p_value

    t_valid = t_statistic[mask_valid]
    d_free_valid = degrees_of_freedom[mask_valid]
    match rule_alternative:
        case AlternativeHypothesisMode.TWO_SIDED:
            p_valid = 2.0 * sci_stats.t.sf(np.abs(t_valid), d_free_valid)
        case AlternativeHypothesisMode.GREATER:
            p_valid = sci_stats.t.sf(t_valid, d_free_valid)
        case AlternativeHypothesisMode.LESS:
            p_valid = sci_stats.t.cdf(t_valid, d_free_valid)

    p_value[mask_valid] = p_valid
    return p_value


def create_t_stat_columns(
    t_statistic_result: TStatisticsResult,
    *,
    p_values: np.ndarray,
    p_adjust: np.ndarray,
):
    return [
        pl.Series(
            name="MeanDiff", values=t_statistic_result.mean_diff, dtype=pl.Float64
        ),
        pl.Series(
            name="TStatistic", values=t_statistic_result.t_statistic, dtype=pl.Float64
        ),
        pl.Series(
            name="DegreesFreedom",
            values=t_statistic_result.degrees_freedom,
            dtype=pl.Float64,
        ),
        pl.Series(name="PValue", values=p_values, dtype=pl.Float64),
        pl.Series(name="PAdjust", values=p_adjust, dtype=pl.Float64),
    ]
