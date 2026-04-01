import numpy as np
import polars as pl
import scipy.stats as sci_stats

from axiomkit.stats.parametric.anova.spec import OneWayStatisticalResult


def calculate_f_test_p_values(
    f_statistic: np.ndarray,
    *,
    degrees_freedom_effect: np.ndarray,
    degrees_freedom_within: np.ndarray,
) -> np.ndarray:
    """
    Calculate p-values for F-tests.

    F-tests are commonly used in ANOVA to test the null hypothesis that group means are equal.
    The F-statistic is calculated as the ratio of the variance between groups to the variance within groups.
    The p-value is then calculated based on the F-distribution with the appropriate degrees of freedom.

    Args:
        f_statistic (np.ndarray): Array of F-statistics.
        degrees_freedom_effect (np.ndarray): Array of degrees of freedom for the effect.
        degrees_freedom_within (np.ndarray): Array of degrees of freedom within groups.

    Returns:
        np.ndarray: Array of p-values corresponding to the F-statistics.
    """
    p_value = np.full_like(f_statistic, np.nan, dtype=np.float64)
    mask_valid = (
        np.isfinite(degrees_freedom_effect)
        & np.isfinite(degrees_freedom_within)
        & ~np.isnan(f_statistic)
    )
    if not np.any(mask_valid):
        return p_value

    p_value[mask_valid] = sci_stats.f.sf(
        f_statistic[mask_valid],
        degrees_freedom_effect[mask_valid],
        degrees_freedom_within[mask_valid],
    )
    return p_value


def create_one_way_stats_columns(
    one_way_stats: OneWayStatisticalResult,
    *,
    p_values: np.ndarray,
    p_adjust: np.ndarray,
):
    return [
        pl.Series(
            name="DegreesFreedomBetween",
            values=one_way_stats.degrees_freedom_between,
            dtype=pl.Float64,
        ),
        pl.Series(
            name="DegreesFreedomWithin",
            values=one_way_stats.degrees_freedom_within,
            dtype=pl.Float64,
        ),
        pl.Series(
            name="FStatistic", values=one_way_stats.f_statistic, dtype=pl.Float64
        ),
        pl.Series(name="PValue", values=p_values, dtype=pl.Float64),
        pl.Series(name="PAdjust", values=p_adjust, dtype=pl.Float64),
    ]
