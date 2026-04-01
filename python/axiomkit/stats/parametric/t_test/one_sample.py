import numpy as np
import polars as pl

from ...p_value import (
    PValueAdjustmentMode,
    calculate_p_adjustment_array,
    normalize_p_value_adjustment_mode,
)
from ..constant import (
    COL_FEATURE_INTERNAL,
    COL_FEATURE_ORDER,
    COLS_SUMMARY_STATS,
)
from ..util import (
    create_feature_frame,
    create_required_columns,
    create_result_schema,
    create_summary_stat_columns,
    normalize_value_frame,
    read_frame_schema,
    select_result_columns,
    validate_required_columns,
)
from .constant import (
    SCHEMA_T_TEST_ONE_SAMPLE_RESULT,
)
from .spec import (
    AlternativeHypothesisMode,
    TStatisticsResult,
)
from .util import (
    calculate_p_values,
    create_t_stat_columns,
    normalize_alternative_hypothesis_mode,
)


def validate_column_layout_one_sample(
    col_value: str,
    col_feature: str | None,
) -> None:
    if col_feature is not None and col_feature == col_value:
        raise ValueError("Arg `col_feature` must be different from `col_value`.")


def calculate_one_sample_test_statistics(
    *,
    mean_value: np.ndarray,
    var_value: np.ndarray,
    n_value: np.ndarray,
    popmean: float,
) -> TStatisticsResult:
    mean_diff = mean_value - popmean
    t_statistic = np.full_like(mean_diff, np.nan, dtype=np.float64)
    df_value = np.full_like(mean_diff, np.nan, dtype=np.float64)

    mask_valid = (n_value >= 2.0) & np.isfinite(mean_value) & np.isfinite(var_value)
    if not np.any(mask_valid):
        return TStatisticsResult(
            mean_diff=mean_diff,
            t_statistic=t_statistic,
            degrees_freedom=df_value,
        )

    mean_value_valid = mean_value[mask_valid]
    var_value_valid = var_value[mask_valid]
    n_value_valid = n_value[mask_valid]

    with np.errstate(divide="ignore", invalid="ignore"):
        df_valid = n_value_valid - 1.0
        denom_valid = np.sqrt(var_value_valid / n_value_valid)
        mask_finite = (
            np.isfinite(denom_valid) & (denom_valid > 0.0) & np.isfinite(df_valid)
        )
        t_valid = np.full_like(denom_valid, np.nan, dtype=np.float64)
        df_valid = np.where(mask_finite, df_valid, np.nan)
        t_valid[mask_finite] = (mean_value_valid[mask_finite] - popmean) / denom_valid[
            mask_finite
        ]

    t_statistic[mask_valid] = t_valid
    df_value[mask_valid] = df_valid
    return TStatisticsResult(
        mean_diff=mean_diff,
        t_statistic=t_statistic,
        degrees_freedom=df_value,
    )


def calculate_t_test_one_sample(
    df: pl.DataFrame | pl.LazyFrame,
    col_value: str = "Value",
    *,
    popmean: float,
    col_feature: str | None = None,
    rule_alternative: AlternativeHypothesisMode | str = "two-sided",
    rule_p_adjust: PValueAdjustmentMode | str | None = None,
) -> pl.DataFrame:
    """
    Calculate tidy one-sample t-tests from a long-format table.

    One-sample t-tests:
    - Compare the mean of a single group (feature) against a specified population mean (`popmean`).
    - Can be performed for multiple features if `col_feature` is specified, with optional p-value adjustment for multiple testing.

    Args:
        df: Input data in long format, with one row per observation.
        col_value: Name of the column containing numeric values to compare.
        popmean: Reference population mean used by the one-sample t-test.
        col_feature: Optional name of the column containing feature labels. If None, all rows are treated as a single feature.
        rule_alternative: Alternative hypothesis for the t-test. See :class:`AlternativeHypothesisMode`.
            - ``two-sided``: (Default) Test if mean is different from `popmean`.
            - ``less``: Test if mean is less than `popmean`.
            - ``greater``: Test if mean is greater than `popmean`.
        rule_p_adjust: Method for adjusting p-values for multiple testing. See :class:`PValueAdjustmentMode`.
            - ``None``: (Default) No adjustment; return raw p-values.
            - ``bonferroni``: Adjust p-values using the Bonferroni correction.
            - ``bh``: Adjust p-values using the Benjamini-Hochberg procedure.
            - ``by``: Adjust p-values using the Benjamini-Yekutieli procedure

    Returns:
        A Polars DataFrame containing one-sample t-test results for each feature, with columns:
            - Column named as `col_feature` (if specified): Feature label for each row.
            - `N`: Sample size used in the one-sample t-test.
            - `Mean`: Sample mean.
            - `PopMean`: Reference population mean.
            - `MeanDiff`: Difference between `Mean` and `PopMean`.
            - `TStatistic`: Calculated t-statistic.
            - `DegreesFreedom`: Degrees of freedom used in the t-test.
            - `PValue`: Raw p-value.
            - `PAdjust`: Adjusted p-value if `rule_p_adjust` is specified, otherwise same as `PValue`.

    Examples:
        ```python
        import polars as pl
        from axiomkit.stats import calculate_t_test_one_sample
        # Example 1: One-sample t-test comparing a single feature against a population mean
        df = pl.DataFrame({
            "Value": [5.1, 4.9, 5.0, 5.2, 4.8]
        })
        result = calculate_t_test_one_sample(
            df,
            col_value="Value",
            popmean=5.0,
            rule_alternative="two-sided",
            rule_p_adjust=None,
        )
        print(result)
        # Example 2: One-sample t-test comparing multiple features against a population mean
        df = pl.DataFrame({
            "Feature": ["A", "A", "A", "B", "B", "B"],
            "Value": [5.1, 4.9, 5.0, 5.2, 4.8, 5.3]
        })
        result = calculate_t_test_one_sample(
            df,
            col_value="Value",
            popmean=5.0,
            col_feature="Feature",
            rule_alternative="greater",
            rule_p_adjust="bh",
        )
        print(result)
        ```
    """
    ############################################################
    # #region Validate input arguments
    validate_column_layout_one_sample(col_value, col_feature)
    rule_alternative = normalize_alternative_hypothesis_mode(rule_alternative)
    rule_p_adjust = (
        normalize_p_value_adjustment_mode(rule_p_adjust)
        if rule_p_adjust is not None
        else None
    )

    try:
        popmean = float(popmean)
    except (TypeError, ValueError) as e:
        raise ValueError(
            f"Arg `popmean` must be castable to float, yours: {popmean!r}."
        ) from e
    if not np.isfinite(popmean):
        raise ValueError(f"Arg `popmean` must be finite, yours: {popmean!r}.")
    # #endregion
    ############################################################
    # #region Validate input DataFrame schema and normalize input data
    schema_input = read_frame_schema(df)
    cols_required = create_required_columns(col_value, col_feature)
    validate_required_columns(cols_in=schema_input, cols_required=cols_required)
    schema_result = create_result_schema(
        col_feature=col_feature,
        dtype_feature=schema_input.get(col_feature)
        if col_feature is not None
        else None,
        schema_result=SCHEMA_T_TEST_ONE_SAMPLE_RESULT,
    )
    # #endregion
    ############################################################
    # #region Calculate summary statistics for each feature and prepare data for t-test calculations
    lf_values = normalize_value_frame(
        df, cols_required, cols_float=col_value, col_feature=col_feature
    )
    df_stats = (
        lf_values.group_by(COL_FEATURE_INTERNAL, maintain_order=True)
        .agg(
            *create_summary_stat_columns(col_value),
        )
        .join(
            create_feature_frame(lf_values),
            on=COL_FEATURE_INTERNAL,
            how="left",
        )
        .sort(COL_FEATURE_ORDER)
        .collect()
    )
    if df_stats.height == 0:
        return pl.DataFrame(schema=schema_result)
    # #endregion
    ############################################################
    # #region Calculate t-test statistics, p-values, and p-value adjustments
    np_stats = df_stats.select(COLS_SUMMARY_STATS).fill_null(np.nan).to_numpy()
    t_test_result = calculate_one_sample_test_statistics(
        mean_value=np_stats[:, 0],
        var_value=np_stats[:, 1],
        n_value=np_stats[:, 2],
        popmean=popmean,
    )
    p_value = calculate_p_values(
        t_statistic=t_test_result.t_statistic,
        degrees_of_freedom=t_test_result.degrees_freedom,
        rule_alternative=rule_alternative,
    )
    p_adjust = calculate_p_adjustment_array(p_value, rule_p_adjust=rule_p_adjust)
    # #endregion
    ############################################################
    # #region Finalize result DataFrame
    df_result = df_stats.with_columns(
        pl.Series(
            name="PopMean", values=np.full(df_stats.height, popmean), dtype=pl.Float64
        ),
        *create_t_stat_columns(t_test_result, p_values=p_value, p_adjust=p_adjust),
    )

    df_result = select_result_columns(
        df_result,
        cols_selected=list(SCHEMA_T_TEST_ONE_SAMPLE_RESULT.keys()),
        col_feature=col_feature,
    )
    # #endregion
    ############################################################

    return df_result
