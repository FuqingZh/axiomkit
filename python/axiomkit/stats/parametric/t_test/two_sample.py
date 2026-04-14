from collections.abc import Sequence

import numpy as np
import polars as pl

from ...p_value import (
    PValueAdjustmentType,
    calculate_p_adjustment_array,
    normalize_p_value_adjustment_mode,
)
from ..constant import (
    COL_FEATURE_INTERNAL,
    COL_FEATURE_ORDER,
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
    COL_VAR_REF,
    COL_VAR_TEST,
    COLS_STATS_TWO_SAMPLE_NUMERIC,
    SCHEMA_T_TEST_TWO_SAMPLE_RESULT,
)
from .spec import (
    AlternativeHypothesisType,
    ContrastPlan,
    ContrastSpec,
    TStatisticsResult,
)
from .util import (
    calculate_p_values,
    create_t_stat_columns,
    normalize_alternative_hypothesis_mode,
)


def validate_column_layout_two_sample(
    col_value: str,
    col_group: str,
    col_feature: str | None,
) -> None:
    if col_value == col_group:
        raise ValueError("Args `col_value` and `col_group` must be different.")
    if col_feature is not None and col_feature in {col_value, col_group}:
        raise ValueError(
            "Arg `col_feature` must be different from `col_value` and `col_group`."
        )


def calculate_two_sample_test_statistics(
    *,
    mean_test: np.ndarray,
    mean_ref: np.ndarray,
    var_test: np.ndarray,
    var_ref: np.ndarray,
    n_test: np.ndarray,
    n_ref: np.ndarray,
    should_assume_equal_variance: bool,
) -> TStatisticsResult:
    mean_diff = mean_test - mean_ref
    t_statistic = np.full_like(mean_diff, np.nan, dtype=np.float64)
    df_value = np.full_like(mean_diff, np.nan, dtype=np.float64)

    mask_valid = (
        (n_test >= 2.0)
        & (n_ref >= 2.0)
        & np.isfinite(mean_test)
        & np.isfinite(mean_ref)
        & np.isfinite(var_test)
        & np.isfinite(var_ref)
    )
    if not np.any(mask_valid):
        return TStatisticsResult(
            mean_diff=mean_diff,
            t_statistic=t_statistic,
            degrees_freedom=df_value,
        )

    mean_test_valid = mean_test[mask_valid]
    mean_ref_valid = mean_ref[mask_valid]
    var_test_valid = var_test[mask_valid]
    var_ref_valid = var_ref[mask_valid]
    n_test_valid = n_test[mask_valid]
    n_ref_valid = n_ref[mask_valid]

    with np.errstate(divide="ignore", invalid="ignore"):
        if should_assume_equal_variance:
            df_valid = n_test_valid + n_ref_valid - 2.0
            var_pooled = (
                ((n_test_valid - 1.0) * var_test_valid)
                + ((n_ref_valid - 1.0) * var_ref_valid)
            ) / df_valid
            denom_valid = np.sqrt(var_pooled * (1.0 / n_test_valid + 1.0 / n_ref_valid))
        else:
            vn_test = var_test_valid / n_test_valid
            vn_ref = var_ref_valid / n_ref_valid
            denom_valid = np.sqrt(vn_test + vn_ref)
            df_valid = (vn_test + vn_ref) ** 2 / (
                (vn_test**2 / (n_test_valid - 1.0)) + (vn_ref**2 / (n_ref_valid - 1.0))
            )

        mask_finite = (
            np.isfinite(denom_valid) & (denom_valid > 0.0) & np.isfinite(df_valid)
        )
        t_valid = np.full_like(denom_valid, np.nan, dtype=np.float64)
        df_valid = np.where(mask_finite, df_valid, np.nan)
        t_valid[mask_finite] = (
            mean_test_valid[mask_finite] - mean_ref_valid[mask_finite]
        ) / denom_valid[mask_finite]

    t_statistic[mask_valid] = t_valid
    df_value[mask_valid] = df_valid
    return TStatisticsResult(
        mean_diff=mean_diff,
        t_statistic=t_statistic,
        degrees_freedom=df_value,
    )


def calculate_t_test_two_sample(
    df: pl.DataFrame | pl.LazyFrame,
    col_value: str = "Value",
    col_group: str = "Group",
    *,
    contrasts: ContrastSpec | Sequence[ContrastSpec],
    col_feature: str | None = None,
    rule_alternative: AlternativeHypothesisType | str = "two-sided",
    should_assume_equal_variance: bool = False,
    rule_p_adjust: PValueAdjustmentType | str | None = None,
) -> pl.DataFrame:
    """
    Calculate tidy two-sample t-tests from a long-format table.

    Two-sample t-tests:
    - Compare the means of two independent groups (test vs ref) for each feature.
    - Can be performed for multiple features if `col_feature` is specified, with optional p-value adjustment for multiple testing.

    Args:
        df: Input data in long format, with one row per observation.
        col_value: Name of the column containing numeric values to compare.
        col_group: Name of the column containing group labels for comparison.
        contrasts: Specification of group contrasts to test, as a :class:`ContrastSpec` or a sequence of :class:`ContrastSpec` items.
        col_feature: Optional name of the column containing feature labels. If None, all rows are treated as a single feature.
        rule_alternative: Alternative hypothesis for the t-test. See :class:`AlternativeHypothesisType`.
            - ``two-sided``: (Default) Test if means are different.
            - ``less``: Test if mean of group_test is less than mean of group_ref.
            - ``greater``: Test if mean of group_test is greater than mean of group_ref.
        should_assume_equal_variance:
            - ``False``: (Default) Use Welch's t-test, which does not assume equal population variances.
            - ``True``: Use Student's t-test, which assumes equal population variances.
        rule_p_adjust: Method for adjusting p-values for multiple testing. See :class:`PValueAdjustmentType`.
            - ``None``: (Default) No adjustment; return raw p-values.
            - ``bonferroni``: Adjust p-values using the Bonferroni correction.
            - ``bh``: Adjust p-values using the Benjamini-Hochberg procedure.
            - ``by``: Adjust p-values using the Benjamini-Yekutieli procedure.

    Raises:
        ValueError:
            If any of the following conditions are met:
            - `col_value` and `col_group` are the same.
            - `col_feature` is the same as `col_value` or `col_group`.
            - `contrasts` is not a `ContrastSpec` or a sequence of `ContrastSpec` items.
            - Any specified contrast has identical `group_test` and `group_ref`.
            - `rule_alternative` is not one of "two-sided", "less", or "greater".
            - `rule_p_adjust` is not a valid p-value adjustment method.
    Returns:
        A Polars DataFrame containing the t-test results for each specified contrast and feature, with the following columns:
            - Column named as `col_feature` (if specified): Feature label for each row.
            - `ContrastId`: Pair of [`group_test`, `group_ref`] for each contrast.
            - `GroupTest`: Name of the test group.
            - `GroupRef`: Name of the reference group.
            - `NGroupTest`: Sample size of the test group.
            - `NGroupRef`: Sample size of the reference group.
            - `MeanGroupTest`: Sample mean of the test group.
            - `MeanGroupRef`: Sample mean of the reference group.
            - `MeanDiff`: Difference in sample means (`MeanGroupTest` - `MeanGroupRef`).
            - `TStatistic`: Calculated t-statistic for the contrast.
            - `DegreesFreedom`: Degrees of freedom used in the t-test.
            - `PValue`: Raw p-value for the contrast.
            - `PAdjust`: Adjusted p-value for the contrast (if `rule_p_adjust` is specified), otherwise same as `PValue`.

    Examples:
        ```python
        import polars as pl
        from axiomkit.stats import ContrastSpec, calculate_t_test_two_sample
        # Example DataFrame
        df = pl.DataFrame({
            "Feature": ["A", "A", "A", "B", "B", "B"],
            "Group": ["X", "X", "Y", "X", "Y", "Y"],
            "Value": [1.2, 1.5, 1.3, 2.1, 2.4, 2.3]
        })
        # Define contrasts
        contrast1 = ContrastSpec(group_test="X", group_ref="Y")
        contrast2 = ContrastSpec(group_test="Y", group_ref="X")
        # Calculate t-tests
        result = calculate_t_test_two_sample(
            df,
            col_value="Value",
            col_group="Group",
            col_feature="Feature",
            contrasts=[contrast1, contrast2],
            rule_alternative="two-sided",
            should_assume_equal_variance=False,
            rule_p_adjust="bonferroni"
        )
        print(result)
        ```
    """
    ############################################################
    # #region Validate input arguments
    validate_column_layout_two_sample(col_value, col_group, col_feature)
    rule_alternative = normalize_alternative_hypothesis_mode(rule_alternative)
    rule_p_adjust = normalize_p_value_adjustment_mode(rule_p_adjust)

    # #endregion
    ############################################################
    # #region Validate input DataFrame schema and normalize input data
    schema_input = read_frame_schema(df)
    cols_required = create_required_columns(col_value, col_group, col_feature)
    validate_required_columns(cols_in=schema_input, cols_required=cols_required)

    schema_result = create_result_schema(
        col_feature=col_feature,
        dtype_feature=schema_input.get(col_feature)
        if col_feature is not None
        else None,
        schema_result=SCHEMA_T_TEST_TWO_SAMPLE_RESULT,
    )
    contrast_plan = ContrastPlan.from_inputs(contrasts)
    if not contrast_plan.group_used:
        return pl.DataFrame(schema=schema_result)
    # #endregion
    ############################################################
    # #region Calculate summary statistics for each group and prepare data for t-test calculations
    lf_values = normalize_value_frame(
        df,
        cols_required,
        cols_float=col_value,
        cols_string=col_group,
        col_feature=col_feature,
    )
    lf_features = create_feature_frame(lf_values)
    lf_summary = lf_values.group_by(
        [COL_FEATURE_INTERNAL, col_group], maintain_order=True
    ).agg(
        *create_summary_stat_columns(col_value),
    )
    lf_contrasts = pl.LazyFrame(
        {
            "ContrastId": list(contrast_plan.contrast_ids),
            "ContrastOrder": list(range(len(contrast_plan.group_test_values))),
            "GroupTest": list(contrast_plan.group_test_values),
            "GroupRef": list(contrast_plan.group_ref_values),
        }
    )
    lf_stats = (
        lf_features.join(lf_contrasts, how="cross")
        .join(
            lf_summary.rename(
                {
                    col_group: "GroupTest",
                    "N": "NGroupTest",
                    "Mean": "MeanGroupTest",
                    "Var": COL_VAR_TEST,
                }
            ),
            on=[COL_FEATURE_INTERNAL, "GroupTest"],
            how="left",
        )
        .join(
            lf_summary.rename(
                {
                    col_group: "GroupRef",
                    "N": "NGroupRef",
                    "Mean": "MeanGroupRef",
                    "Var": COL_VAR_REF,
                }
            ),
            on=[COL_FEATURE_INTERNAL, "GroupRef"],
            how="left",
        )
        .sort([COL_FEATURE_ORDER, "ContrastOrder"])
    )

    df_stats = lf_stats.collect()
    if df_stats.height == 0:
        return pl.DataFrame(schema=schema_result)
    # #endregion
    ############################################################
    # #region Calculate t-test statistics, p-values, and p-value adjustments
    np_stats = (
        df_stats.select(
            # !!! cols order is important
            COLS_STATS_TWO_SAMPLE_NUMERIC
        )
        .fill_null(np.nan)
        .to_numpy()
    )
    t_test_result = calculate_two_sample_test_statistics(
        mean_test=np_stats[:, 0],
        mean_ref=np_stats[:, 1],
        var_test=np_stats[:, 2],
        var_ref=np_stats[:, 3],
        n_test=np_stats[:, 4],
        n_ref=np_stats[:, 5],
        should_assume_equal_variance=should_assume_equal_variance,
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
        *create_t_stat_columns(t_test_result, p_values=p_value, p_adjust=p_adjust)
    )
    df_result = select_result_columns(
        df_result,
        cols_selected=list(SCHEMA_T_TEST_TWO_SAMPLE_RESULT.keys()),
        col_feature=col_feature,
    )
    # #endregion
    ############################################################

    return df_result
