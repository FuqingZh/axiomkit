from collections.abc import Sequence

import numpy as np
import polars as pl
from loguru import logger

from ...p_value import (
    PValueAdjustmentType,
    calculate_p_adjustment_array,
    normalize_p_value_adjustment_mode,
)
from ..comparison import ParametricComparison, ParametricComparisonKind
from ..constant import (
    COL_FEATURE_INTERNAL,
    COL_FEATURE_ORDER,
    COLS_SUMMARY_STATS,
)
from ..spec import ParametricFrameAdapter
from ..util import (
    create_required_columns,
    create_summary_stat_columns,
)
from .constant import (
    SCHEMA_T_TEST_TWO_SAMPLE_RESULT,
)
from .spec import (
    AlternativeHypothesisType,
    ContrastPlan,
    TStatisticsResult,
)
from .util import (
    calculate_p_values,
    create_t_stat_columns,
    normalize_alternative_hypothesis_mode,
)

COL_CONTRAST_COMPARISON_ID = "_ContrastComparisonId"
COL_FEATURE_COMPARISON = "_FeatureComparison"


def validate_column_layout_paired(
    col_value: str,
    col_group: str,
    col_pair: str,
    col_feature: str | None,
    col_comparison: str | None = None,
) -> None:
    cols_used = [col_value, col_group, col_pair]
    if len(set(cols_used)) != len(cols_used):
        raise ValueError(
            "Args `col_value`, `col_group`, and `col_pair` must be different."
        )
    if col_feature is not None and col_feature in {col_value, col_group, col_pair}:
        raise ValueError(
            "Arg `col_feature` must be different from `col_value`, `col_group`, and `col_pair`."
        )
    if col_comparison is None:
        return

    if col_feature is None:
        raise ValueError(
            "Arg `col_feature` is required when `col_comparison` is provided."
        )
    if col_comparison in {col_value, col_group, col_pair, col_feature}:
        raise ValueError(
            "Arg `col_comparison` must be different from `col_value`, "
            "`col_group`, `col_pair`, and `col_feature`."
        )


def calculate_paired_test_statistics(
    *,
    mean_diff: np.ndarray,
    var_diff: np.ndarray,
    n_pair: np.ndarray,
) -> TStatisticsResult:
    t_statistic = np.full_like(mean_diff, np.nan, dtype=np.float64)
    df_value = np.full_like(mean_diff, np.nan, dtype=np.float64)

    mask_valid = (n_pair >= 2.0) & np.isfinite(mean_diff) & np.isfinite(var_diff)
    if not np.any(mask_valid):
        return TStatisticsResult(
            mean_diff=mean_diff,
            t_statistic=t_statistic,
            degrees_freedom=df_value,
        )

    mean_diff_valid = mean_diff[mask_valid]
    var_diff_valid = var_diff[mask_valid]
    n_pair_valid = n_pair[mask_valid]

    with np.errstate(divide="ignore", invalid="ignore"):
        df_valid = n_pair_valid - 1.0
        denom_valid = np.sqrt(var_diff_valid / n_pair_valid)
        mask_finite = (
            np.isfinite(denom_valid) & (denom_valid > 0.0) & np.isfinite(df_valid)
        )
        t_valid = np.full_like(denom_valid, np.nan, dtype=np.float64)
        df_valid = np.where(mask_finite, df_valid, np.nan)
        t_valid[mask_finite] = mean_diff_valid[mask_finite] / denom_valid[mask_finite]

    t_statistic[mask_valid] = t_valid
    df_value[mask_valid] = df_valid
    return TStatisticsResult(
        mean_diff=mean_diff,
        t_statistic=t_statistic,
        degrees_freedom=df_value,
    )


def calculate_t_test_paired(
    df: pl.DataFrame | pl.LazyFrame,
    col_value: str = "Value",
    col_group: str = "Group",
    *,
    col_pair: str,
    col_feature: str | None = None,
    col_comparison: str | None = None,
    comparisons: ParametricComparison | Sequence[ParametricComparison],
    rule_alternative: AlternativeHypothesisType | str = "two-sided",
    rule_p_adjust: PValueAdjustmentType | str | None = None,
) -> pl.DataFrame:
    """
    Calculate tidy paired t-tests from a long-format table.

    Paired t-tests:
    - Compare the mean difference between paired observations of two groups (test vs ref) against zero.
    - Require a column (`col_pair`) that identifies matched pairs of observations across the two groups.
    - Can be performed for multiple features if `col_feature` is specified, with optional p-value adjustment for multiple testing.

    Args:
        df: Input data in long format, with one row per paired observation.
        col_value: Name of the column containing numeric values to compare.
        col_group: Name of the column containing group labels for comparison.
        col_pair: Name of the column identifying matched pairs.
        col_feature: Optional name of the column containing feature labels. If None, all rows are treated as a single feature.
        col_comparison: Optional column that splits data into independent
            analysis layers. Use this for batch-like variables such as
            ``Batch`` when every batch should run the same paired contrast plan.
            If the column already names concrete contrasts, such as proteomics
            labels ``B_vs_A`` and ``C_vs_A``, bind each paired comparison with
            the matching ``comparison_id`` to avoid cross-combining unrelated
            contrasts.
        comparisons: Comparison-plan API using
            :class:`ParametricComparison.ttest_paired`. A comparison with
            ``comparison_id=None`` is unscoped: if ``col_comparison`` is
            provided, the paired contrast is evaluated independently inside
            every comparison layer. A comparison with ``comparison_id="B_vs_A"``
            is scoped and is evaluated only for rows whose ``col_comparison``
            value is ``"B_vs_A"``.
        rule_alternative: Alternative hypothesis for the paired t-test. See :class:`AlternativeHypothesisType`.
            - "two-sided": (Default) Test if the mean difference is not equal to zero.
            - "less": Test if the mean difference is less than zero.
            - "greater": Test if the mean difference is greater than zero.
        rule_p_adjust: Method for adjusting p-values for multiple testing.
            - ``None``: (Default) No adjustment; return raw p-values.
            - "bonferroni": Adjust p-values using the Bonferroni correction.
            - "bh": Adjust p-values using the Benjamini-Hochberg procedure.
            - "by": Adjust p-values using the Benjamini-Yekutieli procedure.

    Returns:
        A Polars DataFrame containing paired t-test results for each specified contrast and feature.
            - Column named as `col_comparison` (if specified): Comparison layer
              for each row.
            - Column named as `col_feature` (if specified): Feature label for each row.
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
        from axiomkit.stats import ParametricComparison, calculate_t_test_paired

        df = pl.DataFrame({
            "Batch": ["batch1", "batch1", "batch1", "batch1"],
            "Feature": ["P1", "P1", "P1", "P1"],
            "PairId": ["s1", "s1", "s2", "s2"],
            "Group": ["A", "B", "A", "B"],
            "Value": [1.0, 2.0, 3.0, 5.0],
        })

        result = calculate_t_test_paired(
            df,
            col_pair="PairId",
            col_feature="Feature",
            col_comparison="Batch",
            comparisons=ParametricComparison.ttest_paired(
                group_test="B",
                group_ref="A",
            ),
            rule_p_adjust="bh",
        )

        scoped = ParametricComparison.ttest_paired(
            group_test="B",
            group_ref="A",
            comparison_id="B_vs_A",
        )
        ```
    """
    ############################################################
    # #region Validate input arguments
    validate_column_layout_paired(
        col_value,
        col_group,
        col_pair,
        col_feature,
        col_comparison=col_comparison,
    )
    rule_alternative = normalize_alternative_hypothesis_mode(rule_alternative)
    rule_p_adjust = normalize_p_value_adjustment_mode(rule_p_adjust)
    # #endregion
    ############################################################
    # #region Validate input DataFrame schema and normalize input data
    pf_adapter = ParametricFrameAdapter(
        df,
        col_feature=col_feature,
        col_comparison=col_comparison,
    ).select_required_cols(
        cols_required=create_required_columns(
            col_value, col_group, col_pair, col_feature, col_comparison
        )
    )
    schema_result = pf_adapter.create_result_schema(SCHEMA_T_TEST_TWO_SAMPLE_RESULT)
    contrast_plan = ContrastPlan.from_inputs(
        comparisons,
        comparison_kind=ParametricComparisonKind.TTEST_PAIRED,
    )
    if not contrast_plan.group_used:
        return pl.DataFrame(schema=schema_result)
    # #endregion
    ############################################################
    # #region Normalize pair data and validate complete pairs
    pf_adapter.cast_cols(
        cols_float=col_value,
        cols_string=[col_group, col_comparison] if col_comparison is not None else col_group,
    ).create_feature_key()
    lf_values = pf_adapter.lf.filter(pl.col(col_group).is_in(contrast_plan.group_used))
    lf_features = pf_adapter.create_feature_frame()
    lf_contrasts = pl.LazyFrame(
        {
            COL_CONTRAST_COMPARISON_ID: list(contrast_plan.comparison_id_values),
            "ContrastOrder": list(range(len(contrast_plan.group_test_values))),
            "GroupTest": list(contrast_plan.group_test_values),
            "GroupRef": list(contrast_plan.group_ref_values),
        }
    )
    if col_comparison is None or not contrast_plan.has_comparison_id:
        lf_contrast_features = lf_features.join(lf_contrasts, how="cross")
    else:
        lf_contrast_features_unscoped = lf_features.join(
            lf_contrasts.filter(pl.col(COL_CONTRAST_COMPARISON_ID).is_null()),
            how="cross",
        )
        lf_contrast_features_scoped = (
            lf_features.with_columns(
                pl.col(COL_FEATURE_INTERNAL)
                .list.get(0)
                .cast(pl.String)
                .alias(COL_FEATURE_COMPARISON)
            )
            .join(
                lf_contrasts.filter(pl.col(COL_CONTRAST_COMPARISON_ID).is_not_null()),
                left_on=COL_FEATURE_COMPARISON,
                right_on=COL_CONTRAST_COMPARISON_ID,
                how="inner",
            )
            .drop(COL_FEATURE_COMPARISON)
        )
        lf_contrast_features = pl.concat(
            [lf_contrast_features_unscoped, lf_contrast_features_scoped],
            how="diagonal_relaxed",
        )

    lf_pairs_test = (
        lf_values.join(
            lf_contrast_features.select(
                COL_FEATURE_INTERNAL,
                "ContrastOrder",
                "GroupTest",
                "GroupRef",
                pl.col("GroupTest").alias("_GroupJoin"),
            ),
            left_on=[COL_FEATURE_INTERNAL, col_group],
            right_on=[COL_FEATURE_INTERNAL, "_GroupJoin"],
            how="inner",
        )
        .group_by(
            [
                COL_FEATURE_INTERNAL,
                "ContrastOrder",
                "GroupTest",
                "GroupRef",
                col_pair,
            ],
            maintain_order=True,
        )
        .agg(
            pl.len().alias("NRowsTest"),
            pl.col(col_value).first().alias("ValueGroupTest"),
        )
        .rename({col_pair: "PairId"})
    )
    lf_pairs_ref = (
        lf_values.join(
            lf_contrast_features.select(
                COL_FEATURE_INTERNAL,
                "ContrastOrder",
                "GroupTest",
                "GroupRef",
                pl.col("GroupRef").alias("_GroupJoin"),
            ),
            left_on=[COL_FEATURE_INTERNAL, col_group],
            right_on=[COL_FEATURE_INTERNAL, "_GroupJoin"],
            how="inner",
        )
        .group_by(
            [
                COL_FEATURE_INTERNAL,
                "ContrastOrder",
                "GroupTest",
                "GroupRef",
                col_pair,
            ],
            maintain_order=True,
        )
        .agg(
            pl.len().alias("NRowsRef"),
            pl.col(col_value).first().alias("ValueGroupRef"),
        )
        .rename({col_pair: "PairId"})
    )

    df_pairs = (
        lf_pairs_test.join(
            lf_pairs_ref,
            on=[
                COL_FEATURE_INTERNAL,
                "ContrastOrder",
                "GroupTest",
                "GroupRef",
                "PairId",
            ],
            how="full",
            coalesce=True,
        )
        .collect()
    )

    mask_missing_pair = df_pairs.select(
        (pl.col("NRowsTest").is_null() | pl.col("NRowsRef").is_null()).any()
    ).item()
    if mask_missing_pair:
        raise ValueError(
            "Paired t-test requires exactly one test row and one ref row for each feature, contrast, and pair."
        )

    mask_duplicate_pair = df_pairs.select(
        ((pl.col("NRowsTest") != 1) | (pl.col("NRowsRef") != 1)).any()
    ).item()
    if mask_duplicate_pair:
        raise ValueError(
            "Paired t-test does not allow duplicate rows for the same feature, contrast, pair, and group."
        )
    # #endregion
    ############################################################
    # #region Calculate paired summary statistics
    df_stats = (
        df_pairs.lazy()
        .with_columns(
            (pl.col("ValueGroupTest") - pl.col("ValueGroupRef")).alias("Diff")
        )
        .group_by(
            [
                COL_FEATURE_INTERNAL,
                "ContrastOrder",
                "GroupTest",
                "GroupRef",
            ],
            maintain_order=True,
        )
        .agg(
            pl.col("ValueGroupTest").count().alias("NGroupTest"),
            pl.col("ValueGroupRef").count().alias("NGroupRef"),
            pl.col("ValueGroupTest").mean().alias("MeanGroupTest"),
            pl.col("ValueGroupRef").mean().alias("MeanGroupRef"),
            *create_summary_stat_columns("Diff"),
        )
        .join(
            lf_features,
            on=COL_FEATURE_INTERNAL,
            how="left",
        )
        .sort([COL_FEATURE_ORDER, "ContrastOrder"])
        .collect()
    )
    if df_stats.height == 0:
        return pl.DataFrame(schema=schema_result)
    logger.debug(
        "Paired t-test prepared {n_rows} rows for p-value adjustment "
        "(comparisons={n_comparisons}, scoped_comparisons={n_scoped}, "
        "col_comparison={col_comparison}).",
        n_rows=df_stats.height,
        n_comparisons=len(contrast_plan.contrast_ids),
        n_scoped=sum(_item is not None for _item in contrast_plan.comparison_id_values),
        col_comparison=col_comparison,
    )
    # #endregion
    ############################################################
    # #region Calculate t-test statistics, p-values, and p-value adjustments
    np_stats = df_stats.select(COLS_SUMMARY_STATS).fill_null(np.nan).to_numpy()
    t_test_result = calculate_paired_test_statistics(
        mean_diff=np_stats[:, 0],
        var_diff=np_stats[:, 1],
        n_pair=np_stats[:, 2],
    )
    p_value = calculate_p_values(
        t_statistic=t_test_result.t_statistic,
        degrees_of_freedom=t_test_result.degrees_freedom,
        rule_alternative=rule_alternative,
    )
    if col_comparison is None:
        p_adjust = calculate_p_adjustment_array(p_value, rule_p_adjust=rule_p_adjust)
    else:
        p_adjust = np.full_like(p_value, np.nan, dtype=np.float64)
        arr_comparison = (
            df_stats[COL_FEATURE_INTERNAL].list.get(0).cast(pl.String).to_numpy()
        )
        for comparison_id in dict.fromkeys(arr_comparison.tolist()):
            mask = arr_comparison == comparison_id
            p_adjust[mask] = calculate_p_adjustment_array(
                p_value[mask],
                rule_p_adjust=rule_p_adjust,
            )
    # #endregion
    ############################################################
    # #region Finalize result DataFrame
    df_result = df_stats.with_columns(
        *create_t_stat_columns(t_test_result, p_values=p_value, p_adjust=p_adjust)
    )
    df_result = pf_adapter.create_result_frame(
        df_result,
        cols_selected=list(SCHEMA_T_TEST_TWO_SAMPLE_RESULT.keys()),
    )
    # #endregion
    ############################################################

    return df_result
