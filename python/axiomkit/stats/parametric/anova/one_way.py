from collections.abc import Sequence

import numpy as np
import polars as pl
from loguru import logger
from polars._typing import SchemaDict

from ...p_value import (
    PValueAdjustmentType,
    calculate_p_adjustment_array,
    normalize_p_value_adjustment_mode,
)
from ..constant import COL_FEATURE_INTERNAL, COL_FEATURE_ORDER
from ..spec import ParametricFrameAdapter
from ..util import (
    create_required_columns,
    create_summary_stat_columns,
)
from ..comparison import ParametricComparison
from .spec import AnovaComparisonPlan, OneWayStatisticalResult
from .util import calculate_f_test_p_values, create_one_way_stats_columns

COL_ANOVA_COMPARISON_ID = "_AnovaComparisonId"
COL_ANOVA_GROUP = "_AnovaGroup"

SCHEMA_ANOVA_ONE_WAY_RESULT: SchemaDict = {
    "NumGroups": pl.Int64,
    "NTotal": pl.Int64,
    "DegreesFreedomBetween": pl.Float64,
    "DegreesFreedomWithin": pl.Float64,
    "FStatistic": pl.Float64,
    "PValue": pl.Float64,
    "PAdjust": pl.Float64,
}


def validate_column_layout_anova_one_way(
    col_value: str,
    col_group: str,
    col_feature: str | None,
    col_comparison: str | None = None,
    col_is_valid: str | None = None,
) -> None:
    if col_value == col_group:
        raise ValueError("Args `col_value` and `col_group` must be different.")
    if col_feature is not None and col_feature in {col_value, col_group}:
        raise ValueError(
            "Arg `col_feature` must be different from `col_value` and `col_group`."
        )
    if col_comparison is None:
        return

    if col_feature is None:
        raise ValueError(
            "Arg `col_feature` is required when `col_comparison` is provided."
        )
    if col_comparison in {col_value, col_group, col_feature}:
        raise ValueError(
            "Arg `col_comparison` must be different from `col_value`, "
            "`col_group`, and `col_feature`."
        )
    if col_is_valid is not None and col_is_valid in {
        col_value,
        col_group,
        col_feature,
        col_comparison,
    }:
        raise ValueError(
            "Arg `col_is_valid` must be different from `col_value`, "
            "`col_group`, `col_feature`, and `col_comparison`."
        )


def calculate_anova_one_way_statistics(
    *,
    num_groups: np.ndarray,
    n_total: np.ndarray,
    weighted_mean_total: np.ndarray,
    weighted_mean_sq_total: np.ndarray,
    ss_within: np.ndarray,
) -> OneWayStatisticalResult:
    df_between = np.full_like(num_groups, np.nan, dtype=np.float64)
    df_within = np.full_like(num_groups, np.nan, dtype=np.float64)
    f_statistic = np.full_like(num_groups, np.nan, dtype=np.float64)

    mask_valid = (
        np.isfinite(num_groups)
        & np.isfinite(n_total)
        & np.isfinite(weighted_mean_total)
        & np.isfinite(weighted_mean_sq_total)
        & np.isfinite(ss_within)
        & (num_groups >= 2.0)
        & (n_total > num_groups)
    )
    if not np.any(mask_valid):
        return OneWayStatisticalResult(
            degrees_freedom_between=df_between,
            degrees_freedom_within=df_within,
            f_statistic=f_statistic,
        )

    num_groups_valid = num_groups[mask_valid]
    n_total_valid = n_total[mask_valid]
    weighted_mean_total_valid = weighted_mean_total[mask_valid]
    weighted_mean_sq_total_valid = weighted_mean_sq_total[mask_valid]
    ss_within_valid = np.maximum(ss_within[mask_valid], 0.0)

    with np.errstate(divide="ignore", invalid="ignore"):
        df_between_valid = num_groups_valid - 1.0
        df_within_valid = n_total_valid - num_groups_valid
        ss_between_valid = weighted_mean_sq_total_valid - (
            weighted_mean_total_valid**2 / n_total_valid
        )
        ss_between_valid = np.maximum(ss_between_valid, 0.0)
        ms_between_valid = ss_between_valid / df_between_valid
        ms_within_valid = ss_within_valid / df_within_valid
        f_valid = ms_between_valid / ms_within_valid

        mask_df_valid = np.isfinite(df_between_valid) & np.isfinite(df_within_valid)
        df_between_valid = np.where(mask_df_valid, df_between_valid, np.nan)
        df_within_valid = np.where(mask_df_valid, df_within_valid, np.nan)
        f_valid = np.where(mask_df_valid, f_valid, np.nan)

    df_between[mask_valid] = df_between_valid
    df_within[mask_valid] = df_within_valid
    f_statistic[mask_valid] = f_valid
    return OneWayStatisticalResult(
        degrees_freedom_between=df_between,
        degrees_freedom_within=df_within,
        f_statistic=f_statistic,
    )


def calculate_anova_one_way(
    df: pl.DataFrame | pl.LazyFrame,
    col_value: str = "Value",
    col_group: str = "Group",
    *,
    col_feature: str | None = None,
    col_comparison: str | None = None,
    col_is_valid: str | None = None,
    comparisons: ParametricComparison | Sequence[ParametricComparison] | None = None,
    rule_p_adjust: PValueAdjustmentType | str | None = None,
) -> pl.DataFrame:
    """
    Calculate tidy one-way ANOVA results from a long-format table.

    Args:
        df: Input data in long format, with one row per observation.
        col_value: Name of the column containing numeric values to compare.
        col_group: Name of the column containing group labels for comparison.
        col_feature: Optional name of the column containing feature labels. If None, all rows are treated as a single feature.
        col_comparison: Optional name of the column defining comparison-specific
            statistical units. When provided, the effective feature key becomes
            ``col_comparison x col_feature``.
        col_is_valid: Optional boolean column indicating whether a
            ``col_comparison x col_feature`` unit should enter testing. Ignored
            unless ``col_comparison`` is provided.
        comparisons: Optional declared comparison plan. When provided, only
            requested ``comparison_id`` values are tested. Each comparison may
            optionally restrict the included groups.
        rule_p_adjust: Method for adjusting p-values for multiple testing.
            - ``None``: (Default) No adjustment; return raw p-values.
            - "bonferroni": Bonferroni correction.
            - "bh": Benjamini-Hochberg procedure.
            - "by": Benjamini-Yekutieli procedure.

    Returns:
        A Polars DataFrame containing one-way ANOVA results for each feature.
            - Column named as `col_comparison` (if specified): Comparison layer
              for each row.
            - Column named as `col_feature` (if specified): Feature label for
              each row.
            - `NumGroups`: Number of observed groups used in the ANOVA.
            - `NTotal`: Total number of observations.
            - `DegreesFreedomBetween`: Between-group degrees of freedom.
            - `DegreesFreedomWithin`: Within-group degrees of freedom.
            - `FStatistic`: F statistic.
            - `PValue`: Raw p-value.
            - `PAdjust`: Adjusted p-value, calculated within each
              `col_comparison` layer when `col_comparison` is provided.

    Examples:
        ```python
        import polars as pl
        from axiomkit.stats import ParametricComparison, calculate_anova_one_way

        df = pl.DataFrame({
            "Comparison": ["cmp1"] * 6 + ["cmp2"] * 6,
            "Feature": ["P1"] * 12,
            "Group": ["A", "A", "B", "B", "C", "C"] * 2,
            "Value": [1.0, 2.0, 5.0, 6.0, 3.0, 4.0,
                      2.0, 3.0, 8.0, 9.0, 5.0, 6.0],
        })

        result = calculate_anova_one_way(
            df,
            col_feature="Feature",
            col_comparison="Comparison",
            comparisons=ParametricComparison.anova_one_way("cmp1"),
            rule_p_adjust="bh",
        )
        ```
    """
    validate_column_layout_anova_one_way(
        col_value,
        col_group,
        col_feature,
        col_comparison=col_comparison,
        col_is_valid=col_is_valid,
    )
    rule_p_adjust = normalize_p_value_adjustment_mode(rule_p_adjust)
    comparison_plan = AnovaComparisonPlan.from_inputs(comparisons)
    if comparison_plan is not None and col_comparison is None:
        raise ValueError(
            "Arg `col_comparison` is required when `comparisons` is provided."
        )

    pf_adapter = (
        ParametricFrameAdapter(
            df,
            col_feature=col_feature,
            col_comparison=col_comparison,
            col_is_valid=col_is_valid,
        )
        .select_required_cols(
            cols_required=create_required_columns(
                col_value,
                col_group,
                col_feature,
                col_comparison,
                col_is_valid if col_comparison is not None else None,
            )
        )
        .cast_cols(
            cols_float=col_value,
            cols_string=[col_group, col_comparison]
            if col_comparison is not None
            else col_group,
            cols_boolean=col_is_valid if col_comparison is not None else None,
        )
        .create_feature_key()
    )
    schema_result = pf_adapter.create_result_schema(SCHEMA_ANOVA_ONE_WAY_RESULT)

    lf_values = pf_adapter.lf
    lf_features = pf_adapter.create_feature_frame()
    if comparison_plan is not None:
        assert col_comparison is not None
        col_comparison_plan = col_comparison
        lf_features = lf_features.filter(
            pl.col(COL_FEATURE_INTERNAL)
            .list.get(0)
            .cast(pl.String)
            .is_in(comparison_plan.comparison_ids)
        )
        if comparison_plan.has_group_filter:
            lf_group_filter = pl.LazyFrame(
                {
                    COL_ANOVA_COMPARISON_ID: list(
                        comparison_plan.group_comparison_ids
                    ),
                    COL_ANOVA_GROUP: list(comparison_plan.group_values),
                }
            )
            lf_values_with_all_groups = lf_values.filter(
                pl.col(col_comparison_plan).is_in(
                    comparison_plan.comparison_ids_all_groups
                )
            )
            lf_values_with_group_filter = lf_values.join(
                lf_group_filter,
                left_on=[col_comparison_plan, col_group],
                right_on=[COL_ANOVA_COMPARISON_ID, COL_ANOVA_GROUP],
                how="inner",
            )
            lf_values = pl.concat(
                [lf_values_with_all_groups, lf_values_with_group_filter],
                how="vertical",
            )
        else:
            lf_values = lf_values.filter(
                pl.col(col_comparison_plan).is_in(comparison_plan.comparison_ids)
            )
    lf_group_stats = (
        lf_values.group_by([COL_FEATURE_INTERNAL, col_group], maintain_order=True)
        .agg(*create_summary_stat_columns(col_value))
        .filter(pl.col("N") > 0)
        .with_columns(
            (pl.col("N") * pl.col("Mean")).alias("_WeightedMean"),
            (pl.col("N") * pl.col("Mean") * pl.col("Mean")).alias("_WeightedMeanSq"),
            pl.when(pl.col("N") >= 2)
            .then((pl.col("N") - 1) * pl.col("Var").fill_null(0.0))
            .otherwise(0.0)
            .alias("_SSWithin"),
        )
    )
    df_stats = (
        lf_features.join(
            lf_group_stats.group_by(COL_FEATURE_INTERNAL, maintain_order=True).agg(
                pl.len().alias("NumGroups"),
                pl.col("N").sum().alias("NTotal"),
                pl.col("_WeightedMean").sum().alias("_WeightedMeanTotal"),
                pl.col("_WeightedMeanSq").sum().alias("_WeightedMeanSqTotal"),
                pl.col("_SSWithin").sum().alias("_SSWithinTotal"),
            ),
            on=COL_FEATURE_INTERNAL,
            how="left",
        )
        .with_columns(
            pl.col("NumGroups").fill_null(0),
            pl.col("NTotal").fill_null(0),
        )
        .sort(COL_FEATURE_ORDER)
        .collect()
    )
    if df_stats.height == 0:
        return pl.DataFrame(schema=schema_result)

    np_stats = (
        df_stats.select(
            "NumGroups",
            "NTotal",
            "_WeightedMeanTotal",
            "_WeightedMeanSqTotal",
            "_SSWithinTotal",
        )
        .fill_null(np.nan)
        .to_numpy()
    )
    anova_result = calculate_anova_one_way_statistics(
        num_groups=np_stats[:, 0],
        n_total=np_stats[:, 1],
        weighted_mean_total=np_stats[:, 2],
        weighted_mean_sq_total=np_stats[:, 3],
        ss_within=np_stats[:, 4],
    )
    p_value = calculate_f_test_p_values(
        anova_result.f_statistic,
        degrees_freedom_effect=anova_result.degrees_freedom_between,
        degrees_freedom_within=anova_result.degrees_freedom_within,
    )
    if col_comparison is None:
        p_adjust = calculate_p_adjustment_array(p_value, rule_p_adjust=rule_p_adjust)
    else:
        p_adjust = np.full_like(p_value, np.nan, dtype=np.float64)
        arr_comparison = (
            df_stats[COL_FEATURE_INTERNAL]
            .list.get(0)
            .cast(pl.String)
            .to_numpy()
        )
        for comparison_id in dict.fromkeys(arr_comparison.tolist()):
            mask = arr_comparison == comparison_id
            p_adjust[mask] = calculate_p_adjustment_array(
                p_value[mask],
                rule_p_adjust=rule_p_adjust,
            )
    logger.debug(
        "One-way ANOVA prepared {n_rows} rows for p-value adjustment "
        "(comparisons={n_comparisons}, has_comparison_filter={has_comparison_filter}, "
        "col_comparison={col_comparison}).",
        n_rows=df_stats.height,
        n_comparisons=0 if comparison_plan is None else len(comparison_plan.comparison_ids),
        has_comparison_filter=comparison_plan is not None,
        col_comparison=col_comparison,
    )

    df_result = df_stats.with_columns(
        *create_one_way_stats_columns(
            anova_result,
            p_values=p_value,
            p_adjust=p_adjust,
        )
    )
    df_result = pf_adapter.create_result_frame(
        df_result,
        cols_selected=list(SCHEMA_ANOVA_ONE_WAY_RESULT.keys()),
    )

    return df_result
