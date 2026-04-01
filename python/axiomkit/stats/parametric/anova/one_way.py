import numpy as np
import polars as pl
from polars._typing import SchemaDict

from ...p_value import (
    PValueAdjustmentMode,
    calculate_p_adjustment_array,
    normalize_p_value_adjustment_mode,
)
from ..constant import COL_FEATURE_INTERNAL, COL_FEATURE_ORDER
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
from .spec import OneWayStatisticalResult
from .util import calculate_f_test_p_values

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
) -> None:
    if col_value == col_group:
        raise ValueError("Args `col_value` and `col_group` must be different.")
    if col_feature is not None and col_feature in {col_value, col_group}:
        raise ValueError(
            "Arg `col_feature` must be different from `col_value` and `col_group`."
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
    rule_p_adjust: PValueAdjustmentMode | str | None = None,
) -> pl.DataFrame:
    """
    Calculate tidy one-way ANOVA results from a long-format table.

    Args:
        df: Input data in long format, with one row per observation.
        col_value: Name of the column containing numeric values to compare.
        col_group: Name of the column containing group labels for comparison.
        col_feature: Optional name of the column containing feature labels. If None, all rows are treated as a single feature.
        rule_p_adjust: Method for adjusting p-values for multiple testing. See :class:`PValueAdjustmentMode`.

    Returns:
        A Polars DataFrame containing one-way ANOVA results for each feature.
    """
    validate_column_layout_anova_one_way(col_value, col_group, col_feature)
    rule_p_adjust = (
        normalize_p_value_adjustment_mode(rule_p_adjust)
        if rule_p_adjust is not None
        else None
    )

    schema_input = read_frame_schema(df)
    cols_required = create_required_columns(col_value, col_group, col_feature)
    validate_required_columns(cols_in=schema_input, cols_required=cols_required)
    schema_result = create_result_schema(
        col_feature=col_feature,
        dtype_feature=schema_input.get(col_feature)
        if col_feature is not None
        else None,
        schema_result=SCHEMA_ANOVA_ONE_WAY_RESULT,
    )

    lf_values = normalize_value_frame(
        df,
        cols_required,
        cols_float=col_value,
        cols_string=col_group,
        col_feature=col_feature,
    )
    lf_features = create_feature_frame(lf_values)
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
    p_adjust = calculate_p_adjustment_array(p_value, rule_p_adjust=rule_p_adjust)

    df_result = df_stats.with_columns(
        pl.Series(
            name="DegreesFreedomBetween",
            values=anova_result.degrees_freedom_between,
            dtype=pl.Float64,
        ),
        pl.Series(
            name="DegreesFreedomWithin",
            values=anova_result.degrees_freedom_within,
            dtype=pl.Float64,
        ),
        pl.Series(name="FStatistic", values=anova_result.f_statistic, dtype=pl.Float64),
        pl.Series(name="PValue", values=p_value, dtype=pl.Float64),
        pl.Series(name="PAdjust", values=p_adjust, dtype=pl.Float64),
    )
    df_result = select_result_columns(
        df_result,
        cols_selected=list(SCHEMA_ANOVA_ONE_WAY_RESULT.keys()),
        col_feature=col_feature,
    )

    return df_result
