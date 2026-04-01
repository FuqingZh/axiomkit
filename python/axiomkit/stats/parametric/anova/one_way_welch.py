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
from .one_way import SCHEMA_ANOVA_ONE_WAY_RESULT, validate_column_layout_anova_one_way
from .spec import OneWayStatisticalResult
from .util import calculate_f_test_p_values, create_one_way_stats_columns


def calculate_one_way_welch_statistics(
    *,
    feature_keys: list[object],
    group_feature_keys: list[object],
    group_n: np.ndarray,
    group_mean: np.ndarray,
    group_var: np.ndarray,
) -> OneWayStatisticalResult:
    degrees_freedom_between = np.full(len(feature_keys), np.nan, dtype=np.float64)
    degrees_freedom_within = np.full(len(feature_keys), np.nan, dtype=np.float64)
    f_statistic = np.full(len(feature_keys), np.nan, dtype=np.float64)

    feature_slices: dict[object, tuple[int, int]] = {}
    idx_start = 0
    while idx_start < len(group_feature_keys):
        feature_key = group_feature_keys[idx_start]
        idx_end = idx_start + 1
        while (
            idx_end < len(group_feature_keys)
            and group_feature_keys[idx_end] == feature_key
        ):
            idx_end += 1
        feature_slices[feature_key] = (idx_start, idx_end)
        idx_start = idx_end

    for idx_feature, feature_key in enumerate(feature_keys):
        slice_feature = feature_slices.get(feature_key)
        if slice_feature is None:
            continue

        idx_group_start, idx_group_end = slice_feature
        n_group = group_n[idx_group_start:idx_group_end]
        mean_group = group_mean[idx_group_start:idx_group_end]
        var_group = group_var[idx_group_start:idx_group_end]
        num_groups = len(n_group)

        mask_group_valid = (
            np.isfinite(n_group)
            & np.isfinite(mean_group)
            & np.isfinite(var_group)
            & (n_group >= 2.0)
            & (var_group > 0.0)
        )
        if num_groups < 2 or not np.all(mask_group_valid):
            continue

        weight_group = n_group / var_group
        weight_total = np.sum(weight_group)
        if not np.isfinite(weight_total) or weight_total <= 0.0:
            continue

        mean_weighted = np.sum(weight_group * mean_group) / weight_total
        sum_term = np.sum(
            (1.0 / (n_group - 1.0)) * ((1.0 - (weight_group / weight_total)) ** 2)
        )
        if not np.isfinite(sum_term) or sum_term <= 0.0:
            continue

        degrees_freedom_between_feature = float(num_groups - 1)
        correction = 1.0 + (
            (2.0 * (num_groups - 2.0) / ((num_groups**2) - 1.0)) * sum_term
        )
        if (
            not np.isfinite(degrees_freedom_between_feature)
            or degrees_freedom_between_feature <= 0.0
            or not np.isfinite(correction)
            or correction <= 0.0
        ):
            continue

        ms_effect = (
            np.sum(weight_group * ((mean_group - mean_weighted) ** 2))
            / degrees_freedom_between_feature
        )
        if not np.isfinite(ms_effect):
            continue

        degrees_freedom_within_feature = ((num_groups**2) - 1.0) / (3.0 * sum_term)
        if (
            not np.isfinite(degrees_freedom_within_feature)
            or degrees_freedom_within_feature <= 0.0
        ):
            continue

        degrees_freedom_between[idx_feature] = degrees_freedom_between_feature
        degrees_freedom_within[idx_feature] = degrees_freedom_within_feature
        f_statistic[idx_feature] = ms_effect / correction

    return OneWayStatisticalResult(
        degrees_freedom_between=degrees_freedom_between,
        degrees_freedom_within=degrees_freedom_within,
        f_statistic=f_statistic,
    )


def calculate_anova_one_way_welch(
    df: pl.DataFrame | pl.LazyFrame,
    col_value: str = "Value",
    col_group: str = "Group",
    *,
    col_feature: str | None = None,
    rule_p_adjust: PValueAdjustmentMode | str | None = None,
) -> pl.DataFrame:
    """
    Calculate tidy Welch one-way ANOVA results from a long-format table.

    Args:
        df: Input data in long format, with one row per observation.
        col_value: Name of the column containing numeric values to compare.
        col_group: Name of the column containing group labels for comparison.
        col_feature: Optional name of the column containing feature labels. If None, all rows are treated as a single feature.
        rule_p_adjust: Method for adjusting p-values for multiple testing. See :class:`PValueAdjustmentMode`.

    Returns:
        A Polars DataFrame containing Welch one-way ANOVA results for each feature.
    """
    validate_column_layout_anova_one_way(col_value, col_group, col_feature)
    rule_p_adjust = (
        normalize_p_value_adjustment_mode(rule_p_adjust)
        if rule_p_adjust is not None
        else None
    )

    schema_input = read_frame_schema(df)
    cols_required = create_required_columns(col_value, col_group, col_feature)
    validate_required_columns(
        cols_in=list(schema_input.keys()), cols_required=cols_required
    )
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
    )
    df_stats = (
        lf_features.join(
            lf_group_stats.group_by(COL_FEATURE_INTERNAL, maintain_order=True).agg(
                pl.len().alias("NumGroups"),
                pl.col("N").sum().alias("NTotal"),
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

    df_group_stats = (
        lf_group_stats.join(
            lf_features,
            on=COL_FEATURE_INTERNAL,
            how="left",
        )
        .sort([COL_FEATURE_ORDER, col_group])
        .collect()
    )
    np_group_stats = (
        df_group_stats.select(COLS_SUMMARY_STATS).fill_null(np.nan).to_numpy()
        if df_group_stats.height > 0
        else np.empty((0, 3), dtype=np.float64)
    )
    one_way_result = calculate_one_way_welch_statistics(
        feature_keys=df_stats[COL_FEATURE_INTERNAL].to_list(),
        group_feature_keys=df_group_stats[COL_FEATURE_INTERNAL].to_list(),
        group_mean=np_group_stats[:, 0],
        group_var=np_group_stats[:, 1],
        group_n=np_group_stats[:, 2],
    )
    p_value = calculate_f_test_p_values(
        one_way_result.f_statistic,
        degrees_freedom_effect=one_way_result.degrees_freedom_between,
        degrees_freedom_within=one_way_result.degrees_freedom_within,
    )
    p_adjust = calculate_p_adjustment_array(p_value, rule_p_adjust=rule_p_adjust)

    df_result = df_stats.with_columns(
        *create_one_way_stats_columns(
            one_way_result, p_values=p_value, p_adjust=p_adjust
        )
    )
    df_result = select_result_columns(
        df_result,
        cols_selected=list(SCHEMA_ANOVA_ONE_WAY_RESULT.keys()),
        col_feature=col_feature,
    )

    return df_result
