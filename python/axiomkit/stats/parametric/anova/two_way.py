from collections.abc import Sequence
from dataclasses import dataclass
from typing import Self

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
from ..comparison import ParametricComparison, ParametricComparisonKind
from ..spec import ParametricFrameAdapter
from ..util import (
    create_required_columns,
    create_summary_stat_columns,
)
from .util import calculate_f_test_p_values

SCHEMA_ANOVA_TWO_WAY_RESULT: SchemaDict = {
    "NumGroupsA": pl.Int64,
    "NumGroupsB": pl.Int64,
    "NTotal": pl.Int64,
    "DegreesFreedomA": pl.Float64,
    "DegreesFreedomB": pl.Float64,
    "DegreesFreedomInteraction": pl.Float64,
    "DegreesFreedomWithin": pl.Float64,
    "FStatisticA": pl.Float64,
    "FStatisticB": pl.Float64,
    "FStatisticInteraction": pl.Float64,
    "PValueA": pl.Float64,
    "PValueB": pl.Float64,
    "PValueInteraction": pl.Float64,
    "PAdjustA": pl.Float64,
    "PAdjustB": pl.Float64,
    "PAdjustInteraction": pl.Float64,
}


def validate_column_layout_anova_two_way(
    col_value: str,
    col_group_a: str,
    col_group_b: str,
    col_feature: str | None,
    col_comparison: str | None = None,
) -> None:
    cols_required = [col_value, col_group_a, col_group_b]
    if len(set(cols_required)) != len(cols_required):
        raise ValueError(
            "Args `col_value`, `col_group_a`, and `col_group_b` must be different."
        )
    if col_feature is not None and col_feature in {
        col_value,
        col_group_a,
        col_group_b,
    }:
        raise ValueError(
            "Arg `col_feature` must be different from `col_value`, `col_group_a`, and `col_group_b`."
        )
    if col_comparison is None:
        return

    if col_feature is None:
        raise ValueError(
            "Arg `col_feature` is required when `col_comparison` is provided."
        )
    if col_comparison in {col_value, col_group_a, col_group_b, col_feature}:
        raise ValueError(
            "Arg `col_comparison` must be different from `col_value`, "
            "`col_group_a`, `col_group_b`, and `col_feature`."
        )


@dataclass(frozen=True, slots=True)
class AnovaTwoWayComparisonPlan:
    comparisons: tuple[ParametricComparison, ...]
    comparison_ids: tuple[str, ...]

    @classmethod
    def from_inputs(
        cls,
        comparisons: ParametricComparison | Sequence[ParametricComparison] | None,
    ) -> Self | None:
        if comparisons is None:
            return None
        if isinstance(comparisons, ParametricComparison):
            items_comparison = (comparisons,)
        elif isinstance(comparisons, Sequence) and not isinstance(comparisons, str):
            items_comparison = tuple(comparisons)
        else:
            raise ValueError(
                "Arg `comparisons` must be a ParametricComparison or a sequence of ParametricComparison items."
            )
        if not items_comparison:
            raise ValueError("Arg `comparisons` must not be empty.")
        if any(
            not isinstance(_item, ParametricComparison)
            or _item.kind != ParametricComparisonKind.ANOVA_TWO_WAY
            for _item in items_comparison
        ):
            raise ValueError(
                "Arg `comparisons` must contain `anova_two_way` ParametricComparison items."
            )

        comparison_ids_seen: set[str] = set()
        comparison_ids: list[str] = []
        for item_comparison in items_comparison:
            comparison_id = item_comparison.comparison_id
            assert comparison_id is not None
            if comparison_id in comparison_ids_seen:
                raise ValueError("Duplicate comparison ids are not allowed.")
            comparison_ids_seen.add(comparison_id)
            comparison_ids.append(comparison_id)

        return cls(
            comparisons=items_comparison,
            comparison_ids=tuple(comparison_ids),
        )


@dataclass(frozen=True, slots=True)
class AnovaTwoWayResult:
    degrees_freedom_a: np.ndarray
    degrees_freedom_b: np.ndarray
    degrees_freedom_interaction: np.ndarray
    degrees_freedom_within: np.ndarray
    f_statistic_a: np.ndarray
    f_statistic_b: np.ndarray
    f_statistic_interaction: np.ndarray


def calculate_anova_two_way_statistics(
    *,
    num_groups_a: np.ndarray,
    num_groups_b: np.ndarray,
    num_cells_observed: np.ndarray,
    n_total: np.ndarray,
    replicate_min: np.ndarray,
    replicate_max: np.ndarray,
    total_sum: np.ndarray,
    cell_total_sq_over_n_sum: np.ndarray,
    sum_a_sq_over_n_sum: np.ndarray,
    sum_b_sq_over_n_sum: np.ndarray,
    ss_within: np.ndarray,
) -> AnovaTwoWayResult:
    df_a = np.full_like(num_groups_a, np.nan, dtype=np.float64)
    df_b = np.full_like(num_groups_a, np.nan, dtype=np.float64)
    df_interaction = np.full_like(num_groups_a, np.nan, dtype=np.float64)
    df_within = np.full_like(num_groups_a, np.nan, dtype=np.float64)
    f_a = np.full_like(num_groups_a, np.nan, dtype=np.float64)
    f_b = np.full_like(num_groups_a, np.nan, dtype=np.float64)
    f_interaction = np.full_like(num_groups_a, np.nan, dtype=np.float64)

    expected_cells = num_groups_a * num_groups_b
    mask_valid = (
        np.isfinite(num_groups_a)
        & np.isfinite(num_groups_b)
        & np.isfinite(num_cells_observed)
        & np.isfinite(n_total)
        & np.isfinite(replicate_min)
        & np.isfinite(replicate_max)
        & np.isfinite(total_sum)
        & np.isfinite(cell_total_sq_over_n_sum)
        & np.isfinite(sum_a_sq_over_n_sum)
        & np.isfinite(sum_b_sq_over_n_sum)
        & np.isfinite(ss_within)
        & (num_groups_a >= 2.0)
        & (num_groups_b >= 2.0)
        & (num_cells_observed == expected_cells)
        & (replicate_min == replicate_max)
        & (replicate_min >= 1.0)
        & (n_total > expected_cells)
    )
    if not np.any(mask_valid):
        return AnovaTwoWayResult(
            degrees_freedom_a=df_a,
            degrees_freedom_b=df_b,
            degrees_freedom_interaction=df_interaction,
            degrees_freedom_within=df_within,
            f_statistic_a=f_a,
            f_statistic_b=f_b,
            f_statistic_interaction=f_interaction,
        )

    num_groups_a_valid = num_groups_a[mask_valid]
    num_groups_b_valid = num_groups_b[mask_valid]
    num_cells_valid = num_cells_observed[mask_valid]
    n_total_valid = n_total[mask_valid]
    total_sum_valid = total_sum[mask_valid]
    cell_total_sq_over_n_sum_valid = cell_total_sq_over_n_sum[mask_valid]
    sum_a_sq_over_n_sum_valid = sum_a_sq_over_n_sum[mask_valid]
    sum_b_sq_over_n_sum_valid = sum_b_sq_over_n_sum[mask_valid]
    ss_within_valid = np.maximum(ss_within[mask_valid], 0.0)

    with np.errstate(divide="ignore", invalid="ignore"):
        correction = total_sum_valid**2 / n_total_valid
        ss_a = np.maximum(sum_a_sq_over_n_sum_valid - correction, 0.0)
        ss_b = np.maximum(sum_b_sq_over_n_sum_valid - correction, 0.0)
        ss_cells = np.maximum(cell_total_sq_over_n_sum_valid - correction, 0.0)
        ss_interaction_valid = np.maximum(ss_cells - ss_a - ss_b, 0.0)

        df_a_valid = num_groups_a_valid - 1.0
        df_b_valid = num_groups_b_valid - 1.0
        df_interaction_valid = df_a_valid * df_b_valid
        df_within_valid = n_total_valid - num_cells_valid

        ms_a_valid = ss_a / df_a_valid
        ms_b_valid = ss_b / df_b_valid
        ms_interaction_valid = ss_interaction_valid / df_interaction_valid
        ms_within_valid = ss_within_valid / df_within_valid

        f_a_valid = ms_a_valid / ms_within_valid
        f_b_valid = ms_b_valid / ms_within_valid
        f_interaction_valid = ms_interaction_valid / ms_within_valid

        mask_df_valid = (
            np.isfinite(df_a_valid)
            & np.isfinite(df_b_valid)
            & np.isfinite(df_interaction_valid)
            & np.isfinite(df_within_valid)
        )
        df_a_valid = np.where(mask_df_valid, df_a_valid, np.nan)
        df_b_valid = np.where(mask_df_valid, df_b_valid, np.nan)
        df_interaction_valid = np.where(mask_df_valid, df_interaction_valid, np.nan)
        df_within_valid = np.where(mask_df_valid, df_within_valid, np.nan)
        f_a_valid = np.where(mask_df_valid, f_a_valid, np.nan)
        f_b_valid = np.where(mask_df_valid, f_b_valid, np.nan)
        f_interaction_valid = np.where(mask_df_valid, f_interaction_valid, np.nan)

    df_a[mask_valid] = df_a_valid
    df_b[mask_valid] = df_b_valid
    df_interaction[mask_valid] = df_interaction_valid
    df_within[mask_valid] = df_within_valid
    f_a[mask_valid] = f_a_valid
    f_b[mask_valid] = f_b_valid
    f_interaction[mask_valid] = f_interaction_valid

    return AnovaTwoWayResult(
        degrees_freedom_a=df_a,
        degrees_freedom_b=df_b,
        degrees_freedom_interaction=df_interaction,
        degrees_freedom_within=df_within,
        f_statistic_a=f_a,
        f_statistic_b=f_b,
        f_statistic_interaction=f_interaction,
    )


def calculate_anova_two_way(
    df: pl.DataFrame | pl.LazyFrame,
    col_value: str = "Value",
    col_group_a: str = "GroupA",
    col_group_b: str = "GroupB",
    *,
    col_feature: str | None = None,
    col_comparison: str | None = None,
    comparisons: ParametricComparison | Sequence[ParametricComparison] | None = None,
    rule_p_adjust: PValueAdjustmentType | str | None = None,
) -> pl.DataFrame:
    """
    Calculate tidy two-way ANOVA results from a long-format table.

    Two-way ANOVA:
    - Compare the main effects of two categorical factors and their interaction for each feature.
    - This v1 implementation assumes a complete balanced design within each feature.
    - Features that do not satisfy the complete balanced-design requirement are kept with NaN ANOVA statistics.

    Args:
        df: Input data in long format, with one row per observation.
        col_value: Name of the column containing numeric values to compare.
        col_group_a: Name of the first factor column.
        col_group_b: Name of the second factor column.
        col_feature: Optional name of the column containing feature labels. If None, all rows are treated as a single feature.
        col_comparison: Optional name of the column defining comparison-specific
            statistical units. Values are compared and returned after string
            normalization.
        comparisons: Optional declared comparison plan using
            :class:`ParametricComparison.anova_two_way`.
        rule_p_adjust: Method for adjusting p-values for multiple testing.
            - ``None``: (Default) No adjustment; return raw p-values.
            - "bonferroni": Bonferroni correction.
            - "bh": Benjamini-Hochberg correction.
            - "by": Benjamini-Yekutieli correction.

    Returns:
        A Polars DataFrame containing two-way ANOVA results for each feature.
            - Column named as `col_comparison` (if specified): Comparison layer
              for each row.
            - Column named as `col_feature` (if specified): Feature label for
              each row.
            - `NumGroupsA` and `NumGroupsB`: Number of observed factor levels.
            - `NTotal`: Total number of observations.
            - `DegreesFreedomA`, `DegreesFreedomB`,
              `DegreesFreedomInteraction`, and `DegreesFreedomWithin`:
              Degrees of freedom for each effect.
            - `FStatisticA`, `FStatisticB`, and `FStatisticInteraction`: F
              statistics for each effect.
            - `PValueA`, `PValueB`, and `PValueInteraction`: Raw p-values.
            - `PAdjustA`, `PAdjustB`, and `PAdjustInteraction`: Adjusted
              p-values, calculated within each `col_comparison` layer when
              `col_comparison` is provided.

    Examples:
        ```python
        import polars as pl
        from axiomkit.stats import ParametricComparison, calculate_anova_two_way

        df = pl.DataFrame({
            "Comparison": ["cmp1"] * 8,
            "Feature": ["P1"] * 8,
            "GroupA": ["A1", "A1", "A1", "A1", "A2", "A2", "A2", "A2"],
            "GroupB": ["B1", "B1", "B2", "B2", "B1", "B1", "B2", "B2"],
            "Value": [8.0, 10.0, 6.0, 8.0, 4.0, 5.0, 3.0, 6.0],
        })

        result = calculate_anova_two_way(
            df,
            col_group_a="GroupA",
            col_group_b="GroupB",
            col_feature="Feature",
            col_comparison="Comparison",
            comparisons=ParametricComparison.anova_two_way("cmp1"),
            rule_p_adjust="bh",
        )
        ```
    """
    validate_column_layout_anova_two_way(
        col_value=col_value,
        col_group_a=col_group_a,
        col_group_b=col_group_b,
        col_feature=col_feature,
        col_comparison=col_comparison,
    )
    rule_p_adjust = normalize_p_value_adjustment_mode(rule_p_adjust)
    comparison_plan = AnovaTwoWayComparisonPlan.from_inputs(comparisons)
    if comparison_plan is not None and col_comparison is None:
        raise ValueError(
            "Arg `col_comparison` is required when `comparisons` is provided."
        )

    pf_adapter = ParametricFrameAdapter(
        df,
        col_feature=col_feature,
        col_comparison=col_comparison,
    ).select_required_cols(
        cols_required=create_required_columns(
            col_value, col_group_a, col_group_b, col_feature, col_comparison
        )
    )
    schema_result = pf_adapter.create_result_schema(SCHEMA_ANOVA_TWO_WAY_RESULT)

    pf_adapter.cast_cols(
        cols_float=col_value,
        cols_string=[col_group_a, col_group_b, col_comparison]
        if col_comparison is not None
        else [col_group_a, col_group_b],
    ).create_feature_key()
    lf_values = pf_adapter.lf
    lf_features = pf_adapter.create_feature_frame()
    if comparison_plan is not None:
        assert col_comparison is not None
        lf_features = lf_features.filter(
            pl.col(COL_FEATURE_INTERNAL)
            .list.get(0)
            .cast(pl.String)
            .is_in(comparison_plan.comparison_ids)
        )
        expr_filter = pl.lit(False)
        for comparison in comparison_plan.comparisons:
            expr_comparison = pl.col(col_comparison) == comparison.comparison_id
            if comparison.groups_a is not None:
                expr_comparison = expr_comparison & pl.col(col_group_a).is_in(
                    comparison.groups_a
                )
            if comparison.groups_b is not None:
                expr_comparison = expr_comparison & pl.col(col_group_b).is_in(
                    comparison.groups_b
                )
            expr_filter = expr_filter | expr_comparison
        lf_values = lf_values.filter(expr_filter)

    lf_cell_stats = (
        lf_values.group_by(
            [COL_FEATURE_INTERNAL, col_group_a, col_group_b], maintain_order=True
        )
        .agg(*create_summary_stat_columns(col_value))
        .filter(pl.col("N") > 0)
        .with_columns(
            (pl.col("N") * pl.col("Mean")).alias("_CellTotal"),
            (pl.col("N") * pl.col("Mean") * pl.col("Mean")).alias("_CellTotalSqOverN"),
            pl.when(pl.col("N") >= 2)
            .then((pl.col("N") - 1) * pl.col("Var").fill_null(0.0))
            .otherwise(0.0)
            .alias("_SSWithin"),
        )
    )
    lf_factor_a_stats = lf_cell_stats.group_by(
        [COL_FEATURE_INTERNAL, col_group_a], maintain_order=True
    ).agg(
        pl.col("_CellTotal").sum().alias("_FactorATotal"),
        pl.col("N").sum().alias("_FactorAN"),
    )
    lf_factor_b_stats = lf_cell_stats.group_by(
        [COL_FEATURE_INTERNAL, col_group_b], maintain_order=True
    ).agg(
        pl.col("_CellTotal").sum().alias("_FactorBTotal"),
        pl.col("N").sum().alias("_FactorBN"),
    )

    df_stats = (
        lf_features.join(
            lf_cell_stats.group_by(COL_FEATURE_INTERNAL, maintain_order=True).agg(
                pl.col(col_group_a).n_unique().alias("NumGroupsA"),
                pl.col(col_group_b).n_unique().alias("NumGroupsB"),
                pl.len().alias("_NumCellsObserved"),
                pl.col("N").sum().alias("NTotal"),
                pl.col("N").min().alias("_ReplicateMin"),
                pl.col("N").max().alias("_ReplicateMax"),
                pl.col("_CellTotal").sum().alias("_TotalSum"),
                pl.col("_CellTotalSqOverN").sum().alias("_CellTotalSqOverNSum"),
                pl.col("_SSWithin").sum().alias("_SSWithinTotal"),
            ),
            on=COL_FEATURE_INTERNAL,
            how="left",
        )
        .join(
            lf_factor_a_stats.group_by(COL_FEATURE_INTERNAL, maintain_order=True).agg(
                (
                    pl.col("_FactorATotal")
                    * pl.col("_FactorATotal")
                    / pl.col("_FactorAN")
                )
                .sum()
                .alias("_FactorATotalSqOverNSum"),
            ),
            on=COL_FEATURE_INTERNAL,
            how="left",
        )
        .join(
            lf_factor_b_stats.group_by(COL_FEATURE_INTERNAL, maintain_order=True).agg(
                (
                    pl.col("_FactorBTotal")
                    * pl.col("_FactorBTotal")
                    / pl.col("_FactorBN")
                )
                .sum()
                .alias("_FactorBTotalSqOverNSum"),
            ),
            on=COL_FEATURE_INTERNAL,
            how="left",
        )
        .with_columns(
            pl.col("NumGroupsA").fill_null(0),
            pl.col("NumGroupsB").fill_null(0),
            pl.col("NTotal").fill_null(0),
        )
        .sort(COL_FEATURE_ORDER)
        .collect()
    )
    if df_stats.height == 0:
        return pl.DataFrame(schema=schema_result)

    np_stats = (
        df_stats.select(
            "NumGroupsA",
            "NumGroupsB",
            "_NumCellsObserved",
            "NTotal",
            "_ReplicateMin",
            "_ReplicateMax",
            "_TotalSum",
            "_CellTotalSqOverNSum",
            "_FactorATotalSqOverNSum",
            "_FactorBTotalSqOverNSum",
            "_SSWithinTotal",
        )
        .fill_null(np.nan)
        .to_numpy()
    )
    anova_result = calculate_anova_two_way_statistics(
        num_groups_a=np_stats[:, 0],
        num_groups_b=np_stats[:, 1],
        num_cells_observed=np_stats[:, 2],
        n_total=np_stats[:, 3],
        replicate_min=np_stats[:, 4],
        replicate_max=np_stats[:, 5],
        total_sum=np_stats[:, 6],
        cell_total_sq_over_n_sum=np_stats[:, 7],
        sum_a_sq_over_n_sum=np_stats[:, 8],
        sum_b_sq_over_n_sum=np_stats[:, 9],
        ss_within=np_stats[:, 10],
    )

    p_value_a = calculate_f_test_p_values(
        anova_result.f_statistic_a,
        degrees_freedom_effect=anova_result.degrees_freedom_a,
        degrees_freedom_within=anova_result.degrees_freedom_within,
    )
    p_value_b = calculate_f_test_p_values(
        anova_result.f_statistic_b,
        degrees_freedom_effect=anova_result.degrees_freedom_b,
        degrees_freedom_within=anova_result.degrees_freedom_within,
    )
    p_value_interaction = calculate_f_test_p_values(
        anova_result.f_statistic_interaction,
        degrees_freedom_effect=anova_result.degrees_freedom_interaction,
        degrees_freedom_within=anova_result.degrees_freedom_within,
    )
    if col_comparison is None:
        p_adjust_a = calculate_p_adjustment_array(p_value_a, rule_p_adjust=rule_p_adjust)
        p_adjust_b = calculate_p_adjustment_array(p_value_b, rule_p_adjust=rule_p_adjust)
        p_adjust_interaction = calculate_p_adjustment_array(
            p_value_interaction, rule_p_adjust=rule_p_adjust
        )
    else:
        arr_comparison = (
            df_stats[COL_FEATURE_INTERNAL]
            .list.get(0)
            .cast(pl.String)
            .to_numpy()
        )
        p_adjust_a = np.full_like(p_value_a, np.nan, dtype=np.float64)
        p_adjust_b = np.full_like(p_value_b, np.nan, dtype=np.float64)
        p_adjust_interaction = np.full_like(
            p_value_interaction,
            np.nan,
            dtype=np.float64,
        )
        for comparison_id in dict.fromkeys(arr_comparison.tolist()):
            mask = arr_comparison == comparison_id
            p_adjust_a[mask] = calculate_p_adjustment_array(
                p_value_a[mask],
                rule_p_adjust=rule_p_adjust,
            )
            p_adjust_b[mask] = calculate_p_adjustment_array(
                p_value_b[mask],
                rule_p_adjust=rule_p_adjust,
            )
            p_adjust_interaction[mask] = calculate_p_adjustment_array(
                p_value_interaction[mask],
                rule_p_adjust=rule_p_adjust,
            )
    logger.debug(
        "Two-way ANOVA prepared {n_rows} rows for p-value adjustment "
        "(comparisons={n_comparisons}, has_comparison_filter={has_comparison_filter}, "
        "col_comparison={col_comparison}).",
        n_rows=df_stats.height,
        n_comparisons=0 if comparison_plan is None else len(comparison_plan.comparison_ids),
        has_comparison_filter=comparison_plan is not None,
        col_comparison=col_comparison,
    )

    df_result = df_stats.with_columns(
        pl.Series(
            name="DegreesFreedomA",
            values=anova_result.degrees_freedom_a,
            dtype=pl.Float64,
        ),
        pl.Series(
            name="DegreesFreedomB",
            values=anova_result.degrees_freedom_b,
            dtype=pl.Float64,
        ),
        pl.Series(
            name="DegreesFreedomInteraction",
            values=anova_result.degrees_freedom_interaction,
            dtype=pl.Float64,
        ),
        pl.Series(
            name="DegreesFreedomWithin",
            values=anova_result.degrees_freedom_within,
            dtype=pl.Float64,
        ),
        pl.Series(
            name="FStatisticA",
            values=anova_result.f_statistic_a,
            dtype=pl.Float64,
        ),
        pl.Series(
            name="FStatisticB",
            values=anova_result.f_statistic_b,
            dtype=pl.Float64,
        ),
        pl.Series(
            name="FStatisticInteraction",
            values=anova_result.f_statistic_interaction,
            dtype=pl.Float64,
        ),
        pl.Series(name="PValueA", values=p_value_a, dtype=pl.Float64),
        pl.Series(name="PValueB", values=p_value_b, dtype=pl.Float64),
        pl.Series(
            name="PValueInteraction", values=p_value_interaction, dtype=pl.Float64
        ),
        pl.Series(name="PAdjustA", values=p_adjust_a, dtype=pl.Float64),
        pl.Series(name="PAdjustB", values=p_adjust_b, dtype=pl.Float64),
        pl.Series(
            name="PAdjustInteraction", values=p_adjust_interaction, dtype=pl.Float64
        ),
    )
    df_result = pf_adapter.create_result_frame(
        df_result,
        cols_selected=list(SCHEMA_ANOVA_TWO_WAY_RESULT.keys()),
    )

    return df_result
