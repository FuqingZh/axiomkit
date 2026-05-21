from collections.abc import Sequence
from dataclasses import dataclass
from typing import Self

import numpy as np
import polars as pl
from loguru import logger

from ...p_value import (
    PValueAdjustmentType,
    calculate_p_adjustment_array,
    normalize_p_value_adjustment_mode,
)
from ..constant import (
    COL_FEATURE_INTERNAL,
    COL_FEATURE_ORDER,
    COLS_SUMMARY_STATS,
)
from ..spec import ParametricFrameAdapter
from ..comparison import ParametricComparison, ParametricComparisonKind
from ..util import (
    create_required_columns,
    create_summary_stat_columns,
)
from .constant import (
    SCHEMA_T_TEST_ONE_SAMPLE_RESULT,
)
from .spec import (
    AlternativeHypothesisType,
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
    col_comparison: str | None = None,
    col_is_valid: str | None = None,
) -> None:
    if col_feature is not None and col_feature == col_value:
        raise ValueError("Arg `col_feature` must be different from `col_value`.")
    if col_comparison is not None:
        if col_feature is None:
            raise ValueError(
                "Arg `col_feature` is required when `col_comparison` is provided."
            )
        if col_comparison in {col_value, col_feature}:
            raise ValueError(
                "Arg `col_comparison` must be different from `col_value` and `col_feature`."
            )
    if col_is_valid is not None:
        if col_comparison is None:
            raise ValueError(
                "Arg `col_comparison` is required when `col_is_valid` is provided."
            )
        if col_is_valid in {col_value, col_feature, col_comparison}:
            raise ValueError(
                "Arg `col_is_valid` must be different from `col_value`, "
                "`col_feature`, and `col_comparison`."
            )


@dataclass(frozen=True, slots=True)
class OneSampleComparisonPlan:
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
            or _item.kind != ParametricComparisonKind.TTEST_ONE_SAMPLE
            for _item in items_comparison
        ):
            raise ValueError(
                "Arg `comparisons` must contain `ttest_one_sample` ParametricComparison items."
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

        return cls(comparison_ids=tuple(comparison_ids))


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
    col_comparison: str | None = None,
    col_is_valid: str | None = None,
    comparisons: ParametricComparison | Sequence[ParametricComparison] | None = None,
    rule_alternative: AlternativeHypothesisType | str = "two-sided",
    rule_p_adjust: PValueAdjustmentType | str | None = None,
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
        col_comparison: Optional name of the column defining comparison-specific
            statistical units.
        col_is_valid: Optional boolean column indicating whether a
            comparison-feature unit should enter testing.
        comparisons: Optional declared comparison plan using
            :class:`ParametricComparison.ttest_one_sample`.
        rule_alternative: Alternative hypothesis for the t-test. See :class:`AlternativeHypothesisType`.
            - ``two-sided``: (Default) Test if mean is different from `popmean`.
            - ``less``: Test if mean is less than `popmean`.
            - ``greater``: Test if mean is greater than `popmean`.
        rule_p_adjust: Method for adjusting p-values for multiple testing. See :class:`PValueAdjustmentType`.
            - ``None``: (Default) No adjustment; return raw p-values.
            - ``bonferroni``: Adjust p-values using the Bonferroni correction.
            - ``bh``: Adjust p-values using the Benjamini-Hochberg procedure.
            - ``by``: Adjust p-values using the Benjamini-Yekutieli procedure

    Returns:
        A Polars DataFrame containing one-sample t-test results for each feature, with columns:
            - Column named as `col_comparison` (if specified): Comparison layer
              for each row.
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
        from axiomkit.stats import ParametricComparison, calculate_t_test_one_sample
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
        # Example 3: Restrict comparison layers and adjust p-values within each
        # comparison.
        df = pl.DataFrame({
            "Comparison": ["cmp1", "cmp1", "cmp2", "cmp2"],
            "Feature": ["A", "B", "A", "B"],
            "Value": [5.1, 5.2, 4.9, 5.3]
        })
        result = calculate_t_test_one_sample(
            df,
            popmean=5.0,
            col_feature="Feature",
            col_comparison="Comparison",
            comparisons=ParametricComparison.ttest_one_sample("cmp1"),
            rule_p_adjust="bh",
        )
        ```
    """
    ############################################################
    # #region Validate input arguments
    validate_column_layout_one_sample(
        col_value,
        col_feature,
        col_comparison=col_comparison,
        col_is_valid=col_is_valid,
    )
    rule_alternative = normalize_alternative_hypothesis_mode(rule_alternative)
    rule_p_adjust = normalize_p_value_adjustment_mode(rule_p_adjust)
    comparison_plan = OneSampleComparisonPlan.from_inputs(comparisons)
    if comparison_plan is not None and col_comparison is None:
        raise ValueError(
            "Arg `col_comparison` is required when `comparisons` is provided."
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
    pf_adapter = ParametricFrameAdapter(
        df,
        col_feature=col_feature,
        col_comparison=col_comparison,
        col_is_valid=col_is_valid,
    ).select_required_cols(
        cols_required=create_required_columns(
            col_value,
            col_feature,
            col_comparison,
            col_is_valid if col_comparison is not None else None,
        )
    )
    schema_result = pf_adapter.create_result_schema(SCHEMA_T_TEST_ONE_SAMPLE_RESULT)
    # #endregion
    ############################################################
    # #region Calculate summary statistics for each feature and prepare data for t-test calculations
    pf_adapter.cast_cols(
        cols_float=col_value,
        cols_string=col_comparison,
        cols_boolean=col_is_valid if col_comparison is not None else None,
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
        lf_values = lf_values.filter(
            pl.col(col_comparison).is_in(comparison_plan.comparison_ids)
        )

    lf_stats = lf_values.group_by(COL_FEATURE_INTERNAL, maintain_order=True).agg(
        *create_summary_stat_columns(col_value),
    )
    df_stats = (
        lf_features.join(
            lf_stats,
            on=COL_FEATURE_INTERNAL,
            how="left",
        )
        .with_columns(
            pl.col("N").fill_null(0),
        )
        .sort(COL_FEATURE_ORDER)
        .collect()
    )
    if df_stats.height == 0:
        return pl.DataFrame(schema=schema_result)
    logger.debug(
        "One-sample t-test prepared {n_rows} rows for p-value adjustment "
        "(comparisons={n_comparisons}, has_comparison_filter={has_comparison_filter}, "
        "col_comparison={col_comparison}).",
        n_rows=df_stats.height,
        n_comparisons=0 if comparison_plan is None else len(comparison_plan.comparison_ids),
        has_comparison_filter=comparison_plan is not None,
        col_comparison=col_comparison,
    )
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
        pl.Series(
            name="PopMean", values=np.full(df_stats.height, popmean), dtype=pl.Float64
        ),
        *create_t_stat_columns(t_test_result, p_values=p_value, p_adjust=p_adjust),
    )
    df_result = pf_adapter.create_result_frame(
        df_result,
        cols_selected=list(SCHEMA_T_TEST_ONE_SAMPLE_RESULT.keys()),
    )
    # #endregion
    ############################################################

    return df_result
