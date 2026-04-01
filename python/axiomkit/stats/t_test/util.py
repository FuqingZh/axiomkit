import numpy as np
import polars as pl
from polars._typing import SchemaDict
from polars.datatypes import DataTypeClass
from scipy import stats as sci_stats

from axiomkit.stats.t_test.constant import COL_FEATURE_INTERNAL, COL_FEATURE_ORDER

from .spec import AlternativeHypothesisMode, TStatisticsResult


def validate_required_columns(cols_in: list[str], cols_required: list[str]) -> None:
    cols_missing = set(cols_required) - set(cols_in)
    if cols_missing:
        raise ValueError(
            f"Input `df` is missing required columns: {', '.join(cols_missing)}."
        )


def normalize_alternative_hypothesis_mode(
    value: AlternativeHypothesisMode | str,
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


def create_t_test_result_schema(
    col_feature: str | None,
    dtype_feature: pl.DataType | DataTypeClass | None,
    schema_t_test_result: SchemaDict,
) -> SchemaDict:
    schema: SchemaDict = {}
    if col_feature is not None:
        schema[col_feature] = dtype_feature or pl.String

    schema.update(schema_t_test_result)

    return schema


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


def create_t_test_result_columns(
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


def create_summary_stat_columns(
    col_value: str,
):
    return [
        pl.col(col_value).count().alias("N"),
        pl.col(col_value).mean().alias("Mean"),
        pl.col(col_value).var(ddof=1).alias("Var"),
    ]


def normalize_t_test_value_frame(
    df: pl.DataFrame | pl.LazyFrame,
    cols_selected: list[str],
    *,
    col_float: str | None = None,
    col_string: str | None = None,
    col_feature: str | None = None,
):
    lf = df.lazy() if isinstance(df, pl.DataFrame) else df
    lf_values = lf.select(cols_selected)

    exprs_cast: list[pl.Expr] = []
    if col_float is not None:
        exprs_cast.append(pl.col(col_float).cast(pl.Float64))
    if col_string is not None:
        exprs_cast.append(pl.col(col_string).cast(pl.String))
    if exprs_cast:
        lf_values = lf_values.with_columns(*exprs_cast)

    if col_feature is None:
        lf_values = lf_values.with_columns(
            pl.lit("__all__").alias(COL_FEATURE_INTERNAL)
        )
    else:
        lf_values = lf_values.rename({col_feature: COL_FEATURE_INTERNAL})

    return lf_values


def create_feature_frame(lf: pl.LazyFrame):
    return (
        lf.select(COL_FEATURE_INTERNAL)
        .unique(maintain_order=True)
        .with_row_index(COL_FEATURE_ORDER)
    )


def select_t_test_result_columns(
    df_result: pl.DataFrame,
    cols_selected: list[str],
    col_feature: str | None,
) -> pl.DataFrame:
    cols_selected_ = cols_selected.copy()
    if col_feature is not None:
        df_result = df_result.rename({COL_FEATURE_INTERNAL: col_feature})
        cols_selected_.insert(0, col_feature)
    return df_result.select(cols_selected_)


def create_required_columns(*cols: str | None) -> list[str]:
    cols_ = list(cols)
    cols_ = [_col for _col in cols_ if _col is not None]
    return cols_


def read_frame_schema(df: pl.DataFrame | pl.LazyFrame) -> SchemaDict:
    return dict(df.schema if isinstance(df, pl.DataFrame) else df.collect_schema())
