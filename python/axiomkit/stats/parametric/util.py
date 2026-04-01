from collections.abc import Mapping, Sequence

import polars as pl
import polars.selectors as pl_sel
from polars._typing import SchemaDict
from polars.datatypes import DataTypeClass

from .constant import (
    COL_FEATURE_INTERNAL,
    COL_FEATURE_ORDER,
)


def validate_required_columns(
    cols_in: Mapping[str, object] | Sequence[str], cols_required: list[str]
) -> None:
    cols_in = list(cols_in.keys()) if isinstance(cols_in, Mapping) else list(cols_in)
    cols_missing = set(cols_required) - set(cols_in)
    if cols_missing:
        raise ValueError(
            f"Input `df` is missing required columns: {', '.join(cols_missing)}."
        )


def create_summary_stat_columns(
    col_value: str,
):
    return [
        pl.col(col_value).count().alias("N"),
        pl.col(col_value).mean().alias("Mean"),
        pl.col(col_value).var(ddof=1).alias("Var"),
    ]


def normalize_value_frame(
    df: pl.DataFrame | pl.LazyFrame,
    cols_selected: list[str],
    *,
    cols_float: str | list[str] | None = None,
    cols_string: str | list[str] | None = None,
    col_feature: str | None = None,
):
    lf = df.lazy() if isinstance(df, pl.DataFrame) else df
    lf_values = lf.select(cols_selected)

    exprs_cast: list[pl.Expr] = []
    if cols_float is not None:
        exprs_cast.append(pl_sel.by_name(cols_float).cast(pl.Float64))
    if cols_string is not None:
        exprs_cast.append(pl_sel.by_name(cols_string).cast(pl.String))
    if exprs_cast:
        lf_values = lf_values.with_columns(*exprs_cast)

    if col_feature is None:
        lf_values = lf_values.with_columns(pl.lit("_all").alias(COL_FEATURE_INTERNAL))
    else:
        lf_values = lf_values.rename({col_feature: COL_FEATURE_INTERNAL})

    return lf_values


def create_feature_frame(lf: pl.LazyFrame):
    return (
        lf.select(COL_FEATURE_INTERNAL)
        .unique(maintain_order=True)
        .with_row_index(COL_FEATURE_ORDER)
    )


def select_result_columns(
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


def create_result_schema(
    col_feature: str | None,
    dtype_feature: pl.DataType | DataTypeClass | None,
    schema_result: SchemaDict,
) -> SchemaDict:
    schema: SchemaDict = {}
    if col_feature is not None:
        schema[col_feature] = dtype_feature or pl.String

    schema.update(schema_result)

    return schema
