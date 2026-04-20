from collections.abc import Mapping, Sequence

import polars as pl
from polars._typing import SchemaDict


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

def create_required_columns(*cols: str | None) -> list[str]:
    cols_ = list(cols)
    cols_ = [_col for _col in cols_ if _col is not None]
    return cols_


def read_frame_schema(df: pl.DataFrame | pl.LazyFrame) -> SchemaDict:
    return dict(df.schema if isinstance(df, pl.DataFrame) else df.collect_schema())
