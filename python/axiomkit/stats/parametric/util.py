import polars as pl

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
