import polars as pl


def get_row_chunk_size(*, width_df: int) -> int:
    """
    Return an appropriate row chunk size for processing based on dataframe width.

    Wider dataframes (with more columns) use smaller row chunks to limit the
    total amount of data processed at once, while narrower dataframes can use
    larger chunks.

    Args:
        width_df (int): The number of columns in the dataframe to be processed.

    Returns:
        int: Recommended number of rows per processing chunk:
             - 1_000 rows if ``width_df >= 8_000``
             - 2_000 rows if ``width_df >= 2_000`` and ``width_df < 8_000``
             - 10_000 rows otherwise.
    """
    # v1: fixed + simple steps.
    if width_df >= 8_000:
        return 1_000
    if width_df >= 2_000:
        return 2_000
    return 10_000


def create_row_chunks(
    df: pl.DataFrame, size_rows_chunk: int, cols_exprs: list[pl.Expr]
):
    n_rows_total = df.height
    n_row_cursor = 0
    while n_row_cursor < n_rows_total:
        n_rows_per_chunk = min(size_rows_chunk, n_rows_total - n_row_cursor)
        df_chunk = df.slice(offset=n_row_cursor, length=n_rows_per_chunk).select(
            cols_exprs
        )
        yield n_row_cursor, df_chunk
        n_row_cursor += n_rows_per_chunk
