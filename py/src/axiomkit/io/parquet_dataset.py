import hashlib
import io
import shutil
import unicodedata
from collections.abc import Sequence
from pathlib import Path

import polars as pl

C_HIVE_NULL = "__HIVE_DEFAULT_PARTITION__"
N_SIZE_BYTES_SEG_MAX = 240
N_SIZE_BYTES_SUFFIX = 1 + 8  # ~ + 8-char hash
N_SIZE_BYTES_CUT = N_SIZE_BYTES_SEG_MAX - N_SIZE_BYTES_SUFFIX


def _hash8(s: str) -> str:
    """Computes an 8-character hexadecimal hash of the input string."""

    return hashlib.blake2b(s.encode("utf-8"), digest_size=4).hexdigest()


def _truncate_bytes(s: str):
    s = unicodedata.normalize("NFKC", s)  # normalize unicode
    if len(s_ := s.encode("utf-8")) <= N_SIZE_BYTES_SEG_MAX:
        return s

    s_cut = s_[:N_SIZE_BYTES_CUT]
    while s_cut and (s_cut[-1] & 0b1100_0000) == 0b1000_0000:
        s_cut = s_cut[:-1]
    s_cut = s_cut.decode("utf-8", errors="ignore")
    return f"{s_cut}~{_hash8(s)}"


def _sanitize_partition_cols(expr: pl.Expr) -> pl.Expr:
    expr_ = (
        expr.cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .fill_null(C_HIVE_NULL)
        # normalize void/problematic values
        .str.replace_all(r"[\\/]", "_")  # replace path separators
        .str.replace_all(r'[:*?"<>|]', "_")  # replace Windows illegal chars
        .str.replace_all(r"\s+", " ")  # normalize whitespace
        # prevent directory traversal and hidden files
        .str.replace_all(r"^\.+", "")  # drop leading dots
        .str.replace_all(r"\.\.", "_")  # prevent directory traversal (..)
        .str.replace_all(r"[ \.]+$", "")  # remove trailing spaces/dots
        # unify empty strings and NULLs
        .str.replace_all(r"^$", C_HIVE_NULL)  # avoid path separator issues
        # prevent leading tilde, which may cause issues on some file systems
        .str.replace_all(r"^~", "_~")
    )

    # Truncate overly long segments with hash suffix.
    # First, use vectorized operations to directly route short values through the Rust/Polars fast path,
    # allowing only a very small number of extremely long strings (longer than N_LEN_SEG_MAX) to fall into the Python UDF.
    # This optimization prevents applying Python callbacks to all strings for performance reasons,
    # reduces the total cost from O(N) Python calls to O(K), where K is the number of long strings,
    # and K << N in typical scenarios.
    b_is_long = expr_.str.len_bytes() > N_SIZE_BYTES_SEG_MAX
    expr_ = (
        pl.when(b_is_long)
        .then(expr_.map_elements(_truncate_bytes, return_dtype=pl.Utf8))
        .otherwise(expr_)
    )

    return expr_


def _compressed_bytes_per_row(
    lf: pl.LazyFrame,
    sample_rows: int = 200_000,
    compression: str = "zstd",
    compression_level: int = 5,
) -> float:
    """
    Estimate the average compressed bytes per row for a Polars LazyFrame by sampling and writing to an in-memory Parquet file.

    Args:
        lf (pl.LazyFrame): The input Polars LazyFrame to sample from.
        sample_rows (int, optional): Number of rows to sample for estimation. Defaults to 200_000.
        compression (str, optional): Compression algorithm to use (e.g., "zstd"). Defaults to "zstd".
        compression_level (int, optional): Compression level for the algorithm. Defaults to 5.

    Returns:
        float: Estimated compressed bytes per row (minimum 1.0).
    """
    # 采样并实际压缩到内存，得到“磁盘上压缩后”的字节/行
    lf_sample = lf.limit(sample_rows)
    n_rows = lf_sample.select(pl.len()).collect().item()
    buf = io.BytesIO()
    lf_sample.sink_parquet(
        buf,
        compression=compression,
        compression_level=compression_level,
        data_page_size=1 << 20,
        # 让行组不至于太小，避免被极端页/组开销误导
        row_group_size=max(50_000, n_rows // 8 or 10_000),
    )
    return max(1.0, buf.tell() / max(1, n_rows))  # 至少 1B/row，避免除0/极端值


def write_parquet_dataset(
    df: pl.LazyFrame | pl.DataFrame,
    dir_out: Path,
    *,
    cols_partitioning: str | Sequence[str] | None = None,
    lvl_compression: int = 5,
    size_mib_per_file_max: int = 8 * 16,
    size_mib_per_row_group_max: int = 8 * 4,
    if_overwrite: bool = True,
) -> None:
    """
    Writes a Polars DataFrame to a directory in Parquet format, with optional partitioning.

    Args:
        df (pl.LazyFrame | pl.DataFrame): The input DataFrame to be written.
        dir_out (Path): The output directory where the Parquet files will be saved.
        cols_partitioning (str | Sequence[str] | None, optional):
            The column name or a sequence of column names to partition the data by.
            If a sequence is provided, multi-column partitioning is applied.
            If None, no partitioning is applied.
            Defaults to None.
        lvl_compression (int, optional):
            The compression level for Zstandard (1-22).
            Defaults to 5.
        size_mib_per_file_max (int, optional):
            The maximum size of each Parquet file in MiB.
            Defaults to 128 MiB.
        size_mib_per_row_group_max (int, optional):
            The maximum size of each row group in MiB.
            Defaults to 32 MiB.
        if_overwrite (bool, optional):
            Whether to overwrite the output directory if it already exists.
            Defaults to True.

    Raises:
        ValueError: If the specified partition column is not found in the DataFrame.
        RuntimeError: If the cardinality of the partition column exceeds 10,000.

    ## Notes:
        - If the DataFrame is empty, a single Parquet file named `__EMPTY__.parquet`
          will be created.
        - The function ensures that partition column values are sanitized to avoid
          path separator issues and other potential problems.
        - The function estimates the average compressed bytes per row to determine
          appropriate row group and file sizes.

    Examples:
        ```python
        from pathlib import Path
        import polars as pl
        # Create a sample DataFrame
        df = pl.LazyFrame({
            "id": [1, 2, 3, 4],
            "value": ["A", "B", "A", "B"],
            "data": [10.5, 20.3, 30.1, 40.2]
        })
        # Write the DataFrame to Parquet files partitioned by 'value'
        write_parquet_dataset(
            df=df,
            dir_out=Path("output/parquet_data"),
            cols_partitioning="value",
        )
        # Write the DataFrame to Parquet files partitioned by multiple columns
        write_parquet_dataset(
            df=df,
            dir_out=Path("output/parquet_data_multi"),
            cols_partitioning=["value", "id"],
        )
        ```
    """
    if not (1 <= (lvl_compression := int(lvl_compression)) <= 22):
        raise ValueError("Compression level must be between 1 and 22.")

    if (n_size_mib_per_file_max := max(32, int(size_mib_per_file_max))) % 8 != 0:
        n_size_mib_per_file_max += 8 - (n_size_mib_per_file_max % 8)
    if (
        n_size_mib_per_row_group_max := min(
            n_size_mib_per_file_max, int(size_mib_per_row_group_max)
        )
    ) % 8 != 0:
        n_size_mib_per_row_group_max += 8 - (n_size_mib_per_row_group_max % 8)
    n_size_bytes_per_file_max = n_size_mib_per_file_max * (1 << 20)

    if (not if_overwrite) and any(dir_out.iterdir()):
        raise FileExistsError(
            f"Arg `if_overwrite` is False, but output directory `{dir_out}` is not empty."
        )

    if if_overwrite and dir_out.exists():
        shutil.rmtree(dir_out, ignore_errors=True)
    dir_out.mkdir(parents=True, exist_ok=True)

    cols_partitioning = (
        cols_partitioning
        if cols_partitioning is None
        else (
            [cols_partitioning]
            if isinstance(cols_partitioning, str)
            else list(cols_partitioning)
        )
    )

    df = df.lazy() if isinstance(df, pl.DataFrame) else df
    b_is_empty = df.limit(1).collect().height == 0
    if b_is_empty:
        pl.LazyFrame(schema=df.collect_schema()).sink_parquet(
            path=dir_out / "__EMPTY__.parquet",
            compression="zstd",
            compression_level=lvl_compression,
            data_page_size=1 << 20,
            row_group_size=10_000,
        )

        return

    if cols_partitioning:
        l_cols_miss = [
            _c for _c in cols_partitioning if _c not in df.collect_schema().names()
        ]
        if l_cols_miss:
            raise ValueError(
                f"Partition column(s) not found in DataFrame columns: `{l_cols_miss}`."
            )

        df = df.with_columns(
            [_sanitize_partition_cols(pl.col(_c)).alias(_c) for _c in cols_partitioning]
        )

    n_size_bytes_per_row = _compressed_bytes_per_row(
        lf=df,
        compression_level=lvl_compression,
    )
    n_rows_per_file_max = max(
        50_000, int(n_size_bytes_per_file_max / n_size_bytes_per_row)
    )
    n_rows_per_row_group_max = max(
        10_000,
        int(
            min(n_size_mib_per_row_group_max, n_size_mib_per_file_max // 8)
            / n_size_bytes_per_row
        ),
    )
    # 保障每文件至少有2个 row groups；且行组不超过文件行数的一半
    n_rows_per_row_group_max = min(
        n_rows_per_row_group_max, max(10_000, n_rows_per_file_max // 2)
    )

    _scheme_partitioning = (
        pl.PartitionMaxSize(
            base_path=dir_out,
            max_size=n_rows_per_file_max,
        )
        if cols_partitioning is None
        else pl.PartitionByKey(
            base_path=dir_out,
            by=cols_partitioning,
        )
    )
    df.sink_parquet(
        _scheme_partitioning,
        compression="zstd",
        compression_level=lvl_compression,
        data_page_size=1 << 20,
        row_group_size=n_rows_per_row_group_max,
        mkdir=True,  # ! unstable API but needed
    )
