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
N_SIZE_BYTES_HASH_DEFAULT = 8  # 64-bit -> 16 hex; reduce collision risk


def _derive_hex_hash(s: str, *, size_digest: int) -> str:
    """Return a hex hash of the input string using the given digest size (bytes)."""
    return hashlib.blake2b(s.encode("utf-8"), digest_size=int(size_digest)).hexdigest()


def _sanitize_and_truncate(
    value: str,
    *,
    size_bytes_seg_max: int = N_SIZE_BYTES_SEG_MAX,
    size_bytes_hash: int = N_SIZE_BYTES_HASH_DEFAULT,
) -> str:
    """
    Sanitize a partition segment and truncate it with a hash suffix if needed.

    This keeps the final UTF-8 segment length under ``size_bytes_seg_max`` while
    preserving uniqueness via a hash suffix when truncation occurs.
    """
    value = unicodedata.normalize("NFKC", value)  # normalize unicode
    if len(value_encoded := value.encode("utf-8")) <= size_bytes_seg_max:
        return value

    len_hash_suffix_byte = 2 * size_bytes_hash
    if (len_seg_max := size_bytes_seg_max - (1 + len_hash_suffix_byte)) < 16:
        raise ValueError(
            "Arg `size_bytes_seg_max` too small for hash suffix: "
            f"max_seg_bytes={size_bytes_seg_max}, hash_bytes={size_bytes_hash}. "
            "Increase `size_bytes_seg_max` or decrease `size_bytes_hash`."
        )
    value_truncated = value_encoded[:len_seg_max]
    while value_truncated and (value_truncated[-1] & 0b1100_0000) == 0b1000_0000:
        value_truncated = value_truncated[:-1]
    value_truncated = value_truncated.decode("utf-8", errors="ignore")
    return f"{value_truncated}~{_derive_hex_hash(value, size_digest=size_bytes_hash)}"


def _sanitize_partition_cols(
    expr: pl.Expr,
    *,
    size_bytes_seg_max: int = N_SIZE_BYTES_SEG_MAX,
    size_bytes_hash: int = N_SIZE_BYTES_HASH_DEFAULT,
) -> pl.Expr:
    """
    Sanitize partition column values into filesystem-safe path segments.

    The output is a Polars expression that normalizes unicode, removes or
    replaces illegal characters, and truncates long segments with a hash suffix.
    """
    expr_ = (
        expr.cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .fill_null(C_HIVE_NULL)
        # normalize void/problematic values
        .str.replace_all(r"[\\/]", "_")  # replace path separators
        .str.replace_all(r'[:*?"<>|]', "_")  # replace Windows illegal chars
        .str.replace_all(r"\s+", " ")  # normalize whitespace
        .str.replace_all(r"\u0000", "_")  # replace null bytes
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
    is_long_segment = expr_.str.len_bytes() > size_bytes_seg_max
    expr_ = (
        pl.when(is_long_segment)
        .then(
            expr_.map_elements(
                lambda x: _sanitize_and_truncate(
                    x,
                    size_bytes_seg_max=size_bytes_seg_max,
                    size_bytes_hash=size_bytes_hash,
                ),
                return_dtype=pl.Utf8,
            )
        )
        .otherwise(expr_)
    )

    return expr_


def _validate_overwrite_permissions(dir_out: Path, dir_allowed: Path | None) -> None:
    """
    Validate that overwriting ``dir_out`` is safe and allowed.

    Refuses to overwrite root, home, or symlink directories. If ``dir_allowed``
    is provided, ``dir_out`` must be within it.
    """
    dir_out_abs = dir_out.expanduser().resolve()
    if dir_out_abs == Path("/"):
        raise PermissionError("Refusing to overwrite root directory '/'.")
    if dir_out_abs == Path.home():
        raise PermissionError("Refusing to overwrite user's home directory.")
    if dir_out_abs.exists() and dir_out_abs.is_symlink():
        raise PermissionError(
            f"Refusing to overwrite symbolic link directory: `{dir_out_abs}`."
        )
    if dir_allowed is not None:
        dir_allowed_abs = dir_allowed.expanduser().resolve()
        try:
            dir_out_abs.relative_to(dir_allowed_abs)
        except ValueError:
            raise PermissionError(
                f"Output directory `{dir_out_abs}` is outside the allowed directory `{dir_allowed_abs}`."
            )


def _estimate_compressed_bytes_per_row(
    lf: pl.LazyFrame,
    sample_rows: int = 200_000,
    compression: str = "zstd",
    compression_level: int = 5,
) -> float:
    """
    Estimate average compressed bytes per row by sampling and writing to memory.

    Args:
        lf: Input Polars LazyFrame to sample from.
        sample_rows: Number of rows to sample for estimation.
        compression: Compression algorithm (e.g., "zstd").
        compression_level: Compression level for the algorithm.

    Returns:
        Estimated compressed bytes per row (minimum 1.0).
    """
    # 采样并实际压缩到内存，得到“磁盘上压缩后”的字节/行
    lf_sample = lf.limit(sample_rows)
    row_count = lf_sample.select(pl.len()).collect().item()
    buf = io.BytesIO()
    lf_sample.sink_parquet(
        buf,
        compression=compression,
        compression_level=compression_level,
        data_page_size=1 << 20,
        # 让行组不至于太小，避免被极端页/组开销误导
        row_group_size=max(50_000, row_count // 8 or 10_000),
    )
    return max(1.0, buf.tell() / max(1, row_count))  # 至少 1B/row，避免除0/极端值


def sink_parquet_dataset(
    df: pl.LazyFrame | pl.DataFrame,
    dir_out: Path,
    *,
    cols_partitioning: str | Sequence[str] | None = None,
    lvl_compression: int = 5,
    size_mib_per_file_max: int = 8 * 16,
    size_mib_per_row_group_max: int = 8 * 4,
    size_bytes_hash: int = N_SIZE_BYTES_HASH_DEFAULT,
    should_overwrite: bool = False,
    dir_allowed: Path | None = None,
) -> None:
    """
    Write a Parquet dataset with optional Hive-style partitioning.

    Args:
        df: Input DataFrame or LazyFrame.
        dir_out: Output directory for the dataset.
        cols_partitioning: Column name(s) to partition by. If None, no partitioning.
        lvl_compression: Zstandard compression level (1-22).
        size_mib_per_file_max: Maximum file size in MiB (rounded up to 8 MiB).
        size_mib_per_row_group_max: Maximum row group size in MiB (rounded up to 8 MiB).
        size_bytes_hash: Hash suffix size in bytes for truncating long partition values.
        should_overwrite: If True, overwrite existing output directory.
        dir_allowed: Optional base directory that bounds overwrite permissions.

    Raises:
        ValueError: If partition columns are missing or compression level is invalid.
        FileExistsError: If output directory is non-empty and overwrite is False.
        PermissionError: If overwrite is unsafe or outside ``dir_allowed``.

    Notes:
        - Empty inputs create a single ``__EMPTY__.parquet`` file.
        - Partition column values are sanitized to avoid filesystem issues.
        - Row group and file sizes are derived from a compressed byte estimate.
    """
    if not (1 <= (lvl_compression := int(lvl_compression)) <= 22):
        raise ValueError("Compression level must be between 1 and 22.")

    if (size_mib_per_file_limit := max(32, int(size_mib_per_file_max))) % 8 != 0:
        size_mib_per_file_limit += 8 - (size_mib_per_file_limit % 8)
    if (
        size_mib_per_row_group_limit := min(
            size_mib_per_file_limit, int(size_mib_per_row_group_max)
        )
    ) % 8 != 0:
        size_mib_per_row_group_limit += 8 - (size_mib_per_row_group_limit % 8)
    size_bytes_per_file_limit = size_mib_per_file_limit * (1 << 20)

    if (not should_overwrite) and dir_out.exists() and any(dir_out.iterdir()):
        raise FileExistsError(
            f"Arg `should_overwrite` is False, but output directory `{dir_out}` is not empty."
        )

    if should_overwrite and dir_out.exists():
        _validate_overwrite_permissions(dir_out=dir_out, dir_allowed=dir_allowed)
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
    is_empty = df.limit(1).collect().height == 0
    if is_empty:
        pl.LazyFrame(schema=df.collect_schema()).sink_parquet(
            path=dir_out / "__EMPTY__.parquet",
            compression="zstd",
            compression_level=lvl_compression,
            data_page_size=1 << 20,
            row_group_size=10_000,
        )

        return

    if cols_partitioning:
        missing_cols = [
            column for column in cols_partitioning if column not in df.collect_schema().names()
        ]
        if missing_cols:
            raise ValueError(
                f"Partition column(s) not found in DataFrame columns: `{missing_cols}`."
            )

        df = df.with_columns(
            [
                _sanitize_partition_cols(
                    pl.col(column), size_bytes_hash=size_bytes_hash
                ).alias(column)
                for column in cols_partitioning
            ]
        )

    size_bytes_per_row = _estimate_compressed_bytes_per_row(
        lf=df,
        compression_level=lvl_compression,
    )
    rows_per_file_max = max(
        50_000, int(size_bytes_per_file_limit / size_bytes_per_row)
    )
    rows_per_row_group_max = max(
        10_000,
        int(
            min(size_mib_per_row_group_limit, size_mib_per_file_limit // 8)
            / size_bytes_per_row
        ),
    )
    # 保障每文件至少有2个 row groups；且行组不超过文件行数的一半
    rows_per_row_group_max = min(
        rows_per_row_group_max, max(10_000, rows_per_file_max // 2)
    )

    df.sink_parquet(
        pl.PartitionBy(
            base_path=dir_out,
            key=cols_partitioning,
            max_rows_per_file=rows_per_file_max,
        ),
        compression="zstd",
        compression_level=lvl_compression,
        data_page_size=1 << 20,
        row_group_size=rows_per_row_group_max,
        mkdir=True,  # ! unstable API but needed
    )
