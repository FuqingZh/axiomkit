import math
from collections import defaultdict
from collections.abc import Generator, Sequence
from typing import Any

import polars as pl

from .conf import (
    N_LEN_EXCEL_SHEET_NAME_MAX,
    N_NCOLS_EXCEL_MAX,
    N_NROWS_EXCEL_MAX,
    TUP_EXCEL_ILLEGAL,
    ColumnIdentifier,
)
from .spec import (
    SpecCellBorder,
    SpecSheetHorizontalMerge,
    SpecSheetSlice,
    SpecXlsxReport,
    SpecXlsxRowChunkPolicy,
    SpecXlsxValuePolicy,
)

################################################################################
# #region CellValueConversion


def convert_nan_inf_to_str(*, x: float, value_policy: SpecXlsxValuePolicy) -> str:
    if math.isnan(x):
        return value_policy.nan_str
    if math.isinf(x):
        return value_policy.posinf_str if x > 0 else value_policy.neginf_str
    raise ValueError("Input is neither NaN nor Inf.")


def convert_cell_value(
    *,
    value: Any,
    if_is_numeric_col: bool,
    if_is_integer_col: bool,
    if_keep_missing_values: bool,
    value_policy: SpecXlsxValuePolicy,
) -> Any:
    if value is None:
        return value_policy.missing_value_str if if_keep_missing_values else None
    if not if_is_numeric_col:
        return str(value)
    if if_is_integer_col:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, float):
            if value_policy.integer_coerce == "coerce":
                return int(value)
            if value.is_integer():
                return int(value)
            return str(value)
        if value_policy.integer_coerce == "coerce":
            try:
                return int(value)
            except Exception:
                return str(value)
        s_val = str(value)
        if s_val.lstrip("+-").isdigit():
            return int(s_val)
        return s_val

    try:
        n_cell_float_value = float(value)
    except Exception:
        return str(value)
    if not math.isfinite(n_cell_float_value):
        return (
            convert_nan_inf_to_str(x=n_cell_float_value, value_policy=value_policy)
            if if_keep_missing_values
            else None
        )

    return n_cell_float_value


# #endregion
################################################################################
# #region DataFrameUtils


def convert_to_polars(df: Any) -> pl.DataFrame:
    return df if isinstance(df, pl.DataFrame) else pl.DataFrame(df)


def _normalize_col_index(df: pl.DataFrame, ref: ColumnIdentifier) -> int:
    if isinstance(ref, int):
        return ref
    try:
        return df.columns.index(ref)
    except ValueError as e:
        raise KeyError(f"Column not found: {ref!r}") from e


def select_sorted_indices_from_refs(
    df: pl.DataFrame, refs: Sequence[ColumnIdentifier] | None
) -> tuple[int, ...]:
    if not refs:
        return ()
    idx = {_normalize_col_index(df, _r) for _r in refs}
    return tuple(sorted(idx))


def validate_unique_columns(df: pl.DataFrame) -> None:
    l_cols = df.columns

    # fast path: no duplicates
    if len(l_cols) == len(set(l_cols)):
        return

    # slow path: collect details only when duplicates exist
    dict_pos: dict[str, list[int]] = defaultdict(list)
    for _idx, _val in enumerate(l_cols):
        dict_pos[_val].append(_idx)

    c_msg = "; ".join(
        f"{c_name!r} x{len(l_pos)} at indices {l_pos}"
        for c_name, l_pos in dict_pos.items()
        if len(l_pos) > 1
    )
    raise ValueError(f"Duplicate column names detected: {c_msg}")


def select_integer_cols(
    df: pl.DataFrame, cols_idx_num: tuple[int, ...]
) -> tuple[int, ...]:
    l_cols_idx_int: list[int] = []
    for _idx in cols_idx_num:
        cfg_col_dtype = df.schema[df.columns[_idx]]
        if cfg_col_dtype.is_integer():
            l_cols_idx_int.append(_idx)
    return tuple(l_cols_idx_int)


def select_numeric_cols(df: pl.DataFrame) -> tuple[int, ...]:
    l_cols_idx_num: list[int] = []
    for _idx, _val in enumerate(df.columns):
        if df.schema[_val].is_numeric():
            l_cols_idx_num.append(_idx)
    return tuple(l_cols_idx_num)


# #endregion
################################################################################
# #region RowChunking


def calculate_row_chunk_size(
    *, width_df: int, policy: SpecXlsxRowChunkPolicy
) -> int:
    """
    Return an appropriate row chunk size for processing based on dataframe width.

    Wider dataframes (with more columns) use smaller row chunks to limit the
    total amount of data processed at once, while narrower dataframes can use
    larger chunks.

    Args:
        width_df (int): The number of columns in the dataframe to be processed.

    Returns:
        int: Recommended number of rows per processing chunk, using thresholds
        defined in ``policy`` (or ``policy.fixed_size`` if provided).
    """
    if policy.fixed_size is not None:
        return policy.fixed_size
    if width_df >= policy.width_large:
        return policy.size_large
    if width_df >= policy.width_medium:
        return policy.size_medium
    return policy.size_default


def generate_row_chunks(
    df: pl.DataFrame, size_rows_chunk: int, cols_exprs: list[pl.Expr]
) -> Generator[tuple[int, pl.DataFrame], Any, None]:
    n_rows_total = df.height
    n_row_cursor = 0
    while n_row_cursor < n_rows_total:
        n_rows_per_chunk = min(size_rows_chunk, n_rows_total - n_row_cursor)
        df_chunk = df.slice(offset=n_row_cursor, length=n_rows_per_chunk).select(
            cols_exprs
        )
        yield n_row_cursor, df_chunk
        n_row_cursor += n_rows_per_chunk


# #endregion
################################################################################
# #region SheetNormalization


def sanitize_sheet_name(name: str, *, replace_to: str = "_") -> str:
    for ch in TUP_EXCEL_ILLEGAL:
        name = name.replace(ch, replace_to)
    name = name.strip() or "Sheet"
    return name[:N_LEN_EXCEL_SHEET_NAME_MAX]


def generate_sheet_slices(
    *,
    height_df: int,
    width_df: int,
    height_header: int,
    sheet_name: str,
    report: SpecXlsxReport,
) -> list[SpecSheetSlice]:
    if height_header <= 0:
        raise ValueError("height_header must be >= 1.")
    if (n_rows_data_max := N_NROWS_EXCEL_MAX - height_header) <= 0:
        raise ValueError(
            f"Header too tall: height_header={height_header} exceeds Excel limit."
        )

    l_col_slices: list[tuple[int, int]] = []
    n_col_start = 0
    while n_col_start < width_df:
        n_col_end = min(width_df, n_col_start + N_NCOLS_EXCEL_MAX)
        l_col_slices.append((n_col_start, n_col_end))
        n_col_start = n_col_end

    l_row_slices: list[tuple[int, int]] = []
    n_row_start = 0
    while n_row_start < height_df:
        n_row_end = min(height_df, n_row_start + n_rows_data_max)
        l_row_slices.append((n_row_start, n_row_end))
        n_row_start = n_row_end
    if not l_row_slices:
        # Ensure we still create a sheet to write headers for 0-row dataframes.
        l_row_slices.append((0, 0))

    n_parts_total = len(l_col_slices) * len(l_row_slices)

    l_sheet_parts: list[SpecSheetSlice] = []
    n_idx_part = 1
    for _col_start, _col_end in l_col_slices:
        for _row_start, _row_end in l_row_slices:
            # IMPORTANT:
            # - If we do NOT split, keep the original sheet name (no "_1").
            # - Only add suffix when we actually have >1 parts.
            c_part_sheet_name = (
                sheet_name
                if n_parts_total == 1
                else _create_sheet_identifier(sheet_name, n_idx_part)
            )
            l_sheet_parts.append(
                SpecSheetSlice(
                    sheet_name=c_part_sheet_name,
                    row_start_inclusive=_row_start,
                    row_end_exclusive=_row_end,
                    col_start_inclusive=_col_start,
                    col_end_exclusive=_col_end,
                )
            )
            n_idx_part += 1

    if n_parts_total > 1:
        report.warn(
            f"Excel limit overflow: split into {len(l_sheet_parts)} sheets (columns-first, then rows)."
        )
    return l_sheet_parts


def _create_sheet_identifier(base_name: str, part_idx_1based: int) -> str:
    c_sheet_name_suffix = f"_{part_idx_1based}"
    n_len_base_name_max = N_LEN_EXCEL_SHEET_NAME_MAX - len(c_sheet_name_suffix)
    c_sheet_name_base = base_name[: max(1, n_len_base_name_max)]
    return f"{c_sheet_name_base}{c_sheet_name_suffix}"


# #endregion
################################################################################
# #region HeaderMergeUtils


def derive_contiguous_ranges(sorted_indices: Sequence[int]) -> list[tuple[int, int]]:
    """
    Convert a sorted sequence of indices into a list of contiguous index ranges.

    Args:
        sorted_indices (Sequence[int]): Sorted sequence of integer indices to group
            into contiguous ranges.

    Returns:
        list[tuple[int, int]]: A list of (start, end) tuples, each representing an
            inclusive contiguous range of indices.

    Examples:
        >>> derive_contiguous_ranges([0, 1, 2, 4, 5, 7])
        [(0, 2), (4, 5), (7, 7)]
        >>> derive_contiguous_ranges([])
        []
    """
    if not sorted_indices:
        return []
    l_contiguous_ranges: list[tuple[int, int]] = []
    n_idx_start = n_idx_end = sorted_indices[0]
    for _idx in sorted_indices[1:]:
        if _idx == n_idx_end + 1:
            n_idx_end = _idx
        else:
            l_contiguous_ranges.append((n_idx_start, n_idx_end))
            n_idx_start = n_idx_end = _idx
    l_contiguous_ranges.append((n_idx_start, n_idx_end))
    return l_contiguous_ranges


def plan_horizontal_merges(
    header_grid: list[list[str]],
) -> dict[int, list[SpecSheetHorizontalMerge]]:
    dict_horizontal_merges_map: dict[int, list[SpecSheetHorizontalMerge]] = {}
    if not header_grid:
        return dict_horizontal_merges_map

    n_rows = len(header_grid)
    n_cols = len(header_grid[0])
    for _row_idx in range(n_rows):
        l_row_val_ = header_grid[_row_idx]
        n_col_idx_ = 0
        while n_col_idx_ < n_cols:
            c_cell_val_ = l_row_val_[n_col_idx_]
            if not c_cell_val_:
                n_col_idx_ += 1
                continue

            n_col_idx_end_ = n_col_idx_ + 1
            while n_col_idx_end_ < n_cols and l_row_val_[n_col_idx_end_] == c_cell_val_:
                n_col_idx_end_ += 1

            if n_col_idx_end_ - n_col_idx_ > 1:
                dict_horizontal_merges_map.setdefault(_row_idx, []).append(
                    SpecSheetHorizontalMerge(
                        row_idx_start=_row_idx,
                        col_idx_start=n_col_idx_,
                        col_idx_end=n_col_idx_end_ - 1,
                        text=c_cell_val_,
                    )
                )
            n_col_idx_ = n_col_idx_end_

    return dict_horizontal_merges_map


def _generate_vertical_runs(
    header_grid: Sequence[Sequence[str]],
) -> Generator[tuple[int, int, int, str], Any, None]:
    """
    Yield vertical runs (length > 1) of identical, non-empty cells.

    Emits tuples (col_idx, row_start, row_end, value).
    """
    if not header_grid:
        return

    n_rows = len(header_grid)
    n_cols = len(header_grid[0])

    for _col_idx in range(n_cols):
        n_row_idx_start_ = 0
        while n_row_idx_start_ < n_rows:
            c_val_cell_current_ = header_grid[n_row_idx_start_][_col_idx]
            if not c_val_cell_current_:
                n_row_idx_start_ += 1
                continue

            n_row_idx_next_ = n_row_idx_start_ + 1

            # is continuing run
            while (
                n_row_idx_next_ < n_rows
                and header_grid[n_row_idx_next_][_col_idx] == c_val_cell_current_
            ):
                n_row_idx_next_ += 1

            n_len_vertical_run = n_row_idx_next_ - n_row_idx_start_
            if n_len_vertical_run > 1:
                yield (
                    _col_idx,
                    n_row_idx_start_,
                    n_row_idx_next_ - 1,
                    c_val_cell_current_,
                )

            n_row_idx_start_ = n_row_idx_next_


def plan_vertical_visual_merge_borders(
    header_grid: Sequence[Sequence[str]],
) -> dict[tuple[int, int], SpecCellBorder]:
    """
    Generate border specifications for visualizing vertically merged header cells.

    The function scans each column of the provided header grid and finds
    consecutive rows that contain the same non-empty value. Each such run
    is treated as a vertical merge block, and border information is generated
    for all cells in the block.

    Args:
        header_grid: A 2D sequence of strings representing the header cells,
            indexed as ``header_grid[row_index][column_index]``. All rows are
            expected to have the same number of columns.

    Returns:
        dict[tuple[int, int], SpecCellBorder]: A mapping from ``(row_index,
        column_index)`` cell coordinates to ``SpecCellBorder`` instances for cells
        that belong to a vertical merge block. For each such cell, the border
        spec indicates which borders (top, bottom, left, right) should be drawn
        to render the merged region.
    """
    dict_plan_vertical_merge_border: dict[tuple[int, int], SpecCellBorder] = {}
    for col_idx, row_start, row_end, _ in _generate_vertical_runs(header_grid):
        for _row_idx_within_merge in range(row_start, row_end + 1):
            dict_plan_vertical_merge_border[(_row_idx_within_merge, col_idx)] = (
                SpecCellBorder(
                    top=1 if _row_idx_within_merge == row_start else 0,
                    bottom=1 if _row_idx_within_merge == row_end else 0,
                    left=1,
                    right=1,
                )
            )
        # blank out text for non-top cells (handled by writer)
    return dict_plan_vertical_merge_border


def apply_vertical_run_text_blankout(header_grid: list[list[str]]) -> list[list[str]]:
    """
    Clear text from vertically merged header cells, keeping only the top cell's text.

    The function iterates over vertical runs of identical, non-empty header values
    (as identified by :func:`_iter_vertical_runs`) and blanks out the text in all
    cells below the first row of each run. This is typically used to prepare a
    header grid for writing to an Excel worksheet where vertical merges are
    represented visually, leaving only the top cell in each merged block
    containing the header text.

    The input grid is modified in place and also returned for convenience.

    Args:
        header_grid (list[list[str]]): A two-dimensional list of header cell
            strings indexed as ``header_grid[row_index][column_index]``.

    Returns:
        list[list[str]]: The same header grid instance with non-top cells in each
        vertical run cleared (set to an empty string).
    """
    if not header_grid:
        return header_grid
    for col_idx, row_start, row_end, _ in _generate_vertical_runs(header_grid):
        for _row_idx_within_merge in range(row_start + 1, row_end + 1):
            header_grid[_row_idx_within_merge][col_idx] = ""
    return header_grid


def create_horizontal_merge_tracker(
    row_horizontal_merge_mapping: dict[int, list[SpecSheetHorizontalMerge]],
) -> dict[tuple[int, int], bool]:
    """
    Mark cells covered by horizontal merges (except leftmost cell),
    so we can skip writing them before merge_range.
    """
    dict_merged_cells_tracker: dict[tuple[int, int], bool] = {}
    for _row_idx, _horizontal_merges in row_horizontal_merge_mapping.items():
        for _merge in _horizontal_merges:
            for _col_idx in range(_merge.col_idx_start + 1, _merge.col_idx_end + 1):
                dict_merged_cells_tracker[(_row_idx, _col_idx)] = True
    return dict_merged_cells_tracker


# #endregion
################################################################################
