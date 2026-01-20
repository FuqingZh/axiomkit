from collections.abc import Sequence

from .models import BorderSpec, HorizontalMerge


def find_contiguous_ranges(sorted_indices: Sequence[int]) -> list[tuple[int, int]]:
    """
    Convert a sorted sequence of indices into a list of contiguous index ranges.

    Args:
        sorted_indices (Sequence[int]): Sorted sequence of integer indices to group
            into contiguous ranges.

    Returns:
        list[tuple[int, int]]: A list of (start, end) tuples, each representing an
            inclusive contiguous range of indices.

    Examples:
        >>> _find_contiguous_ranges([0, 1, 2, 4, 5, 7])
        [(0, 2), (4, 5), (7, 7)]
        >>> _find_contiguous_ranges([])
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
) -> dict[int, list[HorizontalMerge]]:
    dict_horizontal_merges_map: dict[int, list[HorizontalMerge]] = {}
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
                    HorizontalMerge(
                        row_idx_start=_row_idx,
                        col_idx_start=n_col_idx_,
                        col_idx_end=n_col_idx_end_ - 1,
                        text=c_cell_val_,
                    )
                )
            n_col_idx_ = n_col_idx_end_

    return dict_horizontal_merges_map


def _iter_vertical_runs(
    header_grid: Sequence[Sequence[str]],
):
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
) -> dict[tuple[int, int], BorderSpec]:
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
        dict[tuple[int, int], BorderSpec]: A mapping from ``(row_index,
        column_index)`` cell coordinates to ``BorderSpec`` instances for cells
        that belong to a vertical merge block. For each such cell, the border
        spec indicates which borders (top, bottom, left, right) should be drawn
        to render the merged region.
    """
    dict_plan_vertical_merge_border: dict[tuple[int, int], BorderSpec] = {}
    for col_idx, row_start, row_end, _ in _iter_vertical_runs(header_grid):
        for _row_idx_within_merge in range(row_start, row_end + 1):
            dict_plan_vertical_merge_border[(_row_idx_within_merge, col_idx)] = (
                BorderSpec(
                    top=1 if _row_idx_within_merge == row_start else 0,
                    bottom=1 if _row_idx_within_merge == row_end else 0,
                    left=1,
                    right=1,
                )
            )
        # blank out text for non-top cells (handled by writer)
    return dict_plan_vertical_merge_border


def remove_vertical_run_text(header_grid: list[list[str]]) -> list[list[str]]:
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
    for col_idx, row_start, row_end, _ in _iter_vertical_runs(header_grid):
        for _row_idx_within_merge in range(row_start + 1, row_end + 1):
            header_grid[_row_idx_within_merge][col_idx] = ""
    return header_grid


def track_horizontal_merge_cells(
    row_horizontal_merge_mapping: dict[int, list[HorizontalMerge]],
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
