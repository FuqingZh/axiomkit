"""axiomkit XLSX writer.

This module provides `XlsxFormatter`, a small convenience wrapper around
`xlsxwriter.Workbook` to export tabular data (via Polars) to Excel.

It includes:
- Excel sheet splitting for row/column limits
- Header merge planning (true horizontal merges + visual vertical merges)
- Fast-path body writing with optional addon hooks
"""

import math
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, Protocol, Self, TypeAlias

import polars as pl
import xlsxwriter
import xlsxwriter.format
import xlsxwriter.worksheet

################################################################################
# #region Constants

N_NROWS_EXCEL_MAX = 1_048_576
N_NCOLS_EXCEL_MAX = 16_384
N_LEN_EXCEL_SHEET_NAME_MAX = 31
_EXCEL_ILLEGAL = ("*", ":", "?", "/", "\\", "[", "]")

ColRef: TypeAlias = str | int


# #endregion
################################################################################
# #region Dataclasses
@dataclass(frozen=True, slots=True)
class SheetPart:
    sheet_name: str
    row_start_inclusive: int
    row_end_exclusive: int  # exclusive in source df rows
    col_start_inclusive: int
    col_end_exclusive: int  # exclusive in source df cols


@dataclass(slots=True)
class XlsxReport:
    sheets: list[SheetPart]
    warnings: list[str]

    def warn(self, msg: str) -> None:
        self.warnings.append(str(msg))


@dataclass(frozen=True, slots=True)
class HorizontalMerge:
    row_idx_start: int
    col_idx_start: int
    col_idx_end: int  # inclusive
    text: str


@dataclass(frozen=True, slots=True)
class BorderSpec:
    top: int
    bottom: int
    left: int
    right: int


# #endregion
################################################################################
# #region XlsxAddon
class XlsxAddon(Protocol):
    """
    v1 addon contract (performance-first):

    - Column-level overrides MUST be O(1) / near O(1) per column.
    - Per-cell overrides are allowed but will force the writer into the slow per-cell path.
      Use only when necessary.

    Capability declaration
    ----------------------
    Addons SHOULD explicitly declare whether they need per-cell formatting by
    implementing `requires_cell_write()` (preferred) or a boolean attribute
    `requires_cell_write`.

    If neither is provided, the writer may fall back to a compatibility probe
    (deprecated) to decide the path.
    """

    def requires_cell_write(self) -> bool:  # optional, preferred
        return False

    def get_column_format_overrides(
        self,
        *,
        df: pl.DataFrame,
        fmt_sci: xlsxwriter.format.Format,
    ) -> dict[int, xlsxwriter.format.Format]:
        """
        Return {col_idx_0based: Format} column-level overrides.
        Default: {}
        """
        return {}

    def get_cell_format_override(
        self,
        *,
        row_idx: int,
        col_idx: int,
        value: Any,
    ) -> xlsxwriter.format.Format | None:
        """
        Return a per-cell format. If any addon returns non-None,
        writer will fall back to slow per-cell write path.

        Default: None (recommended for speed).
        """
        return None


def _addon_requires_cell_write(ad: XlsxAddon) -> bool:
    """
    Decide whether an addon forces the slow per-cell body write path.

    Preferred (explicit) contract:
      - method: `requires_cell_write() -> bool`
      - attribute: `requires_cell_write: bool`

    Backward-compatible fallback (deprecated):
      - probe `get_cell_format_override(row_idx=0, col_idx=0, value="__probe__")`
        and treat any non-None return (or exception) as requiring per-cell writes.
    """
    # explicit: method
    meth = getattr(ad, "requires_cell_write", None)
    if callable(meth):
        try:
            return bool(meth())
        except Exception:
            # conservative: if addon misbehaves, choose safety (slow path)
            return True

    # explicit: attribute
    if hasattr(ad, "requires_cell_write"):
        try:
            return bool(getattr(ad, "requires_cell_write"))
        except Exception:
            return True

    # compatibility probe (deprecated)
    try:
        return (
            ad.get_cell_format_override(row_idx=0, col_idx=0, value="__probe__")
            is not None
        )
    except Exception:
        return True


# #endregion
################################################################################
# #region DataFrame


def _to_polars(df: Any) -> pl.DataFrame:
    return df if isinstance(df, pl.DataFrame) else pl.DataFrame(df)


def _resolve_col_index(df: pl.DataFrame, ref: ColRef) -> int:
    if isinstance(ref, int):
        return ref
    try:
        return df.columns.index(ref)
    except ValueError as e:
        raise KeyError(f"Column not found: {ref!r}") from e


def _get_sorted_indices_from_refs(
    df: pl.DataFrame, refs: Sequence[ColRef] | None
) -> tuple[int, ...]:
    if not refs:
        return ()
    idx = {_resolve_col_index(df, _r) for _r in refs}
    return tuple(sorted(idx))


def _assert_no_duplicate_columns(df: pl.DataFrame) -> None:
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


# #endregion
################################################################################
# #region ExcelSplitRestrictions/Strategies


def _normalize_sheet_name(name: str, *, replace_to: str = "_") -> str:
    for ch in _EXCEL_ILLEGAL:
        name = name.replace(ch, replace_to)
    name = name.strip() or "Sheet"
    return name[:N_LEN_EXCEL_SHEET_NAME_MAX]


def _make_sheet_name(base_name: str, part_idx_1based: int) -> str:
    c_sheet_name_suffix = f"_{part_idx_1based}"
    n_len_base_name_max = N_LEN_EXCEL_SHEET_NAME_MAX - len(c_sheet_name_suffix)
    c_sheet_name_base = base_name[: max(1, n_len_base_name_max)]
    return f"{c_sheet_name_base}{c_sheet_name_suffix}"


def _generate_sheet_slices(
    *,
    height_df: int,
    width_df: int,
    height_header: int,
    sheet_name: str,
    report: XlsxReport,
) -> list[SheetPart]:
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

    l_sheet_parts: list[SheetPart] = []
    n_idx_part = 1
    for _col_start, _col_end in l_col_slices:
        for _row_start, _row_end in l_row_slices:
            l_sheet_parts.append(
                SheetPart(
                    sheet_name=_make_sheet_name(sheet_name, n_idx_part),
                    row_start_inclusive=_row_start,
                    row_end_exclusive=_row_end,
                    col_start_inclusive=_col_start,
                    col_end_exclusive=_col_end,
                )
            )
            n_idx_part += 1

    if len(l_sheet_parts) > 1:
        report.warn(
            f"Excel limit overflow: split into {len(l_sheet_parts)} sheets (columns-first, then rows)."
        )
    return l_sheet_parts


# #endregion
################################################################################
# #region Chunk&ValueConversion


def _create_row_chunks(
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


def _convert_cell_value(
    value: Any, if_is_numeric_col: bool, if_keep_na: bool
) -> object:
    if value is None:
        return None
    if not if_is_numeric_col:
        return str(value)
    if not math.isfinite(n_cell_float_value := float(value)):
        return _convert_nan_inf_to_str(n_cell_float_value) if if_keep_na else None

    return n_cell_float_value


def _convert_nan_inf_to_str(x: float) -> str:
    if math.isnan(x):
        return "NaN"
    elif math.isinf(x):
        return "Inf" if x > 0 else "-Inf"
    else:
        raise ValueError("Input is neither NaN nor Inf.")


def _get_cell_format_override(
    addons: Sequence[XlsxAddon], *, row_idx: int, col_idx: int, value: Any
) -> xlsxwriter.format.Format | None:
    fmt_cell = None
    for ad in addons:
        fmt_cell = (
            ad.get_cell_format_override(
                row_idx=row_idx,
                col_idx=col_idx,
                value=value,
            )
            or fmt_cell
        )
    return fmt_cell


def _write_cell_with_format(
    ws: xlsxwriter.worksheet.Worksheet,
    addons: Sequence[XlsxAddon],
    *,
    row_idx: int,
    col_idx: int,
    value: Any,
    if_is_numeric_col: bool,
    if_keep_na: bool,
):
    if value is None:
        ws.write_blank(row=row_idx, col=col_idx, blank=None)
        return

    if not if_is_numeric_col:
        c_cell_val = str(value)
        cfg_fmt_cell = _get_cell_format_override(
            addons, row_idx=row_idx, col_idx=col_idx, value=c_cell_val
        )
        ws.write_string(
            row=row_idx,
            col=col_idx,
            string=c_cell_val,
            cell_format=cfg_fmt_cell,
        )

        return

    if not math.isfinite(n_cell_val := float(value)):
        if not if_keep_na:
            ws.write_blank(row=row_idx, col=col_idx, blank=None)
            return

        c_cell_val = _convert_nan_inf_to_str(n_cell_val)
        cfg_fmt_cell = _get_cell_format_override(
            addons, row_idx=row_idx, col_idx=col_idx, value=c_cell_val
        )
        ws.write_string(
            row=row_idx,
            col=col_idx,
            string=c_cell_val,
            cell_format=cfg_fmt_cell,
        )
        return

    cfg_fmt_cell = _get_cell_format_override(
        addons, row_idx=row_idx, col_idx=col_idx, value=n_cell_val
    )
    ws.write_number(
        row=row_idx,
        col=col_idx,
        number=n_cell_val,
        cell_format=cfg_fmt_cell,
    )


# #endregion
################################################################################
# #region HeaderMergeAlgorithm


def _find_contiguous_ranges(sorted_indices: Sequence[int]) -> list[tuple[int, int]]:
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


def _get_row_chunk_size(*, width_df: int) -> int:
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


def _plan_horizontal_merges(
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


def _plan_vertical_visual_merge_borders(
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


def _remove_vertical_run_text(header_grid: list[list[str]]) -> list[list[str]]:
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


def _track_horizontal_merge_cells(
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


# #endregion
################################################################################


# -----------------------------
# Main writer
# -----------------------------
class XlsxFormatter:
    """
    Helper class for writing tabular data to an XLSX workbook using
    ``xlsxwriter`` with sensible defaults and formatting utilities.

    This class wraps :class:`xlsxwriter.Workbook` and provides a small
    formatting layer (default text/number formats, header styles, and
    sheet‑splitting logic) to make it easier to export large tables to
    Excel. It can be used either directly or as a context manager.

    The workbook is created on initialization and closed via :meth:`close`
    or automatically when used in a ``with`` block::

        from pathlib import Path
        from axiomkit import XlsxFormatter

        with XlsxFormatter("report.xlsx") as xf:
            # Use xf methods to add sheets and write data frames / tables
            ...

    Parameters
    ----------
    file_out:
        Path to the output ``.xlsx`` file. Can be a string or
        :class:`pathlib.Path`. The underlying workbook is created
        immediately for this path.
    if_constant_memory:
        If ``True`` (default), enables xlsxwriter's ``constant_memory``
        mode to reduce memory usage when writing large workbooks.
    default_font:
        Base font family name used for text and header formats.
    default_font_size:
        Base font size (in points) used for text and header formats.
    """

    def __init__(
        self,
        file_out: str | Path,
        *,
        if_constant_memory: bool = True,
        default_font: str = "Arial",
        default_font_size: int = 10,
    ) -> None:
        self.file_out = Path(file_out)
        self._existing_sheet_names: set[str] = set()
        self._reports: list[XlsxReport] = []

        self.wb = xlsxwriter.Workbook(
            self.file_out.as_posix(),
            {
                "constant_memory": if_constant_memory,
                # v1：我们自己处理 NaN/Inf（默认写空或写字符串），不写 Excel error。
                "nan_inf_to_errors": False,
            },
        )
        self._if_constant_memory = if_constant_memory

        # Minimal formats
        self.fmt_text = self.wb.add_format(
            {
                "font_name": default_font,
                "font_size": default_font_size,
                "align": "center",
                "valign": "vcenter",
            }
        )
        self.fmt_int = self.wb.add_format({"num_format": "0"})
        self.fmt_dec = self.wb.add_format({"num_format": "0.0000"})
        self.fmt_sci = self.wb.add_format({"num_format": "0.00E+0"})  # for addon

        # Header base spec (borders will be customized per cell via cache)
        self._header_base_spec: dict[str, Any] = {
            "font_name": default_font,
            "font_size": default_font_size,
            "bold": True,
            "align": "center",
            "valign": "vcenter",
        }
        self._header_fmt_cache: dict[
            tuple[int, int, int, int], xlsxwriter.format.Format
        ] = {}

    def close(self) -> None:
        self.wb.close()

    def report(self) -> tuple[XlsxReport, ...]:
        return tuple(self._reports)

    def __enter__(self) -> "XlsxFormatter":
        return self

    def __exit__(
        self, exc_type: type | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()

    def _ensure_unique_sheet_name(self, name: str) -> str:
        if name not in self._existing_sheet_names:
            self._existing_sheet_names.add(name)
            return name

        # deterministic bump: name__2, name__3 ...
        c_base_name = name[: max(1, N_LEN_EXCEL_SHEET_NAME_MAX - 3)]
        i = 2
        c_candidate_name = f"{c_base_name}__{i}"[:N_LEN_EXCEL_SHEET_NAME_MAX]
        while c_candidate_name in self._existing_sheet_names:
            i += 1
            c_candidate_name = f"{c_base_name}__{i}"[:N_LEN_EXCEL_SHEET_NAME_MAX]
        self._existing_sheet_names.add(c_candidate_name)
        return c_candidate_name

    @staticmethod
    def _infer_numeric_cols(df: pl.DataFrame) -> tuple[int, ...]:
        l_cols_idx_num: list[int] = []
        for _idx, _val in enumerate(df.columns):
            if df.schema[_val].is_numeric():
                l_cols_idx_num.append(_idx)
        return tuple(l_cols_idx_num)

    @staticmethod
    def _infer_integer_cols(
        df: pl.DataFrame, cols_idx_num: tuple[int, ...]
    ) -> tuple[int, ...]:
        l_cols_idx_int: list[int] = []
        for _idx in cols_idx_num:
            cfg_col_dtype = df.schema[df.columns[_idx]]
            if cfg_col_dtype.is_integer():
                l_cols_idx_int.append(_idx)
        return tuple(l_cols_idx_int)

    def _get_header_fmt(self, border: BorderSpec | None) -> xlsxwriter.format.Format:
        # default full border
        if border is None:
            tup_border_format_key = (1, 1, 1, 1)
            if tup_border_format_key not in self._header_fmt_cache:
                dict_spec_format = dict(self._header_base_spec)
                dict_spec_format |= {"top": 1, "bottom": 1, "left": 1, "right": 1}
                self._header_fmt_cache[tup_border_format_key] = self.wb.add_format(
                    dict_spec_format
                )
            return self._header_fmt_cache[tup_border_format_key]

        tup_border_format_key = (border.top, border.bottom, border.left, border.right)
        if tup_border_format_key not in self._header_fmt_cache:
            dict_spec_format = dict(self._header_base_spec)
            dict_spec_format |= {
                "top": border.top,
                "bottom": border.bottom,
                "left": border.left,
                "right": border.right,
            }
            self._header_fmt_cache[tup_border_format_key] = self.wb.add_format(
                dict_spec_format
            )
        return self._header_fmt_cache[tup_border_format_key]

    def _set_column_formats(
        self,
        ws: xlsxwriter.worksheet.Worksheet,
        *,
        width_df: int,
        cols_idx_numeric: tuple[int, ...],
        cols_idx_integer: tuple[int, ...],
        cols_idx_decimal: tuple[int, ...],
        cols_fmt_overrides: dict[int, xlsxwriter.format.Format],
    ) -> None:
        if width_df <= 0:
            return

        # default text for all
        ws.set_column(first_col=0, last_col=width_df - 1, cell_format=self.fmt_text)

        set_cols_idx_int = set(cols_idx_integer)
        set_cols_idx_dec = set(cols_idx_decimal)
        set_cols_idx_num = set(cols_idx_numeric)

        # numeric default: decimal; integer override: int
        l_cols_idx_int_sorted = sorted(set_cols_idx_int & set_cols_idx_num)
        l_cols_idx_dec_sorted = sorted(
            (set_cols_idx_num - set_cols_idx_int)
            | (set_cols_idx_dec & set_cols_idx_num)
        )

        for _start, _end in _find_contiguous_ranges(l_cols_idx_dec_sorted):
            ws.set_column(first_col=_start, last_col=_end, cell_format=self.fmt_dec)
        for _start, _end in _find_contiguous_ranges(l_cols_idx_int_sorted):
            ws.set_column(first_col=_start, last_col=_end, cell_format=self.fmt_int)

        for _col_idx, _fmt in cols_fmt_overrides.items():
            if 0 <= _col_idx < width_df:
                ws.set_column(first_col=_col_idx, last_col=_col_idx, cell_format=_fmt)

    def _write_header(
        self,
        ws: xlsxwriter.worksheet.Worksheet,
        *,
        header_grid: list[list[str]],
        if_merge: bool,
    ):
        if not header_grid:
            raise ValueError(
                "header_grid cannot be empty (df_header must have >= 1 row)."
            )
        n_rows = len(header_grid)
        n_cols = len(header_grid[0])

        l_header_grid = header_grid
        dict_border_plan: dict[tuple[int, int], BorderSpec] = {}
        b_visual_merge_vertical = bool(if_merge and n_rows > 1)
        if b_visual_merge_vertical and n_rows > 1:
            dict_border_plan = _plan_vertical_visual_merge_borders(l_header_grid)
            l_header_grid = _remove_vertical_run_text(l_header_grid)

        dict_horizontal_merges_by_row = (
            _plan_horizontal_merges(l_header_grid) if if_merge else {}
        )
        dict_horizontal_merge_tracker = _track_horizontal_merge_cells(
            dict_horizontal_merges_by_row
        )

        # write cells (skip those covered by horizontal merge, except leftmost)
        for _row_idx in range(n_rows):
            for _col_idx in range(n_cols):
                if dict_horizontal_merge_tracker.get((_row_idx, _col_idx), False):
                    continue

                cfg_cell_format_ = self._get_header_fmt(
                    dict_border_plan.get((_row_idx, _col_idx))
                )

                # Use write_string/write_blank explicitly
                c_cell_value_ = l_header_grid[_row_idx][_col_idx]
                if c_cell_value_ == "":
                    ws.write_blank(
                        row=_row_idx,
                        col=_col_idx,
                        blank=None,
                        cell_format=cfg_cell_format_,
                    )
                else:
                    ws.write_string(
                        row=_row_idx,
                        col=_col_idx,
                        string=c_cell_value_,
                        cell_format=cfg_cell_format_,
                    )

            # apply true horizontal merges (same row safe in constant_memory)
            for _merge_block in dict_horizontal_merges_by_row.get(_row_idx, []):
                # For merged block, pick a border spec that at least draws outer box.
                # (Excel treats merged region as one cell; internal per-col borders irrelevant.)
                cfg_block_format_ = self._get_header_fmt(
                    BorderSpec(top=1, bottom=1, left=1, right=1)
                )
                ws.merge_range(
                    first_row=_row_idx,
                    first_col=_merge_block.col_idx_start,
                    last_row=_row_idx,
                    last_col=_merge_block.col_idx_end,
                    data=_merge_block.text,
                    cell_format=cfg_block_format_,
                )

    def write_sheet(
        self,
        df: Any,
        sheet_name: str,
        *,
        df_header: Any | None = None,
        cols_integer: Sequence[ColRef] | None = None,
        cols_decimal: Sequence[ColRef] | None = None,
        col_freeze: int = 0,
        row_freeze: int | None = None,
        if_merge_header: bool = True,
        if_keep_na: bool = False,
        addons: Sequence[XlsxAddon] = (),
    ) -> Self:
        report = XlsxReport(
            sheets=[],
            warnings=[],
        )

        df_custom = _to_polars(df)
        l_colnames_df = df_custom.columns
        n_width_df = df_custom.width
        n_height_df = df_custom.height
        _assert_no_duplicate_columns(df_custom)

        # build header grid
        l_header_grid = [list(df_custom.columns)]
        if df_header is not None:
            df_header_custom = _to_polars(df_header)
            _assert_no_duplicate_columns(df_header_custom)
            if df_header_custom.height == 0:
                raise ValueError(
                    "df_header must have >= 1 row (0-row header is not allowed)."
                )
            if df_header_custom.width != df_custom.width:
                raise ValueError("df_header.width must equal df.width.")

            l_header_grid = [
                ["" if v is None else str(v) for v in row]
                for row in df_header_custom.with_columns(
                    pl.all().cast(pl.String)
                ).iter_rows()
            ]

        # infer numeric and integer columns
        tup_cols_idx_numeric = self._infer_numeric_cols(df_custom)
        tup_cols_idx_integer_inferred = self._infer_integer_cols(
            df_custom, tup_cols_idx_numeric
        )

        tup_cols_idx_integer_specified = _get_sorted_indices_from_refs(
            df_custom, cols_integer
        )
        tup_cols_idx_decimal_specified = _get_sorted_indices_from_refs(
            df_custom, cols_decimal
        )

        # precedence: user override > inferred
        tup_cols_idx_integer = (
            tup_cols_idx_integer_specified
            if tup_cols_idx_integer_specified
            else tup_cols_idx_integer_inferred
        )
        tup_cols_idx_decimal = tup_cols_idx_decimal_specified  # optional

        # warnings: non-numeric columns written as string
        for _colname in l_colnames_df:
            if (cfg_col_dtype_ := df_custom.schema[_colname]).is_numeric():
                continue

            b_is_common_scalar_type = (
                cfg_col_dtype_ == pl.String
                or cfg_col_dtype_ == pl.Categorical
                or cfg_col_dtype_ == pl.Enum
                or cfg_col_dtype_ == pl.Boolean
                or cfg_col_dtype_.is_temporal()
                or cfg_col_dtype_ == pl.Null
            )

            if not b_is_common_scalar_type:
                report.warn(
                    f"Column {_colname!r} dtype {cfg_col_dtype_} will be written as string."
                )

        # header rows count influences split (Excel max rows)
        n_rows_header = len(l_header_grid)
        l_sheet_parts = _generate_sheet_slices(
            height_df=n_height_df,
            width_df=n_width_df,
            height_header=n_rows_header,
            sheet_name=_normalize_sheet_name(sheet_name),
            report=report,
        )

        # decide freeze_row
        row_freeze = n_rows_header if row_freeze is None else row_freeze

        # addon column overrides (fast path)
        dict_cols_fmt_overrides: dict[int, xlsxwriter.format.Format] = {}
        for _ad in addons:
            dict_cols_fmt_overrides |= _ad.get_column_format_overrides(
                df=df_custom, fmt_sci=self.fmt_sci
            )

        # determine whether we must fall back to slow per-cell body write
        # If any addon potentially returns a non-None cell format, we assume slow path.
        # (v1: we do a single probe with a cheap call contract; you can also pass addons=() for fast path.)
        b_any_cell_override = any(_addon_requires_cell_write(_ad) for _ad in addons)

        for _sheet_slice in l_sheet_parts:
            c_sheet_name_unique_ = self._ensure_unique_sheet_name(
                _sheet_slice.sheet_name
            )
            cfg_worksheet_ = self.wb.add_worksheet(c_sheet_name_unique_)

            # Slice df by this part
            l_cols_slice_ = df_custom.columns[
                _sheet_slice.col_start_inclusive : _sheet_slice.col_end_exclusive
            ]
            df_slice_ = df_custom.slice(
                offset=_sheet_slice.row_start_inclusive,
                length=_sheet_slice.row_end_exclusive
                - _sheet_slice.row_start_inclusive,
            ).select(l_cols_slice_)

            # Column formats (relative indices in this sheet)
            tup_cols_idx_numeric_slice_ = tuple(
                _idx - _sheet_slice.col_start_inclusive
                for _idx in tup_cols_idx_numeric
                if _sheet_slice.col_start_inclusive
                <= _idx
                < _sheet_slice.col_end_exclusive
            )
            tup_cols_idx_integer_slice_ = tuple(
                _idx - _sheet_slice.col_start_inclusive
                for _idx in tup_cols_idx_integer
                if _sheet_slice.col_start_inclusive
                <= _idx
                < _sheet_slice.col_end_exclusive
            )
            tup_cols_idx_decimal_slice_ = tuple(
                _idx - _sheet_slice.col_start_inclusive
                for _idx in tup_cols_idx_decimal
                if _sheet_slice.col_start_inclusive
                <= _idx
                < _sheet_slice.col_end_exclusive
            )

            dict_cols_fmt_overrides_slice_ = {
                k - _sheet_slice.col_start_inclusive: v
                for k, v in dict_cols_fmt_overrides.items()
                if _sheet_slice.col_start_inclusive
                <= k
                < _sheet_slice.col_end_exclusive
            }

            self._set_column_formats(
                cfg_worksheet_,
                width_df=df_slice_.width,
                cols_idx_numeric=tup_cols_idx_numeric_slice_,
                cols_idx_integer=tup_cols_idx_integer_slice_,
                cols_idx_decimal=tup_cols_idx_decimal_slice_,
                cols_fmt_overrides=dict_cols_fmt_overrides_slice_,
            )

            # Header (slice columns)
            l_header_grid_slice_ = [
                _row_iter[
                    _sheet_slice.col_start_inclusive : _sheet_slice.col_end_exclusive
                ]
                for _row_iter in l_header_grid
            ]
            self._write_header(
                cfg_worksheet_,
                header_grid=l_header_grid_slice_,
                if_merge=if_merge_header,
            )

            # freeze panes
            cfg_worksheet_.freeze_panes(row_freeze, col_freeze)

            # Body: build cast expressions
            # v1 rule: numeric -> Float64; non-numeric -> String
            set_cols_idx_numeric = set(tup_cols_idx_numeric_slice_)

            l_col_cast_expressions: list[pl.Expr] = []
            for _idx, _val in enumerate(df_slice_.columns):
                if _idx in set_cols_idx_numeric:
                    l_col_cast_expressions.append(
                        pl.col(_val).cast(pl.Float64).alias(_val)
                    )
                else:
                    l_col_cast_expressions.append(
                        pl.col(_val).cast(pl.String).alias(_val)
                    )

            n_rows_chunk_ = _get_row_chunk_size(width_df=df_slice_.width)
            n_cols_ = df_slice_.width
            n_row_start_data_ = n_rows_header
            l_is_numeric_col = [_idx in set_cols_idx_numeric for _idx in range(n_cols_)]

            # Fast path: write_row with python values (no per-cell formats)
            if not b_any_cell_override:
                for _row_idx, _df_chunk in _create_row_chunks(
                    df=df_slice_,
                    size_rows_chunk=n_rows_chunk_,
                    cols_exprs=l_col_cast_expressions,
                ):
                    n_row_idx_excel_0based_ = n_row_start_data_ + _row_idx
                    for _row_idx_chunk, _row_val in enumerate(_df_chunk.iter_rows()):
                        l_row_vals = [
                            _convert_cell_value(
                                value=_col_val,
                                if_is_numeric_col=l_is_numeric_col[_col_idx],
                                if_keep_na=if_keep_na,
                            )
                            for _col_idx, _col_val in enumerate(_row_val)
                        ]
                        cfg_worksheet_.write_row(
                            row=n_row_idx_excel_0based_ + _row_idx_chunk,
                            col=0,
                            data=l_row_vals,
                        )
            else:
                # Slow path: per-cell write to allow cell formats.
                for _row_cursor, _df_chunk in _create_row_chunks(
                    df=df_slice_,
                    size_rows_chunk=n_rows_chunk_,
                    cols_exprs=l_col_cast_expressions,
                ):
                    n_row_idx_excel_0based_ = n_row_start_data_ + _row_cursor
                    for _row_idx_chunk, _row_val in enumerate(_df_chunk.iter_rows()):
                        for _col_idx, _col_val in enumerate(_row_val):
                            _write_cell_with_format(
                                cfg_worksheet_,
                                addons,
                                row_idx=n_row_idx_excel_0based_ + _row_idx_chunk,
                                col_idx=_col_idx,
                                value=_col_val,
                                if_is_numeric_col=l_is_numeric_col[_col_idx],
                                if_keep_na=if_keep_na,
                            )

            report.sheets.append(
                SheetPart(
                    sheet_name=c_sheet_name_unique_,
                    row_start_inclusive=_sheet_slice.row_start_inclusive,
                    row_end_exclusive=_sheet_slice.row_end_exclusive,
                    col_start_inclusive=_sheet_slice.col_start_inclusive,
                    col_end_exclusive=_sheet_slice.col_end_exclusive,
                )
            )

        self._reports.append(report)
        return self
