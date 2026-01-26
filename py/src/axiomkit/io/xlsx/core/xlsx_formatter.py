import math
import os
from collections.abc import Sequence
from pathlib import Path
from types import TracebackType
from typing import Any, Literal, Self

import polars as pl
import xlsxwriter
import xlsxwriter.format
import xlsxwriter.worksheet

from ..conf import DEFAULT_XLSX_FORMATS, N_LEN_EXCEL_SHEET_NAME_MAX, ColRef
from ..hook import XlsxAddon, addon_requires_cell_write, write_cell_with_format
from ..service import (
    create_row_chunks,
    find_contiguous_ranges,
    generate_sheet_slices,
    get_row_chunk_size,
    normalize_sheet_name,
    plan_horizontal_merges,
    plan_vertical_visual_merge_borders,
    remove_vertical_run_text,
    track_horizontal_merge_cells,
)
from ..spec import SpecCellBorder, SpecCellFormat, SpecSheetSlice, SpecXlsxReport
from ..util import (
    assert_no_duplicate_columns,
    convert_cell_value,
    convert_nan_inf_to_str,
    get_sorted_indices_from_refs,
    to_polars,
)


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
        file_out: os.PathLike[str] | str,
        *,
        fmt_text: SpecCellFormat | None = None,
        fmt_integer: SpecCellFormat | None = None,
        fmt_decimal: SpecCellFormat | None = None,
        fmt_scientific: SpecCellFormat | None = None,
        fmt_header: SpecCellFormat | None = None,
    ):
        self.file_out = Path(file_out)
        self.wb = xlsxwriter.Workbook(
            self.file_out.as_posix(),
            {
                "constant_memory": True,
                # v1：我们自己处理 NaN/Inf（默认写空或写字符串），不写 Excel error。
                "nan_inf_to_errors": False,
            },
        )
        self._format_cache: dict[SpecCellFormat, Any] = {}

        self.fmt_text = DEFAULT_XLSX_FORMATS["text"] if fmt_text is None else fmt_text
        self.fmt_int = (
            DEFAULT_XLSX_FORMATS["integer"] if fmt_integer is None else fmt_integer
        )
        self.fmt_dec = (
            DEFAULT_XLSX_FORMATS["decimal"] if fmt_decimal is None else fmt_decimal
        )
        self.fmt_sci = (
            DEFAULT_XLSX_FORMATS["scientific"]
            if fmt_scientific is None
            else fmt_scientific
        )
        self.fmt_header = (
            DEFAULT_XLSX_FORMATS["header"] if fmt_header is None else fmt_header
        )
        self._header_fmt_cache: dict[
            tuple[int, int, int, int], xlsxwriter.format.Format
        ] = {}
        self._existing_sheet_names: set[str] = set()
        self._reports: list[SpecXlsxReport] = []

    def __enter__(self) -> "XlsxFormatter":
        return self

    def __exit__(
        self, exc_type: type | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()

    @staticmethod
    def get_default_xlsx_formats():
        return DEFAULT_XLSX_FORMATS

    def close(self) -> None:
        self.wb.close()

    def report(self) -> tuple[SpecXlsxReport, ...]:
        return tuple(self._reports)

    def _get_or_create_format(self, spec: SpecCellFormat) -> Any:
        fmt = self._format_cache.get(spec)
        if fmt is None:
            fmt = self.wb.add_format(spec.to_xlsxwriter())
            self._format_cache[spec] = fmt
        return fmt

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

    @staticmethod
    def _estimate_width_len(
        value: Any,
        *,
        if_is_numeric_col: bool,
        if_is_integer_col: bool,
        if_keep_missing_values: bool,
    ) -> int:
        """Estimate display string length for column width calculation.

        Notes
        -----
        - Excel column width is not strictly character count; this is a pragmatic
          heuristic good enough for most reports.
        - For numeric columns we approximate based on the workbook formats used
          by this writer (int: "0", dec: "0.0000").
        """
        if value is None:
            return len("NA") if if_keep_missing_values else 0

        s = str(value)
        n_ascii = sum(1 for _chr in s if ord(_chr) < 128)
        n_non_ascii = len(s) - n_ascii
        n_estimated_string_length = n_ascii + int(1.6 * n_non_ascii)
        if not if_is_numeric_col:
            if not s:
                return 0
            return n_estimated_string_length

        try:
            n_val = float(value)
        except Exception:
            return n_estimated_string_length

        if not math.isfinite(n_val):
            if not if_keep_missing_values:
                return 0
            return len(convert_nan_inf_to_str(n_val))

        if if_is_integer_col:
            # avoid 1.0-like strings
            try:
                return len(str(int(n_val)))
            except Exception:
                return len(str(n_val))

        # decimal default: 4 digits
        try:
            return len(f"{n_val:.4f}")
        except Exception:
            return len(str(n_val))

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

    def _get_header_fmt(
        self, border: SpecCellBorder | None
    ) -> xlsxwriter.format.Format:
        # default full border
        if border is None:
            tup_border_format_key = (1, 1, 1, 1)
            if self._header_fmt_cache.get(tup_border_format_key) is None:
                self._header_fmt_cache[tup_border_format_key] = (
                    self._get_or_create_format(
                        self.fmt_header.with_(top=1, bottom=1, left=1, right=1)
                    )
                )
            return self._header_fmt_cache[tup_border_format_key]

        tup_border_format_key = (border.top, border.bottom, border.left, border.right)
        if self._header_fmt_cache.get(tup_border_format_key) is None:
            self._header_fmt_cache[tup_border_format_key] = self._get_or_create_format(
                self.fmt_header.with_(
                    top=border.top,
                    bottom=border.bottom,
                    left=border.left,
                    right=border.right,
                )
            )
        return self._header_fmt_cache[tup_border_format_key]

    def _set_column_formats(
        self,
        ws: xlsxwriter.worksheet.Worksheet,
        *,
        width_df: int,
        cols_idx_numeric: tuple[int, ...],
        cols_idx_integer: tuple[int, ...],
        cols_idx_decimal: tuple[int, ...] | Literal[False],
        cols_fmt_overrides: dict[int, xlsxwriter.format.Format],
    ) -> list[xlsxwriter.format.Format]:
        if width_df <= 0:
            return []

        cfg_fmt_text = self._get_or_create_format(self.fmt_text)
        cfg_fmt_int = self._get_or_create_format(self.fmt_int)
        cfg_fmt_dec = self._get_or_create_format(self.fmt_dec)

        # Track final per-column format for downstream operations (e.g. autofit).
        l_fmt_by_col: list[xlsxwriter.format.Format] = [cfg_fmt_text] * width_df

        # default text for all
        ws.set_column(first_col=0, last_col=width_df - 1, cell_format=cfg_fmt_text)

        set_cols_idx_int = set(cols_idx_integer)
        set_cols_idx_dec = set(cols_idx_decimal) if cols_idx_decimal else False
        set_cols_idx_num = set(cols_idx_numeric)

        # numeric default: decimal; integer override: int
        l_cols_idx_int_sorted = sorted(set_cols_idx_int & set_cols_idx_num)
        l_cols_idx_dec_sorted: list[int] = (
            sorted(
                (set_cols_idx_num - set_cols_idx_int)
                | (set_cols_idx_dec & set_cols_idx_num)
            )
            if set_cols_idx_dec
            else list()
        )

        for _start, _end in find_contiguous_ranges(l_cols_idx_dec_sorted):
            ws.set_column(first_col=_start, last_col=_end, cell_format=cfg_fmt_dec)
            for _i in range(_start, _end + 1):
                l_fmt_by_col[_i] = cfg_fmt_dec
        for _start, _end in find_contiguous_ranges(l_cols_idx_int_sorted):
            ws.set_column(first_col=_start, last_col=_end, cell_format=cfg_fmt_int)
            for _i in range(_start, _end + 1):
                l_fmt_by_col[_i] = cfg_fmt_int
        for _col_idx, _fmt in cols_fmt_overrides.items():
            if 0 <= _col_idx < width_df:
                ws.set_column(first_col=_col_idx, last_col=_col_idx, cell_format=_fmt)

                l_fmt_by_col[_col_idx] = _fmt

        return l_fmt_by_col

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
        dict_border_plan: dict[tuple[int, int], SpecCellBorder] = {}
        b_visual_merge_vertical = bool(if_merge and n_rows > 1)
        if b_visual_merge_vertical and n_rows > 1:
            dict_border_plan = plan_vertical_visual_merge_borders(l_header_grid)
            l_header_grid = remove_vertical_run_text(l_header_grid)

        dict_horizontal_merges_by_row = (
            plan_horizontal_merges(l_header_grid) if if_merge else {}
        )
        dict_horizontal_merge_tracker = track_horizontal_merge_cells(
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
                    SpecCellBorder(top=1, bottom=1, left=1, right=1)
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
        cols_decimal: Sequence[ColRef] | None | Literal[False] = None,
        col_freeze: int = 0,
        row_freeze: int | None = None,
        if_merge_header: bool = False,
        if_keep_missing_values: bool = False,
        if_autofit_columns: bool = True,
        rule_autofit_columns: Literal["header", "body", "all"] = "header",
        lim_inferred_rows_autofit_max: int | None = 20_000,
        lim_width_autofit_cell_min: int = 8,
        lim_width_autofit_cell_max: int = 60,
        padding_autofit_width: int = 2,
        addons: Sequence[XlsxAddon] = (),
    ) -> Self:
        report = SpecXlsxReport(
            sheets=[],
            warnings=[],
        )

        df_custom = to_polars(df)
        l_colnames_df = df_custom.columns
        n_width_df = df_custom.width
        n_height_df = df_custom.height
        assert_no_duplicate_columns(df_custom)

        # build header grid
        l_header_grid = [list(df_custom.columns)]
        if df_header is not None:
            df_header_custom = to_polars(df_header)
            assert_no_duplicate_columns(df_header_custom)
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

        tup_cols_idx_integer_specified = get_sorted_indices_from_refs(
            df_custom, cols_integer
        )
        tup_cols_idx_decimal_specified = (
            get_sorted_indices_from_refs(df_custom, cols_decimal)
            if cols_decimal
            else False
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
        l_sheet_parts = generate_sheet_slices(
            height_df=n_height_df,
            width_df=n_width_df,
            height_header=n_rows_header,
            sheet_name=normalize_sheet_name(sheet_name),
            report=report,
        )

        # decide freeze_row
        row_freeze = n_rows_header if row_freeze is None else row_freeze

        # addon column overrides (fast path)
        dict_cols_fmt_overrides: dict[int, xlsxwriter.format.Format] = {}
        for _ad in addons:
            dict_cols_fmt_overrides |= _ad.get_column_format_overrides(
                df=df_custom, fmt_sci=self._get_or_create_format(self.fmt_sci)
            )

        # determine whether we must fall back to slow per-cell body write
        # If any addon potentially returns a non-None cell format, we assume slow path.
        # (v1: we do a single probe with a cheap call contract; you can also pass addons=() for fast path.)
        b_any_cell_override = any(addon_requires_cell_write(_ad) for _ad in addons)

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
            tup_cols_idx_decimal_slice_ = (
                tuple(
                    _idx - _sheet_slice.col_start_inclusive
                    for _idx in tup_cols_idx_decimal
                    if _sheet_slice.col_start_inclusive
                    <= _idx
                    < _sheet_slice.col_end_exclusive
                )
                if tup_cols_idx_decimal
                else False
            )

            dict_cols_fmt_overrides_slice_ = {
                k - _sheet_slice.col_start_inclusive: v
                for k, v in dict_cols_fmt_overrides.items()
                if _sheet_slice.col_start_inclusive
                <= k
                < _sheet_slice.col_end_exclusive
            }

            # Column formats.
            # Note: we also keep the final per-column formats for optional
            # post-write operations (e.g. autofit width).
            l_fmt_by_col_ = self._set_column_formats(
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

            # Autofit: initialize width estimates from header text before any
            # in-place merge-related normalization (vertical blank-out).
            # Note: for horizontally merged headers, only the left-most cell holds
            # the text. This is acceptable because the merged region spans multiple
            # columns.
            dict_col_widths: dict[str, list[int]] = {
                "header": [0] * df_slice_.width,
                "body": [0] * df_slice_.width,
            }
            if if_autofit_columns and df_slice_.width > 0:
                for _col_idx in range(df_slice_.width):
                    for _row in l_header_grid_slice_:
                        if c_cell_value := _row[_col_idx]:
                            dict_col_widths["header"][_col_idx] = max(
                                dict_col_widths["header"][_col_idx],
                                self._estimate_width_len(
                                    c_cell_value,
                                    if_is_numeric_col=False,
                                    if_is_integer_col=False,
                                    if_keep_missing_values=if_keep_missing_values,
                                ),
                            )

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

            n_rows_chunk_ = get_row_chunk_size(width_df=df_slice_.width)
            n_cols_ = df_slice_.width
            n_row_start_data_ = n_rows_header
            l_is_numeric_col = [_idx in set_cols_idx_numeric for _idx in range(n_cols_)]

            set_cols_idx_int_slice_ = set(tup_cols_idx_integer_slice_)
            l_is_integer_col = [
                _idx in set_cols_idx_int_slice_ for _idx in range(n_cols_)
            ]

            n_rows_seen_for_autofit = 0

            # Fast path: write_row with python values (no per-cell formats)
            if not b_any_cell_override:
                for _row_idx, _df_chunk in create_row_chunks(
                    df=df_slice_,
                    size_rows_chunk=n_rows_chunk_,
                    cols_exprs=l_col_cast_expressions,
                ):
                    n_row_idx_excel_0based_ = n_row_start_data_ + _row_idx
                    for _row_idx_chunk, _row_val in enumerate(_df_chunk.iter_rows()):
                        l_row_vals = [
                            convert_cell_value(
                                value=_col_val,
                                if_is_numeric_col=l_is_numeric_col[_col_idx],
                                if_keep_missing_values=if_keep_missing_values,
                            )
                            for _col_idx, _col_val in enumerate(_row_val)
                        ]

                        # Update width estimates (bounded by num_autofit_rows_max).
                        if if_autofit_columns and (
                            lim_inferred_rows_autofit_max is None
                            or n_rows_seen_for_autofit < lim_inferred_rows_autofit_max
                        ):
                            for _col_idx, _cell_val in enumerate(l_row_vals):
                                dict_col_widths["body"][_col_idx] = max(
                                    dict_col_widths["body"][_col_idx],
                                    self._estimate_width_len(
                                        _cell_val,
                                        if_is_numeric_col=l_is_numeric_col[_col_idx],
                                        if_is_integer_col=l_is_integer_col[_col_idx],
                                        if_keep_missing_values=if_keep_missing_values,
                                    ),
                                )
                            n_rows_seen_for_autofit += 1

                        cfg_worksheet_.write_row(
                            row=n_row_idx_excel_0based_ + _row_idx_chunk,
                            col=0,
                            data=l_row_vals,
                        )
            else:
                # Slow path: per-cell write to allow cell formats.
                for _row_cursor, _df_chunk in create_row_chunks(
                    df=df_slice_,
                    size_rows_chunk=n_rows_chunk_,
                    cols_exprs=l_col_cast_expressions,
                ):
                    n_row_idx_excel_0based_ = n_row_start_data_ + _row_cursor
                    for _row_idx_chunk, _row_val in enumerate(_df_chunk.iter_rows()):
                        for _col_idx, _col_val in enumerate(_row_val):
                            if if_autofit_columns and (
                                lim_inferred_rows_autofit_max is None
                                or n_rows_seen_for_autofit
                                < lim_inferred_rows_autofit_max
                            ):
                                dict_col_widths["body"][_col_idx] = max(
                                    dict_col_widths["body"][_col_idx],
                                    self._estimate_width_len(
                                        _col_val,
                                        if_is_numeric_col=l_is_numeric_col[_col_idx],
                                        if_is_integer_col=l_is_integer_col[_col_idx],
                                        if_keep_missing_values=if_keep_missing_values,
                                    ),
                                )
                            write_cell_with_format(
                                cfg_worksheet_,
                                addons,
                                row_idx=n_row_idx_excel_0based_ + _row_idx_chunk,
                                col_idx=_col_idx,
                                value=_col_val,
                                if_is_numeric_col=l_is_numeric_col[_col_idx],
                                if_keep_missing_values=if_keep_missing_values,
                            )

                        if if_autofit_columns and (
                            lim_inferred_rows_autofit_max is None
                            or n_rows_seen_for_autofit < lim_inferred_rows_autofit_max
                        ):
                            n_rows_seen_for_autofit += 1

            # Apply autofit widths at the end to avoid overriding formats mid-write.
            if if_autofit_columns and df_slice_.width > 0:
                n_min = max(1, int(lim_width_autofit_cell_min))
                n_max = min(255, max(n_min, int(lim_width_autofit_cell_max)))
                n_pad = max(0, int(padding_autofit_width))
                for _col_idx in range(df_slice_.width):
                    n_col_width_recorded_ = (
                        dict_col_widths[rule_autofit_columns][_col_idx]
                        if rule_autofit_columns != "all"
                        else max(
                            dict_col_widths["header"][_col_idx],
                            dict_col_widths["body"][_col_idx],
                        )
                    )
                    n_col_width_final_ = min(
                        n_max,
                        max(n_min, n_col_width_recorded_ + n_pad),
                    )
                    cfg_worksheet_.set_column(
                        first_col=_col_idx,
                        last_col=_col_idx,
                        width=n_col_width_final_,
                        cell_format=l_fmt_by_col_[_col_idx],
                    )

            report.sheets.append(
                SpecSheetSlice(
                    sheet_name=c_sheet_name_unique_,
                    row_start_inclusive=_sheet_slice.row_start_inclusive,
                    row_end_exclusive=_sheet_slice.row_end_exclusive,
                    col_start_inclusive=_sheet_slice.col_start_inclusive,
                    col_end_exclusive=_sheet_slice.col_end_exclusive,
                )
            )

        self._reports.append(report)
        return self
