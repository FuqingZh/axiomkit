import math
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import TracebackType
from typing import Any, ClassVar, Literal, Self

import polars as pl
import xlsxwriter
import xlsxwriter.format
import xlsxwriter.worksheet

from .addon import XlsxAddon, check_addon_cell_write_requirement, write_cell_with_format
from .conf import (
    DEFAULT_XLSX_FORMATS,
    DEFAULT_XLSX_WRITE_OPTIONS,
    LIT_FMT_KEYS,
    N_LEN_EXCEL_SHEET_NAME_MAX,
    ColumnIdentifier,
)
from .spec import (
    SpecCellBorder,
    SpecCellFormat,
    SpecColumnFormatPlan,
    SpecXlsxValuePolicy,
    SpecXlsxWriteOptions,
    SpecSheetSlice,
    SpecXlsxReport,
)
from .util import (
    apply_vertical_run_text_blankout,
    calculate_row_chunk_size,
    convert_cell_value,
    convert_nan_inf_to_str,
    convert_to_polars,
    create_horizontal_merge_tracker,
    generate_row_chunks,
    generate_sheet_slices,
    plan_horizontal_merges,
    plan_vertical_visual_merge_borders,
    sanitize_sheet_name,
    select_integer_cols,
    select_numeric_cols,
    select_sorted_indices_from_refs,
    validate_unique_columns,
)


class XlsxWriter:
    """
    Helper class for writing tabular data to an XLSX workbook using
    ``xlsxwriter`` with sensible defaults and formatting utilities.

    This class wraps :class:`xlsxwriter.Workbook` and provides a small
    formatting layer (default text/number formats, header styles, and
    sheet‑splitting logic) to make it easier to export large tables to
    Excel. It can be used either directly or as a context manager.

    The workbook is created on initialization and closed via :meth:`close`
    or automatically when used in a ``with`` block. Constant-memory mode
    is enabled by default and NaN/Inf are handled explicitly.

        from pathlib import Path
        from axiomkit.io import XlsxWriter

        with XlsxWriter("report.xlsx") as xf:
            # Use xf methods to add sheets and write data frames / tables
            ...
        or

        xf = XlsxWriter("report.xlsx")

    Parameters
    ----------
    file_out:
        Path to the output ``.xlsx`` file. Can be a string or
        :class:`pathlib.Path`. The underlying workbook is created
        immediately for this path.
    fmt_text:
        Base text format for data cells.
    fmt_integer:
        Integer numeric format. Defaults to ``DEFAULT_XLSX_FORMATS["integer"]``.
    fmt_decimal:
        Decimal numeric format. Defaults to ``DEFAULT_XLSX_FORMATS["decimal"]``.
    fmt_scientific:
        Scientific numeric format. Defaults to ``DEFAULT_XLSX_FORMATS["scientific"]``.
    fmt_header:
        Header cell format. Defaults to ``DEFAULT_XLSX_FORMATS["header"]``.
    write_options:
        Optional write-time policies (value formatting, inference, chunking).
    """

    DEFAULT_XLSX_FORMATS: ClassVar[Mapping[LIT_FMT_KEYS, SpecCellFormat]] = (
        DEFAULT_XLSX_FORMATS
    )
    DEFAULT_XLSX_WRITE_OPTIONS: ClassVar[SpecXlsxWriteOptions] = (
        DEFAULT_XLSX_WRITE_OPTIONS
    )

    def __init__(
        self,
        file_out: os.PathLike[str] | str,
        *,
        fmt_text: SpecCellFormat | None = None,
        fmt_integer: SpecCellFormat | None = None,
        fmt_decimal: SpecCellFormat | None = None,
        fmt_scientific: SpecCellFormat | None = None,
        fmt_header: SpecCellFormat | None = None,
        write_options: SpecXlsxWriteOptions | None = None,
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

        self.fmt_text = (
            self.DEFAULT_XLSX_FORMATS["text"] if fmt_text is None else fmt_text
        )
        self.fmt_int = (
            self.DEFAULT_XLSX_FORMATS["integer"] if fmt_integer is None else fmt_integer
        )
        self.fmt_dec = (
            self.DEFAULT_XLSX_FORMATS["decimal"] if fmt_decimal is None else fmt_decimal
        )
        self.fmt_sci = (
            self.DEFAULT_XLSX_FORMATS["scientific"]
            if fmt_scientific is None
            else fmt_scientific
        )
        self.fmt_header = (
            self.DEFAULT_XLSX_FORMATS["header"] if fmt_header is None else fmt_header
        )
        self.write_options = (
            self.DEFAULT_XLSX_WRITE_OPTIONS
            if write_options is None
            else write_options
        )
        self._header_fmt_cache: dict[
            tuple[int, int, int, int], xlsxwriter.format.Format
        ] = {}
        self._existing_sheet_names: set[str] = set()
        self._reports: list[SpecXlsxReport] = []

    def __enter__(self) -> "XlsxWriter":
        """Enter context manager scope."""
        return self

    def __exit__(
        self, exc_type: type | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        """Exit context manager scope and close the workbook."""
        self.close()

    def close(self) -> None:
        """Finalize and close the underlying workbook."""
        self.wb.close()

    def report(self) -> tuple[SpecXlsxReport, ...]:
        """Return reports collected during write operations."""
        return tuple(self._reports)

    @staticmethod
    def _estimate_width_len(
        value: Any,
        *,
        if_is_numeric_col: bool,
        if_is_integer_col: bool,
        if_keep_missing_values: bool,
        value_policy: SpecXlsxValuePolicy,
    ) -> int:
        """
        Estimate display string length for autofit width calculations.

        Args:
            value: Cell value to evaluate.
            if_is_numeric_col: Whether the column is numeric.
            if_is_integer_col: Whether the column is integer-typed.
            if_keep_missing_values: If True, treat missing values as "NA".
            value_policy: Value policy for missing/NaN/Inf string rendering.

        Returns:
            Estimated display width in characters.
        """
        if value is None:
            return (
                len(value_policy.missing_value_str) if if_keep_missing_values else 0
            )

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
            return len(convert_nan_inf_to_str(x=n_val, value_policy=value_policy))

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

    def _create_format_cached(self, spec: SpecCellFormat) -> Any:
        """
        Return a cached xlsxwriter Format for the given spec.

        Creates and caches the format on first use.
        """
        fmt = self._format_cache.get(spec)
        if fmt is None:
            fmt = self.wb.add_format(spec.to_xlsxwriter())
            self._format_cache[spec] = fmt
        return fmt

    def _create_unique_sheet_name(self, name: str) -> str:
        """
        Return a sheet name unique within this workbook.

        If the name already exists, append a deterministic ``__N`` suffix.
        """
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

    def _create_header_fmt(
        self, border: SpecCellBorder | None
    ) -> xlsxwriter.format.Format:
        """
        Return a header format with optional per-cell borders.

        The format is cached by border tuple to avoid duplicate formats.
        """
        # default full border
        if border is None:
            tup_border_format_key = (1, 1, 1, 1)
            if self._header_fmt_cache.get(tup_border_format_key) is None:
                self._header_fmt_cache[tup_border_format_key] = (
                    self._create_format_cached(
                        self.fmt_header.with_(top=1, bottom=1, left=1, right=1)
                    )
                )
            return self._header_fmt_cache[tup_border_format_key]

        tup_border_format_key = (border.top, border.bottom, border.left, border.right)
        if self._header_fmt_cache.get(tup_border_format_key) is None:
            self._header_fmt_cache[tup_border_format_key] = self._create_format_cached(
                self.fmt_header.with_(
                    top=border.top,
                    bottom=border.bottom,
                    left=border.left,
                    right=border.right,
                )
            )
        return self._header_fmt_cache[tup_border_format_key]

    def _plan_column_formats(
        self,
        *,
        width_data: int,
        cols_idx_numeric: tuple[int, ...],
        cols_idx_integer: tuple[int, ...],
        cols_idx_decimal: tuple[int, ...] | Literal[False],
        cols_fmt_overrides: dict[int, SpecCellFormat],
    ) -> SpecColumnFormatPlan:
        """
        Plan per-column formats for data cells and column bases.

        Args:
            width_data: Number of data columns.
            cols_idx_numeric: Numeric column indices (0-based).
            cols_idx_integer: Integer column indices (0-based).
            cols_idx_decimal: Decimal column indices (0-based) or False to disable.
            cols_fmt_overrides: Per-column format overrides.

        Returns:
            Planned column formats for data cells and column bases.
        """
        if width_data <= 0:
            return SpecColumnFormatPlan(
                fmts_by_col=[],
                fmts_base_by_col=[],
            )
        cfg_fmt_text = self.fmt_text
        cfg_fmt_int = self.fmt_int
        cfg_fmt_dec = self.fmt_dec

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

        l_fmt_by_col: list[SpecCellFormat] = [cfg_fmt_text] * width_data
        for _i in l_cols_idx_dec_sorted:
            l_fmt_by_col[_i] = cfg_fmt_dec
        for _i in l_cols_idx_int_sorted:
            l_fmt_by_col[_i] = cfg_fmt_int
        for _col_idx, _fmt in cols_fmt_overrides.items():
            if 0 <= _col_idx < width_data:
                l_fmt_by_col[_col_idx] = l_fmt_by_col[_col_idx].merge(_fmt)

        base_patch = self.write_options.base_format_patch
        l_fmt_base_by_col = [_fmt.merge(base_patch) for _fmt in l_fmt_by_col]

        return SpecColumnFormatPlan(
            fmts_by_col=l_fmt_by_col,
            fmts_base_by_col=l_fmt_base_by_col,
        )

    def _apply_column_formats(
        self,
        ws: xlsxwriter.worksheet.Worksheet,
        plan: SpecColumnFormatPlan,
        *,
        widths: list[float] | None,
    ) -> None:
        """
        Apply base column formats to a worksheet.

        If ``widths`` is provided, column widths are set alongside formats.
        """
        n_cols = len(plan.fmts_base_by_col)
        if n_cols <= 0:
            return

        if widths is None:
            for _col_idx, _fmt in enumerate(plan.fmts_base_by_col):
                ws.set_column(
                    first_col=_col_idx,
                    last_col=_col_idx,
                    cell_format=self._create_format_cached(_fmt),
                )
        else:
            for _col_idx, _width in enumerate(widths):
                ws.set_column(
                    first_col=_col_idx,
                    last_col=_col_idx,
                    width=_width,
                    cell_format=self._create_format_cached(
                        plan.fmts_base_by_col[_col_idx]
                    ),
                )

    def _write_header(
        self,
        ws: xlsxwriter.worksheet.Worksheet,
        *,
        header_grid: list[list[str]],
        if_merge: bool,
    ):
        """
        Write the header grid and apply visual merges when requested.

        Args:
            ws: Target worksheet.
            header_grid: 2D grid of header strings (rows x columns).
            if_merge: Whether to visually merge repeated labels.

        Raises:
            ValueError: If the header grid is empty.
        """
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
            l_header_grid = apply_vertical_run_text_blankout(l_header_grid)

        dict_horizontal_merges_by_row = (
            plan_horizontal_merges(l_header_grid) if if_merge else {}
        )
        dict_horizontal_merge_tracker = create_horizontal_merge_tracker(
            dict_horizontal_merges_by_row
        )

        # write cells (skip those covered by horizontal merge, except leftmost)
        for _row_idx in range(n_rows):
            for _col_idx in range(n_cols):
                if dict_horizontal_merge_tracker.get((_row_idx, _col_idx), False):
                    continue

                cfg_cell_format_ = self._create_header_fmt(
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
                cfg_block_format_ = self._create_header_fmt(
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
        cols_integer: Sequence[ColumnIdentifier] | None = None,
        cols_decimal: Sequence[ColumnIdentifier] | None | Literal[False] = None,
        col_freeze: int = 0,
        row_freeze: int | None = None,
        if_merge_header: bool = False,
        if_keep_missing_values: bool | None = None,
        if_autofit_columns: bool = True,
        rule_autofit_columns: Literal["header", "body", "all"] = "header",
        height_data_autofit_inferred_max: int | None = 20_000,
        width_cell_autofit_min: int = 8,
        width_cell_autofit_max: int = 60,
        width_cell_autofit_padding: int = 2,
        addons: Sequence[XlsxAddon] = (),
    ) -> Self:
        """
        Write a dataframe to one or more Excel worksheets.

        The input is converted to a Polars DataFrame, optionally combined with
        a custom header grid, and written with default text/number formats.
        If the data exceeds Excel limits, the output is split across multiple
        sheets with deterministic suffixes.

        Args:
            df: Any tabular data accepted by Polars.
            sheet_name: Desired worksheet name (will be sanitized and de-duplicated).
            df_header: Optional header grid (same width as df). If omitted,
                df column names are used.
            cols_integer: Column refs to force integer formatting.
            cols_decimal: Column refs to force decimal formatting. If None
                (the default) or False, decimal formatting is disabled for
                non-integer numeric columns.
            col_freeze: Number of leftmost columns to freeze.
            row_freeze: Number of top rows to freeze. Defaults to header height.
            if_merge_header: If True, visually merge repeated header labels.
            if_keep_missing_values: If True, write missing values using the
                configured missing-value string; otherwise leave cells blank.
                If None, uses ``write_options.keep_missing_values``.
            if_autofit_columns: If True, estimate and set column widths.
            rule_autofit_columns: Width estimation source ("header", "body", "all").
            height_data_autofit_inferred_max: Max data rows to sample for autofit.
            width_cell_autofit_min: Minimum column width for autofit.
            width_cell_autofit_max: Maximum column width for autofit.
            width_cell_autofit_padding: Extra width padding for autofit.
            addons: Optional addons that provide per-column or per-cell formats.
                Per-cell addon coordinates are 0-based within the sheet's data
                region (headers excluded).

        Raises:
            ValueError: If header width mismatches df width, or header is empty.

        Returns:
            Self: The writer instance (for chaining).
        """
        inst_report = SpecXlsxReport(
            sheets=[],
            warnings=[],
        )
        cfg_keep_missing_values = (
            self.write_options.keep_missing_values
            if if_keep_missing_values is None
            else if_keep_missing_values
        )
        value_policy = self.write_options.value_policy

        df_custom = convert_to_polars(df)
        l_colnames_df = df_custom.columns
        n_width_df = df_custom.width
        n_height_df = df_custom.height
        validate_unique_columns(df_custom)

        # build header grid
        l_header_grid = [list(df_custom.columns)]
        if df_header is not None:
            df_header_custom = convert_to_polars(df_header)
            validate_unique_columns(df_header_custom)
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

        # infer numeric and integer columns (policy-controlled)
        tup_cols_idx_numeric = (
            select_numeric_cols(df_custom)
            if self.write_options.infer_numeric_cols
            else ()
        )
        tup_cols_idx_integer_inferred = (
            select_integer_cols(df_custom, tup_cols_idx_numeric)
            if self.write_options.infer_integer_cols
            else ()
        )

        tup_cols_idx_integer_specified = select_sorted_indices_from_refs(
            df_custom, cols_integer
        )
        tup_cols_idx_decimal_specified = (
            select_sorted_indices_from_refs(df_custom, cols_decimal)
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
                inst_report.warn(
                    f"Column {_colname!r} dtype {cfg_col_dtype_} will be written as string."
                )

        # header rows count influences split (Excel max rows)
        n_rows_header = len(l_header_grid)
        l_sheet_parts = generate_sheet_slices(
            height_df=n_height_df,
            width_df=n_width_df,
            height_header=n_rows_header,
            sheet_name=sanitize_sheet_name(sheet_name),
            report=inst_report,
        )

        # decide freeze_row
        row_freeze = n_rows_header if row_freeze is None else row_freeze

        # addon column overrides (fast path)
        fmt_map = {
            "text": self.fmt_text,
            "integer": self.fmt_int,
            "decimal": self.fmt_dec,
            "scientific": self.fmt_sci,
            "header": self.fmt_header,
        }
        dict_cols_fmt_overrides: dict[int, SpecCellFormat] = {}
        for _ad in addons:
            for _col_idx, _fmt in _ad.create_column_format_overrides(
                df=df_custom, fmt_map=fmt_map
            ).items():
                if _col_idx in dict_cols_fmt_overrides:
                    if self.write_options.warn_addon_override_conflicts:
                        inst_report.warn(
                            "Column override conflict: "
                            f"col={_col_idx}, addon={_ad.__class__.__name__}. "
                            "Later overrides merge on top of earlier ones."
                        )
                    dict_cols_fmt_overrides[_col_idx] = dict_cols_fmt_overrides[
                        _col_idx
                    ].merge(_fmt)
                else:
                    dict_cols_fmt_overrides[_col_idx] = _fmt

        # determine whether we must fall back to slow per-cell body write
        # If any addon requires per-cell formats, fall back to the slow path.
        b_any_cell_override = any(
            check_addon_cell_write_requirement(_ad) for _ad in addons
        )

        for _sheet_slice in l_sheet_parts:
            c_sheet_name_unique_ = self._create_unique_sheet_name(
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

            # Column formats plan (applied after body/autofit).
            plan_col_formats = self._plan_column_formats(
                width_data=df_slice_.width,
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
                                    if_keep_missing_values=cfg_keep_missing_values,
                                    value_policy=value_policy,
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
            # v1 rule: keep numeric types; non-numeric -> String
            set_cols_idx_numeric = set(tup_cols_idx_numeric_slice_)

            l_col_cast_expressions: list[pl.Expr] = []
            for _idx, _val in enumerate(df_slice_.columns):
                if _idx in set_cols_idx_numeric:
                    l_col_cast_expressions.append(pl.col(_val).alias(_val))
                else:
                    l_col_cast_expressions.append(
                        pl.col(_val).cast(pl.String).alias(_val)
                    )

            n_rows_chunk_ = calculate_row_chunk_size(
                width_df=df_slice_.width,
                policy=self.write_options.row_chunk_policy,
            )
            n_cols_ = df_slice_.width
            n_row_start_data_ = n_rows_header
            l_is_numeric_col = [_idx in set_cols_idx_numeric for _idx in range(n_cols_)]

            set_cols_idx_int_slice_ = set(tup_cols_idx_integer_slice_)
            l_is_integer_col = [
                _idx in set_cols_idx_int_slice_ for _idx in range(n_cols_)
            ]

            # NOTE: base column formats use write_options.base_format_patch;
            # data rows apply per-cell formats to avoid conditional_format side effects.
            l_fmt_data_by_col = [
                self._create_format_cached(_fmt)
                for _fmt in plan_col_formats.fmts_by_col
            ]

            n_rows_seen_for_autofit = 0

            # Fast path: per-cell write with precomputed formats (no addon overrides)
            if not b_any_cell_override:
                for _row_idx, _df_chunk in generate_row_chunks(
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
                                if_is_integer_col=l_is_integer_col[_col_idx],
                                if_keep_missing_values=cfg_keep_missing_values,
                                value_policy=value_policy,
                            )
                            for _col_idx, _col_val in enumerate(_row_val)
                        ]

                        # Update width estimates (bounded by num_autofit_rows_max).
                        if if_autofit_columns and (
                            height_data_autofit_inferred_max is None
                            or n_rows_seen_for_autofit
                            < height_data_autofit_inferred_max
                        ):
                            for _col_idx, _cell_val in enumerate(l_row_vals):
                                dict_col_widths["body"][_col_idx] = max(
                                    dict_col_widths["body"][_col_idx],
                                    self._estimate_width_len(
                                        _cell_val,
                                        if_is_numeric_col=l_is_numeric_col[_col_idx],
                                        if_is_integer_col=l_is_integer_col[_col_idx],
                                        if_keep_missing_values=cfg_keep_missing_values,
                                        value_policy=value_policy,
                                    ),
                                )
                            n_rows_seen_for_autofit += 1

                        for _col_idx, _val in enumerate(l_row_vals):
                            cfg_fmt_cell = l_fmt_data_by_col[_col_idx]
                            if _val is None:
                                cfg_worksheet_.write_blank(
                                    row=n_row_idx_excel_0based_ + _row_idx_chunk,
                                    col=_col_idx,
                                    blank=None,
                                    cell_format=cfg_fmt_cell,
                                )
                            elif isinstance(_val, str):
                                cfg_worksheet_.write_string(
                                    row=n_row_idx_excel_0based_ + _row_idx_chunk,
                                    col=_col_idx,
                                    string=_val,
                                    cell_format=cfg_fmt_cell,
                                )
                            else:
                                cfg_worksheet_.write_number(
                                    row=n_row_idx_excel_0based_ + _row_idx_chunk,
                                    col=_col_idx,
                                    number=_val,
                                    cell_format=cfg_fmt_cell,
                                )
            else:
                # Slow path: per-cell write to allow cell formats.
                for _row_cursor, _df_chunk in generate_row_chunks(
                    df=df_slice_,
                    size_rows_chunk=n_rows_chunk_,
                    cols_exprs=l_col_cast_expressions,
                ):
                    n_row_idx_excel_0based_ = n_row_start_data_ + _row_cursor
                    for _row_idx_chunk, _row_val in enumerate(_df_chunk.iter_rows()):
                        for _col_idx, _col_val in enumerate(_row_val):
                            if if_autofit_columns and (
                                height_data_autofit_inferred_max is None
                                or n_rows_seen_for_autofit
                                < height_data_autofit_inferred_max
                            ):
                                c_cell_val = convert_cell_value(
                                    value=_col_val,
                                    if_is_numeric_col=l_is_numeric_col[_col_idx],
                                    if_is_integer_col=l_is_integer_col[_col_idx],
                                    if_keep_missing_values=cfg_keep_missing_values,
                                    value_policy=value_policy,
                                )
                                dict_col_widths["body"][_col_idx] = max(
                                    dict_col_widths["body"][_col_idx],
                                    self._estimate_width_len(
                                        c_cell_val,
                                        if_is_numeric_col=l_is_numeric_col[_col_idx],
                                        if_is_integer_col=l_is_integer_col[_col_idx],
                                        if_keep_missing_values=cfg_keep_missing_values,
                                        value_policy=value_policy,
                                    ),
                                )
                            write_cell_with_format(
                                cfg_worksheet_,
                                addons,
                                self._create_format_cached,
                                row_idx_sheet=n_row_idx_excel_0based_ + _row_idx_chunk,
                                col_idx_sheet=_col_idx,
                                row_idx_data=_row_cursor + _row_idx_chunk,
                                col_idx_data=_col_idx,
                                value=_col_val,
                                if_is_numeric_col=l_is_numeric_col[_col_idx],
                                if_is_integer_col=l_is_integer_col[_col_idx],
                                if_keep_missing_values=cfg_keep_missing_values,
                                value_policy=value_policy,
                                fmt_base=plan_col_formats.fmts_by_col[_col_idx],
                            )

                        if if_autofit_columns and (
                            height_data_autofit_inferred_max is None
                            or n_rows_seen_for_autofit
                            < height_data_autofit_inferred_max
                        ):
                            n_rows_seen_for_autofit += 1

            # Apply formats (and widths if autofit enabled) at the end.
            if if_autofit_columns and df_slice_.width > 0:
                n_min = max(1, int(width_cell_autofit_min))
                n_max = min(255, max(n_min, int(width_cell_autofit_max)))
                n_pad = max(0, int(width_cell_autofit_padding))
                l_col_widths_final: list[float] = []
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
                    l_col_widths_final.append(n_col_width_final_)
                self._apply_column_formats(
                    cfg_worksheet_,
                    plan_col_formats,
                    widths=l_col_widths_final,
                )
            else:
                self._apply_column_formats(
                    cfg_worksheet_,
                    plan_col_formats,
                    widths=None,
                )

            inst_report.sheets.append(
                SpecSheetSlice(
                    sheet_name=c_sheet_name_unique_,
                    row_start_inclusive=_sheet_slice.row_start_inclusive,
                    row_end_exclusive=_sheet_slice.row_end_exclusive,
                    col_start_inclusive=_sheet_slice.col_start_inclusive,
                    col_end_exclusive=_sheet_slice.col_end_exclusive,
                )
            )

        self._reports.append(inst_report)
        return self
