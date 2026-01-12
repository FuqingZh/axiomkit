# xlsxformatter_v1.py
from __future__ import annotations

import math
from code import interact
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, TypeAlias

import polars as pl
import xlsxwriter
import xlsxwriter.format
import xlsxwriter.worksheet

ColRef: TypeAlias = str | int


# -----------------------------
# Excel constraints
# -----------------------------
EXCEL_MAX_ROWS = 1_048_576
EXCEL_MAX_COLS = 16_384
N_EXCEL_SHEETNAME_MAX_LEN = 31
_EXCEL_ILLEGAL = ("*", ":", "?", "/", "\\", "[", "]")


# -----------------------------
# Report
# -----------------------------
@dataclass(frozen=True, slots=True)
class SheetPart:
    sheet_name: str
    row0: int
    row1: int  # exclusive in source df rows
    col0: int
    col1: int  # exclusive in source df cols


@dataclass(slots=True)
class WriteReport:
    sheets: list[SheetPart] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)


# -----------------------------
# Addon hooks (minimal & performance-safe)
# -----------------------------
class XlsxAddon(Protocol):
    """
    v1 addon contract:
    - MUST be O(1) / near O(1) per call.
    - SHOULD avoid per-cell override for performance.
    """

    def column_format_override(
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

    def cell_format_override(
        self,
        *,
        r0: int,
        c0: int,
        value: Any,
    ) -> xlsxwriter.format.Format | None:
        """
        Return a per-cell format. If any addon returns non-None,
        writer will fall back to slow per-cell write path.

        Default: None (recommended for speed).
        """
        return None


# -----------------------------
# Helpers
# -----------------------------
def _to_polars(df: Any) -> pl.DataFrame:
    if isinstance(df, pl.DataFrame):
        return df
    return pl.DataFrame(df)


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


def _resolve_col_index(df: pl.DataFrame, ref: ColRef) -> int:
    if isinstance(ref, int):
        return ref
    try:
        return df.columns.index(ref)
    except ValueError as e:
        raise KeyError(f"Column not found: {ref!r}") from e


def _indices_from_refs(
    df: pl.DataFrame, refs: Sequence[ColRef] | None
) -> tuple[int, ...]:
    if not refs:
        return ()
    idx = {_resolve_col_index(df, _r) for _r in refs}
    return tuple(sorted(idx))


def _contiguous_runs(sorted_indices: Sequence[int]) -> list[tuple[int, int]]:
    if not sorted_indices:
        return []
    runs: list[tuple[int, int]] = []
    s = e = sorted_indices[0]
    for x in sorted_indices[1:]:
        if x == e + 1:
            e = x
        else:
            runs.append((s, e))
            s = e = x
    runs.append((s, e))
    return runs


def _normalize_sheet_base(name: str, *, replace_to: str = "_") -> str:
    for ch in _EXCEL_ILLEGAL:
        name = name.replace(ch, replace_to)
    name = name.strip() or "Sheet"
    return name[:N_EXCEL_SHEETNAME_MAX_LEN]


def _make_sheet_name(base: str, part_idx_1based: int) -> str:
    suffix = f"_{part_idx_1based}"
    max_base_len = N_EXCEL_SHEETNAME_MAX_LEN - len(suffix)
    base2 = base[: max(1, max_base_len)]
    return f"{base2}{suffix}"


def _pick_chunk_rows(*, width: int) -> int:
    # v1: fixed + simple steps.
    if width >= 8_000:
        return 1_000
    if width >= 2_000:
        return 2_000
    return 10_000


def _is_nan_or_inf(x: float) -> bool:
    return math.isnan(x) or math.isinf(x)


# -----------------------------
# Header planner (horizontal true-merge + vertical visual-merge)
# -----------------------------
@dataclass(frozen=True, slots=True)
class HMerge:
    r0: int
    c0: int
    c1: int  # inclusive
    text: str


@dataclass(frozen=True, slots=True)
class BorderSpec:
    top: int
    bottom: int
    left: int
    right: int


def _plan_horizontal_merges(header_grid: list[list[str]]) -> dict[int, list[HMerge]]:
    merges: dict[int, list[HMerge]] = {}
    if not header_grid:
        return merges
    n_rows = len(header_grid)
    n_cols = len(header_grid[0])

    for r in range(n_rows):
        row = header_grid[r]
        c = 0
        while c < n_cols:
            v = row[c]
            if not v:
                c += 1
                continue
            k = c + 1
            while k < n_cols and row[k] == v:
                k += 1
            if k - c > 1:
                merges.setdefault(r, []).append(HMerge(r0=r, c0=c, c1=k - 1, text=v))
            c = k

    return merges


def _plan_vertical_visual_blocks(
    header_grid: Sequence[Sequence[str]],
) -> dict[tuple[int, int], BorderSpec]:
    """
    Visual vertical merge plan.
    For each column, find consecutive equal non-empty values and hide internal horizontal borders.

    Returns:
      cell -> BorderSpec (top/bottom/left/right in {0,1})
    Cells not in a vertical block will not be included (caller uses default full border).
    """
    plan: dict[tuple[int, int], BorderSpec] = {}
    if not header_grid:
        return plan

    n_rows = len(header_grid)
    n_cols = len(header_grid[0])

    for c in range(n_cols):
        r = 0
        while r < n_rows:
            v = header_grid[r][c]
            if not v:
                r += 1
                continue
            k = r + 1
            while k < n_rows and header_grid[k][c] == v:
                k += 1
            if k - r > 1:
                r0 = r
                r1 = k - 1
                for rr in range(r0, r1 + 1):
                    top = 1 if rr == r0 else 0
                    bottom = 1 if rr == r1 else 0
                    plan[(rr, c)] = BorderSpec(top=top, bottom=bottom, left=1, right=1)
                # blank out text for non-top cells (handled by writer)
            r = k
    return plan


def _blank_vertical_block_text(header_grid: list[list[str]]) -> list[list[str]]:
    """
    Copy grid and blank out texts in vertical runs except the first row of each run.
    """
    if not header_grid:
        return header_grid
    out = [row[:] for row in header_grid]
    n_rows = len(out)
    n_cols = len(out[0])

    for c in range(n_cols):
        r = 0
        while r < n_rows:
            v = out[r][c]
            if not v:
                r += 1
                continue
            k = r + 1
            while k < n_rows and out[k][c] == v:
                k += 1
            if k - r > 1:
                for rr in range(r + 1, k):
                    out[rr][c] = ""
            r = k
    return out


def _cells_covered_by_hmerges(
    hmerges_by_row: dict[int, list[HMerge]],
) -> dict[tuple[int, int], bool]:
    """
    Mark cells covered by horizontal merges (except leftmost cell),
    so we can skip writing them before merge_range.
    """
    covered: dict[tuple[int, int], bool] = {}
    for r, merges in hmerges_by_row.items():
        for m in merges:
            for c in range(m.c0 + 1, m.c1 + 1):
                covered[(r, c)] = True
    return covered


# -----------------------------
# Split plan (columns first, then rows)
# -----------------------------
def _plan_splits(
    *,
    height_df: int,
    width_df: int,
    nrows_header: int,
    sheet_name: str,
    report: WriteReport,
) -> list[SheetPart]:
    if nrows_header <= 0:
        raise ValueError("nrows_header must be >= 1.")
    data_max_rows = EXCEL_MAX_ROWS - nrows_header
    if data_max_rows <= 0:
        raise ValueError(
            f"Header too tall: nrows_header={nrows_header} exceeds Excel limit."
        )

    col_slices: list[tuple[int, int]] = []
    c0 = 0
    while c0 < width_df:
        c1 = min(width_df, c0 + EXCEL_MAX_COLS)
        col_slices.append((c0, c1))
        c0 = c1

    row_slices: list[tuple[int, int]] = []
    r0 = 0
    while r0 < height_df:
        r1 = min(height_df, r0 + data_max_rows)
        row_slices.append((r0, r1))
        r0 = r1

    parts: list[SheetPart] = []
    part_idx = 1
    for c0, c1 in col_slices:
        for r0, r1 in row_slices:
            parts.append(
                SheetPart(
                    sheet_name=_make_sheet_name(sheet_name, part_idx),
                    row0=r0,
                    row1=r1,
                    col0=c0,
                    col1=c1,
                )
            )
            part_idx += 1

    if len(parts) > 1:
        report.warn(
            f"Excel limit overflow: split into {len(parts)} sheets (columns-first, then rows)."
        )
    return parts


# -----------------------------
# Main writer
# -----------------------------
class XlsxFormatter:
    """
    收敛克制版 v1：

    - 默认 constant_memory=True（内存安全优先）
    - body：numeric 写 number；其他写 string
    - df_header：>=1 行，宽度必须等于 df.width
    - header：
        - 横向：真 merge_range（同一行安全）
        - 纵向：视觉合并（写首格，其余空；隐藏内部水平边框）
    - 拆分：先列后行；sheet 命名 base_1, base_2, ...
    - chunk：固定 + 简单阶梯（按列宽）
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

    def __enter__(self) -> "XlsxFormatter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _ensure_unique_sheet_name(self, name: str) -> str:
        if name not in self._existing_sheet_names:
            self._existing_sheet_names.add(name)
            return name

        # deterministic bump: name__2, name__3 ...
        c_base_name = name[: max(1, N_EXCEL_SHEETNAME_MAX_LEN - 3)]
        i = 2
        c_candidate_name = f"{c_base_name}__{i}"[:N_EXCEL_SHEETNAME_MAX_LEN]
        while c_candidate_name in self._existing_sheet_names:
            i += 1
            c_candidate_name = f"{c_base_name}__{i}"[:N_EXCEL_SHEETNAME_MAX_LEN]
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
            dt = df.schema[df.columns[_idx]]
            if isinstance(
                dt,
                (
                    pl.Int8,
                    pl.Int16,
                    pl.Int32,
                    pl.Int64,
                    pl.UInt8,
                    pl.UInt16,
                    pl.UInt32,
                    pl.UInt64,
                ),
            ):
                l_cols_idx_int.append(_idx)
        return tuple(l_cols_idx_int)

    def _get_header_fmt(self, border: BorderSpec | None) -> xlsxwriter.format.Format:
        # default full border
        if border is None:
            key = (1, 1, 1, 1)
            if key not in self._header_fmt_cache:
                spec = dict(self._header_base_spec)
                spec |= {"top": 1, "bottom": 1, "left": 1, "right": 1}
                self._header_fmt_cache[key] = self.wb.add_format(spec)
            return self._header_fmt_cache[key]

        key = (border.top, border.bottom, border.left, border.right)
        if key not in self._header_fmt_cache:
            spec = dict(self._header_base_spec)
            spec |= {
                "top": border.top,
                "bottom": border.bottom,
                "left": border.left,
                "right": border.right,
            }
            self._header_fmt_cache[key] = self.wb.add_format(spec)
        return self._header_fmt_cache[key]

    def _set_column_formats(
        self,
        ws: xlsxwriter.worksheet.Worksheet,
        *,
        width_df: int,
        numeric_cols_idx: tuple[int, ...],
        cols_integer_idx: tuple[int, ...],
        cols_decimal_idx: tuple[int, ...],
        column_fmt_overrides: dict[int, xlsxwriter.format.Format],
    ) -> None:
        if width_df <= 0:
            return

        # default text for all
        ws.set_column(0, width_df - 1, None, self.fmt_text)

        set_int = set(cols_integer_idx)
        set_dec = set(cols_decimal_idx)
        set_num = set(numeric_cols_idx)

        # numeric default: decimal; integer override: int
        num_int = sorted(set_int & set_num)
        num_dec = sorted((set_num - set_int) | (set_dec & set_num))

        for s, e in _contiguous_runs(num_dec):
            ws.set_column(s, e, None, self.fmt_dec)
        for s, e in _contiguous_runs(num_int):
            ws.set_column(s, e, None, self.fmt_int)

        for c0, fmt in column_fmt_overrides.items():
            if 0 <= c0 < width_df:
                ws.set_column(c0, c0, None, fmt)

    def _write_header(
        self,
        ws: xlsxwriter.worksheet.Worksheet,
        *,
        header_grid: list[list[str]],
        if_merge: bool,
    ) -> int:
        n_rows = len(header_grid)
        if not header_grid:
            raise ValueError(
                "header_grid cannot be empty (df_header must have >= 1 row)."
            )
        n_rows = len(header_grid)
        n_cols = len(header_grid[0])

        grid = header_grid
        v_border_plan: dict[tuple[int, int], BorderSpec] = {}
        b_visual_merge_vertical = bool(if_merge and n_rows > 1)
        if b_visual_merge_vertical and n_rows > 1:
            v_border_plan = _plan_vertical_visual_blocks(grid)
            grid = _blank_vertical_block_text(grid)

        hmerges_by_row = _plan_horizontal_merges(grid) if if_merge else {}
        hcovered = _cells_covered_by_hmerges(hmerges_by_row)

        # write cells (skip those covered by horizontal merge, except leftmost)
        for r in range(n_rows):
            for c in range(n_cols):
                if hcovered.get((r, c), False):
                    continue

                txt = grid[r][c]
                fmt = self._get_header_fmt(v_border_plan.get((r, c)))

                # Use write_string/write_blank explicitly
                if txt == "":
                    ws.write_blank(r, c, None, fmt)
                else:
                    ws.write_string(r, c, txt, fmt)

            # apply true horizontal merges (same row safe in constant_memory)
            for m in hmerges_by_row.get(r, []):
                # For merged block, pick a border spec that at least draws outer box.
                # (Excel treats merged region as one cell; internal per-col borders irrelevant.)
                fmt_block = self._get_header_fmt(
                    BorderSpec(top=1, bottom=1, left=1, right=1)
                )
                ws.merge_range(r, m.c0, r, m.c1, m.text, fmt_block)

        return n_rows

    def write_sheet(
        self,
        df: Any,
        sheet_name: str,
        *,
        df_header: Any | None = None,
        if_merge_header: bool = True,
        if_keep_na: bool = False,
        cols_integer: Sequence[ColRef] | None = None,
        cols_decimal: Sequence[ColRef] | None = None,
        col_freeze: int = 0,
        row_freeze: int | None = None,
        addons: Sequence[XlsxAddon] = (),
    ) -> WriteReport:
        report = WriteReport()

        df_ = _to_polars(df)
        l_colnames_df = df_.columns
        n_width_df = df_.width
        n_height_df = df_.height
        _assert_no_duplicate_columns(df_)

        # build header grid
        l_header_grid = [list(df_.columns)]
        if df_header is not None:
            df_header_ = _to_polars(df_header)
            _assert_no_duplicate_columns(df_header_)
            if df_header_.height == 0:
                raise ValueError(
                    "df_header must have >= 1 row (0-row header is not allowed)."
                )
            if df_header_.width != df_.width:
                raise ValueError("df_header.width must equal df.width.")

            l_header_grid = [
                ["" if v is None else str(v) for v in row]
                for row in df_header_.with_columns(pl.all().cast(pl.String)).iter_rows()
            ]

        # infer numeric and integer columns
        tup_cols_idx_num = self._infer_numeric_cols(df_)
        tup_cols_idx_integer_inferred = self._infer_integer_cols(df_, tup_cols_idx_num)

        tup_cols_idx_integer_specified = _indices_from_refs(df_, cols_integer)
        tup_cols_idx_decimal_specified = _indices_from_refs(df_, cols_decimal)

        # precedence: user override > inferred
        tup_cols_idx_integer = (
            tup_cols_idx_integer_specified
            if tup_cols_idx_integer_specified
            else tup_cols_idx_integer_inferred
        )
        tup_cols_idx_decimal = tup_cols_idx_decimal_specified  # optional

        # warnings: non-numeric columns written as string
        for _colname in l_colnames_df:
            if (cls_col_dtype_ := df_.schema[_colname]).is_numeric():
                continue

            b_common_scalar = (
                cls_col_dtype_ == pl.String
                or cls_col_dtype_ == pl.Categorical
                or cls_col_dtype_ == pl.Enum
                or cls_col_dtype_ == pl.Boolean
                or cls_col_dtype_.is_temporal()
                or cls_col_dtype_ == pl.Null
            )

            if not b_common_scalar:
                report.warn(
                    f"Column {_colname!r} dtype {cls_col_dtype_} will be written as string."
                )

        c_sheetname_norm = _normalize_sheet_base(sheet_name)

        # header rows count influences split (Excel max rows)
        n_rows_header = len(l_header_grid)
        parts = _plan_splits(
            height_df=n_height_df,
            width_df=n_width_df,
            nrows_header=n_rows_header,
            sheet_name=c_sheetname_norm,
            report=report,
        )

        # decide freeze_row
        row_freeze = n_rows_header if row_freeze is None else row_freeze

        # addon column overrides (fast path)
        col_overrides_global: dict[int, xlsxwriter.format.Format] = {}
        for ad in addons:
            col_overrides_global |= ad.column_format_override(
                df=df, fmt_sci=self.fmt_sci
            )

        # determine whether we must fall back to slow per-cell body write
        # If any addon potentially returns a non-None cell format, we assume slow path.
        # (v1: we do a single probe with a cheap call contract; you can also pass addons=() for fast path.)
        b_any_cell_override = False
        for ad in addons:
            try:
                if ad.cell_format_override(r0=0, c0=0, value="__probe__") is not None:
                    b_any_cell_override = True
                    break
            except Exception:
                # If addon throws, treat it as requiring slow path (and let actual run raise later if needed).
                b_any_cell_override = True
                break

        for part in parts:
            sheet_name_part = self._ensure_unique_sheet_name(part.sheet_name)
            ws = self.wb.add_worksheet(sheet_name_part)

            # Slice df by this part
            cols_part = df.columns[part.col0 : part.col1]
            df_part = df.slice(part.row0, part.row1 - part.row0).select(
                [pl.col(c) for c in cols_part]
            )

            # Column formats (relative indices in this sheet)
            numeric_cols_part = tuple(
                i - part.col0 for i in tup_cols_idx_num if part.col0 <= i < part.col1
            )
            cols_integer_part = tuple(
                i - part.col0
                for i in tup_cols_idx_integer
                if part.col0 <= i < part.col1
            )
            cols_decimal_part = tuple(
                i - part.col0
                for i in tup_cols_idx_decimal
                if part.col0 <= i < part.col1
            )

            col_overrides_part = {
                k - part.col0: v
                for k, v in col_overrides_global.items()
                if part.col0 <= k < part.col1
            }

            self._set_column_formats(
                ws,
                width_df=df_part.width,
                numeric_cols_idx=numeric_cols_part,
                cols_integer_idx=cols_integer_part,
                cols_decimal_idx=cols_decimal_part,
                column_fmt_overrides=col_overrides_part,
            )

            # Header (slice columns)
            header_grid_part = [row[part.col0 : part.col1] for row in l_header_grid]
            _ = self._write_header(
                ws,
                header_grid=header_grid_part,
                if_merge=if_merge_header,
            )

            # freeze panes
            ws.freeze_panes(row_freeze, col_freeze)

            # Body: build cast expressions
            # v1 rule: numeric -> Float64; non-numeric -> String
            numeric_set = {
                i
                for i, c in enumerate(df_part.columns)
                if df_part.schema[c].is_numeric()
            }

            exprs: list[pl.Expr] = []
            for i, c in enumerate(df_part.columns):
                if i in numeric_set:
                    exprs.append(pl.col(c).cast(pl.Float64).alias(c))
                else:
                    exprs.append(pl.col(c).cast(pl.String).alias(c))

            chunk_rows = _pick_chunk_rows(width=df_part.width)
            n_rows = df_part.height
            data_start_row0 = n_rows_header

            # Fast path: write_row with python values (no per-cell formats)
            if not b_any_cell_override:
                r_cursor = 0
                while r_cursor < n_rows:
                    take = min(chunk_rows, n_rows - r_cursor)
                    chunk = df_part.slice(r_cursor, take).select(exprs)

                    for i_row, row in enumerate(chunk.iter_rows(named=False)):
                        excel_r0 = data_start_row0 + r_cursor + i_row
                        out = []
                        for c0, v in enumerate(row):
                            if c0 in numeric_set:
                                if v is None:
                                    out.append(None)
                                else:
                                    fv = float(v)
                                    if _is_nan_or_inf(fv):
                                        if if_keep_na:
                                            out.append(
                                                "NaN"
                                                if math.isnan(fv)
                                                else ("Inf" if fv > 0 else "-Inf")
                                            )
                                        else:
                                            out.append(None)
                                    else:
                                        out.append(fv)
                            else:
                                if v is None:
                                    out.append(None)
                                else:
                                    out.append(str(v))
                        ws.write_row(excel_r0, 0, out)
                    r_cursor += take

            else:
                # Slow path: per-cell write to allow cell formats.
                r_cursor = 0
                while r_cursor < n_rows:
                    take = min(chunk_rows, n_rows - r_cursor)
                    chunk = df_part.slice(r_cursor, take).select(exprs)

                    for i_row, row in enumerate(chunk.iter_rows(named=False)):
                        excel_r0 = data_start_row0 + r_cursor + i_row
                        for c0, v in enumerate(row):
                            if c0 in numeric_set:
                                if v is None:
                                    ws.write_blank(excel_r0, c0, None)
                                    continue
                                fv = float(v)
                                if _is_nan_or_inf(fv):
                                    if if_keep_na:
                                        s = (
                                            "NaN"
                                            if math.isnan(fv)
                                            else ("Inf" if fv > 0 else "-Inf")
                                        )
                                        fmt_cell = None
                                        for ad in addons:
                                            fmt_cell = (
                                                ad.cell_format_override(
                                                    r0=excel_r0, c0=c0, value=s
                                                )
                                                or fmt_cell
                                            )
                                        if fmt_cell is None:
                                            ws.write_string(excel_r0, c0, s)
                                        else:
                                            ws.write_string(excel_r0, c0, s, fmt_cell)
                                    else:
                                        ws.write_blank(excel_r0, c0, None)
                                    continue

                                fmt_cell = None
                                for ad in addons:
                                    fmt_cell = (
                                        ad.cell_format_override(
                                            r0=excel_r0, c0=c0, value=fv
                                        )
                                        or fmt_cell
                                    )
                                if fmt_cell is None:
                                    ws.write_number(excel_r0, c0, fv)
                                else:
                                    ws.write_number(excel_r0, c0, fv, fmt_cell)
                            else:
                                if v is None:
                                    ws.write_blank(excel_r0, c0, None)
                                    continue
                                s = str(v)
                                fmt_cell = None
                                for ad in addons:
                                    fmt_cell = (
                                        ad.cell_format_override(
                                            r0=excel_r0, c0=c0, value=s
                                        )
                                        or fmt_cell
                                    )
                                if fmt_cell is None:
                                    ws.write_string(excel_r0, c0, s)
                                else:
                                    ws.write_string(excel_r0, c0, s, fmt_cell)

                    r_cursor += take

            report.sheets.append(
                SheetPart(
                    sheet_name=sheet_name_part,
                    row0=part.row0,
                    row1=part.row1,
                    col0=part.col0,
                    col1=part.col1,
                )
            )

        return report


__all__ = [
    "XlsxFormatter",
    "WriteReport",
    "SheetPart",
    "XlsxAddon",
    "ColRef",
]
