from ..conf import (
    N_LEN_EXCEL_SHEET_NAME_MAX,
    N_NCOLS_EXCEL_MAX,
    N_NROWS_EXCEL_MAX,
    TUP_EXCEL_ILLEGAL,
)
from ..spec import SpecSheetSlice, SpecXlsxReport


def normalize_sheet_name(name: str, *, replace_to: str = "_") -> str:
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
                else _make_sheet_name(sheet_name, n_idx_part)
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


def _make_sheet_name(base_name: str, part_idx_1based: int) -> str:
    c_sheet_name_suffix = f"_{part_idx_1based}"
    n_len_base_name_max = N_LEN_EXCEL_SHEET_NAME_MAX - len(c_sheet_name_suffix)
    c_sheet_name_base = base_name[: max(1, n_len_base_name_max)]
    return f"{c_sheet_name_base}{c_sheet_name_suffix}"
