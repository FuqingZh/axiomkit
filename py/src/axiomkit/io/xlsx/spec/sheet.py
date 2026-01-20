from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SpecSheetSlice:
    sheet_name: str
    row_start_inclusive: int
    row_end_exclusive: int  # exclusive in source df rows
    col_start_inclusive: int
    col_end_exclusive: int  # exclusive in source df cols


@dataclass(frozen=True, slots=True)
class SpecSheetHorizontalMerge:
    row_idx_start: int
    col_idx_start: int
    col_idx_end: int  # inclusive
    text: str
