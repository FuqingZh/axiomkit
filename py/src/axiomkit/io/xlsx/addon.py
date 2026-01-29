from collections.abc import Sequence
from typing import Any, Protocol

import polars as pl
import xlsxwriter
import xlsxwriter.worksheet

from .spec import SpecCellFormat
from .spec import SpecXlsxValuePolicy
from .util import convert_cell_value


class XlsxAddon(Protocol):
    """
    v1 addon contract (performance-first):

    - Column-level overrides MUST be O(1) / near O(1) per column.
    - Per-cell overrides are allowed but will force the writer into the slow per-cell path.
      Use only when necessary.

    Capability declaration
    ----------------------
    Addons SHOULD explicitly declare whether they need per-cell formatting by
    implementing `check_cell_write_required()` (preferred) or a boolean attribute
    `check_cell_write_required`.

    Coordinate semantics
    --------------------
    For per-cell overrides, `row_idx` and `col_idx` refer to 0-based coordinates
    within the *data region* (excluding header rows), not the worksheet grid.
    """

    def check_cell_write_required(self) -> bool:  # optional, preferred
        return False

    def create_column_format_overrides(
        self,
        *,
        df: pl.DataFrame,
        fmt_map: dict[str, SpecCellFormat],
    ) -> dict[int, SpecCellFormat]:
        """
        Return {col_idx_0based: SpecCellFormat} column-level overrides.
        Default: {}

        Merge contract:
        - If multiple addons provide overrides for the same column, later addons
          are merged on top of earlier ones (non-None fields win).
        """
        return {}

    def create_cell_format_override(
        self,
        *,
        row_idx: int,
        col_idx: int,
        value: Any,
    ) -> SpecCellFormat | None:
        """
        Return a per-cell SpecCellFormat patch. If any addon returns non-None,
        writer will fall back to slow per-cell write path.

        Coordinates are 0-based and refer to the data region (excluding headers).

        Merge contract:
        - If multiple addons return a patch for the same cell, later addons are
          merged on top of earlier ones (non-None fields win).

        Default: None (recommended for speed).
        """
        return None


def check_addon_cell_write_requirement(ad: XlsxAddon) -> bool:
    """
    Decide whether an addon forces the slow per-cell body write path.

    Contract:
      - method: `check_cell_write_required() -> bool`
      - attribute: `check_cell_write_required: bool`
    """
    # explicit: method
    meth = getattr(ad, "check_cell_write_required", None)
    if callable(meth):
        try:
            return bool(meth())
        except Exception:
            # conservative: if addon misbehaves, choose safety (slow path)
            return True

    # explicit: attribute
    if hasattr(ad, "check_cell_write_required"):
        try:
            return bool(getattr(ad, "check_cell_write_required"))
        except Exception:
            return True
    return False


def write_cell_with_format(
    ws: xlsxwriter.worksheet.Worksheet,
    addons: Sequence[XlsxAddon],
    format_factory: Any,
    *,
    row_idx_sheet: int,
    col_idx_sheet: int,
    row_idx_data: int,
    col_idx_data: int,
    value: Any,
    if_is_numeric_col: bool,
    if_is_integer_col: bool,
    if_keep_missing_values: bool,
    value_policy: SpecXlsxValuePolicy,
    fmt_base: SpecCellFormat,
):
    c_cell_val = convert_cell_value(
        value=value,
        if_is_numeric_col=if_is_numeric_col,
        if_is_integer_col=if_is_integer_col,
        if_keep_missing_values=if_keep_missing_values,
        value_policy=value_policy,
    )
    fmt_spec = _derive_cell_format(
        addons,
        row_idx=row_idx_data,
        col_idx=col_idx_data,
        value=c_cell_val,
        fmt_base=fmt_base,
    )
    cfg_fmt_cell = format_factory(fmt_spec)

    if c_cell_val is None:
        ws.write_blank(
            row=row_idx_sheet,
            col=col_idx_sheet,
            blank=None,
            cell_format=cfg_fmt_cell,
        )
        return
    if isinstance(c_cell_val, str):
        ws.write_string(
            row=row_idx_sheet,
            col=col_idx_sheet,
            string=c_cell_val,
            cell_format=cfg_fmt_cell,
        )
        return

    ws.write_number(
        row=row_idx_sheet,
        col=col_idx_sheet,
        number=c_cell_val,
        cell_format=cfg_fmt_cell,
    )


def _derive_cell_format(
    addons: Sequence[XlsxAddon],
    *,
    row_idx: int,
    col_idx: int,
    value: Any,
    fmt_base: SpecCellFormat,
) -> SpecCellFormat:
    fmt_cell = fmt_base
    for ad in addons:
        cfg_override = ad.create_cell_format_override(
            row_idx=row_idx,
            col_idx=col_idx,
            value=value,
        )
        if cfg_override is not None:
            fmt_cell = fmt_cell.merge(cfg_override)
    return fmt_cell
