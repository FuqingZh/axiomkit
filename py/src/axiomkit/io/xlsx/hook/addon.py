import math
from collections.abc import Sequence
from typing import Any, Protocol

import polars as pl
import xlsxwriter
import xlsxwriter.format
import xlsxwriter.worksheet

from ..util.value_convert import convert_nan_inf_to_str


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


def addon_requires_cell_write(ad: XlsxAddon) -> bool:
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


def write_cell_with_format(
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

        c_cell_val = convert_nan_inf_to_str(n_cell_val)
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
