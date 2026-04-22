import os
import warnings
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import TracebackType
from typing import Any, ClassVar, Literal, Protocol, Self, cast

from ._rs_bridge import create_xlsx_writer_via_rs, is_rs_backend_available
from .constant import (
    DEFAULT_XLSX_FORMATS,
    DEFAULT_XLSX_WRITE_OPTIONS,
    LIT_FMT_KEYS,
    ColumnIdentifier,
)
from .spec import (
    AutofitPolicy,
    CellFormatPatch,
    ScientificPolicy,
    XlsxReport,
    XlsxWriteOptions,
)


class ProtocolXlsxWriterBackend(Protocol):
    def close(self) -> None: ...

    def report(self) -> tuple[XlsxReport, ...]: ...

    def write_sheet(
        self,
        body: Any,
        sheet_name: str,
        *,
        header: Any | None = None,
        cols_integer: Sequence[ColumnIdentifier] | None = None,
        cols_decimal: Sequence[ColumnIdentifier] | None | Literal[False] = None,
        num_frozen_cols: int = 0,
        num_frozen_rows: int | None = None,
        should_merge_header: bool = False,
        should_keep_missing_values: bool | None = None,
        policy_autofit: AutofitPolicy | None = None,
        policy_scientific: ScientificPolicy | None = None,
    ) -> Any: ...


class XlsxWriter:
    """Rust-backed XLSX writer.

    Public API is kept aligned with the previous Python implementation.
    The execution backend is always Rust (`_axiomkit_io_xlsx_rs`) and this class is a
    thin Python facade that preserves call signatures and return types.
    """

    DEFAULT_XLSX_FORMATS: ClassVar[Mapping[LIT_FMT_KEYS, CellFormatPatch]] = (
        DEFAULT_XLSX_FORMATS
    )
    DEFAULT_XLSX_WRITE_OPTIONS: ClassVar[XlsxWriteOptions] = (
        DEFAULT_XLSX_WRITE_OPTIONS
    )

    def __init__(
        self,
        file_out: os.PathLike[str] | str,
        *,
        fmt_text: CellFormatPatch | None = None,
        fmt_integer: CellFormatPatch | None = None,
        fmt_decimal: CellFormatPatch | None = None,
        fmt_scientific: CellFormatPatch | None = None,
        fmt_header: CellFormatPatch | None = None,
        options_write: XlsxWriteOptions | None = None,
    ):
        if not is_rs_backend_available():
            raise RuntimeError(
                "Rust xlsx backend is unavailable. Build/install `_axiomkit_io_xlsx_rs` first."
            )

        self.file_out = Path(file_out)
        self._writer: ProtocolXlsxWriterBackend = cast(
            ProtocolXlsxWriterBackend,
            create_xlsx_writer_via_rs(
                str(self.file_out),
                fmt_text=fmt_text,
                fmt_integer=fmt_integer,
                fmt_decimal=fmt_decimal,
                fmt_scientific=fmt_scientific,
                fmt_header=fmt_header,
                options_write=options_write,
            ),
        )

    def __enter__(self) -> "XlsxWriter":
        return self

    def __exit__(
        self, exc_type: type | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()

    def close(self) -> None:
        self._writer.close()

    def report(self) -> tuple[XlsxReport, ...]:
        return self._writer.report()

    def write_sheet(
        self,
        body: Any,
        sheet_name: str,
        *,
        header: Any | None = None,
        cols_integer: Sequence[ColumnIdentifier] | None = None,
        cols_decimal: Sequence[ColumnIdentifier] | None | Literal[False] = None,
        num_frozen_cols: int = 0,
        num_frozen_rows: int | None = None,
        should_merge_header: bool = False,
        should_keep_missing_values: bool | None = None,
        policy_autofit: AutofitPolicy | None = None,
        policy_scientific: ScientificPolicy | None = None,
    ) -> Self:
        """Write one worksheet to the workbook.

        Args:
            body: Tabular data to write. The object must be convertible to a Polars
                DataFrame by the Rust bridge.
            sheet_name: Requested worksheet name before Excel sanitization and
                uniqueness adjustments.
            header: Optional custom header grid. When provided, it must have the
                same width as ``body`` and at least one row.
            cols_integer:
                Optional column identifiers that should use integer formatting and
                integer conversion rules. Use ``str`` for literal column names and
                ``int`` for zero-based column indices. Pure numeric strings such as
                ``"0"`` are treated as column names, not indices.
            cols_decimal:
                Optional column identifiers that should use decimal formatting.
                Use ``str`` for literal column names and ``int`` for zero-based
                column indices. Pure numeric strings such as ``"0"`` are treated as
                column names, not indices.
                Pass ``False`` to disable explicit decimal-column selection.
            num_frozen_cols: Number of leftmost columns to freeze.
            num_frozen_rows: Number of top rows to freeze. When ``None``, the
                backend uses the resolved header height.
            should_merge_header:
                - ``True``: Merge all adjacent header labels that are identical.
                - ``False``: Don't merge any header labels.
            should_keep_missing_values:
                - ``True``: Write missing, NaN, and Inf values as text tokens.
                - ``False``: Write missing, NaN, and Inf values as blank cells.
                - ``None``: Use the writer-level option for missing value handling.
            policy_autofit: Column autofit policy applied to the sheet.
            policy_scientific: Scientific-number formatting policy applied per
                cell. If ``None``, scientific formatting is disabled by default.
                Only numeric values that fall within the policy scope and
                trigger thresholds use the scientific format; other cells keep
                the column base format.

        Returns:
            Self: The current writer instance for fluent chaining.

        Examples:
            ```python
            with XlsxWriter("output.xlsx") as writer:
                writer.write_sheet(
                    my_dataframe,
                    "Data",
                    cols_integer=["id", "age"],
                    cols_decimal=["score"],
                    num_frozen_cols=1,
                    should_merge_header=True,
                )

            with XlsxWriter("output.xlsx") as writer:
                writer.write_sheet(
                    my_dataframe,
                    "Data",
                    cols_integer=[0, 1],  # using column indices instead of names
                    cols_decimal=[2],
                    num_frozen_rows=2,
                    should_keep_missing_values=True,
                    policy_autofit=AutofitPolicy(
                        mode="all",
                        height_body_inferred_max=20_000,
                        width_cell_min=8,
                        width_cell_max=60,
                        width_cell_padding=2
                    ),
                    policy_scientific=ScientificPolicy(
                        scope="decimal",
                        thr_min=0.0001,
                        thr_max=1_000_000_000_000.0
                    ),
                )
            ```
        """
        _warn_numeric_string_column_selectors(cols_integer, arg_name="cols_integer")
        _warn_numeric_string_column_selectors(cols_decimal, arg_name="cols_decimal")

        self._writer.write_sheet(
            body=body,
            sheet_name=sheet_name,
            header=header,
            cols_integer=cols_integer,
            cols_decimal=cols_decimal,
            num_frozen_cols=num_frozen_cols,
            num_frozen_rows=num_frozen_rows,
            should_merge_header=should_merge_header,
            should_keep_missing_values=should_keep_missing_values,
            policy_autofit=policy_autofit,
            policy_scientific=policy_scientific,
        )
        return self


def _warn_numeric_string_column_selectors(
    value: Sequence[ColumnIdentifier] | None | Literal[False] | object,
    *,
    arg_name: str,
) -> None:
    match value:
        case None | False:
            return
        case str() | int():
            items = (value,)
        case Sequence():
            items = value
        case _:
            return

    for _item in items:
        if isinstance(_item, str) and _item.isascii() and _item.isdigit():
            warnings.warn(
                (
                    f"{arg_name} contains numeric string selector {_item!r}; "
                    "string selectors are treated as literal column names. "
                    "Pass an int to select a zero-based column index."
                ),
                category=UserWarning,
                stacklevel=2,
            )
