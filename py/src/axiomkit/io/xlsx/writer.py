from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path
from types import TracebackType
from typing import Any, ClassVar, Literal, Self

from ._rs_bridge import create_xlsx_writer_via_rs, is_rs_backend_available
from .conf import DEFAULT_XLSX_FORMATS, DEFAULT_XLSX_WRITE_OPTIONS, ColumnIdentifier
from .spec import (
    SpecAutofitCellsPolicy,
    SpecCellFormat,
    SpecScientificPolicy,
    SpecXlsxReport,
    SpecXlsxWriteOptions,
)


class XlsxWriter:
    """Rust-backed XLSX writer.

    Public API is kept aligned with the previous Python implementation.
    The execution backend is always Rust (`_axiomkit_io_xlsx_rs`) and this class is a
    thin Python facade that preserves call signatures and return types.
    """

    DEFAULT_XLSX_FORMATS: ClassVar[dict[str, SpecCellFormat]] = DEFAULT_XLSX_FORMATS
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
        if not is_rs_backend_available():
            raise RuntimeError(
                "Rust xlsx backend is unavailable. Build/install `_axiomkit_io_xlsx_rs` first."
            )

        self.file_out = Path(file_out)
        self._writer = create_xlsx_writer_via_rs(
            str(self.file_out),
            fmt_text=fmt_text,
            fmt_integer=fmt_integer,
            fmt_decimal=fmt_decimal,
            fmt_scientific=fmt_scientific,
            fmt_header=fmt_header,
            write_options=write_options,
        )

    def __enter__(self) -> "XlsxWriter":
        return self

    def __exit__(
        self, exc_type: type | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()

    def close(self) -> None:
        self._writer.close()

    def report(self) -> tuple[SpecXlsxReport, ...]:
        return self._writer.report()

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
        policy_autofit: SpecAutofitCellsPolicy | None = None,
        policy_scientific: SpecScientificPolicy | None = None,
    ) -> Self:
        self._writer.write_sheet(
            df=df,
            sheet_name=sheet_name,
            df_header=df_header,
            cols_integer=cols_integer,
            cols_decimal=cols_decimal,
            col_freeze=col_freeze,
            row_freeze=row_freeze,
            if_merge_header=if_merge_header,
            if_keep_missing_values=if_keep_missing_values,
            policy_autofit=policy_autofit,
            policy_scientific=policy_scientific,
        )
        return self
