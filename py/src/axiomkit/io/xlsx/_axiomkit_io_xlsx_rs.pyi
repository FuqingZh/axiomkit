from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal

from .spec import SpecAutofitCellsPolicy, SpecScientificPolicy, SpecXlsxReport

__bridge_abi__: int
__bridge_contract__: str
__bridge_transport__: str

class XlsxWriter:
    file_out: str

    def __init__(
        self,
        file_out: str,
        *,
        fmt_text: Any = ...,
        fmt_integer: Any = ...,
        fmt_decimal: Any = ...,
        fmt_scientific: Any = ...,
        fmt_header: Any = ...,
        write_options: Any = ...,
    ) -> None: ...

    def __enter__(self) -> XlsxWriter: ...

    def __exit__(
        self,
        exc_type: type | None,
        exc: BaseException | None,
        tb: Any | None,
    ) -> None: ...

    def close(self) -> None: ...

    def report(self) -> tuple[SpecXlsxReport, ...]: ...

    def write_sheet(
        self,
        df: Any,
        sheet_name: str,
        *,
        df_header: Any | None = ...,
        cols_integer: Sequence[str | int] | str | None = ...,
        cols_decimal: Sequence[str | int] | str | Literal[False] | None = ...,
        col_freeze: int = ...,
        row_freeze: int | None = ...,
        if_merge_header: bool = ...,
        if_keep_missing_values: bool | None = ...,
        policy_autofit: SpecAutofitCellsPolicy | None = ...,
        policy_scientific: SpecScientificPolicy | None = ...,
    ) -> XlsxWriter: ...
