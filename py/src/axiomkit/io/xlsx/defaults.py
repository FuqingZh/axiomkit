# Strategy/Preference/Adjustable Parameters for XLSX I/O operations.

from collections.abc import Mapping
from types import MappingProxyType
from typing import Literal

from .formats import XlsxFormatSpec

_LIT_FMT_KEYS = Literal["text", "integer", "decimal", "scientific", "header"]
_cls_base_fmt_spec = XlsxFormatSpec(font_name="Times New Roman", font_size=10, border=1)

DEFAULT_XLSX_FORMATS: Mapping[_LIT_FMT_KEYS, XlsxFormatSpec] = MappingProxyType(
    {
        "text": _cls_base_fmt_spec.with_(align="left", valign="vcenter"),
        "header": _cls_base_fmt_spec.with_(bold=True, align="center", valign="vcenter"),
        "integer": XlsxFormatSpec(num_format="0", border=1),
        "decimal": XlsxFormatSpec(num_format="0.0000", border=1),
        "scientific": XlsxFormatSpec(num_format="0.00E+0", border=1),
    }
)
