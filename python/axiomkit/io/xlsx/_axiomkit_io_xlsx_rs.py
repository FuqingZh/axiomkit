from __future__ import annotations

from axiomkit import _axiomkit_rs as _core_rs

XlsxWriter = _core_rs.XlsxWriter

__bridge_abi__ = _core_rs.__bridge_xlsx_abi__
__bridge_contract__ = _core_rs.__bridge_xlsx_contract__
__bridge_transport__ = _core_rs.__bridge_xlsx_transport__

__all__ = [
    "XlsxWriter",
    "__bridge_abi__",
    "__bridge_contract__",
    "__bridge_transport__",
]
