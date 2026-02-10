from importlib.util import find_spec
from typing import TYPE_CHECKING

from .parquet import sink_parquet_dataset
from .xlsx import SpecCellFormat, XlsxWriter

# fasta 相关：只在 Bio 存在时才暴露
if find_spec("Bio") is not None:
    from .fasta import SpecFastaHeader, read_fasta  # noqa: F401
else:
    # 可选：给静态检查/IDE 用
    if TYPE_CHECKING:
        from .fasta import SpecFastaHeader, read_fasta  # pragma: no cover

__all__ = [
    "sink_parquet_dataset",
    "SpecCellFormat",
    "XlsxWriter",
    "SpecFastaHeader",
    "read_fasta",
]
