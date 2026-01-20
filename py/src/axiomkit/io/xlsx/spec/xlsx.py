from dataclasses import dataclass

from .sheet import SpecSheetSlice


@dataclass(slots=True)
class SpecXlsxReport:
    sheets: list[SpecSheetSlice]
    warnings: list[str]

    def warn(self, msg: str) -> None:
        self.warnings.append(str(msg))
