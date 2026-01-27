from dataclasses import dataclass

from rich.console import Console
from rich.style import Style


@dataclass(frozen=True, slots=True)
class SpecCliTheme:
    h1: str = "#7C3AED"
    h2: str = "#00FFFF"
    h3: str = "#4ADE80"


class CliHeadings:
    def __init__(
        self, *, console: Console | None = None, theme: SpecCliTheme | None = None
    ):
        self.console = console or Console()
        self.theme = theme or SpecCliTheme()

    def h1(self, text: str) -> None:
        self.console.rule(
            f"[bold]{text}[/bold]",
            style=Style(color=self.theme.h1, bold=True),
            characters="=",
        )

    def h2(self, text: str) -> None:
        self.console.rule(text, style=Style(color=self.theme.h2), characters="─")

    def h3(self, text: str) -> None:
        self.console.rule(
            f"[italic]{text}[/italic]",
            style=Style(color=self.theme.h3),
            characters="┄",
        )
