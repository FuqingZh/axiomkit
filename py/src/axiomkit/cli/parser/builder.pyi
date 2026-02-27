from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable
from enum import StrEnum
from typing import Any, Self

from .base import ArgAdder
from .registry import CommandRegistry, ParamRegistry
from .spec import EnumGroupKey, SpecCommand, SpecParam

type ParamKey = str | StrEnum
type ArgumentValueParser = Callable[[str], Any]


def create_param_registry() -> ParamRegistry: ...


class ArgumentGroupHandler:
    key: EnumGroupKey
    _adder: ArgAdder
    _parser_reg: ArgGroupRegistry
    _params: ParamRegistry | None

    def __init__(
        self,
        *,
        key: EnumGroupKey,
        _adder: ArgAdder,
        _parser_reg: ArgGroupRegistry,
        _params: ParamRegistry | None = None,
    ) -> None: ...

    def add_argument(
        self,
        *name_or_flags: str,
        action: str | type[argparse.Action] = ...,
        nargs: int | str | None = ...,
        const: Any = ...,
        default: Any = ...,
        type: ArgumentValueParser | None = ...,
        choices: Iterable[Any] | None = ...,
        required: bool = ...,
        help: str | None = ...,
        metavar: str | tuple[str, ...] | None = ...,
        dest: str | None = ...,
        version: str = ...,
        **kwargs: Any,
    ) -> argparse.Action: ...

    def extract_params(self, *param_keys: ParamKey) -> ArgumentGroupHandler: ...


class ArgGroupRegistry:
    parser: argparse.ArgumentParser
    params: ParamRegistry
    _groups: dict[EnumGroupKey, ArgumentGroupHandler]

    def __init__(
        self,
        parser: argparse.ArgumentParser,
        *,
        params: ParamRegistry | None = None,
    ) -> None: ...

    def select_group(self, key: EnumGroupKey | str) -> ArgumentGroupHandler: ...


class CommandBuilder:
    def __init__(
        self,
        owner: ParserBuilder,
        *,
        id: str,
        help: str,
        arg_builder: Callable[[argparse.ArgumentParser], argparse.ArgumentParser | None]
        | None = None,
        entry: str | None = None,
        group: str = "default",
        order: int = 0,
        param_keys: tuple[ParamKey, ...] = (),
    ) -> None: ...

    def assert_open(self) -> None: ...
    def group(self, key: EnumGroupKey | str) -> GroupBuilder: ...
    def done(self) -> ParserBuilder: ...


class GroupBuilder:
    def __init__(self, *, command_builder: CommandBuilder, key: EnumGroupKey) -> None: ...

    def add_argument(
        self,
        *name_or_flags: str,
        action: str | type[argparse.Action] = ...,
        nargs: int | str | None = ...,
        const: Any = ...,
        default: Any = ...,
        type: ArgumentValueParser | None = ...,
        choices: Iterable[Any] | None = ...,
        required: bool = ...,
        help: str | None = ...,
        metavar: str | tuple[str, ...] | None = ...,
        dest: str | None = ...,
        version: str = ...,
        **kwargs: Any,
    ) -> GroupBuilder: ...

    def extract_params(self, *param_keys: ParamKey) -> GroupBuilder: ...
    def end(self) -> CommandBuilder: ...
    def done(self) -> ParserBuilder: ...


class ParserBuilder:
    parser: argparse.ArgumentParser
    params: ParamRegistry
    commands: CommandRegistry

    def __init__(
        self,
        parser: argparse.ArgumentParser | None = None,
        *,
        prog: str | None = None,
        description: str | None = None,
        kind_formatter: type[argparse.HelpFormatter] | None = ...,
        params: ParamRegistry | None = None,
        commands: CommandRegistry | None = None,
    ) -> None: ...

    def select_group(self, key: EnumGroupKey | str) -> ArgumentGroupHandler: ...
    def register_params(self, *specs: SpecParam | Iterable[SpecParam]) -> Self: ...
    def apply_param_specs(self, *keys: ParamKey) -> Self: ...
    def register_command(self, spec: SpecCommand) -> Self: ...

    def add_command(
        self,
        *,
        id: str,
        help: str,
        arg_builder: Callable[[argparse.ArgumentParser], argparse.ArgumentParser | None],
        entry: str | None = None,
        group: str = "default",
        order: int = 0,
        param_keys: tuple[ParamKey, ...] = (),
    ) -> Self: ...

    def command(
        self,
        id: str,
        *,
        help: str,
        arg_builder: Callable[[argparse.ArgumentParser], argparse.ArgumentParser | None]
        | None = None,
        entry: str | None = None,
        group: str = "default",
        order: int = 0,
        param_keys: tuple[ParamKey, ...] = (),
    ) -> CommandBuilder: ...

    def build(
        self,
        *,
        title: str = "Commands",
        dest: str = "command",
        kind_formatter: type[argparse.HelpFormatter] | None = ...,
        if_required: bool = True,
        if_include_group_in_help: bool = True,
        if_sort_specs: bool = True,
        if_apply_param_keys: bool = True,
    ) -> argparse.ArgumentParser: ...
