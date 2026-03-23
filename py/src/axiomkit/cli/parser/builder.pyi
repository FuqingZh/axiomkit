from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable
from enum import StrEnum
from typing import Any, Generic, Self, TypeVar

from .base import ArgAdder
from .registry import CommandRegistry, ParamRegistry
from .spec import GroupKey, CommandSpec, ParamSpec

type ParamKey = str | StrEnum
type ArgumentValueParser = Callable[[str], Any]

OwnerT = TypeVar("OwnerT")


class ArgumentGroupHandler:
    key: GroupKey
    _adder: ArgAdder
    _parser_reg: ArgGroupRegistry
    _params: ParamRegistry

    def __init__(
        self,
        *,
        key: GroupKey,
        _adder: ArgAdder,
        _parser_reg: ArgGroupRegistry,
        _params: ParamRegistry,
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
    _groups: dict[GroupKey, ArgumentGroupHandler]

    def __init__(
        self,
        parser: argparse.ArgumentParser,
        *,
        params: ParamRegistry,
    ) -> None: ...

    def select_group(self, key: GroupKey | str) -> ArgumentGroupHandler: ...


class CommandBuilder(Generic[OwnerT]):
    def __init__(
        self,
        owner: OwnerT,
        *,
        id: str,
        help: str,
        arg_builder: Callable[[argparse.ArgumentParser], argparse.ArgumentParser | None]
        | None = None,
        group: str = "default",
        order: int = 0,
        param_keys: tuple[ParamKey, ...] = (),
    ) -> None: ...

    @property
    def id(self) -> str: ...
    @property
    def params(self) -> ParamRegistry: ...
    def assert_open(self) -> None: ...
    def register_command(self, spec: CommandSpec) -> None: ...
    def group(self, key: GroupKey | str) -> GroupBuilder[OwnerT]: ...
    def command(
        self,
        id: str,
        *,
        help: str,
        arg_builder: Callable[[argparse.ArgumentParser], argparse.ArgumentParser | None]
        | None = None,
        group: str = "default",
        order: int = 0,
        param_keys: tuple[ParamKey, ...] = (),
    ) -> CommandBuilder[CommandBuilder[OwnerT]]: ...
    def done(self) -> OwnerT: ...
    def done_all(self) -> ParserBuilder: ...


class GroupBuilder(Generic[OwnerT]):
    def __init__(
        self,
        *,
        command_builder: CommandBuilder[OwnerT],
        key: GroupKey,
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
    ) -> GroupBuilder[OwnerT]: ...

    def extract_params(self, *param_keys: ParamKey) -> GroupBuilder[OwnerT]: ...
    def end(self) -> CommandBuilder[OwnerT]: ...


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

    @property
    def open_command_builders(self) -> tuple[CommandBuilder[Any], ...]: ...
    def select_group(self, key: GroupKey | str) -> ArgumentGroupHandler: ...
    def register_params(self, *specs: ParamSpec | Iterable[ParamSpec]) -> Self: ...
    def register_command(self, spec: CommandSpec) -> Self: ...

    def command(
        self,
        id: str,
        *,
        help: str,
        arg_builder: Callable[[argparse.ArgumentParser], argparse.ArgumentParser | None]
        | None = None,
        group: str = "default",
        order: int = 0,
        param_keys: tuple[ParamKey, ...] = (),
    ) -> CommandBuilder[ParserBuilder]: ...

    def build(
        self,
        *,
        title: str = "Commands",
        dest: str = "command",
        kind_formatter: type[argparse.HelpFormatter] | None = ...,
        should_require_command: bool = True,
        should_include_group_in_help: bool = True,
        should_sort_specs: bool = True,
        should_apply_param_keys: bool = True,
    ) -> argparse.ArgumentParser: ...
