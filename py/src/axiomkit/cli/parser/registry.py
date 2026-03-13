"""Registries that materialize command and parameter specs into argparse.

This module is the stateful core of the parser package:

- ``ParamRegistry`` stores and validates reusable ``SpecParam`` objects.
- ``CommandRegistry`` stores ``SpecCommand`` objects and builds subparsers.

Both registries resolve canonical ids and materialize specs into parser objects.
"""

import argparse
import warnings
from collections.abc import Callable, Iterable, Sequence
from enum import StrEnum
from typing import Protocol, Self, cast

from .base import ArgAdder, CanonicalRegistry, SmartFormatter
from .spec import EnumGroupKey, SpecCommand, SpecParam

_RESERVED_PARAM_DESTS: frozenset[str] = frozenset(
    {
        "command",
        "_handler",
        "_cmd_group",
    }
)

type ParamKey = str | StrEnum


def _normalize_param_key(key: ParamKey) -> str:
    """Normalize a canonical parameter key from ``str`` or ``StrEnum`` to ``str``."""
    return str(key)


def default_reserved_param_dests(*, command_dest: str = "command") -> set[str]:
    """Return reserved destination names for parameter materialization."""
    reserved_dests = set(_RESERVED_PARAM_DESTS)
    reserved_dests.add(command_dest)
    return reserved_dests


def _iter_parser_actions(parser: argparse.ArgumentParser) -> Sequence[argparse.Action]:
    """Return parser actions with private-API access isolated in one place."""
    actions = getattr(parser, "_actions", ())
    if not isinstance(actions, Sequence):
        return ()
    return cast(Sequence[argparse.Action], actions)


def _collect_existing_dests(parser: argparse.ArgumentParser) -> set[str]:
    """Collect existing destination names from parser actions."""
    existing_dests: set[str] = set()
    for action in _iter_parser_actions(parser):
        existing_dests.add(action.dest)
    return existing_dests


def _collect_existing_flags(parser: argparse.ArgumentParser) -> set[str]:
    """Collect existing option flags from parser actions."""
    existing_flags: set[str] = set()
    for action in _iter_parser_actions(parser):
        existing_flags |= set(action.option_strings)
    return existing_flags


class ParserRegistry(Protocol):
    """Protocol for parser group selectors used by ``ParamRegistry``.

    Attributes:
        parser: Backing ``argparse.ArgumentParser`` instance.

    Methods:
        select_group: Resolve logical group key into an ``ArgAdder`` target.
    """

    parser: argparse.ArgumentParser

    def select_group(self, key: EnumGroupKey | str) -> ArgAdder: ...


class CommandRegistry:
    """Registry for command specifications.

    This class stores ``SpecCommand`` objects by canonical id.

    Examples:
        >>> reg = CommandRegistry()
        >>> _ = reg.register_command(
        ...     SpecCommand(id="run", help="Run", arg_builder=lambda p: p)
        ... )
        >>> [spec.id for spec in reg.list_commands()]
        ['run']
    """

    def __init__(self) -> None:
        """Initialize an empty command registry."""
        self._core: CanonicalRegistry[SpecCommand] = CanonicalRegistry.new()

    def register_command(self, spec: SpecCommand) -> Self:
        """Register one command specification.

        Args:
            spec: Command specification to register.

        Returns:
            Self: ``self`` for fluent chaining.

        Raises:
            ValueError: If id conflicts with existing registrations.
        """
        self._core.register(spec)
        return self

    def list_commands(self, should_sort: bool = True) -> list[SpecCommand]:
        """List registered command specs.

        Args:
            should_sort:
                Whether to sort by ``(group, order, id)``.
                If ``False``, insertion order is preserved.

        Returns:
            list[SpecCommand]: Registered command specifications.
        """
        if not should_sort:
            return self._core.list_specs(kind_sort="insertion")
        return self._core.list_specs(rule_sort=lambda s: (s.group, s.order, s.id))

    def build_subparsers(
        self,
        parser: argparse.ArgumentParser,
        *,
        title: str = "Commands",
        dest: str = "command",
        kind_formatter: type[argparse.HelpFormatter] | None = SmartFormatter,
        should_require_command: bool = True,
        should_include_group_in_help: bool = True,
        should_sort_specs: bool = True,
        param_registry: "ParamRegistry | None" = None,
        group_registry_factory: Callable[[argparse.ArgumentParser], ParserRegistry]
        | None = None,
        should_apply_param_keys: bool = True,
    ):
        """Build argparse subparsers from command specs.

        Args:
            parser: Root parser receiving subparsers.
            title: Subparser section title in help output.
            dest: Namespace field that stores selected command.
            kind_formatter: Formatter class for each command subparser.
            should_require_command: Whether command selection is required.
            should_include_group_in_help:
                Whether to prefix command help with group tag.
            should_sort_specs: Whether to sort command specs before build.
            param_registry:
                Registry used to apply per-command ``param_keys``.
            group_registry_factory:
                Factory that builds a ``ParserRegistry`` wrapper for each
                command subparser.
            should_apply_param_keys:
                Whether to materialize ``SpecCommand.param_keys``.

        Returns:
            argparse._SubParsersAction: Subparsers action from argparse.

        Raises:
            ValueError:
                If ``should_apply_param_keys=True`` and required dependencies are
                missing for commands that contain ``param_keys``.

        Examples:
            >>> parser = argparse.ArgumentParser(prog="demo")
            >>> reg = CommandRegistry()
            >>> _ = reg.register_command(
            ...     SpecCommand(id="run", help="Run", arg_builder=lambda p: p)
            ... )
            >>> _ = reg.build_subparsers(parser)
            >>> ns = parser.parse_args(["run"])
            >>> ns.command
            'run'
        """
        subparsers = parser.add_subparsers(
            title=title,
            dest=dest,
            required=should_require_command,
        )

        formatter_type = kind_formatter or parser.formatter_class
        for spec in self.list_commands(should_sort=should_sort_specs):
            help_text = spec.help
            if should_include_group_in_help and spec.group:
                help_text = f"\\[{spec.group}] {help_text}"

            subparser = subparsers.add_parser(
                spec.id,
                help=help_text,
                formatter_class=formatter_type,
            )
            subparser.set_defaults(_cmd_group=spec.group)
            spec.arg_builder(subparser)

            if should_apply_param_keys and spec.param_keys:
                if param_registry is None:
                    raise ValueError(
                        "`param_registry` is required when `should_apply_param_keys=True` "
                        "and command has `param_keys`."
                    )
                if group_registry_factory is None:
                    raise ValueError(
                        "`group_registry_factory` is required when applying `param_keys`."
                    )

                param_registry.apply_param_specs(
                    parser_reg=group_registry_factory(subparser),
                    keys=spec.param_keys,
                    reserved_dests=default_reserved_param_dests(command_dest=dest),
                )

        return subparsers


class ParamRegistry:
    """Registry for parameter specifications.

    The registry stores ``SpecParam`` instances and can materialize selected
    parameters into parser groups with collision checks.

    Examples:
        >>> reg = ParamRegistry()
        >>> _ = reg.register_params(
        ...     SpecParam(id="general.flag", arg_builder=lambda g, s: s.add_argument(g))
        ... )
        >>> reg.select_param("general.flag").id
        'general.flag'
    """

    def __init__(self) -> None:
        """Initialize an empty parameter registry."""
        self._core: CanonicalRegistry[SpecParam] = CanonicalRegistry.new()

    def register_params(self, *specs: SpecParam | Iterable[SpecParam]) -> Self:
        """Register one or more parameter specifications.

        Args:
            *specs:
                Parameter specs, or iterables of parameter specs.
                Examples:
                ``register_params(spec1, spec2)``
                ``register_params([spec1, spec2])``

        Returns:
            Self: ``self`` for fluent chaining.

        Raises:
            ValueError: If id conflicts are detected.
        """
        for item in specs:
            if isinstance(item, SpecParam):
                self._core.register(item)
                continue

            for spec in item:
                self._core.register(spec)

        return self

    def select_param(self, key: ParamKey) -> SpecParam:
        """Select a parameter specification by canonical id.

        Args:
            key:
                Canonical parameter id. Supports ``str`` and ``StrEnum``.

        Returns:
            SpecParam: Resolved parameter specification.

        Raises:
            ValueError: If the key is unknown.
        """
        param_key = _normalize_param_key(key)
        try:
            return self._core.get(param_key)
        except ValueError as e:
            available_ids = self._core.list_ids()
            raise ValueError(
                f"Unknown param id: {param_key!r}. "
                "This parameter is not registered in ParamRegistry. "
                "Register it first via `register_params(...)`, then call "
                "`extract_params(...)` / `apply_param_specs(...)`. "
                f"Available ids: {available_ids}."
            ) from e

    def list_params(
        self,
        *,
        group: str | None = None,
        should_sort: bool = True,
    ) -> list[SpecParam]:
        """List registered parameter specs with optional filtering.

        Args:
            group: Optional group filter.
            should_sort:
                Whether to sort by ``(group, order, id)``.
                If ``False``, insertion order is preserved.

        Returns:
            list[SpecParam]: Filtered parameter specs.
        """
        if not should_sort:
            specs = self._core.list_specs(kind_sort="insertion")
        else:
            specs = self._core.list_specs(rule_sort=lambda s: (s.group, s.order, s.id))

        if group is not None:
            specs = [spec for spec in specs if spec.group == group]

        return specs

    def apply_param_specs(
        self,
        *,
        parser_reg: ParserRegistry,
        keys: Sequence[ParamKey],
        reserved_dests: set[str] | None,
    ) -> None:
        """Apply selected parameter specs onto a parser registry.

        Args:
            parser_reg: Parser/group wrapper used to resolve logical groups.
            keys:
                Canonical parameter ids to apply in order.
                Supports ``str`` and ``StrEnum``.
            reserved_dests:
                Dest names that are forbidden for parameter materialization.
                Defaults to the parser metadata dest set from
                ``default_reserved_param_dests()``.

        Raises:
            ValueError:
                If a key is unknown, ``arg_builder`` is missing, or any
                destination/flag collision is detected.
        """
        if reserved_dests is None:
            reserved_dests = default_reserved_param_dests()

        parser = parser_reg.parser

        existing_dests = _collect_existing_dests(parser)
        existing_flags = _collect_existing_flags(parser)

        seen_dests: dict[str, str] = {}
        seen_flags: dict[str, str] = {}

        for key in keys:
            spec = self.select_param(key)
            if spec.is_deprecated:
                warnings.warn(
                    (
                        f"Deprecated param: {spec.id!r}; "
                        f"use {spec.replace_by!r} instead."
                    ),
                    category=UserWarning,
                    stacklevel=2,
                )
            if spec.arg_builder is None:
                raise ValueError(f"`SpecParam` missing `arg_builder`: {spec.id!r}")

            dest = spec.resolved_dest
            flags = spec.resolved_flags

            if dest in reserved_dests:
                raise ValueError(
                    f"Param dest is reserved: {dest!r} (spec id: {spec.id!r})"
                )
            if dest in existing_dests:
                raise ValueError(
                    f"Param dest already exists on parser: {dest!r} (spec id: {spec.id!r})"
                )
            if dest in seen_dests:
                raise ValueError(
                    f"Param dest collision: {dest!r} "
                    f"(spec ids: {seen_dests[dest]!r}, {spec.id!r})"
                )
            seen_dests[dest] = spec.id

            for flag in flags:
                if flag in existing_flags:
                    raise ValueError(
                        f"Param flag already exists on parser: {flag!r} (spec id: {spec.id!r})"
                    )
                if flag in seen_flags:
                    raise ValueError(
                        f"Param flag collision: {flag!r} "
                        f"(spec ids: {seen_flags[flag]!r}, {spec.id!r})"
                    )
                seen_flags[flag] = spec.id

            group = parser_reg.select_group(spec.group)
            spec.arg_builder(group, spec)

            existing_dests.add(dest)
            existing_flags |= set(flags)
