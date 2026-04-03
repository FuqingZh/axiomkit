"""Composable builder for argparse-based command CLIs.

This module provides a thin layered API:

- ``ParamSpec`` and ``CommandSpec`` describe what to add.
- ``ParamRegistry`` and ``CommandRegistry`` own registration/validation.
- ``ParserBuilder`` materializes specs into an ``argparse.ArgumentParser``.

The runtime parse step stays with argparse, using
``axiomkit.cli.parser.ArgumentParser`` by default so compatible actions can
finalize lazy defaults after parsing:

    >>> app = ParserBuilder(prog="demo")
    >>> _ = (
    ...     app.command("run", help="Run")
    ...     .group(GroupKey.GENERAL)
    ...     .add_argument("--dry-run", action="store_true")
    ...     .end()
    ...     .done()
    ... )
    >>> parser = app.build()
    >>> ns = parser.parse_args(["run"])
    >>> ns.command
    'run'
"""

import argparse
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self

from .base import SmartFormatter
from .runtime import ArgumentParser as AxiomkitArgumentParser
from .registry import CommandRegistry, ParamRegistry
from .spec import DICT_ARG_GROUP_META, ArgAdder, GroupKey, CommandSpec, ParamSpec

type ParamKey = str | StrEnum

if TYPE_CHECKING:
    type CommandOwner = ParserBuilder | CommandBuilder


@dataclass(slots=True)
class ArgumentGroupHandler:
    """Thin wrapper over an argparse argument group.

    The handler provides two operations:
    - ``add_argument``: passthrough to the underlying argparse group.
    - ``extract_params``: materialize registered ``ParamSpec`` entries into
      this group in a validated way.

    Attributes:
        key: Logical group key.
        _adder: Underlying argparse argument group object.
        _parser_reg: Parent group registry.
        _params: Parameter registry used for lookup and application.
    """

    key: GroupKey
    _adder: ArgAdder
    _parser_reg: "ArgGroupRegistry"
    _params: "ParamRegistry"

    def add_argument(self, *name_or_flags: str, **kwargs: Any) -> Any:
        """Add an argparse argument to this group.

        Args:
            *name_or_flags: Option strings, for example ``"--foo"``.
            **kwargs: Keyword arguments forwarded to
                ``argparse._ArgumentGroup.add_argument``.

        Returns:
            Any: The created argparse action object.
        """
        return self._adder.add_argument(*name_or_flags, **kwargs)

    def extract_params(self, *param_keys: ParamKey) -> "ArgumentGroupHandler":
        """Apply selected registered params into this argument group.

        Args:
            *param_keys:
                Canonical ids resolvable by ``ParamRegistry``.
                Supports ``str`` and ``StrEnum``.

        Raises:
            ValueError:
                If a selected param belongs to a different group.

        Returns:
            ArgumentGroupHandler: ``self`` for fluent chaining.

        Examples:
            >>> app = ParserBuilder(prog="demo")
            >>> _ = app.register_params(
            ...     ParamSpec(
            ...         id="executables.rscript",
            ...         group=GroupKey.EXECUTABLES,
            ...         arg_builder=lambda g, s: s.add_argument(g, type=str),
            ...     )
            ... )
            >>> _ = app.select_group(GroupKey.EXECUTABLES).extract_params(
            ...     "executables.rscript"
            ... )
            >>> True
            True

        Notes:
            Parameters must be registered before extraction. If a key is
            unknown, register it first via ``register_params``.
        """
        for k in param_keys:
            spec = self._params.select_param(k)
            if spec.group != self.key:
                raise ValueError(
                    f"Param {spec.id!r} belongs to group {spec.group!r}, "
                    f"but you are extracting into group {self.key!r}."
                )

        self._params.apply_param_specs(
            parser_reg=self._parser_reg,
            keys=param_keys,
            reserved_dests=None,
        )
        return self


class ArgGroupRegistry:
    """Manage grouped argparse argument sections.

    This registry lazily creates argparse argument groups and caches handlers
    by ``GroupKey``.

    Examples:
        >>> parser = argparse.ArgumentParser(prog="demo")
        >>> reg = ArgGroupRegistry(parser, params=ParamRegistry())
        >>> grp = reg.select_group("inputs")
        >>> grp.add_argument("--file-in", type=str)
        _StoreAction(...)
    """

    def __init__(
        self,
        parser: argparse.ArgumentParser,
        *,
        params: "ParamRegistry",
    ) -> None:
        """Initialize a group registry.

        Args:
            parser: Target parser that owns all generated argument groups.
            params: Parameter registry used by ``extract_params``.
        """
        self.parser = parser
        self.params = params
        self._groups: dict[GroupKey, ArgumentGroupHandler] = {}

    def select_group(self, key: GroupKey | str) -> ArgumentGroupHandler:
        """Get or create a logical argument group.

        Args:
            key: Group key enum or string value.

        Returns:
            ArgumentGroupHandler: Cached or newly created group handler.
        """
        if (group_key := GroupKey(key)) not in self._groups:
            title, desc = DICT_ARG_GROUP_META[group_key]
            group = self.parser.add_argument_group(title, description=desc)
            self._groups[group_key] = ArgumentGroupHandler(
                key=group_key,
                _adder=group,
                _parser_reg=self,
                _params=self.params,
            )

        return self._groups[group_key]


class CommandBuilder:
    """Fluent command-scoped builder for grouped arguments.

    A ``CommandBuilder`` records group-level operations, then compiles them
    into one ``CommandSpec`` when :meth:`done` is called.

    Examples:
        >>> app = ParserBuilder(prog="demo")
        >>> _ = (
        ...     app.command("run", help="Run")
        ...     .group(GroupKey.GENERAL)
        ...     .add_argument("--dry-run", action="store_true")
        ...     .end()
        ...     .done()
        ... )
        >>> parser = app.build()
        >>> parser.parse_args(["run", "--dry-run"]).dry_run
        True
    """

    def __init__(
        self,
        owner: "CommandOwner",
        *,
        id: str,
        help: str,
        arg_builder: Callable[[argparse.ArgumentParser], argparse.ArgumentParser | None]
        | None = None,
        group: str = "default",
        order: int = 0,
        param_keys: tuple[ParamKey, ...] = (),
    ) -> None:
        """Initialize a command builder.

        Args:
            owner: Parent parser or command builder.
            id: Canonical command id.
            help: Command help text.
            arg_builder:
                Optional raw parser callback executed before grouped ops.
            group: Logical command group used for help sorting.
            order: Sort key inside command group.
            param_keys:
                Root-level param ids to auto-apply onto this command.
                Supports ``str`` and ``StrEnum``.
        """
        self._owner = owner
        self._root = owner if isinstance(owner, ParserBuilder) else owner._root
        self._id = id if isinstance(owner, ParserBuilder) else f"{owner._id}.{id}"
        self._help = help
        self._arg_builder = arg_builder
        self._group = group
        self._order = order
        self._param_keys = param_keys
        self.group_ops: list[Callable[[ArgGroupRegistry], None]] = []
        self._children: list[CommandSpec] = []
        self.is_closed = False
        self._root.append_open_command_builder(self)

    def assert_open(self) -> None:
        if self.is_closed:
            raise ValueError(
                f"Command builder {self._id!r} is already closed; "
                "do not mutate after done()."
            )

    @property
    def id(self) -> str:
        """Expose the canonical command id for this builder scope."""
        return self._id

    @property
    def params(self) -> ParamRegistry:
        """Expose the shared parameter registry from the parent scope."""
        return self._owner.params

    def group(self, key: GroupKey | str) -> "GroupBuilder":
        """Enter a logical argument group context.

        Args:
            key: Group key enum or string value.

        Returns:
            GroupBuilder: Group-scoped fluent builder.
        """
        self.assert_open()
        return GroupBuilder(command_builder=self, key=GroupKey(key))

    def register_command(self, spec: CommandSpec) -> None:
        """Register one nested child command spec."""
        self.assert_open()
        self._children.append(spec)

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
    ) -> "CommandBuilder":
        """Create a nested command builder under the current command."""
        self.assert_open()
        return CommandBuilder(
            self,
            id=id,
            help=help,
            arg_builder=arg_builder,
            group=group,
            order=order,
            param_keys=param_keys,
        )

    def done(self) -> "CommandOwner":
        """Finalize this command and register it into parent builder.

        Returns:
            CommandOwner: Parent builder for continued chaining.
        """
        self.assert_open()
        self.is_closed = True
        self._root.remove_open_command_builder(self)

        group_ops = tuple(self.group_ops)
        base_arg_builder = self._arg_builder
        params = self._owner.params

        def _build_args(
            parser: argparse.ArgumentParser,
        ) -> argparse.ArgumentParser | None:
            if base_arg_builder is not None:
                base_arg_builder(parser)

            group_registry = ArgGroupRegistry(parser=parser, params=params)
            for operation in group_ops:
                operation(group_registry)
            return parser

        self._owner.register_command(
            CommandSpec(
                id=self._id,
                help=self._help,
                arg_builder=_build_args,
                group=self._group,
                order=self._order,
                param_keys=self._param_keys,
                children=tuple(self._children),
            )
        )
        return self._owner

    def done_all(self) -> "ParserBuilder":
        """Finalize the current command chain and return the root parser.

        This is equivalent to repeatedly calling :meth:`done` until the root
        ``ParserBuilder`` is reached.

        Returns:
            ParserBuilder: Root parser builder.
        """
        scope: CommandOwner = self
        while isinstance(scope, CommandBuilder):
            scope = scope.done()
        return scope


class GroupBuilder:
    """Fluent group-scoped builder nested under ``CommandBuilder``."""

    def __init__(self, *, command_builder: CommandBuilder, key: GroupKey) -> None:
        """Initialize a group builder.

        Args:
            command_builder: Parent command builder.
            key: Logical argument group key.
        """
        self._command_builder = command_builder
        self._key = key

    def add_argument(self, *name_or_flags: str, **kwargs: Any) -> "GroupBuilder":
        """Record one argparse argument operation in this group.

        Args:
            *name_or_flags: Option strings, for example ``"--file-in"``.
            **kwargs: Keyword arguments forwarded to argparse.

        Returns:
            GroupBuilder: ``self`` for fluent chaining.
        """
        self._command_builder.assert_open()
        flags = tuple(name_or_flags)
        keyword_args = dict(kwargs)
        group_key = self._key

        def _op(reg: ArgGroupRegistry) -> None:
            reg.select_group(group_key).add_argument(*flags, **keyword_args)

        self._command_builder.group_ops.append(_op)
        return self

    def extract_params(self, *param_keys: ParamKey) -> "GroupBuilder":
        """Record parameter extraction operation for this group.

        Args:
            *param_keys:
                Param ids to inject during build.
                Supports ``str`` and ``StrEnum``.

        Returns:
            GroupBuilder: ``self`` for fluent chaining.

        Examples:
            >>> app = ParserBuilder(prog="demo")
            >>> _ = app.register_params(
            ...     ParamSpec(
            ...         id="executables.rscript",
            ...         group=GroupKey.EXECUTABLES,
            ...         arg_builder=lambda g, s: s.add_argument(g, type=str),
            ...     )
            ... )
            >>> _ = (
            ...     app.command("run", help="Run")
            ...     .group(GroupKey.EXECUTABLES)
            ...     .extract_params("executables.rscript")
            ...     .end()
            ...     .done()
            ... )
            >>> parser = app.build()
            >>> parser.parse_args(["run", "--rscript", "Rscript"]).rscript
            'Rscript'

        Notes:
            Always register params first. ``extract_params`` only resolves
            previously registered ids.
        """
        self._command_builder.assert_open()
        param_key_tuple = tuple(param_keys)
        group_key = self._key

        def _op(reg: ArgGroupRegistry) -> None:
            reg.select_group(group_key).extract_params(*param_key_tuple)

        self._command_builder.group_ops.append(_op)
        return self

    def end(self) -> CommandBuilder:
        """Return to command scope.

        Returns:
            CommandBuilder: Parent command builder.
        """
        return self._command_builder


class ParserBuilder:
    """Fluent builder for command-style CLI parsers.

    The builder composes three responsibilities:
    - parameter registration (``ParamRegistry``)
    - command registration (``CommandRegistry``)
    - parser materialization (subparsers + argument groups)

    Parsing itself remains explicit and delegated to argparse.

    Examples:
        Minimal command without grouped specs:
            >>> app = ParserBuilder(prog="demo")
            >>> _ = app.command(
            ...     "ping",
            ...     help="Ping command",
            ...     arg_builder=lambda p: p.add_argument("--name", required=True) or p,
            ... ).done()
            >>> parser = app.build()
            >>> ns = parser.parse_args(["ping", "--name", "alice"])
            >>> (ns.command, ns.name)
            ('ping', 'alice')

        Fluent DSL with grouped reusable params:
        >>> app = ParserBuilder(prog="demo")
        >>> _ = app.register_params(
        ...     ParamSpec(
        ...         id="executables.rscript",
        ...         group=GroupKey.EXECUTABLES,
        ...         arg_builder=lambda g, s: s.add_argument(g, type=str),
        ...     )
        ... )
        >>> _ = (
        ...     app.command("run", help="Run demo")
        ...     .group(GroupKey.EXECUTABLES)
        ...     .extract_params("executables.rscript")
        ...     .end()
        ...     .done()
        ... )
        >>> parser = app.build()
        >>> ns = parser.parse_args(["run", "--rscript", "Rscript"])
        >>> ns.command
        'run'
        >>> isinstance(parser, argparse.ArgumentParser)
        True
    """

    def __init__(
        self,
        parser: argparse.ArgumentParser | None = None,
        *,
        prog: str | None = None,
        description: str | None = None,
        formatter_kind: type[argparse.HelpFormatter] | None = SmartFormatter,
        params: ParamRegistry | None = None,
        commands: CommandRegistry | None = None,
    ) -> None:
        """Initialize a parser builder.

        Args:
            parser:
                Existing parser instance. When provided, ``prog`` and
                ``description`` are ignored. When omitted, a
                :class:`axiomkit.cli.parser.ArgumentParser` is created so lazy
                action defaults are finalized automatically after parsing.
            prog: Program name used when a new parser is created.
            description: Top-level parser description.
            formatter_kind: Help formatter type for new parser creation.
            params: Optional parameter registry.
            commands: Optional command registry.
        """
        if parser is None:
            parser = AxiomkitArgumentParser(
                prog=prog,
                description=description,
                formatter_class=formatter_kind or SmartFormatter,
            )

        self.parser = parser
        self.params = params or ParamRegistry()
        self.commands = commands or CommandRegistry()
        self._groups = ArgGroupRegistry(parser=self.parser, params=self.params)
        self._open_command_builders: list[CommandBuilder] = []

    def select_group(self, key: GroupKey | str) -> ArgumentGroupHandler:
        """Select a logical argument group from the underlying registry.

        Args:
            key: Group key enum or string value.

        Returns:
            ArgumentGroupHandler: Selected group handler.
        """
        return self._groups.select_group(key)

    @property
    def open_command_builders(self) -> tuple[CommandBuilder, ...]:
        """Expose unclosed command builders as a read-only snapshot.

        This keeps the internal tracking list private while allowing external
        callers to inspect pending fluent builder scopes without triggering
        private-member diagnostics in static analyzers.
        """
        return tuple(self._open_command_builders)

    def append_open_command_builder(self, builder: CommandBuilder) -> None:
        """Track one newly opened command builder scope."""
        self._open_command_builders.append(builder)

    def remove_open_command_builder(self, builder: CommandBuilder) -> None:
        """Stop tracking a command builder scope after it is closed."""
        self._open_command_builders.remove(builder)

    def register_params(self, *specs: ParamSpec | Iterable[ParamSpec]) -> Self:
        """Register one or more parameter specifications.

        Args:
            *specs:
                Parameter specs, or iterables of parameter specs.
                Examples:
                ``register_params(spec1, spec2)``
                ``register_params([spec1, spec2])``

        Returns:
            Self: ``self`` for fluent chaining.
        """
        self.params.register_params(*specs)
        return self

    def register_command(self, spec: CommandSpec) -> Self:
        """Register one command specification.

        Args:
            spec: Command specification.

        Returns:
            Self: ``self`` for fluent chaining.
        """
        self.commands.register_command(spec)
        return self

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
    ) -> CommandBuilder:
        """Create a fluent command builder.

        This is the DSL-style entrypoint for command construction with grouped
        argument operations.

        Args:
            id: Canonical command id.
            help: Short help text used in subcommand listing.
            arg_builder:
                Optional raw parser callback executed before grouped operations.
            group: Logical command group used for help sorting.
            order: Sort key inside the command group.
            param_keys:
                Root-level param ids auto-applied onto command.
                Supports ``str`` and ``StrEnum``.

        Returns:
            CommandBuilder: Command-scoped fluent builder.

        Examples:
            >>> app = ParserBuilder(prog="demo")
            >>> _ = (
            ...     app.command("run", help="Run")
            ...     .group(GroupKey.GENERAL)
            ...     .add_argument("--dry-run", action="store_true")
            ...     .end()
            ...     .done()
            ... )
            >>> parser = app.build()
            >>> parser.parse_args(["run", "--dry-run"]).dry_run
            True
        """
        return CommandBuilder(
            self,
            id=id,
            help=help,
            arg_builder=arg_builder,
            group=group,
            order=order,
            param_keys=param_keys,
        )

    def build(
        self,
        *,
        title: str = "Commands",
        dest: str = "command",
        kind_formatter: type[argparse.HelpFormatter] | None = SmartFormatter,
        should_require_command: bool = True,
        should_include_group_in_help: bool = True,
        should_sort_specs: bool = True,
        should_apply_param_keys: bool = True,
    ) -> argparse.ArgumentParser:
        """Materialize command specs into subparsers and return parser.

        This is a pure build step. It does not parse argv. Keep parse explicit
        at the call site:

            ``ns = parser.parse_args(argv)``

        Args:
            title: Subparser section title shown in help.
            dest: Namespace field that stores selected command id.
            kind_formatter: Subparser help formatter class.
            should_require_command: Whether command selection is mandatory.
            should_include_group_in_help: Whether to prefix help by command group.
            should_sort_specs: Whether command specs are sorted.
            should_apply_param_keys: Whether ``CommandSpec.param_keys`` are materialized.

        Returns:
            argparse.ArgumentParser: The underlying parser.

        Raises:
            ValueError: When command ``param_keys`` are requested but required
                registries/factories are not provided.
        """
        if self._open_command_builders:
            ids_open = ", ".join(builder.id for builder in self._open_command_builders)
            raise ValueError(
                "Unclosed command builders detected before build(); "
                f"missing done() for: {ids_open}"
            )

        self.commands.build_subparsers(
            parser=self.parser,
            title=title,
            dest=dest,
            kind_formatter=kind_formatter,
            should_require_command=should_require_command,
            should_include_group_in_help=should_include_group_in_help,
            should_sort_specs=should_sort_specs,
            param_registry=self.params,
            group_registry_factory=lambda p: ArgGroupRegistry(
                parser=p,
                params=self.params,
            ),
            should_apply_param_keys=should_apply_param_keys,
        )
        return self.parser
