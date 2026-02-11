"""Composable builder for argparse-based command CLIs.

This module provides a thin layered API:

- ``SpecParam`` and ``SpecCommand`` describe what to add.
- ``ParamRegistry`` and ``CommandRegistry`` own registration/validation.
- ``ParserBuilder`` materializes specs into an ``argparse.ArgumentParser``.

The runtime parse step intentionally stays with raw argparse:

    >>> app = ParserBuilder(prog="demo")
    >>> _ = (
    ...     app.command("run", help="Run")
    ...     .group(EnumGroupKey.GENERAL)
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
from typing import Any, Self

from .base import SmartFormatter
from .registry import CommandRegistry, ParamRegistry, default_reserved_param_dests
from .spec import DICT_ARG_GROUP_META, ArgAdder, EnumGroupKey, SpecCommand, SpecParam

type ParamKey = str | StrEnum


def create_param_registry() -> ParamRegistry:
    """Create an empty parameter registry.

    Returns:
        ParamRegistry: Empty registry ready for parameter spec registration.

    Examples:
        >>> registry = create_param_registry()
        >>> registry.contains_param("demo.flag")
        False
    """
    return ParamRegistry()


@dataclass(slots=True)
class ArgumentGroupHandler:
    """Thin wrapper over an argparse argument group.

    The handler provides two operations:
    - ``add_argument``: passthrough to the underlying argparse group.
    - ``extract_params``: materialize registered ``SpecParam`` entries into
      this group in a validated way.

    Attributes:
        key: Logical group key.
        _adder: Underlying argparse argument group object.
        _parser_reg: Parent group registry.
        _params: Parameter registry used for lookup and application.
    """

    key: EnumGroupKey
    _adder: ArgAdder
    _parser_reg: "ArgGroupRegistry"
    _params: "ParamRegistry | None" = None

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
                If this handler has no ``ParamRegistry`` binding.
            ValueError:
                If a selected param belongs to a different group.

        Returns:
            ArgumentGroupHandler: ``self`` for fluent chaining.

        Examples:
            >>> app = ParserBuilder(prog="demo")
            >>> _ = app.register_params(
            ...     SpecParam(
            ...         id="executables.rscript",
            ...         group=EnumGroupKey.EXECUTABLES,
            ...         arg_builder=lambda g, s: s.add_argument(g, type=str),
            ...     )
            ... )
            >>> _ = app.select_group(EnumGroupKey.EXECUTABLES).extract_params(
            ...     "executables.rscript"
            ... )
            >>> True
            True

        Notes:
            Parameters must be registered before extraction. If a key is
            unknown, register it first via ``register_params``.
        """
        if self._params is None:
            raise ValueError(
                "ArgGroupRegistry was created without ParamRegistry; "
                "pass params=... to enable extract_params()."
            )

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
    by ``EnumGroupKey``.

    Examples:
        >>> parser = argparse.ArgumentParser(prog="demo")
        >>> reg = ArgGroupRegistry(parser)
        >>> grp = reg.select_group("inputs")
        >>> grp.add_argument("--file-in", type=str)
        _StoreAction(...)
    """

    def __init__(
        self,
        parser: argparse.ArgumentParser,
        *,
        params: "ParamRegistry | None" = None,
    ) -> None:
        """Initialize a group registry.

        Args:
            parser: Target parser that owns all generated argument groups.
            params:
                Optional parameter registry used by ``extract_params``.
                If omitted, an empty registry is created.
        """
        self.parser = parser
        self.params = params or create_param_registry()
        self._groups: dict[EnumGroupKey, ArgumentGroupHandler] = {}

    def select_group(self, key: EnumGroupKey | str) -> ArgumentGroupHandler:
        """Get or create a logical argument group.

        Args:
            key: Group key enum or string value.

        Returns:
            ArgumentGroupHandler: Cached or newly created group handler.
        """
        if (c_key := EnumGroupKey(key)) not in self._groups:
            title, desc = DICT_ARG_GROUP_META[c_key]
            group = self.parser.add_argument_group(title, description=desc)
            self._groups[c_key] = ArgumentGroupHandler(
                key=c_key,
                _adder=group,
                _parser_reg=self,
                _params=self.params,
            )

        return self._groups[c_key]


class CommandBuilder:
    """Fluent command-scoped builder for grouped arguments.

    A ``CommandBuilder`` records group-level operations, then compiles them
    into one ``SpecCommand`` when :meth:`done` is called.

    Examples:
        >>> app = ParserBuilder(prog="demo")
        >>> _ = (
        ...     app.command("run", help="Run")
        ...     .group(EnumGroupKey.GENERAL)
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
        owner: "ParserBuilder",
        *,
        id: str,
        help: str,
        arg_builder: Callable[[argparse.ArgumentParser], argparse.ArgumentParser | None]
        | None = None,
        entry: str | None = None,
        group: str = "default",
        order: int = 0,
        param_keys: tuple[ParamKey, ...] = (),
    ) -> None:
        """Initialize a command builder.

        Args:
            owner: Parent parser builder.
            id: Canonical command id.
            help: Command help text.
            arg_builder:
                Optional raw parser callback executed before grouped ops.
            entry: Optional command entry id/path for downstream dispatch.
            group: Logical command group used for help sorting.
            order: Sort key inside command group.
            param_keys:
                Root-level param ids to auto-apply onto this command.
                Supports ``str`` and ``StrEnum``.
        """
        self._owner = owner
        self._id = id
        self._help = help
        self._arg_builder = arg_builder
        self._entry = entry
        self._group = group
        self._order = order
        self._param_keys = param_keys
        self.group_ops: list[Callable[[ArgGroupRegistry], None]] = []
        self._if_closed = False

    def assert_open(self) -> None:
        if self._if_closed:
            raise ValueError(
                f"Command builder {self._id!r} is already closed; "
                "do not mutate after done()."
            )

    def group(self, key: EnumGroupKey | str) -> "GroupBuilder":
        """Enter a logical argument group context.

        Args:
            key: Group key enum or string value.

        Returns:
            GroupBuilder: Group-scoped fluent builder.
        """
        self.assert_open()
        return GroupBuilder(command_builder=self, key=EnumGroupKey(key))

    def done(self) -> "ParserBuilder":
        """Finalize this command and register it into parent builder.

        Returns:
            ParserBuilder: Parent builder for continued chaining.
        """
        self.assert_open()
        self._if_closed = True

        l_group_ops = tuple(self.group_ops)
        fn_base = self._arg_builder
        cls_params = self._owner.params

        def _build_args(
            parser: argparse.ArgumentParser,
        ) -> argparse.ArgumentParser | None:
            if fn_base is not None:
                fn_base(parser)

            reg = ArgGroupRegistry(parser=parser, params=cls_params)
            for op in l_group_ops:
                op(reg)
            return parser

        self._owner.register_command(
            SpecCommand(
                id=self._id,
                help=self._help,
                arg_builder=_build_args,
                entry=self._entry,
                group=self._group,
                order=self._order,
                param_keys=self._param_keys,
            )
        )
        return self._owner


class GroupBuilder:
    """Fluent group-scoped builder nested under ``CommandBuilder``."""

    def __init__(self, *, command_builder: CommandBuilder, key: EnumGroupKey) -> None:
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
        tup_flags = tuple(name_or_flags)
        dict_kwargs = dict(kwargs)
        c_key = self._key

        def _op(reg: ArgGroupRegistry) -> None:
            reg.select_group(c_key).add_argument(*tup_flags, **dict_kwargs)

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
            ...     SpecParam(
            ...         id="executables.rscript",
            ...         group=EnumGroupKey.EXECUTABLES,
            ...         arg_builder=lambda g, s: s.add_argument(g, type=str),
            ...     )
            ... )
            >>> _ = (
            ...     app.command("run", help="Run")
            ...     .group(EnumGroupKey.EXECUTABLES)
            ...     .extract_params("executables.rscript")
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
        tup_keys = tuple(param_keys)
        c_key = self._key

        def _op(reg: ArgGroupRegistry) -> None:
            reg.select_group(c_key).extract_params(*tup_keys)

        self._command_builder.group_ops.append(_op)
        return self

    def end(self) -> CommandBuilder:
        """Return to command scope.

        Returns:
            CommandBuilder: Parent command builder.
        """
        return self._command_builder

    def done(self) -> "ParserBuilder":
        """Shortcut for ``end().done()``.

        Returns:
            ParserBuilder: Parent parser builder.
        """
        return self.end().done()


class ParserBuilder:
    """Fluent builder for command-style CLI parsers.

    The builder composes three responsibilities:
    - parameter registration (``ParamRegistry``)
    - command registration (``CommandRegistry``)
    - parser materialization (subparsers + argument groups)

    Parsing itself remains explicit and delegated to argparse.

    Examples:
        >>> app = ParserBuilder(prog="demo")
        >>> _ = app.register_params(
        ...     SpecParam(
        ...         id="executables.rscript",
        ...         group=EnumGroupKey.EXECUTABLES,
        ...         arg_builder=lambda g, s: s.add_argument(g, type=str),
        ...     )
        ... )
        >>> _ = (
        ...     app.command("run", help="Run demo")
        ...     .group(EnumGroupKey.EXECUTABLES)
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
        kind_formatter: type[argparse.HelpFormatter] | None = SmartFormatter,
        params: ParamRegistry | None = None,
        commands: CommandRegistry | None = None,
    ) -> None:
        """Initialize a parser builder.

        Args:
            parser:
                Existing parser instance. When provided, ``prog`` and
                ``description`` are ignored.
            prog: Program name used when a new parser is created.
            description: Top-level parser description.
            kind_formatter: Help formatter type for new parser creation.
            params: Optional parameter registry.
            commands: Optional command registry.
        """
        if parser is None:
            parser = argparse.ArgumentParser(
                prog=prog,
                description=description,
                formatter_class=kind_formatter or SmartFormatter,
            )

        self.parser = parser
        self.params = params or create_param_registry()
        self.commands = commands or CommandRegistry()
        self._groups = ArgGroupRegistry(parser=self.parser, params=self.params)
        self._command_dest = "command"

    def select_group(self, key: EnumGroupKey | str) -> ArgumentGroupHandler:
        """Select a logical argument group from the underlying registry.

        Args:
            key: Group key enum or string value.

        Returns:
            ArgumentGroupHandler: Selected group handler.
        """
        return self._groups.select_group(key)

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
        """
        self.params.register_params(*specs)
        return self

    def apply_param_specs(self, *keys: ParamKey) -> Self:
        """Apply selected param specs directly to the root parser groups.

        Args:
            *keys:
                Canonical param ids known by ``self.params``.
                Supports ``str`` and ``StrEnum``.

        Returns:
            Self: ``self`` for fluent chaining.
        """
        self.params.apply_param_specs(
            parser_reg=self._groups,
            keys=keys,
            reserved_dests=default_reserved_param_dests(command_dest=self._command_dest),
        )
        return self

    def register_command(self, spec: SpecCommand) -> Self:
        """Register one command specification.

        Args:
            spec: Command specification.

        Returns:
            Self: ``self`` for fluent chaining.
        """
        self.commands.register_command(spec)
        return self

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
    ) -> Self:
        """Create and register a command specification inline.

        Args:
            id: Canonical command id.
            help: Short help text used in subcommand listing.
            arg_builder:
                Callback that receives command subparser and adds arguments.
            entry: Optional command entry id/path for downstream dispatch.
            group: Logical command group used for help sorting.
            order: Sort key inside the command group.
            param_keys:
                Param ids to auto-apply onto this command.
                Supports ``str`` and ``StrEnum``.

        Returns:
            Self: ``self`` for fluent chaining.

        Examples:
            >>> app = ParserBuilder(prog="demo")
            >>> _ = app.add_command(
            ...     id="run",
            ...     help="Run",
            ...     arg_builder=lambda p: p,
            ... )
            >>> True
            True
        """
        return self.register_command(
            SpecCommand(
                id=id,
                help=help,
                arg_builder=arg_builder,
                entry=entry,
                group=group,
                order=order,
                param_keys=param_keys,
            )
        )

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
    ) -> CommandBuilder:
        """Create a fluent command builder.

        This is the DSL-style entrypoint for command construction with grouped
        argument operations.

        Args:
            id: Canonical command id.
            help: Short help text used in subcommand listing.
            arg_builder:
                Optional raw parser callback executed before grouped operations.
            entry: Optional command entry id/path for downstream dispatch.
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
            ...     .group(EnumGroupKey.GENERAL)
            ...     .add_argument("--dry-run", action="store_true")
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
            entry=entry,
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
        if_required: bool = True,
        if_include_group_in_help: bool = True,
        if_sort_specs: bool = True,
        if_apply_param_keys: bool = True,
    ) -> argparse.ArgumentParser:
        """Materialize command specs into subparsers and return parser.

        This is a pure build step. It does not parse argv. Keep parse explicit
        at the call site:

            ``ns = parser.parse_args(argv)``

        Args:
            title: Subparser section title shown in help.
            dest: Namespace field that stores selected command id.
            kind_formatter: Subparser help formatter class.
            if_required: Whether command selection is mandatory.
            if_include_group_in_help: Whether to prefix help by command group.
            if_sort_specs: Whether command specs are sorted.
            if_apply_param_keys: Whether ``SpecCommand.param_keys`` are materialized.

        Returns:
            argparse.ArgumentParser: The underlying parser.

        Raises:
            ValueError: When command ``param_keys`` are requested but required
                registries/factories are not provided.
        """
        self._command_dest = dest
        self.commands.build_subparsers(
            parser=self.parser,
            title=title,
            dest=dest,
            kind_formatter=kind_formatter,
            if_required=if_required,
            if_include_group_in_help=if_include_group_in_help,
            if_sort_specs=if_sort_specs,
            param_registry=self.params,
            group_registry_factory=lambda p: ArgGroupRegistry(
                parser=p,
                params=self.params,
            ),
            if_apply_param_keys=if_apply_param_keys,
        )
        return self.parser
