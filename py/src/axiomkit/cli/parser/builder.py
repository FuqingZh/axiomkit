import argparse
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, Self

from .base import SmartFormatter
from .registry import CommandRegistry, ParamRegistry, default_reserved_param_dests
from .spec import DICT_ARG_GROUP_META, ArgAdder, EnumGroupKey, SpecCommand, SpecParam


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

    def extract_params(self, *param_keys: str) -> "ArgumentGroupHandler":
        """Apply selected registered params into this argument group.

        Args:
            *param_keys: Canonical ids or aliases resolvable by ``ParamRegistry``.

        Raises:
            ValueError:
                If this handler has no ``ParamRegistry`` binding.
            ValueError:
                If a selected param belongs to a different group.

        Returns:
            ArgumentGroupHandler: ``self`` for fluent chaining.

        Examples:
            >>> # handler.extract_params("executables.rscript")
            >>> # handler.add_argument("--extra", type=str)
            >>> True
            True
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


class ParserBuilder:
    """Fluent builder for command-style CLI parsers.

    The builder composes three responsibilities:
    - parameter registration (``ParamRegistry``)
    - command registration (``CommandRegistry``)
    - parser materialization (subparsers + argument groups)

    Examples:
        >>> app = ParserBuilder(prog="demo")
        >>> _ = app.add_command(
        ...     id="run",
        ...     help="Run demo",
        ...     arg_builder=lambda p: p,
        ... )
        >>> parser = app.build_parser()
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

    def register_param(self, spec: SpecParam) -> Self:
        """Register one parameter specification.

        Args:
            spec: Parameter specification.

        Returns:
            Self: ``self`` for fluent chaining.
        """
        self.params.register_param(spec)
        return self

    def register_params(self, specs: Iterable[SpecParam]) -> Self:
        """Register multiple parameter specifications.

        Args:
            specs: Iterable of parameter specifications.

        Returns:
            Self: ``self`` for fluent chaining.
        """
        for spec in specs:
            self.register_param(spec)
        return self

    def apply_param_specs(self, *keys: str) -> Self:
        """Apply selected param specs directly to the root parser groups.

        Args:
            *keys: Param ids or aliases known by ``self.params``.

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
        aliases: tuple[str, ...] = (),
        param_keys: tuple[str, ...] = (),
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
            aliases: Alternative command names.
            param_keys: Param ids/aliases to auto-apply onto this command.

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
                aliases=aliases,
                param_keys=param_keys,
            )
        )

    def build_parser(
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
        """Materialize registered command specs into argparse subparsers.

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

    def parse_args(self, args: list[str] | None = None) -> argparse.Namespace:
        """Parse CLI args and resolve selected command alias in-place.

        Args:
            args: Optional argv list. Uses ``sys.argv`` when omitted.

        Returns:
            argparse.Namespace: Parsed namespace with canonical command id.
        """
        ns = self.parser.parse_args(args)
        if getattr(ns, self._command_dest, None) is not None:
            self.commands.resolve_command_namespace(ns, attr=self._command_dest)
        return ns

    def resolve_command_namespace(
        self,
        ns: argparse.Namespace,
        *,
        attr: str = "command",
    ) -> str:
        """Resolve and rewrite command alias on a namespace.

        Args:
            ns: Parsed namespace object.
            attr: Namespace attribute holding command id or alias.

        Returns:
            str: Canonical command id.
        """
        return self.commands.resolve_command_namespace(ns, attr=attr)
