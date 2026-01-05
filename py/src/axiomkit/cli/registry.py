import argparse
import keyword
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Generic, Literal, Protocol, Self, TypeVar, cast

from loguru import logger
from rich_argparse import ArgumentDefaultsRichHelpFormatter, RawTextRichHelpFormatter

# from .params_defs import ParamKey

################################################################################
# #region CoreRegistryClasses

_RE_REGISTRY_TOKEN = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._-]*$")


def _validate_registry_token(token: str, *, kind: str) -> None:
    if not token:
        raise ValueError(f"{kind} must be non-empty")
    if not _RE_REGISTRY_TOKEN.fullmatch(token):
        raise ValueError(
            f"Invalid {kind}: {token!r}. Allowed pattern: {_RE_REGISTRY_TOKEN.pattern}"
        )


class HasId(Protocol):
    @property
    def id(self) -> str: ...


T = TypeVar("T", bound="HasId")


@dataclass(slots=True)
class AliasIndex:
    """
    Index of string aliases that resolve to canonical identifiers.

    This helper class maintains a mapping from one or more alias strings to a
    single canonical identifier, which can be used to normalize user input
    (for example, command names or registry keys) before lookup.

    Methods
    -------
    new() -> AliasIndex
        Create an empty alias index.

    add(canonical_id: str, *, aliases: tuple[str, ...]) -> None
        Register one or more aliases for a canonical identifier. Raises
        :class:`ValueError` if any alias is already present in the index.

    resolve(key_or_alias: str) -> str
        Return the canonical identifier associated with ``key_or_alias`` if it
        is a registered alias, otherwise return ``key_or_alias`` unchanged.

    list_aliases() -> list[str]
        Return a sorted list of all alias strings currently registered.
    """

    _alias_to_id: dict[str, str]

    @classmethod
    def new(cls) -> "AliasIndex":
        return cls(_alias_to_id={})

    def add(self, canonical_id: str, *, aliases: tuple[str, ...]) -> None:
        """
        Register one or more aliases that resolve to a canonical identifier.

        Args:
            canonical_id (str): The canonical identifier that all provided aliases
                should map to.
            aliases (tuple[str, ...]): A tuple of alias strings to register for
                the given canonical identifier.

        Raises:
            ValueError: If any of the provided aliases is already registered and
                mapped to a (possibly different) canonical identifier.
            ValueError: If any alias in ``aliases`` is already registered in this
                index and mapped to a canonical identifier.
        """
        _validate_registry_token(canonical_id, kind="canonical_id")
        for a in aliases:
            _validate_registry_token(a, kind="alias")
            if a in self._alias_to_id:
                raise ValueError(
                    f"Alias already registered: {a!r} -> {self._alias_to_id[a]!r}"
                )
            self._alias_to_id[a] = canonical_id

    def resolve(self, key_or_alias: str) -> str:
        """
        Resolve a key (which may be an alias) to its canonical id.

        Args:
            key_or_alias (str): Command identifier to resolve, either a canonical id
                or an alias.

        Returns:
            str: The canonical id if ``key_or_alias`` is a registered alias, otherwise
                the original ``key_or_alias``.
        """
        return self._alias_to_id.get(key_or_alias, key_or_alias)

    def list_aliases(self) -> list[str]:
        return sorted(self._alias_to_id.keys())

    def iter_alias_pairs(self) -> Iterable[tuple[str, str]]:
        """Iterate over (alias, canonical_id) pairs."""
        return self._alias_to_id.items()


@dataclass(slots=True)
class RegistryCore(Generic[T]):
    """
    Generic in-memory registry for items addressable by a canonical id and
    optional aliases.

    The registry stores objects of type :data:`T` in a dictionary keyed by a
    canonical identifier. An accompanying :class:`AliasIndex` maintains a
    mapping from aliases (and canonical ids) back to the canonical id, so that
    items can be retrieved using either their primary id or any registered
    alias.

    Type parameters:
        T: The type of objects stored in the registry.

    Instances are typically created via :meth:`RegistryCore.new`, which
    initializes an empty registry and alias index.

    Attributes:
        _items (dict[str, T]): Mapping from canonical id to the registered
            item.
        _aliases (AliasIndex): Index that resolves aliases to canonical ids
            for all registered items.
    """

    _items: dict[str, T]
    _aliases: AliasIndex

    @classmethod
    def new(cls) -> "RegistryCore[T]":
        return cls(_items={}, _aliases=AliasIndex.new())

    def register(
        self, spec: T, *, canonical_id: str | None = None, aliases: tuple[str, ...] = ()
    ) -> T:
        """
        Register a spec under a canonical identifier and optional aliases.

        The spec is stored under ``canonical_id`` in the registry. Any provided
        ``aliases`` are registered in the associated :class:`AliasIndex` so that
        they resolve back to the same canonical identifier.

        Args:
            spec (T): The spec object to register.
            canonical_id (str): The unique canonical identifier under which the
                spec will be stored.
            aliases (tuple[str, ...], optional): Zero or more alternative names
                that should resolve to ``canonical_id``. Defaults to ``()``.

        Raises:
            ValueError: If a spec is already registered for ``canonical_id``,
                or if any alias in ``aliases`` is already registered in the
                alias index and mapped to some canonical identifier.

        Returns:
            T: The same spec object that was registered.
        """
        if canonical_id is None:
            canonical_id = spec.id
        elif canonical_id != spec.id:
            raise ValueError(
                f"canonical_id must match spec.id: {canonical_id!r} != {spec.id!r}"
            )

        _validate_registry_token(canonical_id, kind="canonical_id")

        # Prevent canonical ids from shadowing existing aliases.
        resolved = self._aliases.resolve(canonical_id)
        if resolved != canonical_id:
            raise ValueError(
                f"Canonical id conflicts with an existing alias: {canonical_id!r} -> {resolved!r}"
            )

        # Prevent new aliases from shadowing existing canonical ids.
        for a in aliases:
            _validate_registry_token(a, kind="alias")
            if a == canonical_id:
                raise ValueError(f"Alias must not equal canonical id: {a!r}")
            if a in self._items:
                raise ValueError(
                    f"Alias conflicts with an existing canonical id: {a!r}"
                )

        if canonical_id in self._items:
            raise ValueError(f"Spec already registered: {canonical_id!r}")
        self._items[canonical_id] = spec
        self._aliases.add(canonical_id, aliases=aliases)
        return spec

    def resolve_alias(self, key_or_alias: str) -> str:
        """
        Resolve an alias or key to its canonical id.

        Args:
            key_or_alias (str): The alias or key to resolve.

        Returns:
            str: The canonical id associated with the alias or key.
        """
        return self._aliases.resolve(key_or_alias)

    def get(self, key_or_alias: str) -> T:
        """
        Retrieve a registered ``Spec`` by canonical id or alias.

        Args:
            key_or_alias (str): The command identifier to look up. This may be either a
                canonical command id or an alias previously registered for that
                command.

        Raises:
            ValueError: If ``key_or_alias`` does not resolve to any registered command
                id or alias.

        Returns:
            Spec: The specification associated with the resolved
            canonical id.
        """
        canonical_id = self._aliases.resolve(key_or_alias)
        try:
            return self._items[canonical_id]
        except KeyError as e:
            ids = self.list_ids()
            aliases = self._aliases.list_aliases()
            raise ValueError(
                f"Unknown key/alias: {key_or_alias!r}. "
                f"Available ids: {ids}. Available aliases: {aliases}."
            ) from e

    def list_ids(self) -> list[str]:
        return sorted(self._items.keys())

    def iter_alias_pairs(self) -> Iterable[tuple[str, str]]:
        """Iterate over all registered (alias, canonical_id) pairs."""
        return self._aliases.iter_alias_pairs()

    def list_specs(
        self,
        *,
        kind_sort: Literal["id", "insertion"] = "id",
        rule_sort: Callable[[T], Any] | None = None,
    ) -> list[T]:
        """
        Return all registered specs, with configurable base ordering and an optional
        custom sort rule.

        Args:
            kind_sort (Literal["id", "insertion"], optional): Controls the base
                ordering of specs before applying ``rule_sort``. Use ``"id"`` (the
                default) to order specs by their canonical ids, or ``"insertion"`` to
                preserve the insertion (registration) order.
            rule_sort (Callable[[T], Any] | None, optional): A key function applied
                to each spec to determine a custom sort order on top of the ordering
                defined by ``kind_sort``. If ``None`` (the default), the specs are
                returned in the order specified by ``kind_sort`` without any additional
                custom sorting.

        Returns:
            list[T]: The list of registered specs, ordered according to
            ``kind_sort`` and, if provided, further sorted using ``rule_sort``.
        """
        if kind_sort == "id":
            l_specs = [self._items[k] for k in sorted(self._items.keys())]
        elif kind_sort == "insertion":
            l_specs = list(self._items.values())

        if rule_sort is not None:
            l_specs = sorted(l_specs, key=rule_sort)

        return l_specs


class SmartFormatter(ArgumentDefaultsRichHelpFormatter, RawTextRichHelpFormatter):
    """
    Keep manual newlines/indentation AND show (default: ...) in help.
    """

    pass


class ArgAdder(Protocol):
    """
    Protocol for objects that support add_argument().

    This protocol works for both ArgumentParser and argument groups without
    touching private types.
    """

    def add_argument(self, *name_or_flags: str, **kwargs: Any) -> Any: ...


class GroupKey(StrEnum):
    CONTRACT = "contract"
    EXECUTABLES = "executables"
    INPUTS = "inputs"
    OUTPUTS = "outputs"
    RULES = "rules"
    THRESHOLDS = "thresholds"
    SWITCHES = "switches"
    PLOTS = "plots"
    PERFORMANCE = "performance"
    GENERAL = "general"


_DICT_ARG_GROUP_META = {
    GroupKey.CONTRACT: (
        "Contract",
        "Upstream run contract: meta entrypoint, validation, and provenance.",
    ),
    GroupKey.EXECUTABLES: (
        "Executables",
        "Paths to external executables (optional). If omitted, commands are resolved via PATH.",
    ),
    GroupKey.INPUTS: ("Inputs", "Input files and directories."),
    GroupKey.OUTPUTS: ("Outputs", "Output files and directories."),
    GroupKey.RULES: ("Rules", "Filtering and processing rules."),
    GroupKey.THRESHOLDS: ("Thresholds", "Cutoffs and threshold parameters."),
    GroupKey.SWITCHES: ("Switches", "Boolean flags and toggles."),
    GroupKey.PLOTS: ("Plots", "Plotting and graphics settings."),
    GroupKey.PERFORMANCE: (
        "Performance",
        "Parallelism, memory, and performance tuning.",
    ),
    GroupKey.GENERAL: ("General", "General settings and defaults."),
}


class ParamKey(StrEnum):
    CONTRACT_META = "contract.file_in_meta"
    EXE_RSCRIPT = "executables.rscript"
    THR_STATS_PVAL = "stats.thr_p_value"
    THR_STATS_PADJ = "stats.thr_p_adjusted"
    THR_STATS_MISSING_RATE = "stats.thr_missing_rate"
    THR_STATS_MISSING_COUNT = "stats.thr_missing_count"
    THR_STATS_FOLD_CHANGE = "stats.thr_fold_change"
    RULES_STATS_TTEST = "stats.rule_t_test"
    RULES_STATS_ANOVA = "stats.rule_anova"
    RULES_STATS_PADJ = "stats.rule_p_adjusted"
    RULES_STATS_LOG_TRANS = "stats.rule_log_transform"
    PERF_ZSTD_LVL = "zstd.lvl_zstd"
    PERF_DT_THREADS = "data_table.threads_dt"


# #endregion
################################################################################
# #region ParserRegistry


@dataclass(slots=True)
class GroupView:
    """
    A thin wrapper over an argparse argument group.

    - behaves like ArgAdder (delegates add_argument)
    - can "pull" registered ParamSpec into this group via ParamRegistry
    """

    key: GroupKey
    _adder: ArgAdder
    _parser_reg: "ParserRegistry"
    _params: "ParamRegistry | None" = None

    # Keep ArgAdder compatibility
    def add_argument(self, *name_or_flags: str, **kwargs: Any) -> Any:
        return self._adder.add_argument(*name_or_flags, **kwargs)

    # Your desired sugar
    def extract_params(self, *param_keys: ParamKey) -> "GroupView":
        """
        Add registered params into THIS group.

        Usage:
            pr.get_group(GroupKey.THRESHOLDS).extract_params(
                ParamKey.THR_TTEST_PVAL, ParamKey.THR_TTEST_PADJ
            )
        """
        if self._params is None:
            raise ValueError(
                "ParserRegistry was created without ParamRegistry; "
                "pass params=... to enable extract_params()."
            )

        # Validate: all requested params must belong to this group
        for k in param_keys:
            spec = self._params.get(k)
            if spec.group != self.key:
                raise ValueError(
                    f"Param {spec.id!r} belongs to group {spec.group!r}, "
                    f"but you are extracting into group {self.key!r}."
                )

        self._params.apply(
            parser_reg=self._parser_reg,
            keys=param_keys,
            reserved_dests=None,
        )
        return self


class ParserRegistry:
    def __init__(
        self, parser: argparse.ArgumentParser, *, params: "ParamRegistry | None" = None
    ) -> None:
        from . import default_param_registry

        self.parser = parser
        self.params = params or default_param_registry()
        self._groups: dict[GroupKey, GroupView] = {}

    def get_group(
        self,
        key: GroupKey | str,
    ) -> GroupView:
        if (c_key := GroupKey(key)) not in self._groups:
            title, desc = _DICT_ARG_GROUP_META[c_key]
            g = self.parser.add_argument_group(title, description=desc)

            self._groups[c_key] = GroupView(
                key=c_key,
                _adder=g,
                _parser_reg=self,
                _params=self.params,
            )

        return self._groups[c_key]


# #endregion
################################################################################
# #region ParamRegistry


_RE_DEST = re.compile(r"[^0-9A-Za-z_]+")


def _infer_dest_from_id(base_id: str) -> str:
    c_base = base_id.replace("-", "_")
    c_base = _RE_DEST.sub("_", c_base).strip("_")
    if not c_base:
        raise ValueError(f"Cannot infer dest from id: {base_id!r}")
    if keyword.iskeyword(c_base):
        c_base = f"{c_base}_"
    return c_base


class Scope(StrEnum):
    FRONT = "front"
    INTERNAL = "internal"


@dataclass(frozen=True, slots=True)
class ParamSpec:
    id: str
    dest: str | None = None  # canonical runtime field name
    flags: tuple[str, ...] | None = None  # e.g. ("--thr_pval",)
    help: str | None = None
    group: GroupKey = GroupKey.GENERAL  # e.g. "thresholds" / "plot" / "rules"
    scope: Scope = Scope.INTERNAL
    order: int = 0
    aliases: tuple[str, ...] = ()
    if_deprecated: bool = False
    replace_by: str | None = None

    # single source of truth: how to add this argument
    args_builder: Callable[[ArgAdder, "ParamSpec"], None] | None = None

    @property
    def base_id(self) -> str:
        return self.id.split(".")[-1]

    @property
    def resolved_dest(self) -> str:
        return self.dest or _infer_dest_from_id(self.base_id)

    @property
    def resolved_flags(self) -> tuple[str, ...]:
        if self.flags:
            return self.flags
        return (f"--{self.base_id}",)

    def add_argument(self, g: ArgAdder, /, **kwargs: Any) -> Any:
        kwargs.setdefault("dest", self.resolved_dest)
        if self.help is not None:
            kwargs.setdefault("help", self.help)
        return g.add_argument(*self.resolved_flags, **kwargs)


class ParamRegistry:
    def __init__(self) -> None:
        self._core: RegistryCore[ParamSpec] = RegistryCore.new()

    def register(self, spec: ParamSpec) -> ParamSpec:
        return self._core.register(spec, aliases=spec.aliases)

    def get(self, key_or_alias: str) -> ParamSpec:
        return self._core.get(key_or_alias)

    def list_specs(
        self,
        *,
        scope: Scope | None = None,
        group: str | None = None,
        if_sort: bool = True,
    ) -> list[ParamSpec]:
        if not if_sort:
            cls_specs = self._core.list_specs(kind_sort="insertion")
        else:
            cls_specs = self._core.list_specs(
                rule_sort=lambda s: (s.group, s.order, s.id)
            )
        if scope is not None:
            cls_specs = [s for s in cls_specs if s.scope == scope]
        if group is not None:
            cls_specs = [s for s in cls_specs if s.group == group]
        return cls_specs

    def apply(
        self,
        *,
        parser_reg: ParserRegistry,
        keys: Sequence[str],
        reserved_dests: set[str] | None,
    ) -> None:
        if reserved_dests is None:
            reserved_dests = {"command", "_handler"}

        set_existing_dests: set[str] = set()

        parser = parser_reg.parser
        for _act in getattr(parser, "_actions", []):
            if isinstance(_dest := getattr(_act, "dest", None), str):
                set_existing_dests.add(_dest)

        set_existing_flags: set[str] = set()
        if isinstance(
            (_osa := getattr(parser, "_option_string_actions", None)), Mapping
        ):
            _osa = cast(Mapping[str, object], _osa)
            set_existing_flags |= set(_osa.keys())

        dict_seen_dests: dict[str, str] = {}
        dict_seen_flags: dict[str, str] = {}
        for k in keys:
            cls_spec_ = self.get(k)
            if cls_spec_.if_deprecated:
                logger.warning(
                    f"Deprecated param: {cls_spec_.id!r}; use {cls_spec_.replace_by!r} instead."
                )
            if cls_spec_.args_builder is None:
                raise ValueError(
                    f"`ParamSpec` missing arg `args_builder`: {cls_spec_.id!r}"
                )

            c_dest_ = cls_spec_.resolved_dest
            tup_flags_ = cls_spec_.resolved_flags

            if c_dest_ in reserved_dests:
                raise ValueError(
                    f"Param dest is reserved: {c_dest_!r} (spec id: {cls_spec_.id!r})"
                )
            if c_dest_ in set_existing_dests:
                raise ValueError(
                    f"Param dest already exists on parser: {c_dest_!r} (spec id: {cls_spec_.id!r})"
                )
            if c_dest_ in dict_seen_dests:
                raise ValueError(
                    f"Param dest collision: {c_dest_!r} (spec ids: {dict_seen_dests[c_dest_]!r}, {cls_spec_.id!r})"
                )
            dict_seen_dests[c_dest_] = cls_spec_.id

            for _f in tup_flags_:
                if _f in set_existing_flags:
                    raise ValueError(
                        f"Param flag already exists on parser: {_f!r} (spec id: {cls_spec_.id!r})"
                    )
                if _f in dict_seen_flags:
                    raise ValueError(
                        f"Param flag collision: {_f!r} (spec ids: {dict_seen_flags[_f]!r}, {cls_spec_.id!r})"
                    )
                dict_seen_flags[_f] = cls_spec_.id

            cls_group = parser_reg.get_group(cls_spec_.group)
            cls_spec_.args_builder(cls_group, cls_spec_)

            # update for this apply-run
            set_existing_dests.add(c_dest_)
            set_existing_flags |= set(tup_flags_)


# #endregion
################################################################################
# #region CommandRegistry

ArgsBuilder = Callable[[argparse.ArgumentParser], argparse.ArgumentParser | None]


@dataclass(frozen=True, slots=True)
class CommandSpec:
    """
    Immutable specification for a CLI command registered in ``CommandRegistry``.

    Each instance describes a single command, including:

    - ``id``: canonical command identifier used for registration and lookup.
    - ``help``: short help string shown in command listings.
    - ``args_builder``: callback that configures an ``argparse.ArgumentParser``
      with this command's arguments and options.
    - ``entry``: optional entry point (such as a module path or script path)
      associated with the command.
    - ``group``: logical group name used to organize commands in the registry.
    - ``order``: numeric sort key controlling display order within a group.
    - ``aliases``: additional names that may be resolved to the canonical ``id``.
    """

    id: str
    help: str
    args_builder: ArgsBuilder
    entry: str | Path | None = None
    group: str = "default"
    order: int = 0
    aliases: tuple[str, ...] = ()
    param_keys: tuple[str, ...] = ()


class CommandRegistry:
    """
    Maintain a registry of CLI command specifications and their aliases.

    The registry stores immutable :class:`CommandSpec` instances keyed by a
    canonical command identifier and optionally exposes additional alias names
    for each command. It provides utilities to:

    - register new command specifications, along with any aliases
    - resolve an arbitrary command key (canonical id or alias) to its
      canonical identifier
    - build and attach :mod:`argparse` subparsers from the registered
      command specifications, using each command's ``args_builder``
      callback to configure the individual subparser.
    """

    def __init__(self) -> None:
        self._core: RegistryCore[CommandSpec] = RegistryCore.new()

    def register(self, spec: CommandSpec) -> Self:
        self._core.register(spec, aliases=spec.aliases)
        return self

    def get(self, key_or_alias: str) -> CommandSpec:
        return self._core.get(key_or_alias)

    def list_specs(self, if_sort: bool = True) -> list[CommandSpec]:
        """
        Return all registered command specifications.

        If ``if_sort`` is True, the specifications are returned sorted by
        ``(group, order, id)``; otherwise they are returned in insertion order.

        Args:
            if_sort (bool, optional): Whether to sort the returned specifications
                by group, order, and id. Defaults to True.

        Returns:
            list[CommandSpec]: The list of registered command specifications.
        """
        if not if_sort:
            return self._core.list_specs(kind_sort="insertion")
        return self._core.list_specs(rule_sort=lambda s: (s.group, s.order, s.id))

    def canonicalize_namespace(
        self, ns: argparse.Namespace, *, attr: str = "command"
    ) -> str:
        """
        Normalize a parsed subcommand string (which may be an alias) to its
        canonical command id and write it back to the given namespace.

        The value of ``getattr(ns, attr)`` is resolved using the underlying
        registry core (``self._core.resolve_alias``) and the resulting canonical
        id is written back to ``ns.<attr>``.

        Args:
            ns (argparse.Namespace): The namespace whose attribute should be
                canonicalized in-place.
            attr (str, optional): The name of the attribute on ``ns`` that
                contains the command id or alias to normalize. Defaults to
                ``"command"``.

        Raises:
            ValueError: If the namespace does not have an attribute named
                ``attr``.

        Returns:
            str: The canonical command id that was written back to ``ns.<attr>``.

        Examples:
            >>> def build_list(p: argparse.ArgumentParser) -> None:
            ...     p.add_argument("--in", default="")
            >>> registry = CommandRegistry()
            >>> registry.register(CommandSpec(id="list", help="", args_builder=build_list, aliases=("ls",)))
            Suppose ``ns.stages`` currently contains the alias ``"ls"`` and that
            ``"ls"`` has been registered as an alias for the canonical id
            ``"list"``. Then:

            >>> registry.canonicalize_namespace(ns)
            'list'
            >>> ns.stages
            'list'
        """
        if not hasattr(ns, attr):
            raise ValueError(f"Namespace has no attribute {attr!r}")
        if (v := getattr(ns, attr, None)) is None:
            # If subparser is required=True, this usually won't happen.
            raise ValueError(f"No command selected (ns.{attr} is None).")
        c_id = self._core.resolve_alias(v)
        setattr(ns, attr, c_id)
        return c_id

    def build_subparsers(
        self,
        parser: argparse.ArgumentParser,
        *,
        title: str = "Commands",
        dest: str = "command",
        kind_formatter: type[argparse.HelpFormatter] | None = SmartFormatter,
        if_required: bool = True,
        if_include_group_in_help: bool = True,
        if_sort_specs: bool = True,
    ):
        """
        Build argparse subparsers for all registered commands, creating a
        subcommand interface on the given parser.

        Each registered :class:`CommandSpec` becomes a subcommand. Any aliases
        registered for a command id are also exposed as subcommands that execute
        the same underlying command.

        Args:
            parser (argparse.ArgumentParser): The base parser on which to add
                the subparsers.
            title (str, optional): Title for the subcommands section in the
                help output. Defaults to ``"Commands"``.
            dest (str, optional): Name of the attribute on the parsed namespace
                that will store the selected subcommand id. Defaults to
                ``"command"``.
            kind_formatter (type[argparse.HelpFormatter] | None, optional):
                Custom help formatter class for subcommand parsers. If ``None``,
                argparse's default formatter is used. Defaults to ``SmartFormatter``.
            if_required (bool, optional): Whether selecting a subcommand is
                required. If ``True``, argparse will error if no subcommand is
                provided. Defaults to ``True``.
            if_include_group_in_help (bool, optional): If ``True``, include the
                command's group name in its help text when displaying the list
                of subcommands. Defaults to ``True``.
            if_sort_specs (bool, optional): If ``True``, subcommands are ordered
                according to their ``order`` attribute; otherwise the registry
                insertion order is used. Defaults to ``True``.

        Returns:
            argparse._SubParsersAction: The subparsers action object created by
            :meth:`argparse.ArgumentParser.add_subparsers`.

        Examples:
            >>> parser = argparse.ArgumentParser(prog="tool")
            >>> registry = CommandRegistry()
            >>> _ = registry.build_subparsers(parser)
        """
        cls_sub = parser.add_subparsers(title=title, dest=dest, required=if_required)

        # canonical -> [aliases...]
        dict_aliases_by_id: dict[str, list[str]] = {
            k: [] for k in self._core.list_ids()
        }
        for _ali, _id in self._core.iter_alias_pairs():
            dict_aliases_by_id.setdefault(_id, []).append(_ali)

        cls_fmt = kind_formatter or parser.formatter_class
        for spec in self.list_specs(if_sort=if_sort_specs):
            c_help = spec.help
            print(spec.id, spec.group, repr(c_help))
            if if_include_group_in_help and spec.group:
                c_help = f"\\[{spec.group}] {c_help}"

            l_aliases = sorted(dict_aliases_by_id.get(spec.id, []))
            sub = cls_sub.add_parser(
                spec.id,
                help=c_help,
                formatter_class=cls_fmt,
                aliases=l_aliases if l_aliases else [],
            )
            sub.set_defaults(
                _cmd_id=spec.id, _cmd_entry=spec.entry, _cmd_group=spec.group
            )
            spec.args_builder(sub)

        return cls_sub


# #endregion
################################################################################
################################################################################
