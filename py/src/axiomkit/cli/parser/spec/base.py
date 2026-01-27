import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, Generic, Literal, Protocol, TypeVar

from rich_argparse import ArgumentDefaultsRichHelpFormatter, RawTextRichHelpFormatter


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
