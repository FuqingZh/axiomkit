"""Low-level parser primitives shared by parser registries/builders.

This module keeps generic utilities independent from concrete CLI business
fields, mainly:

- help formatter composition,
- generic canonical-id registry with collision validation.
"""

import argparse
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

    def add_argument(
        self,
        *name_or_flags: str,
        **kwargs: Any,
    ) -> argparse.Action: ...


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
class CanonicalRegistry(Generic[T]):
    """
    Generic in-memory registry for items addressable by canonical id.

    The registry stores objects of type :data:`T` in a dictionary keyed by a
    canonical identifier.

    Type parameters:
        T: The type of objects stored in the registry.

    Instances are typically created via :meth:`CanonicalRegistry.new`, which
    initializes an empty registry.

    Attributes:
        _items (dict[str, T]): Mapping from canonical id to the registered item.
    """

    _items: dict[str, T]

    @classmethod
    def new(cls) -> "CanonicalRegistry[T]":
        return cls(_items={})

    def register(self, spec: T, *, canonical_id: str | None = None) -> T:
        """
        Register a spec under a canonical identifier.

        Args:
            spec (T): The spec object to register.
            canonical_id (str): The unique canonical identifier under which the
                spec will be stored.

        Raises:
            ValueError: If a spec is already registered for ``canonical_id``.

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

        if canonical_id in self._items:
            raise ValueError(f"Spec already registered: {canonical_id!r}")
        self._items[canonical_id] = spec
        return spec

    def get(self, key: str) -> T:
        """
        Retrieve a registered ``Spec`` by canonical id.

        Args:
            key (str): The canonical identifier to look up.

        Raises:
            ValueError: If ``key`` does not resolve to any registered id.

        Returns:
            Spec: The specification associated with ``key``.
        """
        try:
            return self._items[key]
        except KeyError as e:
            ids = self.list_ids()
            raise ValueError(
                f"Unknown id: {key!r}. Available ids: {ids}."
            ) from e

    def list_ids(self) -> list[str]:
        return sorted(self._items.keys())

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
