from __future__ import annotations

from collections.abc import Sequence
from importlib import import_module
from types import ModuleType
from typing import Any


def _format_extra_names(extras: Sequence[str]) -> str:
    return ",".join(dict.fromkeys(extras))


def build_optional_dependency_error(
    *,
    feature: str,
    extras: Sequence[str],
    missing_module: str | None,
) -> ModuleNotFoundError:
    extras_text = _format_extra_names(extras)
    missing_text = (
        f"Missing optional dependency `{missing_module}`."
        if missing_module
        else "Missing optional dependency."
    )
    message = (
        f"{feature} is unavailable. {missing_text} "
        f"Install extras with `pip install \"axiomkit[{extras_text}]\"` "
        f"or sync in development with `pdm sync -G dev -G {extras_text}`."
    )
    return ModuleNotFoundError(message)


def import_optional_module(
    *,
    module_name: str,
    package: str,
    feature: str,
    extras: Sequence[str],
    required_modules: Sequence[str],
) -> ModuleType:
    try:
        return import_module(module_name, package=package)
    except ModuleNotFoundError as exc:
        parts_missing = (exc.name or "").split(".")
        missing_candidates = set(parts_missing)
        missing_candidates.add(parts_missing[0] if parts_missing else "")
        missing_candidates.add(parts_missing[-1] if parts_missing else "")

        required_candidates: set[str] = set()
        for item in required_modules:
            parts_required = item.split(".")
            required_candidates |= set(parts_required)
            required_candidates.add(parts_required[0])
            required_candidates.add(parts_required[-1])

        if not required_modules or bool(missing_candidates & required_candidates):
            raise build_optional_dependency_error(
                feature=feature,
                extras=extras,
                missing_module=exc.name,
            ) from exc
        raise


def import_optional_attr(
    *,
    module_name: str,
    attr_name: str,
    package: str,
    feature: str,
    extras: Sequence[str],
    required_modules: Sequence[str],
) -> Any:
    module = import_optional_module(
        module_name=module_name,
        package=package,
        feature=feature,
        extras=extras,
        required_modules=required_modules,
    )
    return getattr(module, attr_name)
