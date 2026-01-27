import argparse
from dataclasses import dataclass
from typing import Any

from ..spec import (
    DICT_ARG_GROUP_META,
    ArgAdder,
    EnumGroupKey,
    EnumParamKey,
    RegistryParam,
)


@dataclass(slots=True)
class GroupView:
    """
    A thin wrapper over an argparse argument group.

    - behaves like ArgAdder (delegates add_argument)
    - can "pull" registered ParamSpec into this group via ParamRegistry
    """

    key: EnumGroupKey
    _adder: ArgAdder
    _parser_reg: "BuilderParser"
    _params: "RegistryParam | None" = None

    # Keep ArgAdder compatibility
    def add_argument(self, *name_or_flags: str, **kwargs: Any) -> Any:
        return self._adder.add_argument(*name_or_flags, **kwargs)

    # Your desired sugar
    def extract_params(self, *param_keys: EnumParamKey) -> "GroupView":
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


class BuilderParser:
    def __init__(
        self,
        parser: argparse.ArgumentParser,
        *,
        params: "RegistryParam | None" = None,
    ) -> None:
        from .. import default_param_registry

        self.parser = parser
        self.params = params or default_param_registry()
        self._groups: dict[EnumGroupKey, GroupView] = {}

    def get_group(
        self,
        key: EnumGroupKey | str,
    ) -> GroupView:
        if (c_key := EnumGroupKey(key)) not in self._groups:
            title, desc = DICT_ARG_GROUP_META[c_key]
            g = self.parser.add_argument_group(title, description=desc)

            self._groups[c_key] = GroupView(
                key=c_key,
                _adder=g,
                _parser_reg=self,
                _params=self.params,
            )

        return self._groups[c_key]
