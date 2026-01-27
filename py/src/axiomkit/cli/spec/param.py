import keyword
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, cast

from loguru import logger

from .base import ArgAdder, RegistryCore
from .group import EnumGroupKey
from .parser import BuilderParser


class EnumMethodAnova(StrEnum):
    ONE_WAY_POOLED = "one_way_pooled"  # 单因素，等方差
    ONE_WAY_WELCH = "one_way_welch"  # 单因素，异方差（默认）
    TWO_WAY_INDEPENDENT = "two_way_independent"  # 双因素，无交互
    TWO_WAY_INTERACTION = "two_way_interaction"  # 双因素，含交互
    REPEATED_MEASURES = "repeated_measures"  # 重复测量
    RANDOM_EFFECTS = "random_effects"  # 随机效应/混合


class EnumMethodTTest(StrEnum):
    NONE = "none"  # 不进行t检验，仅计算FC
    TWO_SAMPLE_POOLED = "two_sample_pooled"  # 双样本，等方差（Student's t-test）
    TWO_SAMPLE_WELCH = "two_sample_welch"  # 双样本，异方差（Welch's t-test）
    ONE_SAMPLE = "one_sample"  # 单样本，均值与0比较
    PAIRED = "paired"  # 配对双样本


class EnumMethodPAdjust(StrEnum):
    NONE = "none"  # 不进行多重检验校正
    BH = "BH"  # Benjamini-Hochberg (默认)
    BY = "BY"  # Benjamini-Yekutieli
    HOLM = "holm"  # Holm
    HOCHBERG = "hochberg"  # Hochberg
    HOMMEL = "hommel"  # Hommel
    BONFERRONI = "bonferroni"  # Bonferroni


class EnumParamKey(StrEnum):
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


class Scope(StrEnum):
    FRONT = "front"
    INTERNAL = "internal"


_RE_DEST = re.compile(r"[^0-9A-Za-z_]+")


def _infer_dest_from_id(base_id: str) -> str:
    c_base = base_id.replace("-", "_")
    c_base = _RE_DEST.sub("_", c_base).strip("_")
    if not c_base:
        raise ValueError(f"Cannot infer dest from id: {base_id!r}")
    if keyword.iskeyword(c_base):
        c_base = f"{c_base}_"
    return c_base


@dataclass(frozen=True, slots=True)
class SpecParam:
    id: str
    dest: str | None = None  # canonical runtime field name
    flags: tuple[str, ...] | None = None  # e.g. ("--thr_pval",)
    help: str | None = None
    group: EnumGroupKey = EnumGroupKey.GENERAL  # e.g. "thresholds" / "plot" / "rules"
    scope: Scope = Scope.INTERNAL
    order: int = 0
    aliases: tuple[str, ...] = ()
    if_deprecated: bool = False
    replace_by: str | None = None

    # single source of truth: how to add this argument
    args_builder: Callable[[ArgAdder, "SpecParam"], None] | None = None

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


class RegistryParam:
    def __init__(self) -> None:
        self._core: RegistryCore[SpecParam] = RegistryCore.new()

    def register(self, spec: SpecParam) -> SpecParam:
        return self._core.register(spec, aliases=spec.aliases)

    def get(self, key_or_alias: str) -> SpecParam:
        return self._core.get(key_or_alias)

    def list_specs(
        self,
        *,
        scope: Scope | None = None,
        group: str | None = None,
        if_sort: bool = True,
    ) -> list[SpecParam]:
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
        parser_reg: BuilderParser,
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
