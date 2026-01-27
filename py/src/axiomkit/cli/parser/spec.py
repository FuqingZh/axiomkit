import argparse
import keyword
import re
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from .base import ArgAdder


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


class EnumScope(StrEnum):
    FRONT = "front"
    INTERNAL = "internal"


class EnumGroupKey(StrEnum):
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


DICT_ARG_GROUP_META = {
    EnumGroupKey.CONTRACT: (
        "Contract",
        "Upstream run contract: meta entrypoint, validation, and provenance.",
    ),
    EnumGroupKey.EXECUTABLES: (
        "Executables",
        "Paths to external executables (optional). If omitted, commands are resolved via PATH.",
    ),
    EnumGroupKey.INPUTS: ("Inputs", "Input files and directories."),
    EnumGroupKey.OUTPUTS: ("Outputs", "Output files and directories."),
    EnumGroupKey.RULES: ("Rules", "Filtering and processing rules."),
    EnumGroupKey.THRESHOLDS: ("Thresholds", "Cutoffs and threshold parameters."),
    EnumGroupKey.SWITCHES: ("Switches", "Boolean flags and toggles."),
    EnumGroupKey.PLOTS: ("Plots", "Plotting and graphics settings."),
    EnumGroupKey.PERFORMANCE: (
        "Performance",
        "Parallelism, memory, and performance tuning.",
    ),
    EnumGroupKey.GENERAL: ("General", "General settings and defaults."),
}


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
    scope: EnumScope = EnumScope.INTERNAL
    order: int = 0
    aliases: tuple[str, ...] = ()
    if_deprecated: bool = False
    replace_by: str | None = None

    # single source of truth: how to add this argument
    arg_builder: Callable[[ArgAdder, "SpecParam"], None] | None = None

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


@dataclass(frozen=True, slots=True)
class SpecCommand:
    """
    Immutable specification for a CLI command registered in ``RegistryCommand``.

    Each instance describes a single command, including:

    - ``id``: canonical command identifier used for registration and lookup.
    - ``help``: short help string shown in command listings.
    - ``arg_builder``: callback that configures an ``argparse.ArgumentParser``
      with this command's arguments and options.
    - ``entry``: optional entry point (such as a module path or script path)
      associated with the command.
    - ``group``: logical group name used to organize commands in the registry.
    - ``order``: numeric sort key controlling display order within a group.
    - ``aliases``: additional names that may be resolved to the canonical ``id``.
    """

    id: str
    help: str
    arg_builder: Callable[[argparse.ArgumentParser], argparse.ArgumentParser | None]
    entry: str | Path | None = None
    group: str = "default"
    order: int = 0
    aliases: tuple[str, ...] = ()
    param_keys: tuple[str, ...] = ()
