from atexit import register
from enum import StrEnum

from .actions import NumericRangeAction, PathAction
from .registry import GroupKey, ParamKey, ParamRegistry, ParamSpec

# Reusable param-key groups for commands


class MethodAnova(StrEnum):
    ONE_WAY_POOLED = "one_way_pooled"  # 单因素，等方差
    ONE_WAY_WELCH = "one_way_welch"  # 单因素，异方差（默认）
    TWO_WAY_INDEPENDENT = "two_way_independent"  # 双因素，无交互
    TWO_WAY_INTERACTION = "two_way_interaction"  # 双因素，含交互
    REPEATED_MEASURES = "repeated_measures"  # 重复测量
    RANDOM_EFFECTS = "random_effects"  # 随机效应/混合


class MethodTTest(StrEnum):
    NONE = "none"  # 不进行t检验，仅计算FC
    TWO_SAMPLE_POOLED = "two_sample_pooled"  # 双样本，等方差（Student's t-test）
    TWO_SAMPLE_WELCH = "two_sample_welch"  # 双样本，异方差（Welch's t-test）
    ONE_SAMPLE = "one_sample"  # 单样本，均值与0比较
    PAIRED = "paired"  # 配对双样本


class MethodPAdjust(StrEnum):
    NONE = "none"  # 不进行多重检验校正
    BH = "BH"  # Benjamini-Hochberg (默认)
    BY = "BY"  # Benjamini-Yekutieli
    HOLM = "holm"  # Holm
    HOCHBERG = "hochberg"  # Hochberg
    HOMMEL = "hommel"  # Hommel
    BONFERRONI = "bonferroni"  # Bonferroni


THR_TTEST_PARAMS: tuple[ParamKey, ...] = (
    ParamKey.THR_STATS_PVAL,
    ParamKey.THR_STATS_PADJ,
    ParamKey.RULES_STATS_TTEST,
)


def build_param_registry(registry: ParamRegistry) -> None:
    """Build a parameter registry with all defined parameters."""
    registry.register(
        ParamSpec(
            id=ParamKey.CONTRACT_META,
            help="Path to metadata file (JSON)",
            group=GroupKey.CONTRACT,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg, action=PathAction.file(exts=["json"])
            ),
        )
    )

    registry.register(
        ParamSpec(
            id=ParamKey.EXE_RSCRIPT,
            help="Path to Rscript executable",
            group=GroupKey.EXECUTABLES,
            args_builder=lambda _arg, _spec: _spec.add_argument(_arg, type=str),
        )
    )

    registry.register(
        ParamSpec(
            id=ParamKey.THR_STATS_FOLD_CHANGE,
            help="Override: Fold-change threshold for feature filtering, non-negative float.",
            group=GroupKey.THRESHOLDS,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg,
                type=float,
                action=NumericRangeAction.build(min_value=0),
                default=None,
            ),
        )
    )

    registry.register(
        ParamSpec(
            id=ParamKey.THR_STATS_MISSING_RATE,
            help="Override: Missing rate threshold for feature filtering, [0,1].",
            group=GroupKey.THRESHOLDS,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg,
                type=float,
                action=NumericRangeAction.build(min_value=0, max_value=1),
                default=None,
            ),
        )
    )

    registry.register(
        ParamSpec(
            id=ParamKey.THR_STATS_MISSING_COUNT,
            help="Override: Missing count threshold for feature filtering, non-negative integer.",
            group=GroupKey.THRESHOLDS,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg,
                type=int,
                action=NumericRangeAction.build(kind_value="int", min_value=0),
                default=None,
            ),
        )
    )

    registry.register(
        ParamSpec(
            id=ParamKey.THR_STATS_PVAL,
            help="Override: T-test p-value threshold, [0,1].",
            group=GroupKey.THRESHOLDS,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg,
                action=NumericRangeAction.p_value,
                default=None,
            ),
        )
    )
    registry.register(
        ParamSpec(
            id=ParamKey.THR_STATS_PADJ,
            help="Override: Adjusted p-value threshold, [0,1].",
            group=GroupKey.THRESHOLDS,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg,
                action=NumericRangeAction.p_value,
                default=None,
            ),
        )
    )

    registry.register(
        ParamSpec(
            id=ParamKey.RULES_STATS_PADJ,
            help=(
                """
                Override: 
                Method for multiple testing correction (p-value adjustment).
                Choices:
                    none: No adjustment
                    BH: Benjamini-Hochberg (FDR) (default)
                    BY: Benjamini-Yekutieli
                    holm: Holm
                    hochberg: Hochberg
                    hommel: Hommel
                    bonferroni: Bonferroni
                """
            ),
            group=GroupKey.RULES,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg, type=str, choices=MethodPAdjust, default=None
            ),
        )
    )

    registry.register(
        ParamSpec(
            id=ParamKey.RULES_STATS_TTEST,
            help=(
                """
                Override: 
                T-test method to use for two-group comparison.
                Choices:
                    none: No t-test, only compute fold-change (FC)
                    two_sample_pooled: Two-sample t-test, assuming equal variance (Student's t-test)
                    two_sample_welch: Two-sample t-test, not assuming equal variance (Welch's t-test)
                    one_sample: One-sample t-test, comparing mean to 0
                    paired: Paired two-sample t-test
                """
            ),
            group=GroupKey.RULES,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg, type=str, choices=MethodTTest, default=None
            ),
        )
    )

    registry.register(
        ParamSpec(
            id=ParamKey.RULES_STATS_ANOVA,
            help=(
                """
                Override: 
                ANOVA method to use for multi-group comparison.
                Choices:
                    one_way_pooled: One-way ANOVA, assuming equal variance (pooled)
                    one_way_welch: One-way ANOVA, not assuming equal variance (Welch
                    two_way_independent: Two-way ANOVA, without interaction
                    two_way_interaction: Two-way ANOVA, with interaction
                    repeated_measures: Repeated measures ANOVA
                    random_effects: Random effects / Mixed ANOVA
                """
            ),
            group=GroupKey.RULES,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg, type=str, choices=MethodAnova, default=None
            ),
        )
    )
    
    registry.register(
        ParamSpec(
            id=ParamKey.RULES_STATS_LOG_TRANS,
            help=(
                """
                Override: 
                Log transformation rule for statistical analysis.
                Choices:
                    none: No log transformation
                    log2: Base-2 logarithm transformation
                    log10: Base-10 logarithm transformation
                    ln: Natural logarithm transformation
                """
            ),
            group=GroupKey.RULES,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg, type=str, choices=["none", "log2", "log10", "ln"], default=None
            ),
        )
    )

    registry.register(
        ParamSpec(
            id=ParamKey.PERF_ZSTD_LVL,
            help=(
                """
                Parquet ZSTD compression level (1-22); -1=none/uncompressed; 0=auto
                If =-1, no compression will be used.
                If =0, auto: 
                    If input file size is unknown, will use level 5;
                    bytes < 100 MB, no compression; 
                    100MB <= bytes <= 8 GB, level 5;
                    8GB < bytes <= 32 GB, level 7.
                !Note: 
                    Higher level means smaller file but more CPU cost.
                    So compression is not always better:
                        For small files (<100MB) it may actually slow down I/O.
                        For large files, compression usually helps a lot.
                        You can try different levels to find the best trade-off.
                    See https://arrow.apache.org/docs/r/reference/write_parquet.html#arg-compression for details.
                """
            ),
            group=GroupKey.PERFORMANCE,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg,
                type=int,
                action=NumericRangeAction.build(
                    kind_value="int", min_value=-1, max_value=22
                ),
                default=5,
            ),
        )
    )

    registry.register(
        ParamSpec(
            id=ParamKey.PERF_DT_THREADS,
            help=(
                """
                data.table threads; -1=auto; 0=all cores,
                !Note: 
                    This only affects data.table operations, not arrow.
                    Arrow uses its own internal thread pool, usually equals to number of CPU cores.
                    So if you have 8 cores, setting this to 8 may oversubscribe CPU.
                    Setting this to -1 lets data.table decide the best number of threads.
                    If you have many large tables to process, you can set this to a smaller number to avoid oversubscription.
                    If you have only one large table to process, you can set this to 0 to use all cores.
                See https://search.r-project.org/CRAN/refmans/data.table/html/openmp-utils.html for details.
                """
            ),
            group=GroupKey.PERFORMANCE,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg,
                type=int,
                action=NumericRangeAction.build(kind_value="int", min_value=-1),
                default=-1,
            ),
        )
    )
