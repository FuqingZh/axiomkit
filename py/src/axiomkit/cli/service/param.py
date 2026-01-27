from ..spec import (
    ActionNumericRange,
    ActionPath,
    EnumGroupKey,
    EnumMethodAnova,
    EnumMethodPAdjust,
    EnumMethodTTest,
    EnumParamKey,
    RegistryParam,
    SpecParam,
)

# Reusable param-key groups for commands


THR_TTEST_PARAMS: tuple[EnumParamKey, ...] = (
    EnumParamKey.THR_STATS_PVAL,
    EnumParamKey.THR_STATS_PADJ,
    EnumParamKey.RULES_STATS_TTEST,
)


def build_param_registry(registry: RegistryParam) -> None:
    """Build a parameter registry with all defined parameters."""
    registry.register(
        SpecParam(
            id=EnumParamKey.CONTRACT_META,
            help="Path to metadata file (JSON)",
            group=EnumGroupKey.CONTRACT,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg, action=ActionPath.file(exts=["json"])
            ),
        )
    )

    registry.register(
        SpecParam(
            id=EnumParamKey.EXE_RSCRIPT,
            help="Path to Rscript executable",
            group=EnumGroupKey.EXECUTABLES,
            args_builder=lambda _arg, _spec: _spec.add_argument(_arg, type=str),
        )
    )

    registry.register(
        SpecParam(
            id=EnumParamKey.THR_STATS_FOLD_CHANGE,
            help="Override: Fold-change threshold for feature filtering, non-negative float.",
            group=EnumGroupKey.THRESHOLDS,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg,
                type=float,
                action=ActionNumericRange.build(min_value=0),
                default=None,
            ),
        )
    )

    registry.register(
        SpecParam(
            id=EnumParamKey.THR_STATS_MISSING_RATE,
            help="Override: Missing rate threshold for feature filtering, [0,1].",
            group=EnumGroupKey.THRESHOLDS,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg,
                type=float,
                action=ActionNumericRange.build(min_value=0, max_value=1),
                default=None,
            ),
        )
    )

    registry.register(
        SpecParam(
            id=EnumParamKey.THR_STATS_MISSING_COUNT,
            help="Override: Missing count threshold for feature filtering, non-negative integer.",
            group=EnumGroupKey.THRESHOLDS,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg,
                type=int,
                action=ActionNumericRange.build(kind_value="int", min_value=0),
                default=None,
            ),
        )
    )

    registry.register(
        SpecParam(
            id=EnumParamKey.THR_STATS_PVAL,
            help="Override: T-test p-value threshold, [0,1].",
            group=EnumGroupKey.THRESHOLDS,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg,
                action=ActionNumericRange.p_value,
                default=None,
            ),
        )
    )
    registry.register(
        SpecParam(
            id=EnumParamKey.THR_STATS_PADJ,
            help="Override: Adjusted p-value threshold, [0,1].",
            group=EnumGroupKey.THRESHOLDS,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg,
                action=ActionNumericRange.p_value,
                default=None,
            ),
        )
    )

    registry.register(
        SpecParam(
            id=EnumParamKey.RULES_STATS_PADJ,
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
            group=EnumGroupKey.RULES,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg, type=str, choices=EnumMethodPAdjust, default=None
            ),
        )
    )

    registry.register(
        SpecParam(
            id=EnumParamKey.RULES_STATS_TTEST,
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
            group=EnumGroupKey.RULES,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg, type=str, choices=EnumMethodTTest, default=None
            ),
        )
    )

    registry.register(
        SpecParam(
            id=EnumParamKey.RULES_STATS_ANOVA,
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
            group=EnumGroupKey.RULES,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg, type=str, choices=EnumMethodAnova, default=None
            ),
        )
    )

    registry.register(
        SpecParam(
            id=EnumParamKey.RULES_STATS_LOG_TRANS,
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
            group=EnumGroupKey.RULES,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg, type=str, choices=["none", "log2", "log10", "ln"], default=None
            ),
        )
    )

    registry.register(
        SpecParam(
            id=EnumParamKey.PERF_ZSTD_LVL,
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
            group=EnumGroupKey.PERFORMANCE,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg,
                type=int,
                action=ActionNumericRange.build(
                    kind_value="int", min_value=-1, max_value=22
                ),
                default=5,
            ),
        )
    )

    registry.register(
        SpecParam(
            id=EnumParamKey.PERF_DT_THREADS,
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
            group=EnumGroupKey.PERFORMANCE,
            args_builder=lambda _arg, _spec: _spec.add_argument(
                _arg,
                type=int,
                action=ActionNumericRange.build(kind_value="int", min_value=-1),
                default=-1,
            ),
        )
    )
