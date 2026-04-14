import numpy as np
import polars as pl
from loguru import logger
from numpy.typing import ArrayLike
from scipy import stats

from axiomkit.stats.p_value import (
    PValueAdjustmentType,
    calculate_p_adjustment_array,
    normalize_p_value_adjustment_mode,
)


def _validate_non_negative(value: int | float, name: str) -> None:
    if value < 0:
        raise ValueError(f"Arg `{name}` must be non-negative, yours: '{value}'.")


def _validate_unity_interval(value: float, name: str) -> None:
    if not np.isfinite(value) or value < 0.0 or value > 1.0:
        raise ValueError(f"Arg `{name}` must be in [0.0, 1.0], yours: '{value}'.")


def _calculate_hypergeometric_right_tail_pvalue(
    fg_hits: ArrayLike,
    bg_hits: ArrayLike,
    bg_total: int,
    fg_total: int,
):
    """
    Compute hypergeometric right-tail p-values for over-representation analysis.

    This function evaluates, for each element, the probability of observing at
    least the given number of foreground hits under a hypergeometric null model
    defined by the background and foreground set sizes.

    Args:
        fg_hits (ArrayLike):
            Number of hits in the foreground set for each term or feature (k).
            Must be broadcastable to the same shape as ``bg_hits``.
        bg_hits (ArrayLike):
            Number of hits in the background universe for each term or feature (n).
            Must be broadcastable to the same shape as ``fg_hits``.
        bg_total (int):
            Total number of elements in the background universe (M),
            population size in the hypergeometric model.
        fg_total (int):
            Total number of elements in the foreground set (N),
            sample size in the hypergeometric model.

    Returns:
        np.ndarray: Array of hypergeometric survival-function p-values
        (right-tail), with the same shape as ``fg_hits``. Entries corresponding
        to invalid parameter combinations are left at the default value of 1.0.
    """
    fg_hits = np.asarray(fg_hits, dtype=np.int64)
    bg_hits = np.asarray(bg_hits, dtype=np.int64)
    nd_p_values = np.ones_like(fg_hits, dtype=np.float64)
    is_valid_mask = (
        (fg_hits >= 0)
        & (bg_hits >= 0)
        & (fg_total > 0)
        & (bg_total > 0)
        & (fg_hits <= np.minimum(bg_hits, fg_total))
        & (bg_hits <= bg_total)
        & (fg_total <= bg_total)
    )
    if np.any(is_valid_mask):
        nd_p_values[is_valid_mask] = stats.hypergeom.sf(
            fg_hits[is_valid_mask] - 1,
            bg_total,
            bg_hits[is_valid_mask],
            fg_total,
        )
    return nd_p_values


def calculate_ora(
    df: pl.DataFrame | pl.LazyFrame,
    col_elements: str = "ElementId",
    col_terms: str = "TermId",
    *,
    foreground_elements: set[str],
    background_elements: set[str] | None = None,
    rule_p_adjust: PValueAdjustmentType | str | None = "bh",
    thr_bg_hits_min: int = 0,
    thr_bg_hits_max: int | None = None,
    thr_fg_hits_min: int = 0,
    thr_fg_hits_max: int | None = None,
    thr_p_value: float = 0.05,
    thr_p_adjust: float = 1.0,
    should_keep_fg_members: bool = True,
    should_keep_bg_members: bool = False,
) -> pl.DataFrame:
    """
    Over-representation analysis (ORA) using hypergeometric test.

    Notes:
        - Statistical model (per term):
            - Universe size: M = BgTotal
            - Number of "success" states in universe: n = BgHits
            - Sample size (foreground): N = FgTotal
            - Observed successes in sample: k = FgHits
            - P-value is computed as P(X >= k) where X ~ Hypergeometric(M, n, N)

    Args:
        df (pl.DataFrame | pl.LazyFrame): DataFrame with at least two columns: ``col_elements`` and ``col_terms``.
        col_elements (str, optional): Column name for elements. Defaults to "ElementId".
        col_terms (str, optional): Column name for terms. Defaults to "TermId".
        foreground_elements (set[str]): Foreground elements.
            The effective foreground used in counting and in ``FgTotal`` is ``foreground_elements ∩ background_elements``.
        background_elements (set[str] | None, optional): Background elements. Defaults to None.
            - If None, the universe is inferred from `df` (all unique elements present in the mapping table);
            - If provided, the mapping is restricted to these elements.
                `BgTotal` is ``len(background_elements)`` and the mapping table is restricted to those elements.
                The provided universe may include elements not present in ``df`` (elements without any mapping),
                those elements contribute to ``BgTotal`` but not to ``BgHits``.
        rule_p_adjust (PValueAdjustmentType | str | None, optional):
            Method for p-value adjustment. see :class:`PValueAdjustmentType`.
            - "bh": (Default) Benjamini–Hochberg FDR;
            - "by": Benjamini–Yekutieli FDR;
            - "bonferroni": Bonferroni correction;
            - None: no adjustment (PAdjust == PValue).
        thr_bg_hits_min (int, optional):
            Minimum number of background hits to consider a term. [0, ∞).
            Defaults to 0.
        thr_bg_hits_max (int | None, optional):
            Maximum number of background hits to consider a term.
            [``thr_bg_hits_min``, ∞) or None for no maximum.
            Defaults to None.
        thr_fg_hits_min (int, optional):
            Minimum number of foreground hits to consider a term. [0, ∞).
            Defaults to 0.
        thr_fg_hits_max (int | None, optional):
            Maximum number of foreground hits to consider a term.
            [``thr_fg_hits_min``, ∞) or None for no maximum.
            Defaults to None.
        thr_p_value (float, optional): P-value threshold for significance. Defaults to 0.05.
        thr_p_adjust (float, optional): Adjusted p-value threshold for significance. Defaults to 1.0.
        should_keep_fg_members (bool, optional): Whether to keep foreground members in the result. Defaults to True.
        should_keep_bg_members (bool, optional): Whether to keep background members in the result. Defaults to False.

    Returns:
        pl.DataFrame: DataFrame with ORA results and columns:
            - Column named by `col_terms` (default: `TermId`): Term identifier
            - `FgHits`: Number of foreground hits
            - `FgTotal`: Total number of foreground elements
            - `BgHits`: Number of background hits
            - `BgTotal`: Total number of background elements
            - `FoldEnrichment`: Fold enrichment of foreground hits over background hits
            - `PValue`: Raw p-value from hypergeometric test
            - `PAdjust`: Adjusted p-value
            - `FgMembers` (optional): List of foreground members for each term
            - `BgMembers` (optional): List of background members for each term

    Examples:
        ```python
        import polars as pl
        df = pl.DataFrame(
            {
                "ElementId": ["g1", "g1", "g2", "g3"],
                "TermId": ["t1", "t2", "t2", "t2"],
            }
        )
        foreground_ids = {"g1", "g2"}
        df_result = calculate_ora(
            df,
            col_elements="ElementId",
            col_terms="TermId",
            foreground_elements=foreground_ids,
        )
        ```
    """

    ############################################################
    # #region checkExistence
    _validate_unity_interval(thr_p_value, "thr_p_value")
    _validate_unity_interval(thr_p_adjust, "thr_p_adjust")
    _validate_non_negative(thr_bg_hits_min, "thr_bg_hits_min")
    _validate_non_negative(thr_fg_hits_min, "thr_fg_hits_min")
    if thr_bg_hits_max is not None and thr_bg_hits_max < thr_bg_hits_min:
        raise ValueError(
            f"Arg `thr_bg_hits_max` must be in [`thr_bg_hits_min`, ∞) or None, yours: '{thr_bg_hits_max}'."
        )
    if thr_fg_hits_max is not None and thr_fg_hits_max < thr_fg_hits_min:
        raise ValueError(
            f"Arg `thr_fg_hits_max` must be in [`thr_fg_hits_min`, ∞) or None, yours: '{thr_fg_hits_max}'."
        )
    rule_p_adjust = normalize_p_value_adjustment_mode(rule_p_adjust)
    # endregion
    ############################################################
    # #region createEmptyResult
    df_empty = pl.DataFrame(
        schema={
            col_terms: pl.Utf8,
            "FgHits": pl.Int64,
            "FgTotal": pl.Int64,
            "BgHits": pl.Int64,
            "BgTotal": pl.Int64,
            "FoldEnrichment": pl.Float64,
            "PValue": pl.Float64,
            "PAdjust": pl.Float64,
            "FgMembers": pl.List(pl.Utf8),
            "BgMembers": pl.List(pl.Utf8),
        },
    )
    if not should_keep_bg_members:
        df_empty = df_empty.drop("BgMembers")
    if not should_keep_fg_members:
        df_empty = df_empty.drop("FgMembers")

    # #endregion
    ############################################################
    # #region checkEmpty

    # #tag checkElements
    if len(foreground_elements) == 0:
        logger.warning(
            "Arg `foreground_elements` is an empty set, I will return an empty DataFrame with schema."
        )
        return df_empty
    if background_elements is not None and len(background_elements) == 0:
        logger.warning(
            "Arg `background_elements` is an empty set, I will return an empty DataFrame with schema."
        )
        return df_empty

    # #tag checkDf
    lf = df.lazy() if isinstance(df, pl.DataFrame) else df
    lf_mappings = lf.select([col_elements, col_terms]).unique([col_elements, col_terms])

    # #tag checkConsistency
    if background_elements is None:
        lf_bg_elements = lf_mappings.select(col_elements).unique()
    else:
        lf_bg_elements = pl.LazyFrame(
            {col_elements: pl.Series(list(background_elements), dtype=pl.Utf8)}
        ).unique()
        lf_mappings = lf_mappings.join(lf_bg_elements, on=col_elements, how="inner")

    lf_fg_elements = pl.LazyFrame(
        {col_elements: pl.Series(list(foreground_elements), dtype=pl.Utf8)}
    ).unique()

    lf_fg_in_bg = lf_fg_elements.join(lf_bg_elements, on=col_elements, how="inner")

    df_total_counts = (
        lf_bg_elements.select(pl.len().alias("BgTotal"))
        .join(lf_fg_in_bg.select(pl.len().alias("FgTotal")), how="cross")
        .collect()
    )
    bg_total = df_total_counts.item(0, "BgTotal")
    fg_total = df_total_counts.item(0, "FgTotal")

    if fg_total == 0:
        logger.warning(
            "No foreground elements found in background elements, I will return an empty DataFrame with schema."
        )
        return df_empty

    lf_mappings_marked = lf_mappings.join(
        lf_fg_in_bg.select(col_elements).with_columns(pl.lit(True).alias("IsFg")),
        on=col_elements,
        how="left",
    ).with_columns(pl.col("IsFg").fill_null(False))

    # #endregion
    ############################################################
    # #region calculateCounts

    df_ora = (
        lf_mappings_marked.group_by(col_terms)
        .agg(
            BgHits=pl.col(col_elements).n_unique(),
            FgHits=pl.col(col_elements).filter(pl.col("IsFg")).n_unique(),
        )
        .with_columns(FgTotal=pl.lit(fg_total), BgTotal=pl.lit(bg_total))
        .filter(
            pl.col("BgHits").ge(thr_bg_hits_min),
            pl.col("BgHits").le(thr_bg_hits_max)
            if thr_bg_hits_max is not None
            else True,
            pl.col("FgHits").ge(thr_fg_hits_min),
            pl.col("FgHits").le(thr_fg_hits_max)
            if thr_fg_hits_max is not None
            else True,
        )
        .with_columns(
            FgRatio=pl.col("FgHits") / pl.col("FgTotal"),
            BgRatio=pl.col("BgHits") / pl.col("BgTotal"),
        )
        .with_columns(
            FoldEnrichment=pl.when(pl.col("BgRatio") > 0)
            .then(pl.col("FgRatio") / pl.col("BgRatio"))
            .otherwise(np.inf),
        )
        .collect()
    )

    if df_ora.height == 0:
        logger.warning(
            f"""
            No terms pass filters.
            BgTotal={bg_total}, FgTotal={fg_total};
            thr_bg_hits_min={thr_bg_hits_min}, thr_fg_hits_min={thr_fg_hits_min},
            thr_bg_hits_max={thr_bg_hits_max}, thr_fg_hits_max={thr_fg_hits_max}.
            Possible causes:
                (1) no mappings,
                (2) background not overlapping mapping,
                (3) very small fg∩bg,
                (4) thresholds too stringent.
            """
        )
        return df_empty

    # #endregion
    ############################################################
    # #region calculatePValues
    nd_p_vals = _calculate_hypergeometric_right_tail_pvalue(
        fg_hits=df_ora["FgHits"].to_numpy(),
        bg_hits=df_ora["BgHits"].to_numpy(),
        bg_total=bg_total,
        fg_total=fg_total,
    )

    nd_p_adj = calculate_p_adjustment_array(nd_p_vals, rule_p_adjust=rule_p_adjust)

    df_ora = (
        df_ora.with_columns(
            pl.Series(name="PValue", values=nd_p_vals, dtype=pl.Float64),
            pl.Series(name="PAdjust", values=nd_p_adj, dtype=pl.Float64),
        )
        .filter(
            pl.col("PValue") <= thr_p_value,
            pl.col("PAdjust") <= thr_p_adjust,
        )
        .sort(
            ["PAdjust", "PValue", "FoldEnrichment", col_terms],
            descending=[False, False, True, False],
        )
        .select(
            [
                col_terms,
                "FgHits",
                "FgTotal",
                "BgHits",
                "BgTotal",
                "FoldEnrichment",
                "PValue",
                "PAdjust",
            ]
        )
    )

    # #endregion
    ############################################################
    # #region calculateMembers
    if df_ora.height > 0 and (should_keep_bg_members or should_keep_fg_members):
        significant_terms = set(df_ora[col_terms].to_list())

        df_members = (
            lf_mappings_marked.filter(pl.col(col_terms).is_in(significant_terms))
            .group_by(col_terms)
            .agg(
                *(
                    [pl.col(col_elements).unique().alias("BgMembers")]
                    if should_keep_bg_members
                    else []
                ),
                *(
                    [
                        pl.col(col_elements)
                        .filter(pl.col("IsFg"))
                        .unique()
                        .alias("FgMembers")
                    ]
                    if should_keep_fg_members
                    else []
                ),
            )
            .collect()
        )
        df_ora = df_ora.join(df_members, on=col_terms, how="left")
    # #endregion
    ############################################################

    return df_ora
