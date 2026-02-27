from typing import Literal

import numpy as np
import polars as pl
from loguru import logger
from numpy.typing import ArrayLike
from scipy import stats


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
    b_valid_mask = (
        (fg_hits >= 0)
        & (bg_hits >= 0)
        & (fg_total > 0)
        & (bg_total > 0)
        & (fg_hits <= np.minimum(bg_hits, fg_total))
        & (bg_hits <= bg_total)
        & (fg_total <= bg_total)
    )
    if np.any(b_valid_mask):
        nd_p_values[b_valid_mask] = stats.hypergeom.sf(
            fg_hits[b_valid_mask] - 1,
            bg_total,
            bg_hits[b_valid_mask],
            fg_total,
        )
    return nd_p_values


def _calculate_bh_fdr(p_values: np.ndarray):
    """
    Benjamini-Hochberg FDR correction.

    Args:
        p_values (np.ndarray): Array of p-values to be corrected for multiple testing.

    Returns:
        np.ndarray: Array of Benjamini–Hochberg FDR-adjusted p-values, matching the
            shape of the input.
    """
    p_values = np.asarray(p_values, dtype=np.float64)
    if (n_size := p_values.size) == 0:
        return p_values

    np_order = np.argsort(p_values, kind="mergesort")
    np_ranks = (
        p_values[np_order] * n_size / (np.arange(1, n_size + 1, dtype=np.float64))
    )
    np_adjusted = np.minimum.accumulate(np_ranks[::-1])[::-1]
    np_adjusted = np.clip(np_adjusted, 0.0, 1.0)

    np_p = np.empty_like(np_adjusted)
    np_p[np_order] = np_adjusted
    return np_p


def calculate_ora(
    df: pl.DataFrame | pl.LazyFrame,
    col_elements: str = "ElementId",
    col_terms: str = "TermId",
    *,
    foreground_elements: set[str],
    background_elements: set[str] | None = None,
    rule_p_adjust: Literal["bh", "bonferroni"] | None = "bh",
    thr_bg_hits_min: int = 0,
    thr_fg_hits_min: int = 0,
    thr_p_value: float = 0.05,
    thr_p_adjust: float = 1.0,
    if_keep_fg_members: bool = True,
    if_keep_bg_members: bool = False,
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
        rule_p_adjust (Literal["bh", "bonferroni"] | None, optional): Method for p-value adjustment. Defaults to "bh".
            - "bh": Benjamini–Hochberg FDR;
            - "bonferroni": Bonferroni correction;
            - None: no adjustment (PAdjust == PValue).
        thr_bg_hits_min (int, optional): Minimum number of background hits to consider a term. Defaults to 0.
        thr_fg_hits_min (int, optional): Minimum number of foreground hits to consider a term. Defaults to 0.
        thr_p_value (float, optional): P-value threshold for significance. Defaults to 0.05.
        thr_p_adjust (float, optional): Adjusted p-value threshold for significance. Defaults to 1.0.
        if_keep_fg_members (bool, optional): Whether to keep foreground members in the result. Defaults to True.
        if_keep_bg_members (bool, optional): Whether to keep background members in the result. Defaults to False.

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
        >>> import polars as pl
        >>> df = pl.DataFrame(
        ...     {
        ...         "ElementId": ["g1", "g1", "g2", "g3"],
        ...         "TermId": ["t1", "t2", "t2", "t2"],
        ...     }
        ... )
        >>> set_foreground = {"g1", "g2"}
        >>> df_result = calculate_ora(
        ...     df,
        ...     col_elements="ElementId",
        ...     col_terms="TermId",
        ...     foreground_elements=set_foreground,
        ... )
    """

    ############################################################
    # #region checkExistence
    if thr_p_value < 0.0 or thr_p_value > 1.0:
        raise ValueError(
            f"Arg `thr_p_value` must in [0.0, 1.0], yours: '{thr_p_value}'."
        )
    if thr_p_adjust < 0.0 or thr_p_adjust > 1.0:
        raise ValueError(
            f"Arg `thr_p_adjust` must in [0.0, 1.0], yours: '{thr_p_adjust}'."
        )
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
    if not if_keep_bg_members:
        df_empty = df_empty.drop("BgMembers")
    if not if_keep_fg_members:
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
        .collect(engine="streaming")
    )
    n_bg_total = int(df_total_counts["BgTotal"][0])
    n_fg_total = int(df_total_counts["FgTotal"][0])

    if n_fg_total == 0:
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
        .with_columns(FgTotal=pl.lit(n_fg_total), BgTotal=pl.lit(n_bg_total))
        .filter(
            pl.col("BgHits") >= thr_bg_hits_min,
            pl.col("FgHits") >= thr_fg_hits_min,
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
        .collect(engine="streaming")
    )

    if df_ora.height == 0:
        logger.warning(
            f"""
            No terms pass filters.
            BgTotal={n_bg_total}, FgTotal={n_fg_total}, thr_bg_hits_min={thr_bg_hits_min}, thr_fg_hits_min={thr_fg_hits_min}.
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
        bg_total=n_bg_total,
        fg_total=n_fg_total,
    )

    match rule_p_adjust:
        case "bh":
            nd_p_adj = _calculate_bh_fdr(nd_p_vals)
        case "bonferroni":
            nd_p_adj = np.minimum(nd_p_vals * df_ora.height, 1.0)
        case _:
            nd_p_adj = nd_p_vals

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
    if df_ora.height > 0 and (if_keep_bg_members or if_keep_fg_members):
        set_sign_terms = set(df_ora[col_terms].to_list())

        df_members = (
            lf_mappings_marked.filter(pl.col(col_terms).is_in(set_sign_terms))
            .group_by(col_terms)
            .agg(
                *(
                    [pl.col(col_elements).unique().alias("BgMembers")]
                    if if_keep_bg_members
                    else []
                ),
                *(
                    [
                        pl.col(col_elements)
                        .filter(pl.col("IsFg"))
                        .unique()
                        .alias("FgMembers")
                    ]
                    if if_keep_fg_members
                    else []
                ),
            )
            .collect(engine="streaming")
        )
        df_ora = df_ora.join(df_members, on=col_terms, how="left")
    # #endregion
    ############################################################

    return df_ora
