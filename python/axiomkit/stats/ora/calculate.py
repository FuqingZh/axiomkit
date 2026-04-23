from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import numpy as np
import polars as pl
from loguru import logger

from axiomkit.stats.p_value import (
    calculate_p_adjustment_array,
    normalize_p_value_adjustment_mode,
)

from .constant import (
    COL_COMPARISON,
    COL_ELEMENT,
    COL_TERM,
    COLS_COMPARISON_UNIT,
    SCHEMA_COMPARISON,
    SCHEMA_ORA_CONFIG_FIELDS,
)
from .spec import OraComparison, OraOptions
from .util import (
    calculate_hypergeometric_right_tail_pvalue,
    create_empty_result,
    normalize_comparisons,
    resolve_comparisons,
    select_required_columns,
    validate_comparisons,
)


def calculate_ora(
    annotation: pl.DataFrame | pl.LazyFrame,
    col_elements: str = "ElementId",
    col_terms: str = "TermId",
    *,
    comparisons: OraComparison | Iterable[OraComparison],
    options: OraOptions | None = None,
) -> pl.DataFrame:
    """
    Over-representation analysis (ORA) using a comparison-aware batch model.

    Notes:
        - Statistical model (per term):
            - Universe size: M = BgTotal
            - Number of "success" states in universe: n = BgHits
            - Sample size (foreground): N = FgTotal
            - Observed successes in sample: k = FgHits
            - P-value is computed as P(X >= k) where X ~ Hypergeometric(M, n, N)
        - `ComparisonId` output rule:
            - Included for multi-comparison input
            - Included for single comparisons with an explicit `comparison_id`
            - Omitted only for single comparisons without an identifier

    Args:
        annotation:
            Mapping table with at least ``col_elements`` and ``col_terms``.
        col_elements:
            Name of the element identifier column in ``annotation``.
        col_terms:
            Name of the term identifier column in ``annotation``.
        comparisons:
            One :class:`OraComparison` or an iterable of comparisons. A single
            comparison is treated as a batch of size 1.
        options:
            Query-level default options. When omitted, built-in defaults are used.

    Raises:
        ValueError:
            If required annotation columns are missing, if `comparisons` is
            empty or contains invalid items, or if comparison identifiers are
            missing or duplicated in multi-comparison input.

    Returns:
        pl.DataFrame: DataFrame with ORA results and columns:
            - `ComparisonId` (optional): Included for multi-comparison input and
              for single comparisons with an explicit `comparison_id`; omitted
              only for single comparisons without an identifier.
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
    """
    options = OraOptions() if options is None else options

    comparisons_normalized = normalize_comparisons(comparisons)
    validate_comparisons(comparisons_normalized)
    assert len(comparisons_normalized) >= 1
    should_include_comparison_column = len(comparisons_normalized) > 1 or (
        comparisons_normalized[0].comparison_id is not None
    )
    comparisons_resolved = resolve_comparisons(comparisons_normalized, options)

    should_include_fg_members = any(
        _item.options.should_keep_fg_members for _item in comparisons_resolved
    )
    should_include_bg_members = any(
        _item.options.should_keep_bg_members for _item in comparisons_resolved
    )
    df_empty = create_empty_result(
        should_include_comparison=should_include_comparison_column,
        should_include_fg_members=should_include_fg_members,
        should_include_bg_members=should_include_bg_members,
    )

    lf_annotation = (
        select_required_columns(
            annotation,
            col_elements,
            col_terms,
        )
        .rename({col_elements: COL_ELEMENT, col_terms: COL_TERM})
        .unique()
    )
    comparison_ids: list[str] = []
    fg_element_lists: list[list[str]] = []
    bg_element_lists: list[list[str] | None] = []
    config_columns: dict[str, list[Any]] = {COL_COMPARISON: []} | {
        _column: [] for _column in SCHEMA_ORA_CONFIG_FIELDS
    }
    ids_bg_derived: list[str] = []
    for _item in comparisons_resolved:
        comparison_ids.append(_item.comparison_id)
        fg_element_lists.append(list(_item.foreground_elements))
        if _item.background_elements is None:
            ids_bg_derived.append(_item.comparison_id)
            bg_element_lists.append(None)
        else:
            bg_element_lists.append(list(_item.background_elements))

        config_columns[COL_COMPARISON].append(_item.comparison_id)
        config_columns["RulePAdjust"].append(_item.options.rule_p_adjust)
        config_columns["ThrBgHitsMin"].append(_item.options.thr_bg_hits_min)
        config_columns["ThrBgHitsMax"].append(_item.options.thr_bg_hits_max)
        config_columns["ThrFgHitsMin"].append(_item.options.thr_fg_hits_min)
        config_columns["ThrFgHitsMax"].append(_item.options.thr_fg_hits_max)
        config_columns["ThrPValue"].append(_item.options.thr_p_value)
        config_columns["ThrPAdjust"].append(_item.options.thr_p_adjust)
        config_columns["ShouldKeepFgMembers"].append(
            _item.options.should_keep_fg_members
        )
        config_columns["ShouldKeepBgMembers"].append(
            _item.options.should_keep_bg_members
        )

    schema_ora_config = {COL_COMPARISON: pl.String} | SCHEMA_ORA_CONFIG_FIELDS
    lf_comparison = pl.LazyFrame(
        {
            COL_COMPARISON: pl.Series(comparison_ids, dtype=pl.String),
            "ForegroundElements": pl.Series(fg_element_lists),
            "BackgroundElements": pl.Series(bg_element_lists),
        }
        | {
            _column: pl.Series(_values, dtype=schema_ora_config[_column])
            for _column, _values in config_columns.items()
        }
    )
    lf_fg_elements = (
        lf_comparison.explode("ForegroundElements")
        .select(COL_COMPARISON, pl.col("ForegroundElements").alias(COL_ELEMENT))
        .drop_nulls(COL_ELEMENT)
        .unique(COLS_COMPARISON_UNIT)
    )
    lf_bg_explicit = (
        lf_comparison.explode("BackgroundElements")
        .select(COL_COMPARISON, pl.col("BackgroundElements").alias(COL_ELEMENT))
        .drop_nulls(COL_ELEMENT)
        .unique(COLS_COMPARISON_UNIT)
    )
    lf_bg_derived = (
        pl.LazyFrame({COL_COMPARISON: pl.Series(ids_bg_derived, dtype=pl.String)})
        .unique()
        .join(lf_annotation.select(COL_ELEMENT).unique(), how="cross")
        if ids_bg_derived
        else pl.LazyFrame(schema=SCHEMA_COMPARISON).drop(COL_TERM)
    )
    lf_bg_elements = pl.concat([lf_bg_explicit, lf_bg_derived], how="vertical").unique(
        COLS_COMPARISON_UNIT
    )
    lf_fg_in_bg = lf_fg_elements.join(
        lf_bg_elements,
        on=COLS_COMPARISON_UNIT,
        how="inner",
    ).unique(COLS_COMPARISON_UNIT)
    lf_config = lf_comparison.select(COL_COMPARISON, *SCHEMA_ORA_CONFIG_FIELDS)
    df_totals = (
        lf_config.join(
            lf_bg_elements.group_by(COL_COMPARISON).agg(
                BgTotal=pl.len().cast(pl.Int64),
            ),
            on=COL_COMPARISON,
            how="left",
        )
        .join(
            lf_fg_in_bg.group_by(COL_COMPARISON).agg(
                FgTotal=pl.len().cast(pl.Int64),
            ),
            on=COL_COMPARISON,
            how="left",
        )
        .with_columns(
            pl.col("BgTotal").fill_null(0),
            pl.col("FgTotal").fill_null(0),
        )
        .collect()
    )
    df_invalid = df_totals.filter(pl.col("BgTotal").le(0) | pl.col("FgTotal").le(0))
    for _row in df_invalid.iter_rows(named=True):
        logger.warning(
            "Skipping comparison `%s` because BgTotal=%s and FgTotal=%s.",
            _row[COL_COMPARISON],
            _row["BgTotal"],
            _row["FgTotal"],
        )
    df_config_valid = df_totals.filter(
        pl.col("BgTotal").gt(0) & pl.col("FgTotal").gt(0)
    )
    if df_config_valid.height == 0:
        return df_empty

    lf_mappings_marked = (
        lf_annotation.join(lf_bg_elements, on=COL_ELEMENT, how="inner")
        .join(
            lf_fg_in_bg.with_columns(pl.lit(True).alias("_IsFg")),
            on=COLS_COMPARISON_UNIT,
            how="left",
        )
        .with_columns(pl.col("_IsFg").fill_null(False))
    )
    df_ora = (
        lf_mappings_marked.group_by([COL_COMPARISON, COL_TERM])
        .agg(
            BgHits=pl.col(COL_ELEMENT).n_unique().cast(pl.Int64),
            FgHits=pl.col(COL_ELEMENT)
            .filter(pl.col("_IsFg"))
            .n_unique()
            .cast(pl.Int64),
        )
        .join(df_config_valid.lazy(), on=COL_COMPARISON, how="inner")
        .filter(
            pl.col("BgHits").ge(pl.col("ThrBgHitsMin"))
            & pl.when(pl.col("ThrBgHitsMax").is_null())
            .then(True)
            .otherwise(pl.col("BgHits").le(pl.col("ThrBgHitsMax")))
            & pl.col("FgHits").ge(pl.col("ThrFgHitsMin"))
            & pl.when(pl.col("ThrFgHitsMax").is_null())
            .then(True)
            .otherwise(pl.col("FgHits").le(pl.col("ThrFgHitsMax")))
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
        return df_empty

    nd_p_vals = calculate_hypergeometric_right_tail_pvalue(
        fg_hits=df_ora["FgHits"].to_numpy(),
        bg_hits=df_ora["BgHits"].to_numpy(),
        bg_total=df_ora["BgTotal"].to_numpy(),
        fg_total=df_ora["FgTotal"].to_numpy(),
    )
    nd_p_adj = np.empty_like(nd_p_vals)
    arr_comparison = df_ora[COL_COMPARISON].to_numpy()
    arr_rule = df_ora["RulePAdjust"].to_numpy()
    for _comparison_id in dict.fromkeys(arr_comparison.tolist()):
        mask = arr_comparison == _comparison_id
        rule_p_adjust = normalize_p_value_adjustment_mode(arr_rule[mask][0])
        nd_p_adj[mask] = calculate_p_adjustment_array(
            nd_p_vals[mask],
            rule_p_adjust=rule_p_adjust,
        )

    df_ora = df_ora.with_columns(
        pl.Series(name="PValue", values=nd_p_vals, dtype=pl.Float64),
        pl.Series(name="PAdjust", values=nd_p_adj, dtype=pl.Float64),
    ).filter(
        pl.col("PValue").le(pl.col("ThrPValue")),
        pl.col("PAdjust").le(pl.col("ThrPAdjust")),
    )
    if df_ora.height == 0:
        return df_empty

    if should_include_fg_members or should_include_bg_members:
        df_significant = df_ora.select([COL_COMPARISON, COL_TERM]).unique()
        df_members = (
            lf_mappings_marked.join(
                df_significant.lazy(),
                on=[COL_COMPARISON, COL_TERM],
                how="inner",
            )
            .group_by([COL_COMPARISON, COL_TERM])
            .agg(
                pl.col(COL_ELEMENT).unique().alias("BgMembers"),
                pl.col(COL_ELEMENT).filter(pl.col("_IsFg")).unique().alias("FgMembers"),
            )
            .collect()
        )
        df_ora = df_ora.join(
            df_members,
            on=[COL_COMPARISON, COL_TERM],
            how="left",
        ).with_columns(
            pl.when(pl.col("ShouldKeepFgMembers"))
            .then(pl.col("FgMembers"))
            .otherwise(pl.lit(None, dtype=pl.List(pl.String)))
            .alias("FgMembers"),
            pl.when(pl.col("ShouldKeepBgMembers"))
            .then(pl.col("BgMembers"))
            .otherwise(pl.lit(None, dtype=pl.List(pl.String)))
            .alias("BgMembers"),
        )

    sort_cols = [COL_COMPARISON, "PAdjust", "PValue", "FoldEnrichment", COL_TERM]
    sort_desc = [False, False, False, True, False]
    if not should_include_comparison_column:
        sort_cols = sort_cols[1:]
        sort_desc = sort_desc[1:]
    df_ora = df_ora.sort(sort_cols, descending=sort_desc)

    cols_select = []
    if should_include_comparison_column:
        cols_select.append(COL_COMPARISON)
    cols_select.extend(
        [
            COL_TERM,
            "FgHits",
            "FgTotal",
            "BgHits",
            "BgTotal",
            "FoldEnrichment",
            "PValue",
            "PAdjust",
        ]
    )
    if should_include_fg_members:
        cols_select.append("FgMembers")
    if should_include_bg_members:
        cols_select.append("BgMembers")

    return df_ora.select(cols_select)
