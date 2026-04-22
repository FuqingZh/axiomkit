from __future__ import annotations

from typing import Any

import numpy as np
import polars as pl
from loguru import logger

from axiomkit.stats.p_value import (
    calculate_p_adjustment_array,
    normalize_p_value_adjustment_mode,
)

from .constant import (
    COLS_COMPARISON_UNIT,
    SCHEMA_COMPARISON,
    SCHEMA_ORA_CONFIG_FIELDS,
    COL_COMPARISON,
    COL_ELEMENT,
    COL_TERM,
)
from .spec import OraComparison, OraOptions
from .util import (
    calculate_hypergeometric_right_tail_pvalue,
    create_empty_result,
    normalize_comparisons,
    resolve_comparisons,
    select_required_columns,
)


def calculate_ora(
    annotation: pl.DataFrame | pl.LazyFrame,
    col_elements: str = "ElementId",
    col_terms: str = "TermId",
    *,
    comparisons: OraComparison | tuple[OraComparison, ...],
    options: OraOptions | None = None,
) -> pl.DataFrame:
    """
    Over-representation analysis (ORA) using a comparison-aware batch model.

    Args:
        annotation:
            Mapping table with at least ``col_elements`` and ``col_terms``.
        col_elements:
            Name of the element identifier column in ``annotation``.
        col_terms:
            Name of the term identifier column in ``annotation``.
        comparisons:
            One :class:`OraComparison` or a sequence of comparisons. A single
            comparison is treated as a batch of size 1.
        options:
            Query-level default options. When omitted, built-in defaults are used.

    Returns:
        A Polars DataFrame containing ORA results. Single-comparison results omit
        ``ComparisonId``; multi-comparison results include it as the leading column.
    """
    comparisons_normalized = normalize_comparisons(comparisons)
    options = OraOptions() if options is None else options
    comparisons_resolved = resolve_comparisons(comparisons_normalized, options)

    should_include_comparison = len(comparisons_resolved) > 1
    should_include_fg_members = any(
        item.options.should_keep_fg_members for item in comparisons_resolved
    )
    should_include_bg_members = any(
        item.options.should_keep_bg_members for item in comparisons_resolved
    )
    df_empty = create_empty_result(
        should_include_comparison=should_include_comparison,
        should_include_fg_members=should_include_fg_members,
        should_include_bg_members=should_include_bg_members,
    )

    lf_annotation = select_required_columns(
        annotation,
        col_elements,
        col_terms,
    ).rename(
        {
            col_elements: COL_ELEMENT,
            col_terms: COL_TERM
        }
    ).unique()
    comparison_ids: list[str] = []
    fg_element_lists: list[list[str]] = []
    bg_element_lists: list[list[str] | None] = []
    config_columns: dict[str, list[Any]] = {COL_COMPARISON: []} | {
        column: [] for column in SCHEMA_ORA_CONFIG_FIELDS
    }
    ids_bg_derived: list[str] = []
    for item in comparisons_resolved:
        comparison_ids.append(item.comparison_id)
        fg_element_lists.append(list(item.foreground_elements))
        if item.background_elements is None:
            ids_bg_derived.append(item.comparison_id)
            bg_element_lists.append(None)
        else:
            bg_element_lists.append(list(item.background_elements))

        config_columns[COL_COMPARISON].append(item.comparison_id)
        config_columns["RulePAdjust"].append(item.options.rule_p_adjust)
        config_columns["ThrBgHitsMin"].append(item.options.thr_bg_hits_min)
        config_columns["ThrBgHitsMax"].append(item.options.thr_bg_hits_max)
        config_columns["ThrFgHitsMin"].append(item.options.thr_fg_hits_min)
        config_columns["ThrFgHitsMax"].append(item.options.thr_fg_hits_max)
        config_columns["ThrPValue"].append(item.options.thr_p_value)
        config_columns["ThrPAdjust"].append(item.options.thr_p_adjust)
        config_columns["ShouldKeepFgMembers"].append(
            item.options.should_keep_fg_members
        )
        config_columns["ShouldKeepBgMembers"].append(
            item.options.should_keep_bg_members
        )

    schema_ora_config = {COL_COMPARISON: pl.String} | SCHEMA_ORA_CONFIG_FIELDS
    lf_comparison = pl.LazyFrame(
        {
            COL_COMPARISON: pl.Series(comparison_ids, dtype=pl.String),
            "ForegroundElements": pl.Series(fg_element_lists),
            "BackgroundElements": pl.Series(bg_element_lists),
        }
        | {
            column: pl.Series(values, dtype=schema_ora_config[column])
            for column, values in config_columns.items()
        }
    )
    lf_fg_elements = (
        lf_comparison
        .explode("ForegroundElements")
        .select(COL_COMPARISON, pl.col("ForegroundElements").alias(COL_ELEMENT))
        .drop_nulls(COL_ELEMENT)
        .unique(COLS_COMPARISON_UNIT)
    )
    lf_bg_explicit = (
        lf_comparison
        .explode("BackgroundElements")
        .select(COL_COMPARISON, pl.col("BackgroundElements").alias(COL_ELEMENT))
        .drop_nulls(COL_ELEMENT)
        .unique(COLS_COMPARISON_UNIT)
    )
    lf_bg_derived = (
        pl.LazyFrame({COL_COMPARISON: pl.Series(ids_bg_derived, dtype=pl.String)})
        .unique()
        .join(lf_annotation.select(COL_ELEMENT).unique(), how="cross")
        if ids_bg_derived
        else pl.LazyFrame(
            schema=SCHEMA_COMPARISON
        ).drop(COL_TERM)
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
        lf_config
        .join(
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
    df_invalid = df_totals.filter(
        pl.col("BgTotal").le(0) | pl.col("FgTotal").le(0)
    )
    for row in df_invalid.iter_rows(named=True):
        logger.warning(
            "Skipping comparison `%s` because BgTotal=%s and FgTotal=%s.",
            row[COL_COMPARISON],
            row["BgTotal"],
            row["FgTotal"],
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
            FgHits=pl.col(COL_ELEMENT).filter(pl.col("_IsFg")).n_unique().cast(pl.Int64),
        )
        .join(df_config_valid.lazy(), on=COL_COMPARISON, how="inner")
        .filter(
            pl.col("BgHits").ge(pl.col("ThrBgHitsMin")) &
            pl.when(pl.col("ThrBgHitsMax").is_null())
            .then(True)
            .otherwise(pl.col("BgHits").le(pl.col("ThrBgHitsMax"))) &
            pl.col("FgHits").ge(pl.col("ThrFgHitsMin")) &
            pl.when(pl.col("ThrFgHitsMax").is_null())
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
    for comparison_id in dict.fromkeys(arr_comparison.tolist()):
        mask = arr_comparison == comparison_id
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
                pl.col(COL_ELEMENT)
                .filter(pl.col("_IsFg"))
                .unique()
                .alias("FgMembers"),
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
    if not should_include_comparison:
        sort_cols = sort_cols[1:]
        sort_desc = sort_desc[1:]
    df_ora = df_ora.sort(sort_cols, descending=sort_desc)

    cols_select = []
    if should_include_comparison:
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
