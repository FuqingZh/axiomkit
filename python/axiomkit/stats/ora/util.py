from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import polars as pl
from numpy.typing import ArrayLike
from polars._typing import SchemaDict
from scipy import stats
import polars.selectors as pl_sel
from .constant import (
    SCHEMA_ORA_STATS,
    COL_COMPARISON,
    COL_TERM,
)
from .spec import OraComparison, OraOptions, ResolvedOraComparison, ResolvedOraOptions


def calculate_hypergeometric_right_tail_pvalue(
    fg_hits: ArrayLike,
    bg_hits: ArrayLike,
    bg_total: ArrayLike,
    fg_total: ArrayLike,
) -> np.ndarray:
    """Compute hypergeometric right-tail p-values with broadcasting semantics."""
    fg_hits = np.asarray(fg_hits, dtype=np.int64)
    bg_hits = np.asarray(bg_hits, dtype=np.int64)
    bg_total = np.asarray(bg_total, dtype=np.int64)
    fg_total = np.asarray(fg_total, dtype=np.int64)
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
            bg_total[is_valid_mask],
            bg_hits[is_valid_mask],
            fg_total[is_valid_mask],
        )
    return nd_p_values


def create_empty_result(
    *,
    should_include_comparison: bool,
    should_include_fg_members: bool,
    should_include_bg_members: bool,
) -> pl.DataFrame:
    schema: SchemaDict = {}
    if should_include_comparison:
        schema[COL_COMPARISON] = pl.String
    schema[COL_TERM] = pl.String
    schema.update(SCHEMA_ORA_STATS)
    if should_include_fg_members:
        schema["FgMembers"] = pl.List(pl.String)
    if should_include_bg_members:
        schema["BgMembers"] = pl.List(pl.String)
    return pl.DataFrame(schema=schema)


def normalize_comparisons(
    comparisons: OraComparison | Sequence[OraComparison],
) -> tuple[OraComparison, ...]:
    items: tuple[OraComparison, ...]
    match comparisons:
        case OraComparison():
            items = (comparisons,)
        case Sequence():
            items = tuple(comparisons)
    if len(items) == 0:
        raise ValueError("Arg `comparisons` must not be empty.")
    if any(not isinstance(_item, OraComparison) for _item in items):
        raise ValueError(
            "Arg `comparisons` must be an OraComparison or a sequence of OraComparison items."
        )
    return items


def resolve_comparisons(
    comparisons: tuple[OraComparison, ...],
    options: OraOptions,
) -> tuple[ResolvedOraComparison, ...]:
    num_comparisons = len(comparisons)
    ids_seen: set[str] = set()
    items_resolved: list[ResolvedOraComparison] = []
    for _comparison in comparisons:
        comparison_id = _comparison.comparison_id
        if num_comparisons == 1:
            comparison_id = comparison_id or "default"
        elif comparison_id is None:
            raise ValueError(
                "Arg `comparison_id` is required for each comparison when multiple comparisons are provided."
            )
        
        if comparison_id in ids_seen:
            raise ValueError("Duplicate comparison ids are not allowed.")
        ids_seen.add(comparison_id)

        option_override = _comparison.option_override
        if (
            option_override is not None
            and "background_elements" in option_override.model_fields_set
        ):
            background_override = option_override.background_elements
            background_elements = (
                None if background_override is None else frozenset(background_override)
            )
        elif options.background_elements is not None:
            background_elements = frozenset(options.background_elements)
        else:
            background_elements = None

        items_resolved.append(
            ResolvedOraComparison(
                comparison_id=comparison_id,
                foreground_elements=frozenset(_comparison.foreground_elements),
                background_elements=background_elements,
                options=ResolvedOraOptions.from_options(
                    base=options, override=option_override
                )
            )
        )

    return tuple(items_resolved)


def select_required_columns(
    annotation: pl.DataFrame | pl.LazyFrame,
    *cols: str
) -> pl.LazyFrame:
    lf = annotation.lazy() if isinstance(annotation, pl.DataFrame) else annotation
    schema = dict(lf.collect_schema())
    if missing := set(cols) - set(schema):
        raise ValueError(
            "Input `annotation` is missing required columns: "
            + ", ".join(sorted(missing))
            + "."
        )
    
    return lf.select(pl_sel.by_name(cols).cast(pl.String))
