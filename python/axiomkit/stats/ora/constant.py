from __future__ import annotations

import polars as pl

COL_COMPARISON = "ComparisonId"
COL_ELEMENT = "ElementId"
COL_TERM = "TermId"

COLS_COMPARISON_UNIT = (COL_COMPARISON, COL_ELEMENT)

SCHEMA_COMPARISON = {
    _key: pl.String
    for _key in [COL_COMPARISON, COL_ELEMENT, COL_TERM]
}

SCHEMA_ORA_CONFIG_FIELDS = {
    "RulePAdjust": pl.String,
    "ThrBgHitsMin": pl.Int64,
    "ThrBgHitsMax": pl.Int64,
    "ThrFgHitsMin": pl.Int64,
    "ThrFgHitsMax": pl.Int64,
    "ThrPValue": pl.Float64,
    "ThrPAdjust": pl.Float64,
    "ShouldKeepFgMembers": pl.Boolean,
    "ShouldKeepBgMembers": pl.Boolean,
}

SCHEMA_ORA_STATS = {
    "FgHits": pl.Int64,
    "FgTotal": pl.Int64,
    "BgHits": pl.Int64,
    "BgTotal": pl.Int64,
    "FoldEnrichment": pl.Float64,
    "PValue": pl.Float64,
    "PAdjust": pl.Float64,
}

FIELDS_RESOLVED_ORA_OPTIONS = (
    "rule_p_adjust",
    "thr_bg_hits_min",
    "thr_bg_hits_max",
    "thr_fg_hits_min",
    "thr_fg_hits_max",
    "thr_p_value",
    "thr_p_adjust",
    "should_keep_fg_members",
    "should_keep_bg_members",
)