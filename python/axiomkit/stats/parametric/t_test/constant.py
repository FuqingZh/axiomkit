import polars as pl
from polars._typing import SchemaDict

COL_VAR_TEST = "_VarGroupTest"
COL_VAR_REF = "_VarGroupRef"
COLS_STATS_TWO_SAMPLE_NUMERIC = (
    "MeanGroupTest",
    "MeanGroupRef",
    COL_VAR_TEST,
    COL_VAR_REF,
    "NGroupTest",
    "NGroupRef",
)

SCHEMA_T_TEST_STATS: SchemaDict = {
    "MeanDiff": pl.Float64,
    "TStatistic": pl.Float64,
    "DegreesFreedom": pl.Float64,
    "PValue": pl.Float64,
    "PAdjust": pl.Float64,
}
SCHEMA_T_TEST_TWO_SAMPLE_RESULT: SchemaDict = {
    "ContrastId": pl.Array(pl.String, 2),
    "GroupTest": pl.String,
    "GroupRef": pl.String,
    "NGroupTest": pl.Int64,
    "NGroupRef": pl.Int64,
    "MeanGroupTest": pl.Float64,
    "MeanGroupRef": pl.Float64,
} | SCHEMA_T_TEST_STATS

SCHEMA_T_TEST_ONE_SAMPLE_RESULT: SchemaDict = {
    "N": pl.Int64,
    "Mean": pl.Float64,
    "PopMean": pl.Float64,
} | SCHEMA_T_TEST_STATS
