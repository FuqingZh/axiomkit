from __future__ import annotations

import json
import statistics
import time
from pathlib import Path

import polars as pl
import pytest

from axiomkit.stats import (
    TTestContrast,
    calculate_anova_one_way,
    calculate_t_test_two_sample,
)

_BASELINE_PATH = Path(__file__).with_name(
    "test_stats_parametric_performance_baseline.json"
)
_REGRESSION_FACTOR = 3.0
_WARMUP_RUNS = 1
_MEASURE_RUNS = 5
_MEASURE_RUNS_EXTREME = 3


def _load_baseline() -> dict[str, dict[str, float]]:
    return json.loads(_BASELINE_PATH.read_text())


def _make_two_sample_df(n_features: int, n_replicates: int) -> pl.DataFrame:
    rows: list[tuple[str, str, float]] = []
    for idx_feature in range(n_features):
        feature_id = f"f{idx_feature}"
        for group, offset in (("A", 1.0), ("B", 2.0), ("C", 3.0)):
            for idx_replicate in range(n_replicates):
                value = offset + ((idx_feature + idx_replicate) % 7) * 0.1
                rows.append((feature_id, group, value))

    return pl.DataFrame(
        rows,
        schema=["FeatureId", "Group", "Value"],
        orient="row",
    )


def _make_one_way_df(n_features: int, n_replicates: int) -> pl.DataFrame:
    rows: list[tuple[str, str, float]] = []
    for idx_feature in range(n_features):
        feature_id = f"f{idx_feature}"
        for group, offset in (("A", 1.0), ("B", 2.0), ("C", 3.0)):
            for idx_replicate in range(n_replicates):
                value = offset + ((idx_feature + idx_replicate) % 5) * 0.2
                rows.append((feature_id, group, value))

    return pl.DataFrame(
        rows,
        schema=["FeatureId", "Group", "Value"],
        orient="row",
    )


def _measure_runtime_seconds(fn, /, *args, measure_runs: int = _MEASURE_RUNS, **kwargs) -> float:
    for _ in range(_WARMUP_RUNS):
        fn(*args, **kwargs)

    elapsed_seconds: list[float] = []
    for _ in range(measure_runs):
        t0 = time.perf_counter()
        fn(*args, **kwargs)
        elapsed_seconds.append(time.perf_counter() - t0)

    return statistics.median(elapsed_seconds)


@pytest.mark.parametrize(
    ("case_name", "n_features", "n_replicates", "measure_runs"),
    [
        ("small", 100, 4, _MEASURE_RUNS),
        ("large", 2_000, 4, _MEASURE_RUNS),
        ("xlarge", 10_000, 4, _MEASURE_RUNS),
        ("extreme", 50_000, 4, _MEASURE_RUNS_EXTREME),
    ],
)
def test_calculate_t_test_two_sample_performance_regression(
    case_name: str,
    n_features: int,
    n_replicates: int,
    measure_runs: int,
) -> None:
    df_values = _make_two_sample_df(n_features, n_replicates)
    elapsed_seconds = _measure_runtime_seconds(
        calculate_t_test_two_sample,
        df_values,
        measure_runs=measure_runs,
        col_feature="FeatureId",
        contrasts=[
            TTestContrast(group_test="B", group_ref="A"),
            TTestContrast(group_test="C", group_ref="A"),
        ],
        rule_p_adjust="bh",
    )

    baseline_seconds = _load_baseline()["calculate_t_test_two_sample"][case_name]
    assert elapsed_seconds <= (baseline_seconds * _REGRESSION_FACTOR), (
        "Performance regression detected for calculate_t_test_two_sample "
        f"({case_name}): current={elapsed_seconds:.6f}s, "
        f"baseline={baseline_seconds:.6f}s, "
        f"limit={(baseline_seconds * _REGRESSION_FACTOR):.6f}s"
    )


@pytest.mark.parametrize(
    ("case_name", "n_features", "n_replicates", "measure_runs"),
    [
        ("small", 100, 4, _MEASURE_RUNS),
        ("large", 2_000, 4, _MEASURE_RUNS),
        ("xlarge", 10_000, 4, _MEASURE_RUNS),
        ("extreme", 50_000, 4, _MEASURE_RUNS_EXTREME),
    ],
)
def test_calculate_anova_one_way_performance_regression(
    case_name: str,
    n_features: int,
    n_replicates: int,
    measure_runs: int,
) -> None:
    df_values = _make_one_way_df(n_features, n_replicates)
    elapsed_seconds = _measure_runtime_seconds(
        calculate_anova_one_way,
        df_values,
        measure_runs=measure_runs,
        col_feature="FeatureId",
        rule_p_adjust="bh",
    )

    baseline_seconds = _load_baseline()["calculate_anova_one_way"][case_name]
    assert elapsed_seconds <= (baseline_seconds * _REGRESSION_FACTOR), (
        "Performance regression detected for calculate_anova_one_way "
        f"({case_name}): current={elapsed_seconds:.6f}s, "
        f"baseline={baseline_seconds:.6f}s, "
        f"limit={(baseline_seconds * _REGRESSION_FACTOR):.6f}s"
    )
