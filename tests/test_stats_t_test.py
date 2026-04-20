from __future__ import annotations

import math

import polars as pl
import pytest
from scipy import stats

from axiomkit.stats import (
    ContrastSpec,
    calculate_t_test_one_sample,
    calculate_t_test_paired,
    calculate_t_test_two_sample,
)


def test_calculate_t_test_one_sample_matches_scipy() -> None:
    df_values = pl.DataFrame(
        {
            "Value": [1.0, 2.0, None, 4.0],
        }
    )

    df_result = calculate_t_test_one_sample(
        df_values,
        popmean=1.5,
    )

    assert df_result.columns == [
        "N",
        "Mean",
        "PopMean",
        "MeanDiff",
        "TStatistic",
        "DegreesFreedom",
        "PValue",
        "PAdjust",
    ]
    assert df_result.height == 1

    row = df_result.row(0, named=True)
    expected = stats.ttest_1samp([1.0, 2.0, 4.0], popmean=1.5)

    assert row["N"] == 3
    assert row["Mean"] == pytest.approx((1.0 + 2.0 + 4.0) / 3.0)
    assert row["PopMean"] == pytest.approx(1.5)
    assert row["MeanDiff"] == pytest.approx(((1.0 + 2.0 + 4.0) / 3.0) - 1.5)
    assert row["TStatistic"] == pytest.approx(expected.statistic)
    assert row["DegreesFreedom"] == pytest.approx(expected.df)
    assert row["PValue"] == pytest.approx(expected.pvalue)
    assert row["PAdjust"] == pytest.approx(expected.pvalue)


def test_calculate_t_test_one_sample_supports_feature_and_p_adjust() -> None:
    df_values = pl.DataFrame(
        {
            "FeatureId": ["f1", "f1", "f1", "f2", "f2", "f2"],
            "Value": [1.0, 2.0, 4.0, 8.0, 10.0, 12.0],
        }
    )

    df_result = calculate_t_test_one_sample(
        df_values.lazy(),
        col_feature="FeatureId",
        popmean=2.0,
        rule_p_adjust="bh",
    )

    assert df_result.columns == [
        "FeatureId",
        "N",
        "Mean",
        "PopMean",
        "MeanDiff",
        "TStatistic",
        "DegreesFreedom",
        "PValue",
        "PAdjust",
    ]
    assert df_result["FeatureId"].to_list() == ["f1", "f2"]

    row_f1 = df_result.row(0, named=True)
    row_f2 = df_result.row(1, named=True)
    expected_f1 = stats.ttest_1samp([1.0, 2.0, 4.0], popmean=2.0)
    expected_f2 = stats.ttest_1samp([8.0, 10.0, 12.0], popmean=2.0)

    assert row_f1["PValue"] == pytest.approx(expected_f1.pvalue)
    assert row_f1["PAdjust"] >= row_f1["PValue"]
    assert row_f2["PValue"] == pytest.approx(expected_f2.pvalue)
    assert row_f2["PAdjust"] >= row_f2["PValue"]


def test_calculate_t_test_one_sample_keeps_insufficient_rows_with_nan_stats() -> None:
    df_values = pl.DataFrame(
        {
            "FeatureId": ["f1", "f2", "f2"],
            "Value": [1.0, 10.0, 12.0],
        }
    )

    df_result = calculate_t_test_one_sample(
        df_values,
        col_feature="FeatureId",
        popmean=2.0,
        rule_p_adjust="bh",
    )

    row_f1 = df_result.row(0, named=True)
    row_f2 = df_result.row(1, named=True)
    assert math.isnan(row_f1["TStatistic"])
    assert math.isnan(row_f1["DegreesFreedom"])
    assert math.isnan(row_f1["PValue"])
    assert math.isnan(row_f1["PAdjust"])
    assert not math.isnan(row_f2["TStatistic"])
    assert row_f2["DegreesFreedom"] == pytest.approx(1.0)


def test_calculate_t_test_paired_matches_scipy_for_single_contrast() -> None:
    df_values = pl.DataFrame(
        {
            "PairId": ["p1", "p1", "p2", "p2", "p3", "p3"],
            "Group": ["A", "B", "A", "B", "A", "B"],
            "Value": [1.0, 2.0, 3.0, 5.0, 4.0, 6.0],
        }
    )

    df_result = calculate_t_test_paired(
        df_values,
        col_pair="PairId",
        contrasts=ContrastSpec(group_test="B", group_ref="A"),
    )

    assert df_result.columns == [
        "ContrastId",
        "GroupTest",
        "GroupRef",
        "NGroupTest",
        "NGroupRef",
        "MeanGroupTest",
        "MeanGroupRef",
        "MeanDiff",
        "TStatistic",
        "DegreesFreedom",
        "PValue",
        "PAdjust",
    ]
    row = df_result.row(0, named=True)
    expected = stats.ttest_rel([2.0, 5.0, 6.0], [1.0, 3.0, 4.0])

    assert row["ContrastId"] == ["B", "A"]
    assert row["GroupTest"] == "B"
    assert row["GroupRef"] == "A"
    assert row["NGroupTest"] == 3
    assert row["NGroupRef"] == 3
    assert row["MeanGroupTest"] == pytest.approx((2.0 + 5.0 + 6.0) / 3.0)
    assert row["MeanGroupRef"] == pytest.approx((1.0 + 3.0 + 4.0) / 3.0)
    assert row["MeanDiff"] == pytest.approx(((2.0 - 1.0) + (5.0 - 3.0) + (6.0 - 4.0)) / 3.0)
    assert row["TStatistic"] == pytest.approx(expected.statistic)
    assert row["DegreesFreedom"] == pytest.approx(expected.df)
    assert row["PValue"] == pytest.approx(expected.pvalue)
    assert row["PAdjust"] == pytest.approx(expected.pvalue)


def test_calculate_t_test_paired_supports_feature_and_multiple_contrasts() -> None:
    df_values = pl.DataFrame(
        {
            "FeatureId": [
                "f1", "f1", "f1", "f1", "f1", "f1", "f1", "f1", "f1",
                "f2", "f2", "f2", "f2", "f2", "f2", "f2", "f2", "f2",
            ],
            "PairId": [
                "p1", "p1", "p1", "p2", "p2", "p2", "p3", "p3", "p3",
                "p1", "p1", "p1", "p2", "p2", "p2", "p3", "p3", "p3",
            ],
            "Group": [
                "A", "B", "C", "A", "B", "C", "A", "B", "C",
                "A", "B", "C", "A", "B", "C", "A", "B", "C",
            ],
            "Value": [
                1.0, 2.0, 3.0, 2.0, 4.0, 5.0, 4.0, 7.0, 8.0,
                2.0, 5.0, 6.0, 4.0, 7.0, 9.0, 6.0, 10.0, 12.0,
            ],
        }
    )

    df_result = calculate_t_test_paired(
        df_values.lazy(),
        col_pair="PairId",
        col_feature="FeatureId",
        contrasts=[
            ContrastSpec(group_test="B", group_ref="A"),
            ContrastSpec(group_test="C", group_ref="A"),
        ],
        rule_p_adjust="bonferroni",
    )

    assert df_result.columns == [
        "FeatureId",
        "ContrastId",
        "GroupTest",
        "GroupRef",
        "NGroupTest",
        "NGroupRef",
        "MeanGroupTest",
        "MeanGroupRef",
        "MeanDiff",
        "TStatistic",
        "DegreesFreedom",
        "PValue",
        "PAdjust",
    ]
    assert df_result.select("FeatureId", "ContrastId").rows() == [
        ("f1", ["B", "A"]),
        ("f1", ["C", "A"]),
        ("f2", ["B", "A"]),
        ("f2", ["C", "A"]),
    ]


def test_calculate_t_test_paired_supports_rule_alternative() -> None:
    df_values = pl.DataFrame(
        {
            "PairId": ["p1", "p1", "p2", "p2", "p3", "p3"],
            "Group": ["A", "B", "A", "B", "A", "B"],
            "Value": [2.0, 1.0, 3.0, 1.0, 4.0, 1.0],
        }
    )

    df_result = calculate_t_test_paired(
        df_values,
        col_pair="PairId",
        contrasts=ContrastSpec(group_test="B", group_ref="A"),
        rule_alternative="less",
    )

    expected = stats.ttest_rel([1.0, 1.0, 1.0], [2.0, 3.0, 4.0], alternative="less")
    row = df_result.row(0, named=True)
    assert row["PValue"] == pytest.approx(expected.pvalue)


def test_calculate_t_test_paired_rejects_missing_pairs() -> None:
    df_values = pl.DataFrame(
        {
            "PairId": ["p1", "p1", "p2"],
            "Group": ["A", "B", "A"],
            "Value": [1.0, 2.0, 3.0],
        }
    )

    with pytest.raises(ValueError, match="requires exactly one test row and one ref row"):
        calculate_t_test_paired(
            df_values,
            col_pair="PairId",
            contrasts=ContrastSpec(group_test="B", group_ref="A"),
        )


def test_calculate_t_test_paired_rejects_duplicate_pairs() -> None:
    df_values = pl.DataFrame(
        {
            "PairId": ["p1", "p1", "p1", "p2", "p2"],
            "Group": ["A", "B", "B", "A", "B"],
            "Value": [1.0, 2.0, 3.0, 4.0, 5.0],
        }
    )

    with pytest.raises(ValueError, match="does not allow duplicate rows"):
        calculate_t_test_paired(
            df_values,
            col_pair="PairId",
            contrasts=ContrastSpec(group_test="B", group_ref="A"),
        )


def test_calculate_t_test_two_sample_matches_scipy_for_single_contrast() -> None:
    df_values = pl.DataFrame(
        {
            "Group": ["A", "A", "A", "B", "B", "B", "B"],
            "Value": [1.0, 2.0, None, 4.0, 5.0, 6.0, 7.0],
        }
    )

    df_result = calculate_t_test_two_sample(
        df_values,
        contrasts=ContrastSpec(group_test="A", group_ref="B"),
    )

    assert df_result.columns == [
        "ContrastId",
        "GroupTest",
        "GroupRef",
        "NGroupTest",
        "NGroupRef",
        "MeanGroupTest",
        "MeanGroupRef",
        "MeanDiff",
        "TStatistic",
        "DegreesFreedom",
        "PValue",
        "PAdjust",
    ]
    assert df_result.height == 1

    row = df_result.row(0, named=True)
    expected = stats.ttest_ind([1.0, 2.0], [4.0, 5.0, 6.0, 7.0], equal_var=False)

    assert row["ContrastId"] == ["A", "B"]
    assert row["GroupTest"] == "A"
    assert row["GroupRef"] == "B"
    assert row["NGroupTest"] == 2
    assert row["NGroupRef"] == 4
    assert row["MeanGroupTest"] == pytest.approx(1.5)
    assert row["MeanGroupRef"] == pytest.approx(5.5)
    assert row["MeanDiff"] == pytest.approx(-4.0)
    assert row["TStatistic"] == pytest.approx(expected.statistic)
    assert row["DegreesFreedom"] == pytest.approx(expected.df)
    assert row["PValue"] == pytest.approx(expected.pvalue)
    assert row["PAdjust"] == pytest.approx(expected.pvalue)


def test_calculate_t_test_two_sample_supports_feature_and_multiple_contrasts() -> None:
    df_values = pl.DataFrame(
        {
            "FeatureId": [
                "f1",
                "f1",
                "f1",
                "f1",
                "f1",
                "f1",
                "f2",
                "f2",
                "f2",
                "f2",
                "f2",
                "f2",
            ],
            "Group": [
                "A",
                "A",
                "B",
                "B",
                "C",
                "C",
                "A",
                "A",
                "B",
                "B",
                "C",
                "C",
            ],
            "Value": [1.0, 2.0, 5.0, 6.0, 3.0, 4.0, 4.0, 4.0, 6.0, 8.0, 9.0, 11.0],
        }
    )

    df_result = calculate_t_test_two_sample(
        df_values,
        col_feature="FeatureId",
        contrasts=[
            ContrastSpec(group_test="B", group_ref="A"),
            ContrastSpec(group_test="C", group_ref="A"),
        ],
        rule_p_adjust="bonferroni",
    )

    assert df_result.columns == [
        "FeatureId",
        "ContrastId",
        "GroupTest",
        "GroupRef",
        "NGroupTest",
        "NGroupRef",
        "MeanGroupTest",
        "MeanGroupRef",
        "MeanDiff",
        "TStatistic",
        "DegreesFreedom",
        "PValue",
        "PAdjust",
    ]
    assert df_result.select("FeatureId", "ContrastId").rows() == [
        ("f1", ["B", "A"]),
        ("f1", ["C", "A"]),
        ("f2", ["B", "A"]),
        ("f2", ["C", "A"]),
    ]

    row_f1 = df_result.row(0, named=True)
    row_f2 = df_result.row(3, named=True)
    expected_f1 = stats.ttest_ind([5.0, 6.0], [1.0, 2.0], equal_var=False)
    expected_f2 = stats.ttest_ind([9.0, 11.0], [4.0, 4.0], equal_var=False)

    assert row_f1["PValue"] == pytest.approx(expected_f1.pvalue)
    assert row_f1["PAdjust"] >= row_f1["PValue"]
    assert row_f2["PValue"] == pytest.approx(expected_f2.pvalue)
    assert row_f2["PAdjust"] >= row_f2["PValue"]


def test_calculate_t_test_two_sample_supports_comparison_and_validity_gate() -> None:
    df_values = pl.DataFrame(
        {
            "Comparison": [
                "B_vs_A",
                "B_vs_A",
                "B_vs_A",
                "B_vs_A",
                "C_vs_A",
                "C_vs_A",
                "C_vs_A",
                "C_vs_A",
                "C_vs_A",
                "C_vs_A",
                "C_vs_A",
                "C_vs_A",
            ],
            "FeatureId": [
                "T1",
                "T1",
                "T1",
                "T1",
                "T1",
                "T1",
                "T1",
                "T1",
                "T2",
                "T2",
                "T2",
                "T2",
            ],
            "Group": [
                "A",
                "A",
                "B",
                "B",
                "A",
                "A",
                "C",
                "C",
                "A",
                "A",
                "C",
                "C",
            ],
            "Value": [10.0, 11.0, 20.0, 22.0, 10.0, 10.0, 0.0, 0.0, 4.0, 5.0, 7.0, 9.0],
            "IsValid": [
                True,
                True,
                True,
                True,
                False,
                False,
                False,
                False,
                True,
                True,
                True,
                True,
            ],
        }
    )

    df_result = calculate_t_test_two_sample(
        df_values,
        col_feature="FeatureId",
        col_comparison="Comparison",
        col_is_valid="IsValid",
        contrasts=[
            ContrastSpec(group_test="B", group_ref="A"),
            ContrastSpec(group_test="C", group_ref="A"),
        ],
        rule_p_adjust="bonferroni",
    )

    assert df_result.columns == [
        "Comparison",
        "FeatureId",
        "ContrastId",
        "GroupTest",
        "GroupRef",
        "NGroupTest",
        "NGroupRef",
        "MeanGroupTest",
        "MeanGroupRef",
        "MeanDiff",
        "TStatistic",
        "DegreesFreedom",
        "PValue",
        "PAdjust",
    ]
    assert (
        df_result.select("Comparison", "FeatureId", "ContrastId")
        .sort(["Comparison", "FeatureId"])
        .rows()
    ) == [
        ("B_vs_A", "T1", ["B", "A"]),
        ("C_vs_A", "T2", ["C", "A"]),
    ]

    row_b = (
        df_result.filter(pl.col("Comparison") == "B_vs_A").row(0, named=True)
    )
    row_c = (
        df_result.filter(pl.col("Comparison") == "C_vs_A").row(0, named=True)
    )
    expected_b = stats.ttest_ind([20.0, 22.0], [10.0, 11.0], equal_var=False)
    expected_c = stats.ttest_ind([7.0, 9.0], [4.0, 5.0], equal_var=False)

    assert row_b["PValue"] == pytest.approx(expected_b.pvalue)
    assert row_c["PValue"] == pytest.approx(expected_c.pvalue)


def test_calculate_t_test_two_sample_supports_comparison_without_validity_gate() -> None:
    df_values = pl.DataFrame(
        {
            "Comparison": ["cmp1", "cmp1", "cmp1", "cmp1", "cmp2", "cmp2", "cmp2", "cmp2"],
            "FeatureId": ["f1", "f1", "f1", "f1", "f1", "f1", "f1", "f1"],
            "Group": ["A", "A", "B", "B", "A", "A", "B", "B"],
            "Value": [1.0, 2.0, 4.0, 5.0, 2.0, 3.0, 6.0, 8.0],
        }
    )

    df_result = calculate_t_test_two_sample(
        df_values,
        col_feature="FeatureId",
        col_comparison="Comparison",
        contrasts=ContrastSpec(group_test="B", group_ref="A"),
    )

    assert df_result.select("Comparison", "FeatureId", "ContrastId").rows() == [
        ("cmp1", "f1", ["B", "A"]),
        ("cmp2", "f1", ["B", "A"]),
    ]


def test_calculate_t_test_two_sample_builds_contrast_id_as_pair() -> None:
    df_values = pl.DataFrame(
        {
            "Group": ["case", "case", "ctrl", "ctrl"],
            "Value": [1.0, 2.0, 4.0, 5.0],
        }
    )

    df_result = calculate_t_test_two_sample(
        df_values,
        contrasts=ContrastSpec(group_test="case", group_ref="ctrl"),
    )

    assert df_result["ContrastId"].to_list() == [["case", "ctrl"]]


def test_calculate_t_test_two_sample_normalizes_group_values_to_strings() -> None:
    df_values = pl.DataFrame(
        {
            "Group": [0, 0, 1, 1],
            "Value": [1.0, 2.0, 4.0, 5.0],
        }
    )

    df_result = calculate_t_test_two_sample(
        df_values,
        contrasts=ContrastSpec(group_test=0, group_ref=1),
    )

    row = df_result.row(0, named=True)
    assert row["ContrastId"] == ["0", "1"]
    assert row["GroupTest"] == "0"
    assert row["GroupRef"] == "1"


def test_calculate_t_test_two_sample_supports_rule_alternative_and_equal_variance() -> None:
    df_values = pl.DataFrame(
        {
            "Group": ["A", "A", "B", "B"],
            "Value": [1.0, 3.0, 2.0, 4.0],
        }
    )

    df_result = calculate_t_test_two_sample(
        df_values.lazy(),
        contrasts=ContrastSpec(group_test="A", group_ref="B"),
        rule_alternative="less",
        should_assume_equal_variance=True,
    )

    expected = stats.ttest_ind([1.0, 3.0], [2.0, 4.0], alternative="less", equal_var=True)
    row = df_result.row(0, named=True)
    assert row["DegreesFreedom"] == pytest.approx(expected.df)
    assert row["PValue"] == pytest.approx(expected.pvalue)


def test_calculate_t_test_two_sample_keeps_insufficient_rows_with_nan_stats() -> None:
    df_values = pl.DataFrame(
        {
            "FeatureId": ["f1", "f1", "f1", "f2", "f2", "f2"],
            "Group": ["A", "A", "B", "A", "B", "B"],
            "Value": [1.0, 2.0, 3.0, 10.0, 20.0, 30.0],
        }
    )

    df_result = calculate_t_test_two_sample(
        df_values,
        col_feature="FeatureId",
        contrasts=[ContrastSpec(group_test="A", group_ref="B")],
        rule_p_adjust="bh",
    )

    row_f1 = df_result.row(0, named=True)
    row_f2 = df_result.row(1, named=True)
    assert math.isnan(row_f1["TStatistic"])
    assert math.isnan(row_f1["DegreesFreedom"])
    assert math.isnan(row_f1["PValue"])
    assert math.isnan(row_f1["PAdjust"])
    assert math.isnan(row_f2["TStatistic"])
    assert math.isnan(row_f2["DegreesFreedom"])
    assert math.isnan(row_f2["PValue"])
    assert math.isnan(row_f2["PAdjust"])


def test_calculate_t_test_two_sample_rejects_invalid_contrast_inputs() -> None:
    df_values = pl.DataFrame({"Group": ["A", "B"], "Value": [1.0, 2.0]})

    with pytest.raises(ValueError, match="ContrastSpec or a sequence"):
        calculate_t_test_two_sample(
            df_values,
            contrasts="A_vs_B",
        )

    with pytest.raises(ValueError, match="ContrastSpec or a sequence"):
        calculate_t_test_two_sample(
            df_values,
            contrasts=[ContrastSpec(group_test="A", group_ref="B"), "bad"],
        )

    with pytest.raises(ValueError, match="Duplicate contrast pairs"):
        calculate_t_test_two_sample(
            df_values,
            contrasts=[
                ContrastSpec(group_test="A", group_ref="B"),
                ContrastSpec(group_test="A", group_ref="B"),
            ],
        )

    with pytest.raises(ValueError, match="must be different from `group_ref`"):
        calculate_t_test_two_sample(
            df_values,
            contrasts=[ContrastSpec(group_test="A", group_ref="A")],
        )

    with pytest.raises(ValueError, match="Invalid p-value adjustment mode"):
        calculate_t_test_two_sample(
            df_values,
            contrasts=ContrastSpec(group_test="A", group_ref="B"),
            rule_p_adjust="bad",
        )

    with pytest.raises(ValueError, match="`col_feature` is required"):
        calculate_t_test_two_sample(
            df_values,
            contrasts=ContrastSpec(group_test="A", group_ref="B"),
            col_comparison="Comparison",
        )

    with pytest.raises(ValueError, match="`col_comparison` must be different"):
        calculate_t_test_two_sample(
            df_values.rename({"Group": "Comparison"}),
            contrasts=ContrastSpec(group_test="A", group_ref="B"),
            col_group="Comparison",
            col_comparison="Comparison",
            col_feature="FeatureId",
        )

    df_validity = pl.DataFrame(
        {
            "Comparison": ["cmp1", "cmp1", "cmp1", "cmp1"],
            "FeatureId": ["f1", "f1", "f1", "f1"],
            "Group": ["A", "A", "B", "B"],
            "Value": [1.0, 2.0, 3.0, 4.0],
            "IsValid": [True, False, True, False],
        }
    )
    with pytest.raises(ValueError, match="must be consistent within each"):
        calculate_t_test_two_sample(
            df_validity,
            contrasts=ContrastSpec(group_test="B", group_ref="A"),
            col_feature="FeatureId",
            col_comparison="Comparison",
            col_is_valid="IsValid",
        )


def test_calculate_t_test_two_sample_rejects_non_numeric_value_column() -> None:
    df_values = pl.DataFrame(
        {
            "Group": ["A", "A", "B", "B"],
            "Value": ["low", "high", "higher", "highest"],
        }
    )

    with pytest.raises(
        pl.exceptions.InvalidOperationError,
        match=r"conversion from `str` to `f64` failed",
    ):
        calculate_t_test_two_sample(
            df_values,
            contrasts=ContrastSpec(group_test="A", group_ref="B"),
        )
