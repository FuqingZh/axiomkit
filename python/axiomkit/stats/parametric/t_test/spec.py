from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, Self

import numpy as np
import polars as pl

from ...p_value import PValueAdjustmentMode
from ..comparison import ParametricComparison, ParametricComparisonKind


@dataclass(frozen=True, slots=True)
class TTestContext:
    lf: pl.LazyFrame
    schema_input: dict[str, pl.DataType]
    schema_result: dict[str, pl.DataType]
    rule_p_adjust: PValueAdjustmentMode | None


class AlternativeHypothesisMode(StrEnum):
    TWO_SIDED = "two-sided"
    LESS = "less"
    GREATER = "greater"


AlternativeHypothesisType = Literal["two-sided", "less", "greater"]


@dataclass(frozen=True, slots=True)
class TStatisticsResult:
    mean_diff: np.ndarray
    t_statistic: np.ndarray
    degrees_freedom: np.ndarray


@dataclass(frozen=True, slots=True)
class ContrastPlan:
    comparison_id_values: tuple[str | None, ...]
    contrast_ids: tuple[tuple[str, str], ...]
    group_test_values: tuple[str, ...]
    group_ref_values: tuple[str, ...]
    group_used: tuple[str, ...]

    @property
    def has_comparison_id(self) -> bool:
        return any(_item is not None for _item in self.comparison_id_values)

    @classmethod
    def from_inputs(
        cls,
        contrasts: ParametricComparison | Sequence[ParametricComparison],
        *,
        comparison_kind: ParametricComparisonKind,
    ) -> Self:
        items_contrast: Sequence[ParametricComparison]
        if isinstance(contrasts, ParametricComparison):
            items_contrast = [contrasts]
        elif isinstance(contrasts, Sequence) and not isinstance(contrasts, str):
            items_contrast = contrasts
        else:
            raise ValueError(
                "Arg `comparisons` must be a ParametricComparison or a sequence of ParametricComparison items."
            )

        if any(
            not isinstance(_item, ParametricComparison)
            for _item in items_contrast
        ):
            raise ValueError(
                "Arg `comparisons` must be a ParametricComparison or a sequence of ParametricComparison items."
            )

        contrasts_seen = set()
        comparison_id_values: list[str | None] = []
        contrast_ids: list[tuple[str, str]] = []
        group_test_values: list[str] = []
        group_ref_values: list[str] = []
        group_used: list[str] = []
        for item_contrast in items_contrast:
            if item_contrast.kind != comparison_kind:
                raise ValueError(
                    f"Arg `comparisons` must contain `{comparison_kind.value}` items."
                )
            assert item_contrast.group_test is not None
            assert item_contrast.group_ref is not None
            comparison_id = item_contrast.comparison_id
            group_test = item_contrast.group_test
            group_ref = item_contrast.group_ref

            pair_key = (group_test, group_ref)
            contrast_key = (comparison_id, *pair_key)
            if contrast_key in contrasts_seen:
                raise ValueError("Duplicate contrast pairs are not allowed.")
            contrasts_seen.add(contrast_key)
            comparison_id_values.append(comparison_id)
            contrast_ids.append(pair_key)
            group_test_values.append(group_test)
            group_ref_values.append(group_ref)
            group_used.extend(pair_key)

        return cls(
            comparison_id_values=tuple(comparison_id_values),
            contrast_ids=tuple(contrast_ids),
            group_test_values=tuple(group_test_values),
            group_ref_values=tuple(group_ref_values),
            group_used=tuple(group_used),
        )
