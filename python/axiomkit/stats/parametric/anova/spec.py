from collections.abc import Sequence
from dataclasses import dataclass
from typing import Self

import numpy as np

from ..comparison import ParametricComparison, ParametricComparisonKind


@dataclass(frozen=True, slots=True)
class OneWayStatisticalResult:
    degrees_freedom_between: np.ndarray
    degrees_freedom_within: np.ndarray
    f_statistic: np.ndarray


class AnovaComparison(ParametricComparison):
    """Declare one comparison unit for one-way ANOVA.

    Attributes:
        comparison_id: Identifier matching the ``col_comparison`` value.
        groups: Optional group subset to include for this comparison. If
            omitted, all observed groups in the comparison are used.
    """

    def __init__(
        self,
        comparison_id: str,
        groups: Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            comparison_id,
            ParametricComparisonKind.ANOVA_ONE_WAY,
            groups=groups,
        )


@dataclass(frozen=True, slots=True)
class AnovaComparisonPlan:
    comparison_ids: tuple[str, ...]
    comparison_ids_all_groups: tuple[str, ...]
    group_comparison_ids: tuple[str, ...]
    group_values: tuple[str, ...]

    @property
    def has_group_filter(self) -> bool:
        return bool(self.group_values)

    @classmethod
    def from_inputs(
        cls,
        comparisons: ParametricComparison | Sequence[ParametricComparison] | None,
        *,
        comparison_kind: ParametricComparisonKind = ParametricComparisonKind.ANOVA_ONE_WAY,
    ) -> Self | None:
        if comparisons is None:
            return None
        if isinstance(comparisons, ParametricComparison):
            items_comparison = (comparisons,)
        elif isinstance(comparisons, Sequence) and not isinstance(comparisons, str):
            items_comparison = tuple(comparisons)
        else:
            raise ValueError(
                "Arg `comparisons` must be a ParametricComparison or a sequence of ParametricComparison items."
            )

        if not items_comparison:
            raise ValueError("Arg `comparisons` must not be empty.")
        if any(
            not isinstance(_item, ParametricComparison)
            or _item.kind != comparison_kind
            for _item in items_comparison
        ):
            raise ValueError(
                f"Arg `comparisons` must contain `{comparison_kind.value}` ParametricComparison items."
            )

        comparison_ids_seen: set[str] = set()
        comparison_ids: list[str] = []
        comparison_ids_all_groups: list[str] = []
        group_comparison_ids: list[str] = []
        group_values: list[str] = []
        for item_comparison in items_comparison:
            comparison_id = item_comparison.comparison_id
            assert comparison_id is not None
            if comparison_id in comparison_ids_seen:
                raise ValueError("Duplicate comparison ids are not allowed.")
            comparison_ids_seen.add(comparison_id)
            comparison_ids.append(comparison_id)
            if item_comparison.groups is None:
                comparison_ids_all_groups.append(comparison_id)
                continue

            for group in item_comparison.groups:
                group_comparison_ids.append(comparison_id)
                group_values.append(group)

        return cls(
            comparison_ids=tuple(comparison_ids),
            comparison_ids_all_groups=tuple(comparison_ids_all_groups),
            group_comparison_ids=tuple(group_comparison_ids),
            group_values=tuple(group_values),
        )
