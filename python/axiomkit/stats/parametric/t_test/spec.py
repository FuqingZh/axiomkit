from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, Self

import numpy as np
import polars as pl

from ...p_value import PValueAdjustmentMode


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
class TTestContrast:
    """Specify the direction of a two-group t-test contrast.

    A contrast compares the mean of ``group_test`` against the mean of
    ``group_ref``. The reported mean difference and t statistic numerator use
    the direction ``mean(group_test) - mean(group_ref)``.

    ``group_ref`` is the reference or baseline group for the contrast. It is
    not the denominator of the t statistic; the t statistic denominator is the
    standard error of the mean difference.

    Attributes:
        group_test: Group label on the test side of the contrast. For a
            one-sided ``greater`` alternative, this is the group whose mean is
            tested as greater than ``group_ref``. For ``less``, it is tested as
            less than ``group_ref``.
        group_ref: Reference or baseline group label. The contrast direction is
            defined relative to this group.

    Raises:
        ValueError: If ``group_test`` and ``group_ref`` refer to the same group.

    Examples:
        >>> TTestContrast(group_test="B", group_ref="A")
        TTestContrast(group_test='B', group_ref='A')

        This contrast represents ``mean(B) - mean(A)``. With a ``greater``
        alternative, it tests whether ``mean(B) > mean(A)``.
    """

    group_test: str
    group_ref: str

    def __post_init__(self) -> None:
        group_test = str(self.group_test)
        group_ref = str(self.group_ref)
        object.__setattr__(self, "group_test", group_test)
        object.__setattr__(self, "group_ref", group_ref)

        if self.group_test == self.group_ref:
            raise ValueError(
                "Arg `group_test` must be different from `group_ref`, yours: "
                f"{self.group_test!r}."
            )


@dataclass(frozen=True, slots=True)
class ContrastPlan:
    contrast_ids: tuple[tuple[str, str], ...]
    group_test_values: tuple[str, ...]
    group_ref_values: tuple[str, ...]
    group_used: tuple[str, ...]

    @classmethod
    def from_inputs(
        cls,
        contrasts: TTestContrast | Sequence[TTestContrast],
    ) -> Self:
        items_contrast: Sequence[TTestContrast]
        if isinstance(contrasts, TTestContrast):
            items_contrast = [contrasts]
        elif isinstance(contrasts, Sequence) and not isinstance(contrasts, str):
            items_contrast = contrasts
        else:
            raise ValueError(
                "Arg `contrasts` must be a TTestContrast or a sequence of TTestContrast items."
            )

        if any(not isinstance(_item, TTestContrast) for _item in items_contrast):
            raise ValueError(
                "Arg `contrasts` must be a TTestContrast or a sequence of TTestContrast items."
            )

        pairs_seen = set()
        contrast_ids: list[tuple[str, str]] = []
        group_test_values: list[str] = []
        group_ref_values: list[str] = []
        group_used: list[str] = []
        for item_contrast in items_contrast:
            pair_key = (item_contrast.group_test, item_contrast.group_ref)
            if pair_key in pairs_seen:
                raise ValueError("Duplicate contrast pairs are not allowed.")
            pairs_seen.add(pair_key)
            contrast_ids.append(pair_key)
            group_test_values.append(item_contrast.group_test)
            group_ref_values.append(item_contrast.group_ref)
            group_used.extend(pair_key)

        return cls(
            contrast_ids=tuple(contrast_ids),
            group_test_values=tuple(group_test_values),
            group_ref_values=tuple(group_ref_values),
            group_used=tuple(group_used),
        )
