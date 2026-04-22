from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Self, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from axiomkit.stats.p_value import (
    PValueAdjustmentMode,
    PValueAdjustmentType,
    normalize_p_value_adjustment_mode,
)
from .constant import FIELDS_RESOLVED_ORA_OPTIONS


class _MissingType:
    __slots__ = ()


_OPTION_UNSET = _MissingType()


class OraOptions(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    background_elements: set[str] | None = None
    rule_p_adjust: PValueAdjustmentType | str | None = "bh"
    thr_bg_hits_min: int = Field(default=0, ge=0)
    thr_bg_hits_max: int | None = None
    thr_fg_hits_min: int = Field(default=0, ge=0)
    thr_fg_hits_max: int | None = None
    thr_p_value: float = Field(default=0.05, ge=0.0, le=1.0)
    thr_p_adjust: float = Field(default=1.0, ge=0.0, le=1.0)
    should_keep_fg_members: bool = True
    should_keep_bg_members: bool = False

    @field_validator("rule_p_adjust", mode="after")
    @classmethod
    def _normalize_rule_p_adjust(
        cls,
        value: PValueAdjustmentType | str | None,
    ) -> PValueAdjustmentMode | None:
        return normalize_p_value_adjustment_mode(value)

    @model_validator(mode="after")
    def _validate_ranges(self) -> Self:
        if (
            self.thr_bg_hits_max is not None
            and self.thr_bg_hits_max < self.thr_bg_hits_min
        ):
            raise ValueError(
                "Arg `thr_bg_hits_max` must be in [`thr_bg_hits_min`, ∞) or None."
            )
        if (
            self.thr_fg_hits_max is not None
            and self.thr_fg_hits_max < self.thr_fg_hits_min
        ):
            raise ValueError(
                "Arg `thr_fg_hits_max` must be in [`thr_fg_hits_min`, ∞) or None."
            )
        return self

    def with_(
        self,
        *,
        background_elements: set[str] | None = cast(Any, _OPTION_UNSET),
        rule_p_adjust: PValueAdjustmentType | str | None = cast(Any, _OPTION_UNSET),
        thr_bg_hits_min: int = cast(Any, _OPTION_UNSET),
        thr_bg_hits_max: int | None = cast(Any, _OPTION_UNSET),
        thr_fg_hits_min: int = cast(Any, _OPTION_UNSET),
        thr_fg_hits_max: int | None = cast(Any, _OPTION_UNSET),
        thr_p_value: float = cast(Any, _OPTION_UNSET),
        thr_p_adjust: float = cast(Any, _OPTION_UNSET),
        should_keep_fg_members: bool = cast(Any, _OPTION_UNSET),
        should_keep_bg_members: bool = cast(Any, _OPTION_UNSET),
    ) -> Self:
        data = self.model_dump(mode="python")
        if background_elements is not _OPTION_UNSET:
            data["background_elements"] = background_elements
        if rule_p_adjust is not _OPTION_UNSET:
            data["rule_p_adjust"] = rule_p_adjust
        if thr_bg_hits_min is not _OPTION_UNSET:
            data["thr_bg_hits_min"] = thr_bg_hits_min
        if thr_bg_hits_max is not _OPTION_UNSET:
            data["thr_bg_hits_max"] = thr_bg_hits_max
        if thr_fg_hits_min is not _OPTION_UNSET:
            data["thr_fg_hits_min"] = thr_fg_hits_min
        if thr_fg_hits_max is not _OPTION_UNSET:
            data["thr_fg_hits_max"] = thr_fg_hits_max
        if thr_p_value is not _OPTION_UNSET:
            data["thr_p_value"] = thr_p_value
        if thr_p_adjust is not _OPTION_UNSET:
            data["thr_p_adjust"] = thr_p_adjust
        if should_keep_fg_members is not _OPTION_UNSET:
            data["should_keep_fg_members"] = should_keep_fg_members
        if should_keep_bg_members is not _OPTION_UNSET:
            data["should_keep_bg_members"] = should_keep_bg_members
        return self.__class__.model_validate(data)

class OraComparison(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    comparison_id: str | None = None
    foreground_elements: set[str]
    option_override: OraOptions | None = None

    @field_validator("comparison_id", mode="before")
    @classmethod
    def _normalize_comparison_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        if not value:
            raise ValueError("`comparison_id` must be non-empty when provided.")
        return value


@dataclass(frozen=True, slots=True)
class ResolvedOraOptions:
    rule_p_adjust: PValueAdjustmentType | None
    thr_bg_hits_min: int
    thr_bg_hits_max: int | None
    thr_fg_hits_min: int
    thr_fg_hits_max: int | None
    thr_p_value: float
    thr_p_adjust: float
    should_keep_fg_members: bool
    should_keep_bg_members: bool
    
    @classmethod
    def from_options(
        cls,
        base: OraOptions,
        override: OraOptions | None
    ) -> Self:
        if override is None:
            data = {_key: getattr(base, _key) for _key in FIELDS_RESOLVED_ORA_OPTIONS}
        else:
            data = {
                _key: (
                    getattr(override, _key)
                    if _key in override.model_fields_set
                    else getattr(base, _key)
                )
                for _key in FIELDS_RESOLVED_ORA_OPTIONS
            }
        
        return cls(**data)


@dataclass(frozen=True, slots=True)
class ResolvedOraComparison:
    comparison_id: str
    foreground_elements: frozenset[str]
    background_elements: frozenset[str] | None
    options: ResolvedOraOptions
