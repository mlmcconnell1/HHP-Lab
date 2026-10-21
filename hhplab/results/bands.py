"""Exact interval validation and evaluation for declarative finding bands."""

from __future__ import annotations

import itertools
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from typing import Any


class BandValidationError(ValueError):
    """A band condition is invalid, unsatisfiable, or overlaps another band."""


@dataclass(frozen=True)
class _Interval:
    lower: Fraction | None = None
    lower_closed: bool = True
    upper: Fraction | None = None
    upper_closed: bool = True

    def intersect(self, other: _Interval) -> _Interval:
        lower, lower_closed = self.lower, self.lower_closed
        if other.lower is not None:
            if lower is None or other.lower > lower:
                lower, lower_closed = other.lower, other.lower_closed
            elif other.lower == lower:
                lower_closed = lower_closed and other.lower_closed

        upper, upper_closed = self.upper, self.upper_closed
        if other.upper is not None:
            if upper is None or other.upper < upper:
                upper, upper_closed = other.upper, other.upper_closed
            elif other.upper == upper:
                upper_closed = upper_closed and other.upper_closed

        return _Interval(lower, lower_closed, upper, upper_closed)

    def is_nonempty(self) -> bool:
        if self.lower is None or self.upper is None:
            return True
        if self.lower < self.upper:
            return True
        return self.lower == self.upper and self.lower_closed and self.upper_closed

    def contains(self, value: Fraction) -> bool:
        if self.lower is not None:
            if value < self.lower or (value == self.lower and not self.lower_closed):
                return False
        if self.upper is not None:
            if value > self.upper or (value == self.upper and not self.upper_closed):
                return False
        return True


@dataclass(frozen=True)
class BandOverlap:
    """One exactly satisfiable overlap between two finding bands."""

    first: str
    second: str
    witness: Mapping[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {"first": self.first, "second": self.second, "witness": dict(self.witness)}


_CONDITION_NAMES = frozenset(
    {"equivalent_to", "ci_above", "ci_below", "ci_excludes", "stat_gte", "stat_lte"}
)
_RESERVED_LABELS = frozenset({"inconclusive", "ambiguous_rule", "not_evaluated"})


def _finite_fraction(value: Any, *, context: str) -> Fraction:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal, Fraction)):
        raise BandValidationError(f"{context} must be a finite number")
    if isinstance(value, float) and not math.isfinite(value):
        raise BandValidationError(f"{context} must be a finite number")
    if isinstance(value, Decimal) and not value.is_finite():
        raise BandValidationError(f"{context} must be a finite number")
    try:
        if isinstance(value, Fraction):
            return value
        return Fraction(Decimal(str(value)))
    except (ValueError, ZeroDivisionError) as error:
        raise BandValidationError(f"{context} must be a finite number") from error


def _resolve_number(value: Any, parameters: Mapping[str, Any], *, context: str) -> Fraction:
    if isinstance(value, Mapping):
        if set(value) != {"param"}:
            raise BandValidationError(f"{context} parameter reference must contain only 'param'")
        name = value["param"]
        if not isinstance(name, str) or name not in parameters:
            raise BandValidationError(f"{context} references unknown parameter {name!r}")
        value = parameters[name]
    return _finite_fraction(value, context=context)


def _predicate_constraint(
    name: str,
    raw: Any,
    parameters: Mapping[str, Any],
    *,
    context: str,
) -> list[dict[str, _Interval]]:
    if name == "ci_excludes":
        value = _resolve_number(raw, parameters, context=f"{context}.ci_excludes")
        return [
            {"ci_hi": _Interval(upper=value, upper_closed=False)},
            {"ci_lo": _Interval(lower=value, lower_closed=False)},
        ]

    if name in {"equivalent_to", "ci_above", "ci_below"}:
        if not isinstance(raw, Mapping):
            raise BandValidationError(f"{context}.{name} must be an object")
        allowed_keys = {"value", "margin"}
        required_keys = {"value", "margin"} if name == "equivalent_to" else {"value"}
        if set(raw) - allowed_keys or required_keys - set(raw):
            raise BandValidationError(f"{context}.{name} requires {sorted(required_keys)}")
        value = _resolve_number(raw["value"], parameters, context=f"{context}.{name}.value")
        margin = _resolve_number(
            raw.get("margin", 0), parameters, context=f"{context}.{name}.margin"
        )
        if margin < 0:
            raise BandValidationError(f"{context}.{name}.margin must be nonnegative")
        if name == "equivalent_to":
            return [
                {
                    "ci_lo": _Interval(lower=value - margin),
                    "ci_hi": _Interval(upper=value + margin),
                }
            ]
        if name == "ci_above":
            return [{"ci_lo": _Interval(lower=value + margin, lower_closed=False)}]
        return [{"ci_hi": _Interval(upper=value - margin, upper_closed=False)}]

    if name in {"stat_gte", "stat_lte"}:
        if not isinstance(raw, Mapping) or set(raw) != {"stat", "value"}:
            raise BandValidationError(f"{context}.{name} requires exactly 'stat' and 'value'")
        stat = raw["stat"]
        if not isinstance(stat, str) or not stat:
            raise BandValidationError(f"{context}.{name}.stat must be a non-empty string")
        value = _resolve_number(raw["value"], parameters, context=f"{context}.{name}.value")
        variable = f"stat:{stat}"
        if name == "stat_gte":
            return [{variable: _Interval(lower=value)}]
        return [{variable: _Interval(upper=value)}]

    raise BandValidationError(f"{context} has unsupported condition {name!r}")


def _band_alternatives(
    band: Mapping[str, Any], parameters: Mapping[str, Any], *, index: int
) -> tuple[str, list[dict[str, _Interval]]]:
    if not isinstance(band, Mapping):
        raise BandValidationError(f"bands[{index}] must be an object")
    label = band.get("label")
    if not isinstance(label, str) or not label.strip():
        raise BandValidationError(f"bands[{index}].label must be a non-empty string")
    if label in _RESERVED_LABELS:
        raise BandValidationError(f"band label {label!r} is reserved")
    condition_names = set(band) - {"label"}
    unknown = condition_names - _CONDITION_NAMES
    if unknown:
        raise BandValidationError(f"band {label!r} has unsupported conditions: {sorted(unknown)}")
    if not condition_names:
        raise BandValidationError(f"band {label!r} must declare at least one condition")

    alternatives: list[dict[str, _Interval]] = [{}]
    for name in sorted(condition_names):
        choices = _predicate_constraint(
            name,
            band[name],
            parameters,
            context=f"band {label!r}",
        )
        alternatives = [
            _merge_constraints(existing, choice)
            for existing, choice in itertools.product(alternatives, choices)
        ]
    return label, alternatives


def _merge_constraints(
    first: Mapping[str, _Interval], second: Mapping[str, _Interval]
) -> dict[str, _Interval]:
    merged = dict(first)
    for variable, interval in second.items():
        merged[variable] = merged.get(variable, _Interval()).intersect(interval)
    return merged


def _satisfiable(constraints: Mapping[str, _Interval]) -> bool:
    if any(not interval.is_nonempty() for interval in constraints.values()):
        return False
    lower = constraints.get("ci_lo", _Interval())
    upper = constraints.get("ci_hi", _Interval())
    if lower.lower is None or upper.upper is None:
        return True
    if lower.lower < upper.upper:
        return True
    return lower.lower == upper.upper and lower.lower_closed and upper.upper_closed


def _choose(interval: _Interval) -> Fraction:
    if interval.lower is not None and interval.upper is not None:
        if interval.lower == interval.upper:
            return interval.lower
        return (interval.lower + interval.upper) / 2
    if interval.lower is not None:
        return interval.lower if interval.lower_closed else interval.lower + 1
    if interval.upper is not None:
        return interval.upper if interval.upper_closed else interval.upper - 1
    return Fraction(0)


def _witness(constraints: Mapping[str, _Interval]) -> dict[str, str]:
    resolved = dict(constraints)
    lower = resolved.get("ci_lo", _Interval())
    upper = resolved.get("ci_hi", _Interval())
    if upper.upper is not None:
        lower = lower.intersect(_Interval(upper=upper.upper, upper_closed=upper.upper_closed))
    lo_value = _choose(lower)
    upper = upper.intersect(_Interval(lower=lo_value))
    values: dict[str, Fraction] = {"ci_lo": lo_value, "ci_hi": _choose(upper)}
    for variable, interval in resolved.items():
        if variable not in values:
            values[variable] = _choose(interval)
    return {name: _fraction_text(value) for name, value in sorted(values.items())}


def _fraction_text(value: Fraction) -> str:
    if value.denominator == 1:
        return str(value.numerator)
    return f"{value.numerator}/{value.denominator}"


def _compile_bands(
    bands: Sequence[Mapping[str, Any]], parameters: Mapping[str, Any]
) -> list[tuple[str, list[dict[str, _Interval]]]]:
    if not isinstance(bands, Sequence) or isinstance(bands, (str, bytes)):
        raise BandValidationError("bands must be an array")
    if not bands:
        raise BandValidationError("at least one band is required")
    if not isinstance(parameters, Mapping):
        raise BandValidationError("parameters must be an object")

    compiled = [
        _band_alternatives(band, parameters, index=index) for index, band in enumerate(bands)
    ]
    labels = [label for label, _ in compiled]
    if len(labels) != len(set(labels)):
        raise BandValidationError("band labels must be unique")
    for label, alternatives in compiled:
        if not any(_satisfiable(constraints) for constraints in alternatives):
            raise BandValidationError(f"band {label!r} is unsatisfiable")
    return compiled


def validate_band_exclusivity(
    bands: Sequence[Mapping[str, Any]],
    *,
    parameters: Mapping[str, Any] | None = None,
) -> tuple[BandOverlap, ...]:
    """Validate each band and return exact witnesses for any pairwise overlap.

    Numeric parameters may be direct finite values or ``{"param": name}``
    references resolved by ``parameters``. A returned witness uses exact
    integer or rational strings, including for strict and very narrow bounds.
    """
    resolved_parameters = parameters or {}
    compiled = _compile_bands(bands, resolved_parameters)
    overlaps: list[BandOverlap] = []
    for left_index, (left_label, left_alternatives) in enumerate(compiled):
        for right_label, right_alternatives in compiled[left_index + 1 :]:
            witness = next(
                (
                    _witness(intersection)
                    for left, right in itertools.product(left_alternatives, right_alternatives)
                    if _satisfiable(intersection := _merge_constraints(left, right))
                ),
                None,
            )
            if witness is not None:
                overlaps.append(BandOverlap(left_label, right_label, witness))
    return tuple(overlaps)


def _criterion_fraction(criteria: Mapping[str, Any], name: str) -> Fraction | None:
    value = criteria.get(name)
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        return _finite_fraction(value, context=f"criterion {name!r}")
    except BandValidationError:
        return None


def _criterion_record(criteria: Mapping[str, Any], name: str) -> dict[str, Any]:
    """Return a JSON-safe value and reason pair for a criterion."""
    value = criteria.get(name)
    fraction = _criterion_fraction(criteria, name)
    if fraction is None:
        if name not in criteria:
            reason = f"missing_criterion:{name}"
        elif value is None:
            reason = f"missing_criterion:{name}"
        elif isinstance(value, float) and not math.isfinite(value):
            reason = f"nonfinite_criterion:{name}"
        elif isinstance(value, Decimal) and not value.is_finite():
            reason = f"nonfinite_criterion:{name}"
        else:
            reason = f"invalid_criterion:{name}"
        return {"value": None, "reason": reason}
    return {"value": _json_number(value, fraction), "reason": None}


def evaluate_bands(
    bands: Sequence[Mapping[str, Any]],
    criteria: Mapping[str, Any],
    *,
    reference_values: Sequence[Any] = (),
    otherwise: str = "inconclusive",
    parameters: Mapping[str, Any] | None = None,
    failure_reason: str | None = None,
) -> dict[str, Any]:
    """Evaluate every band and report CI compatibility independently.

    ``criteria`` is a flat mapping of ``ci_lo``, ``ci_hi`` and named
    statistics such as ``fit``. The result matches the schema evaluation
    shape: status, label, compatible_with, and reason.
    """
    if not isinstance(criteria, Mapping):
        raise BandValidationError("criteria must be an object")
    if any(not isinstance(name, str) for name in criteria):
        raise BandValidationError("criterion names must be strings")
    if otherwise != "inconclusive":
        raise BandValidationError("otherwise must be 'inconclusive' in Phase 1")
    compiled = _compile_bands(bands, parameters or {})
    references = tuple(
        _finite_fraction(value, context="reference value") for value in reference_values
    )
    if len(references) != len(set(references)):
        raise BandValidationError("reference_values must be unique")

    ci_lo = _criterion_fraction(criteria, "ci_lo")
    ci_hi = _criterion_fraction(criteria, "ci_hi")
    compatible: list[Any] = []
    interval_valid = ci_lo is not None and ci_hi is not None and ci_lo <= ci_hi
    if interval_valid:
        compatible = [
            _json_number(raw, value)
            for raw, value in zip(reference_values, references, strict=True)
            if ci_lo <= value <= ci_hi
        ]

    needed: set[str] = set()
    for _label, alternatives in compiled:
        for branch in alternatives:
            for variable in branch:
                needed.add(
                    "ci_lo"
                    if variable == "ci_lo"
                    else "ci_hi"
                    if variable == "ci_hi"
                    else variable[5:]
                )
    criterion_records = {
        name: _criterion_record(criteria, name) for name in sorted(needed | set(criteria))
    }

    if failure_reason:
        return _not_evaluated(failure_reason, compatible, criterion_records)
    if ci_lo is not None and ci_hi is not None and ci_lo > ci_hi:
        return _not_evaluated("invalid_interval:ci_lo_gt_ci_hi", [], criterion_records)

    undefined = sorted(name for name in needed if _criterion_fraction(criteria, name) is None)
    if undefined:
        reason = criterion_records[undefined[0]]["reason"]
        return _not_evaluated(reason, compatible, criterion_records)

    resolved: dict[str, Fraction] = {
        name: _criterion_fraction(criteria, name)  # type: ignore[dict-item]
        for name in needed
    }
    matched_labels: list[str] = []
    for label, alternatives in compiled:
        for branch in alternatives:
            satisfied = True
            for variable, interval in branch.items():
                criterion_name = variable[5:] if variable.startswith("stat:") else variable
                if not interval.contains(resolved[criterion_name]):
                    satisfied = False
                    break
            if satisfied:
                matched_labels.append(label)
                break

    if len(matched_labels) > 1:
        return _not_evaluated(
            "multiple_bands_match", compatible, criterion_records, status="ambiguous_rule"
        )
    if matched_labels:
        label = matched_labels[0]
        return {
            "status": label,
            "label": label,
            "compatible_with": compatible,
            "reason": None,
            "criteria": criterion_records,
        }
    return {
        "status": "inconclusive",
        "label": "inconclusive",
        "compatible_with": compatible,
        "reason": None,
        "criteria": criterion_records,
    }


def _not_evaluated(
    reason: str,
    compatible_with: list[Any],
    criteria: Mapping[str, Any],
    *,
    status: str = "not_evaluated",
) -> dict[str, Any]:
    return {
        "status": status,
        "label": None,
        "compatible_with": compatible_with,
        "reason": reason,
        "criteria": dict(criteria),
    }


def _json_number(original: Any, value: Fraction) -> int | float:
    """Retain caller value types while ensuring derived values stay finite."""
    if isinstance(original, int) and not isinstance(original, bool):
        return original
    if value.denominator == 1:
        return value.numerator
    try:
        converted = float(value)
    except OverflowError as error:
        raise BandValidationError("criterion is outside finite JSON number range") from error
    if not math.isfinite(converted):
        raise BandValidationError("number is outside finite JSON number range")
    return converted
