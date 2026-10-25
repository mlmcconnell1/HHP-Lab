"""Deterministic row membership and exclusion provenance for analysis samples."""

from __future__ import annotations

import math
import numbers
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

from .contracts import ResolvedSampleFrame


class SampleResolutionError(ValueError):
    """A malformed sample request or readiness failure during resolution."""

    def __init__(self, message: str, *, code: str = "invalid_sample") -> None:
        super().__init__(message)
        self.code = code


def group_required_columns(
    member_columns: Sequence[Sequence[str]], group_require: Sequence[str] = ()
) -> tuple[str, ...]:
    """Union member columns in declaration order, then append group extras."""
    ordered: list[str] = []
    for index, columns in enumerate(member_columns):
        ordered.extend(_string_list(columns, f"member_columns[{index}]"))
    ordered.extend(_string_list(group_require, "group_require"))
    return tuple(dict.fromkeys(ordered))


def resolve_sample(
    panel: pd.DataFrame,
    plan: Mapping[str, Any],
    *,
    mask_provenance: pd.DataFrame | None = None,
    annotation_provenance: pd.DataFrame | None = None,
    base_membership: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve a normalized analysis or sample-group context over a full panel.

    ``plan`` is a plain mapping produced by recipe planning. It supplies key
    names, panel identity, ordered required and numeric columns, the normalized
    sample mapping, fixed-effect dimensions, and sample-source identifiers.
    This module deliberately has no dependency on RecipeV2 schema classes.

    ``annotation_provenance`` is the sidecar of versioned (``id``,
    ``revision``) adjustment/caveat keys built by mask/derive stages; the
    result reports the ordered, deduplicated union of keys attached to the
    rows and columns actually used by the sample.

    The return value contains a ``ResolvedSampleFrame`` for included rows and a
    full-panel ``membership`` DataFrame with keys, ``included``, ``reason``, and
    ``sample_source``. Inference-only variants pass the exact base result via
    ``base_membership`` to reuse its per-key decisions without re-evaluation.
    """
    if not isinstance(panel, pd.DataFrame):
        raise SampleResolutionError("panel must be a pandas DataFrame", code="panel_contract")
    normalized = _normalize_plan(plan)
    geo_key = normalized["geo_key"]
    time_key = normalized["time_key"]
    _validate_panel(panel, geo_key=geo_key, time_key=time_key)
    required = normalized["required_columns"]
    numeric = normalized["numeric_columns"]
    weights = normalized["weight_columns"]
    fixed_effects = normalized["fixed_effects"]
    sample = normalized["sample"]
    needed = list(dict.fromkeys([*required, *normalized["predicate_columns"], *fixed_effects]))
    missing_columns = [column for column in needed if column not in panel.columns]
    if missing_columns:
        raise SampleResolutionError(
            f"sample references missing panel columns: {missing_columns}",
            code="readiness_error",
        )
    if not set(numeric).issubset(required):
        raise SampleResolutionError("numeric_columns must be included in required_columns")
    if not set(weights).issubset(numeric):
        raise SampleResolutionError("weight_columns must be included in numeric_columns")

    if base_membership is not None:
        decisions = _reuse_base_membership(
            panel,
            normalized,
            base_membership,
            geo_key=geo_key,
            time_key=time_key,
        )
    else:
        masks = _validate_mask_provenance(mask_provenance, panel)
        decisions = _resolve_decisions(
            panel,
            required=required,
            numeric=numeric,
            weights=weights,
            fixed_effects=fixed_effects,
            sample=sample,
            masks=masks,
            geo_key=geo_key,
            time_key=time_key,
        )

    sample_source = normalized["sample_source"]
    membership = pd.DataFrame(
        {
            geo_key: panel[geo_key].to_numpy(copy=True),
            time_key: panel[time_key].to_numpy(copy=True),
            "included": [decision[0] for decision in decisions],
            "reason": [decision[1] for decision in decisions],
            "sample_source": [sample_source] * len(panel),
        },
        index=panel.index,
    )
    included_positions = [i for i, decision in enumerate(decisions) if decision[0]]
    included_frame = panel.iloc[included_positions].copy(deep=True)
    row_keys = tuple(
        (included_frame[geo_key].iloc[i], included_frame[time_key].iloc[i])
        for i in range(len(included_frame))
    )
    resolved = ResolvedSampleFrame(
        frame=included_frame,
        row_keys=row_keys,
        geo_key=geo_key,
        time_key=time_key,
    )
    annotation_masks = _validate_annotation_provenance(annotation_provenance, panel)
    annotation_dependencies = set()
    for position, (included, reason) in enumerate(decisions):
        if included:
            columns = needed
        elif reason and reason.startswith(("masked:", "missing:", "nonfinite:")):
            columns = [reason.split(":", 1)[1]]
        elif reason == "invalid_weight":
            columns = weights
        elif reason == "singleton_fe":
            columns = fixed_effects
        elif reason == "geography_excluded":
            allowed = (sample.get("geographies") or {}).get("include")
            columns = normalized["predicate_columns"] if (
                allowed is None or panel[geo_key].iat[position] in allowed
            ) else []
        else:
            columns = []
        for column in columns:
            for key in _annotation_cell_keys(annotation_masks, column, position):
                annotation_dependencies.add((
                    panel[geo_key].iat[position], int(panel[time_key].iat[position]), column, key,
                ))
    if base_membership is not None:
        annotation_dependencies.update(
            base_membership.get("annotations", {}).get("dependencies", ())
        )
    annotation_counts = Counter(dependency[3] for dependency in annotation_dependencies)
    annotation_keys = tuple(sorted(annotation_counts))
    exclusions = Counter(reason for included, reason in decisions if not included)
    return {
        "resolved": resolved,
        "membership": membership,
        "panel_identity": normalized["panel_identity"],
        "context_key": normalized["context_key"],
        "geo_key": geo_key,
        "time_key": time_key,
        "required_columns": tuple(required),
        "numeric_columns": tuple(numeric),
        "weight_columns": tuple(weights),
        "fixed_effects": tuple(fixed_effects),
        "predicate_columns": tuple(normalized["predicate_columns"]),
        "sample_spec": dict(sample),
        "sample_source": sample_source,
        "n_panel": len(panel),
        "n_included": len(included_positions),
        "n_excluded": len(panel) - len(included_positions),
        "exclusions": dict(sorted(exclusions.items())),
        "annotations": {
            "keys": annotation_keys,
            "counts": dict(sorted(annotation_counts.items())),
            "dependencies": tuple(sorted(annotation_dependencies, key=repr)),
        },
    }


def _normalize_plan(plan: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(plan, Mapping):
        raise SampleResolutionError("sample plan must be a mapping")
    geo_key = _required_string(plan, "geo_key", "sample plan")
    time_key = _required_string(plan, "time_key", "sample plan")
    if geo_key == time_key:
        raise SampleResolutionError("geo_key and time_key must be distinct")
    if "panel_identity" not in plan or plan["panel_identity"] is None:
        raise SampleResolutionError("sample plan requires panel_identity")
    sample = plan.get("sample", {})
    if not isinstance(sample, Mapping):
        raise SampleResolutionError("sample must be a normalized mapping")
    unsupported_sample_keys = sorted(
        set(sample)
        - {
            "years",
            "exclude_years",
            "geographies",
            "require",
            "min_obs_per_geography",
            "drop_singletons",
        }
    )
    if unsupported_sample_keys:
        raise SampleResolutionError(f"sample has unsupported keys: {unsupported_sample_keys}")
    required = _string_list(plan.get("required_columns"), "required_columns")
    if len(set(required)) != len(required):
        raise SampleResolutionError("required_columns must be unique")
    sample_required = _string_list(sample.get("require", []), "sample.require")
    required = list(dict.fromkeys([*required, *sample_required]))
    numeric = _string_list(plan.get("numeric_columns", []), "numeric_columns")
    weights = _string_list(plan.get("weight_columns", []), "weight_columns")
    fixed_effects = _string_list(plan.get("fixed_effects", []), "fixed_effects")
    predicate_columns = _string_list(plan.get("predicate_columns", []), "predicate_columns")
    geography = sample.get("geographies")
    if geography is not None:
        if not isinstance(geography, Mapping):
            raise SampleResolutionError("sample.geographies must be a mapping")
        unsupported_geography_keys = sorted(set(geography) - {"include", "exclude"})
        if unsupported_geography_keys:
            raise SampleResolutionError(
                f"sample.geographies has unsupported keys: {unsupported_geography_keys}"
            )
        predicate = geography.get("exclude")
        if predicate is not None:
            if not isinstance(predicate, Mapping):
                raise SampleResolutionError("sample.geographies.exclude must be a mapping")
            unsupported_predicate_keys = sorted(set(predicate) - {"column", "equals"})
            if unsupported_predicate_keys:
                raise SampleResolutionError(
                    f"sample.geographies.exclude has unsupported keys: {unsupported_predicate_keys}"
                )
            predicate_column = _required_string(predicate, "column", "sample.geographies.exclude")
            if predicate_column not in predicate_columns:
                predicate_columns.append(predicate_column)
    if len(set(required)) != len(required):
        raise SampleResolutionError("required_columns must be unique")
    if len(set(numeric)) != len(numeric):
        raise SampleResolutionError("numeric_columns must be unique")
    if len(set(weights)) != len(weights):
        raise SampleResolutionError("weight_columns must be unique")
    if len(set(fixed_effects)) != len(fixed_effects):
        raise SampleResolutionError("fixed_effects must be unique")
    if not set(numeric).issubset(required):
        raise SampleResolutionError("numeric_columns must be included in required_columns")
    if not set(weights).issubset(numeric):
        raise SampleResolutionError("weight_columns must be included in numeric_columns")
    if not set(fixed_effects).issubset(required):
        raise SampleResolutionError("fixed_effects must be included in required_columns")
    if "drop_singletons" in sample and not isinstance(sample["drop_singletons"], bool):
        raise SampleResolutionError("sample.drop_singletons must be a boolean")
    return {
        "geo_key": geo_key,
        "time_key": time_key,
        "panel_identity": plan["panel_identity"],
        "required_columns": required,
        "numeric_columns": numeric,
        "weight_columns": weights,
        "fixed_effects": fixed_effects,
        "predicate_columns": predicate_columns,
        "sample": dict(sample),
        "sample_source": _required_string(plan, "sample_source", "sample plan"),
        "context_key": plan.get("group_context_key") or plan.get("result_key"),
        "base_membership_key": plan.get("base_membership_key"),
    }


def _resolve_decisions(
    panel: pd.DataFrame,
    *,
    required: list[str],
    numeric: list[str],
    weights: list[str],
    fixed_effects: list[str],
    sample: Mapping[str, Any],
    masks: pd.DataFrame,
    geo_key: str,
    time_key: str,
) -> list[tuple[bool, str | None]]:
    count = len(panel)
    years = _year_selection(sample.get("years"))
    excluded_years = set(_integer_list(sample.get("exclude_years", []), "exclude_years"))
    geography = sample.get("geographies") or {}
    include_geographies = geography.get("include")
    if include_geographies is not None:
        include_geographies = set(_string_list(include_geographies, "geographies.include"))
    predicate = geography.get("exclude")
    predicate_column = None
    predicate_value = None
    if predicate is not None:
        predicate_column = _required_string(predicate, "column", "geographies.exclude")
        if "equals" not in predicate:
            raise SampleResolutionError("geographies.exclude requires equals")
        predicate_value = predicate["equals"]

    decisions: list[tuple[bool, str | None]] = [(True, None)] * count
    row_years = [int(value) for value in panel[time_key].to_numpy(dtype=object)]
    row_geographies = panel[geo_key].to_numpy(dtype=object)
    for position, year in enumerate(row_years):
        if years is not None and year not in years:
            decisions[position] = (False, "year_out_of_range")
        elif year in excluded_years:
            decisions[position] = (False, "year_excluded")
        elif (
            include_geographies is not None and row_geographies[position] not in include_geographies
        ):
            decisions[position] = (False, "geography_excluded")
        elif predicate_column is not None and _values_equal(
            panel[predicate_column].iat[position], predicate_value
        ):
            decisions[position] = (False, "geography_excluded")

    # The mask scan is one pass across all remaining rows in required-column order.
    for position, (included, _reason) in enumerate(decisions):
        if not included:
            continue
        for column in required:
            if _has_mask(masks[column].iat[position]):
                decisions[position] = (False, f"masked:{column}")
                break

    # Storage nulls precede all nonfinite classifications.
    for position, (included, _reason) in enumerate(decisions):
        if not included:
            continue
        for column in required:
            if _is_storage_null(panel[column].iat[position]):
                decisions[position] = (False, f"missing:{column}")
                break

    # Nonnumeric values in numeric roles are readiness errors, never exclusions.
    for position, (included, _reason) in enumerate(decisions):
        if not included:
            continue
        for column in numeric:
            value = _python_scalar(panel[column].iat[position])
            if _is_storage_null(value) or _is_nonfinite_numeric(value):
                continue
            if not _is_numeric(value):
                raise SampleResolutionError(
                    f"required numeric column {column!r} contains a nonnumeric value "
                    f"at row position {position}",
                    code="readiness_error",
                )

    # Nonfinite values share the same deterministic required-column order.
    for position, (included, _reason) in enumerate(decisions):
        if not included:
            continue
        for column in required:
            value = _python_scalar(panel[column].iat[position])
            if _is_nonfinite_numeric(value):
                decisions[position] = (
                    False,
                    "invalid_weight" if column in weights else f"nonfinite:{column}",
                )
                break

    # Finite nonpositive weights are tested only after nonfinite precedence.
    if weights:
        for position, (included, _reason) in enumerate(decisions):
            if not included:
                continue
            for column in required:
                if column not in weights:
                    continue
                value = _python_scalar(panel[column].iat[position])
                if _is_numeric(value) and value <= 0:
                    decisions[position] = (False, "invalid_weight")
                    break

    _iterative_drops(
        panel,
        decisions,
        fixed_effects=fixed_effects,
        min_obs_per_geography=_positive_integer(
            sample.get("min_obs_per_geography", 1), "min_obs_per_geography"
        ),
        drop_singletons=bool(sample.get("drop_singletons", bool(fixed_effects)))
        and bool(fixed_effects),
        geo_key=geo_key,
    )
    return decisions


def _iterative_drops(
    panel: pd.DataFrame,
    decisions: list[tuple[bool, str | None]],
    *,
    fixed_effects: list[str],
    min_obs_per_geography: int,
    drop_singletons: bool,
    geo_key: str,
) -> None:
    while True:
        included_positions = [i for i, (included, _reason) in enumerate(decisions) if included]
        geography_counts = Counter(
            _typed_value(panel[geo_key].iat[position]) for position in included_positions
        )
        removed = False
        for position in included_positions:
            key = _typed_value(panel[geo_key].iat[position])
            if geography_counts[key] < min_obs_per_geography:
                decisions[position] = (False, "insufficient_obs")
                removed = True

        if drop_singletons and fixed_effects:
            included_positions = [i for i, (included, _reason) in enumerate(decisions) if included]
            singleton_positions: set[int] = set()
            for dimension in fixed_effects:
                counts = Counter(
                    _typed_value(panel[dimension].iat[position]) for position in included_positions
                )
                singleton_positions.update(
                    position
                    for position in included_positions
                    if counts[_typed_value(panel[dimension].iat[position])] == 1
                )
            for position in sorted(singleton_positions):
                decisions[position] = (False, "singleton_fe")
                removed = True
        if not removed:
            return


def _reuse_base_membership(
    panel: pd.DataFrame,
    plan: Mapping[str, Any],
    base: Mapping[str, Any],
    *,
    geo_key: str,
    time_key: str,
) -> list[tuple[bool, str | None]]:
    if plan.get("base_membership_key") is None:
        raise SampleResolutionError(
            "base membership reuse requires an explicit base_membership_key",
            code="sample_reuse_mismatch",
        )
    if base.get("panel_identity") != plan["panel_identity"]:
        raise SampleResolutionError(
            "base sample reuse requires the same panel identity", code="sample_reuse_mismatch"
        )
    if base.get("geo_key") != geo_key or base.get("time_key") != time_key:
        raise SampleResolutionError(
            "base sample reuse requires the same panel key columns", code="sample_reuse_mismatch"
        )
    if base.get("context_key") != plan["base_membership_key"]:
        raise SampleResolutionError(
            "base membership context key does not match the plan", code="sample_reuse_mismatch"
        )
    base_required = tuple(base.get("required_columns", ()))
    if tuple(plan["required_columns"]) != base_required:
        raise SampleResolutionError(
            "inference-only reuse must preserve base required columns",
            code="sample_reuse_mismatch",
        )
    for field in (
        "numeric_columns",
        "weight_columns",
        "fixed_effects",
        "predicate_columns",
    ):
        if tuple(plan[field]) != tuple(base.get(field, ())):
            raise SampleResolutionError(
                f"inference-only reuse must preserve base {field}",
                code="sample_reuse_mismatch",
            )
    if dict(plan["sample"]) != base.get("sample_spec"):
        raise SampleResolutionError(
            "inference-only reuse must preserve the base sample specification",
            code="sample_reuse_mismatch",
        )
    if plan["sample_source"] != base.get("sample_source"):
        raise SampleResolutionError(
            "inference-only reuse must preserve the base sample source",
            code="sample_reuse_mismatch",
        )
    membership = base.get("membership")
    if not isinstance(membership, pd.DataFrame):
        raise SampleResolutionError("base membership result is missing its audit frame")
    if not {geo_key, time_key, "included", "reason"}.issubset(membership.columns):
        raise SampleResolutionError("base membership audit frame has an invalid schema")
    current_keys = _panel_key_index(panel, geo_key, time_key)
    base_keys = _panel_key_index(membership, geo_key, time_key)
    if len(set(base_keys)) != len(base_keys):
        raise SampleResolutionError("base membership audit frame contains duplicate keys")
    if len(current_keys) != len(base_keys) or set(current_keys) != set(base_keys):
        raise SampleResolutionError(
            "base sample reuse requires an identical full panel key set",
            code="sample_reuse_mismatch",
        )
    base_decisions: dict[tuple[tuple[type, Any], tuple[type, Any]], tuple[bool, str | None]] = {}
    for key, row in zip(base_keys, membership.itertuples(index=False), strict=True):
        if not isinstance(row.included, (bool, np.bool_)):
            raise SampleResolutionError("base membership included values must be booleans")
        if row.included:
            if row.reason is not None and row.reason is not pd.NA:
                raise SampleResolutionError("included base membership rows cannot have reasons")
            base_decisions[key] = (True, None)
        else:
            if not isinstance(row.reason, str) or not row.reason:
                raise SampleResolutionError("excluded base membership rows require a reason")
            base_decisions[key] = (False, row.reason)
    return [base_decisions[key] for key in current_keys]


def _validate_mask_provenance(masks: pd.DataFrame | None, panel: pd.DataFrame) -> pd.DataFrame:
    if masks is None:
        return pd.DataFrame(
            {column: [()] * len(panel) for column in panel.columns}, index=panel.index
        )
    if not isinstance(masks, pd.DataFrame):
        raise SampleResolutionError("mask_provenance must be a DataFrame")
    if not masks.index.equals(panel.index) or not masks.columns.equals(panel.columns):
        raise SampleResolutionError(
            "mask_provenance rows and columns must exactly match the full panel"
        )
    for column in masks.columns:
        for position, raw in enumerate(masks[column].array):
            value = _python_scalar(raw)
            if not isinstance(value, tuple) or any(
                not isinstance(mask_id, str) or not mask_id.strip() for mask_id in value
            ):
                raise SampleResolutionError(
                    f"mask provenance for {column!r} at row position {position} "
                    "must be a tuple of non-empty mask IDs"
                )
    return masks


def _validate_annotation_provenance(
    annotations: pd.DataFrame | None, panel: pd.DataFrame
) -> pd.DataFrame:
    if annotations is None:
        return pd.DataFrame(
            {column: [()] * len(panel) for column in panel.columns}, index=panel.index
        )
    if not isinstance(annotations, pd.DataFrame):
        raise SampleResolutionError("annotation_provenance must be a DataFrame")
    if not annotations.index.equals(panel.index) or not annotations.columns.equals(panel.columns):
        raise SampleResolutionError(
            "annotation_provenance rows and columns must exactly match the full panel"
        )
    for column in annotations.columns:
        for position, raw in enumerate(annotations[column].array):
            value = _python_scalar(raw)
            if not isinstance(value, tuple) or any(
                not isinstance(item, tuple)
                or len(item) != 2
                or not isinstance(item[0], str)
                or not item[0]
                or not isinstance(item[1], int)
                or isinstance(item[1], bool)
                or item[1] < 1
                for item in value
            ):
                raise SampleResolutionError(
                    f"annotation provenance for {column!r} at row position {position} "
                    "must be a tuple of (id, revision)"
                )
    return annotations


def _annotation_cell_keys(
    annotations: pd.DataFrame, column: str, position: int
) -> tuple[tuple[str, int], ...]:
    value = _python_scalar(annotations[column].iat[position])
    if isinstance(value, tuple) and all(
        isinstance(item, tuple)
        and len(item) == 2
        and isinstance(item[0], str)
        and isinstance(item[1], int)
        for item in value
    ):
        return value
    if value is None or value is pd.NA:
        return ()
    raise SampleResolutionError(
        f"annotation provenance for {column!r} at row position {position} "
        "must be a tuple of (id, revision)"
    )


def _validate_panel(panel: pd.DataFrame, *, geo_key: str, time_key: str) -> None:
    if not panel.columns.is_unique:
        raise SampleResolutionError("panel column names must be unique", code="panel_contract")
    missing = {geo_key, time_key} - set(panel.columns)
    if missing:
        raise SampleResolutionError(
            f"panel is missing key columns: {sorted(missing)}", code="panel_contract"
        )
    seen: set[tuple[tuple[type, Any], tuple[type, Any]]] = set()
    for position, (geography, year) in enumerate(
        zip(panel[geo_key].array, panel[time_key].array, strict=True)
    ):
        geography = _python_scalar(geography)
        year = _python_scalar(year)
        if not isinstance(geography, (str, int)) or isinstance(geography, bool):
            raise SampleResolutionError(
                f"panel geography key is invalid at row position {position}",
                code="panel_contract",
            )
        if not isinstance(year, numbers.Integral) or isinstance(year, bool):
            raise SampleResolutionError(
                f"panel time key must be an integer at row position {position}",
                code="panel_contract",
            )
        key = (_typed_value(geography), _typed_value(int(year)))
        if key in seen:
            raise SampleResolutionError(
                f"panel contains duplicate keys at row position {position}",
                code="panel_contract",
            )
        seen.add(key)


def _panel_key_index(
    frame: pd.DataFrame, geo_key: str, time_key: str
) -> list[tuple[tuple[type, Any], tuple[type, Any]]]:
    return [
        (_typed_value(_python_scalar(geo)), _typed_value(_python_scalar(year)))
        for geo, year in zip(frame[geo_key].array, frame[time_key].array, strict=True)
    ]


def _year_selection(value: Any) -> set[int] | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        start = value.get("from", value.get("from_year"))
        end = value.get("to", value.get("to_year"))
        step = value.get("step", 1)
        start = _positive_or_integer(start, "years.from")
        end = _positive_or_integer(end, "years.to")
        step = _positive_integer(step, "years.step")
        if start > end:
            raise SampleResolutionError("years.from must not exceed years.to")
        return set(range(start, end + 1, step))
    return set(_integer_list(value, "years"))


def _integer_list(value: Any, context: str) -> list[int]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise SampleResolutionError(f"{context} must be a list of integers")
    if any(not isinstance(item, numbers.Integral) or isinstance(item, bool) for item in value):
        raise SampleResolutionError(f"{context} must be a list of integers")
    return [int(item) for item in value]


def _positive_integer(value: Any, context: str) -> int:
    if not isinstance(value, numbers.Integral) or isinstance(value, bool) or value < 1:
        raise SampleResolutionError(f"{context} must be a positive integer")
    return int(value)


def _positive_or_integer(value: Any, context: str) -> int:
    if not isinstance(value, numbers.Integral) or isinstance(value, bool):
        raise SampleResolutionError(f"{context} must be an integer")
    return int(value)


def _required_string(value: Mapping[str, Any], key: str, context: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result.strip():
        raise SampleResolutionError(f"{context}.{key} must be a non-empty string")
    return result


def _string_list(value: Any, context: str) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise SampleResolutionError(f"{context} must be a list of strings")
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise SampleResolutionError(f"{context} must be a list of non-empty strings")
    return list(value)


def _has_mask(value: Any) -> bool:
    return bool(value)


def _is_storage_null(value: Any) -> bool:
    value = _python_scalar(value)
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    if isinstance(value, np.datetime64):
        return bool(np.isnat(value))
    return False


def _is_numeric(value: Any) -> bool:
    value = _python_scalar(value)
    return isinstance(value, numbers.Real) and not isinstance(value, bool)


def _is_nonfinite_numeric(value: Any) -> bool:
    value = _python_scalar(value)
    if not _is_numeric(value):
        return False
    if isinstance(value, numbers.Integral):
        return False
    try:
        return not math.isfinite(float(value))
    except OverflowError:
        return False


def _python_scalar(value: Any) -> Any:
    return value.item() if isinstance(value, np.generic) else value


def _values_equal(left: Any, right: Any) -> bool:
    left = _python_scalar(left)
    right = _python_scalar(right)
    if _is_storage_null(left) or _is_storage_null(right):
        return left is right
    try:
        result = left == right
    except (TypeError, ValueError):
        return False
    return bool(result) if isinstance(result, (bool, np.bool_)) else False


def _typed_value(value: Any) -> tuple[type, Any]:
    value = _python_scalar(value)
    return type(value), value
