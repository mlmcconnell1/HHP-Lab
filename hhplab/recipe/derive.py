"""Pure, key-preserving masks and derived-column operations for RecipeV2 panels."""

from __future__ import annotations

import math
import numbers
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd
from pydantic import BaseModel


class DerivationError(ValueError):
    """Raised when a mask or derivation cannot be applied safely."""


_SEMANTICS = frozenset({"extensive", "intensive", "rate"})
_NULL = object()
_MASK_KEYS = frozenset({"id", "panels", "columns", "years", "reason", "geographies"})


def apply_masks(
    frame: pd.DataFrame,
    masks: Sequence[Mapping[str, Any] | BaseModel],
    *,
    panel_name: str,
    geo_key: str,
    time_key: str,
) -> dict[str, Any]:
    """Apply cell masks without changing row identity or conflating null and NaN.

    The returned mapping contains the copied ``frame``, a same-index
    ``mask_provenance`` frame whose cells are tuples of mask IDs, a same-index
    ``value_reasons`` frame, and ``mask_definitions`` for audit detail.
    """
    _validate_panel(frame, geo_key=geo_key, time_key=time_key)
    result = frame.copy(deep=True)
    provenance = _empty_provenance(result)
    reasons = _empty_reasons(result)
    definitions: dict[str, dict[str, Any]] = {}

    seen_ids: set[str] = set()
    for raw_mask in masks:
        mask = _as_mapping(raw_mask, context="mask")
        unknown_keys = sorted(set(mask) - _MASK_KEYS)
        if unknown_keys:
            raise DerivationError(f"mask has unsupported keys: {unknown_keys}")
        mask_id = _required_string(mask, "id", context="mask")
        if mask_id in seen_ids:
            raise DerivationError(f"duplicate mask id {mask_id!r}")
        seen_ids.add(mask_id)
        panels = mask.get("panels", [])
        columns = mask.get("columns")
        years = mask.get("years")
        reason = _required_string(mask, "reason", context=f"mask {mask_id!r}")
        if not _is_string_list(panels):
            raise DerivationError(f"mask {mask_id!r} panels must be a list of strings")
        if not _is_string_list(columns) or not columns:
            raise DerivationError(f"mask {mask_id!r} requires a non-empty columns list")
        if len(set(columns)) != len(columns):
            raise DerivationError(f"mask {mask_id!r} columns must be unique")
        key_columns = sorted(set(columns) & {geo_key, time_key})
        if key_columns:
            raise DerivationError(f"mask {mask_id!r} cannot mask panel key columns: {key_columns}")
        if not isinstance(years, Sequence) or isinstance(years, (str, bytes)) or not years:
            raise DerivationError(f"mask {mask_id!r} requires a non-empty years list")
        if any(isinstance(year, bool) or not isinstance(year, numbers.Integral) for year in years):
            raise DerivationError(f"mask {mask_id!r} years must be integers")

        geography = mask.get("geographies")
        if geography is not None:
            geography = _as_mapping(geography, context=f"mask {mask_id!r}.geographies")
            unsupported_geography_keys = sorted(set(geography) - {"include"})
            if unsupported_geography_keys:
                raise DerivationError(
                    f"mask {mask_id!r}.geographies has unsupported keys: "
                    f"{unsupported_geography_keys}"
                )
            include = geography.get("include")
            if include is not None and not _is_string_list(include):
                raise DerivationError(
                    f"mask {mask_id!r} geography include must be a list of strings"
                )

        if panels and panel_name not in panels:
            continue
        unknown_columns = sorted(set(columns) - set(result.columns))
        if unknown_columns:
            raise DerivationError(f"mask {mask_id!r} references unknown columns: {unknown_columns}")

        selection = result[time_key].isin({int(year) for year in years})
        if geography is not None:
            include = geography.get("include")
            if include is not None:
                selection &= result[geo_key].isin(include)
        positions = [position for position, selected in enumerate(selection.to_numpy()) if selected]

        for column in columns:
            values = result[column].to_numpy(dtype=object, copy=True)
            mask_values = provenance[column].to_numpy(dtype=object, copy=True)
            reason_values = reasons[column].to_numpy(dtype=object, copy=True)
            for position in positions:
                values[position] = pd.NA
                mask_values[position] = tuple(dict.fromkeys((*mask_values[position], mask_id)))
                reason_values[position] = f"masked:{column}"
            result[column] = pd.Series(values, index=result.index, dtype=object)
            provenance[column] = pd.Series(mask_values, index=result.index, dtype=object)
            reasons[column] = pd.Series(reason_values, index=result.index, dtype=object)

        definitions[mask_id] = {
            "reason": reason,
            "columns": list(columns),
            "years": [int(year) for year in years],
            "panels": list(panels),
            "geographies": geography,
        }

    return {
        "frame": result,
        "mask_provenance": provenance,
        "value_reasons": reasons,
        "mask_definitions": definitions,
        "measures": {},
    }


def _is_string_list(value: Any) -> bool:
    return (
        isinstance(value, Sequence)
        and not isinstance(value, (str, bytes))
        and all(isinstance(item, str) for item in value)
    )


def derive_panel(
    panel: pd.DataFrame | Mapping[str, Any],
    derivations: Sequence[Mapping[str, Any] | BaseModel],
    *,
    panel_name: str,
    geo_key: str,
    time_key: str,
    input_measures: Mapping[str, Mapping[str, Any]],
    parameters: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate a closed derivation graph once on the full keyed panel.

    ``input_measures`` supplies ``units`` and ``aggregation_semantics`` for
    every source column used by a derivation. ``source_lineage`` defaults to
    the source column name. The result keeps mask and null-reason sidecars
    separate from panel values so consumers can preserve IEEE NaN as
    nonfinite rather than treating it as storage null.
    """
    state = _normalize_panel_state(panel)
    frame = state["frame"]
    _validate_panel(frame, geo_key=geo_key, time_key=time_key)
    resolved_parameters = parameters or {}
    active = _active_derivations(derivations, panel_name)
    order = _topological_order(active, set(frame.columns))

    if any(not isinstance(name, str) for name in input_measures):
        raise DerivationError("input measure names must be strings")
    metadata: dict[str, dict[str, Any]] = {
        name: _normalize_measure(name, measure) for name, measure in input_measures.items()
    }
    result = frame.copy(deep=True)
    provenance = state["mask_provenance"].copy(deep=True)
    reasons = state["value_reasons"].copy(deep=True)
    calendar = _calendar_index(result, geo_key=geo_key, time_key=time_key)
    for spec in order:
        output = spec["id"]
        inputs = _inputs(spec)
        missing_metadata = [column for column in inputs if column not in metadata]
        if missing_metadata:
            raise DerivationError(
                f"derivation {output!r} lacks measure metadata for {missing_metadata}"
            )
        measure = _derived_measure(spec, inputs, metadata)
        values: list[Any] = []
        mask_values: list[tuple[str, ...]] = []
        reason_values: list[str | None] = []
        for position in range(len(result)):
            value, mask_ids, reason = _evaluate_cell(
                spec,
                position,
                result,
                provenance,
                reasons,
                calendar,
                geo_key=geo_key,
                time_key=time_key,
                parameters=resolved_parameters,
            )
            values.append(value)
            mask_values.append(mask_ids)
            reason_values.append(reason)
        result[output] = pd.Series(values, index=result.index, dtype=object)
        provenance[output] = pd.Series(mask_values, index=result.index, dtype=object)
        reasons[output] = pd.Series(reason_values, index=result.index, dtype=object)
        metadata[output] = measure

    state["frame"] = result
    state["mask_provenance"] = provenance
    state["value_reasons"] = reasons
    state["measures"] = {
        spec["id"]: {
            "definition": spec,
            **metadata[spec["id"]],
        }
        for spec in order
    }
    return state


def _as_mapping(value: Mapping[str, Any] | BaseModel, *, context: str) -> dict[str, Any]:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="python", by_alias=True, exclude_none=True)
    if not isinstance(value, Mapping):
        raise DerivationError(f"{context} must be a mapping")
    if any(not isinstance(key, str) for key in value):
        raise DerivationError(f"{context} keys must be strings")
    return dict(value)


def _required_string(value: Mapping[str, Any], key: str, *, context: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result.strip():
        raise DerivationError(f"{context}.{key} must be a non-empty string")
    return result


def _empty_provenance(frame: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {column: pd.Series([()] * len(frame), index=frame.index, dtype=object) for column in frame},
        index=frame.index,
    )


def _empty_reasons(frame: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            column: pd.Series([None] * len(frame), index=frame.index, dtype=object)
            for column in frame
        },
        index=frame.index,
    )


def _normalize_panel_state(panel: pd.DataFrame | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(panel, pd.DataFrame):
        frame = panel.copy(deep=True)
        return {
            "frame": frame,
            "mask_provenance": _empty_provenance(frame),
            "value_reasons": _empty_reasons(frame),
            "mask_definitions": {},
            "measures": {},
        }
    if not isinstance(panel, Mapping) or not isinstance(panel.get("frame"), pd.DataFrame):
        raise DerivationError("panel must be a DataFrame or apply_masks result")
    frame = panel["frame"].copy(deep=True)
    provenance = panel.get("mask_provenance", _empty_provenance(frame)).copy(deep=True)
    reasons = panel.get("value_reasons", _empty_reasons(frame)).copy(deep=True)
    if not provenance.index.equals(frame.index) or not reasons.index.equals(frame.index):
        raise DerivationError("panel provenance and reason rows must match frame rows and order")
    if not provenance.columns.equals(frame.columns) or not reasons.columns.equals(frame.columns):
        raise DerivationError("panel provenance and reason columns must match panel columns")
    return {
        "frame": frame,
        "mask_provenance": provenance,
        "value_reasons": reasons,
        "mask_definitions": dict(panel.get("mask_definitions", {})),
        "measures": dict(panel.get("measures", {})),
    }


def _validate_panel(frame: pd.DataFrame, *, geo_key: str, time_key: str) -> None:
    if not isinstance(frame, pd.DataFrame):
        raise DerivationError("panel must be a pandas DataFrame")
    if not geo_key or not time_key or geo_key == time_key:
        raise DerivationError("geo_key and time_key must be distinct non-empty column names")
    if not frame.columns.is_unique:
        raise DerivationError("panel column names must be unique")
    missing = [column for column in (geo_key, time_key) if column not in frame]
    if missing:
        raise DerivationError(f"panel is missing key columns: {missing}")
    if frame[[geo_key, time_key]].isna().any().any():
        raise DerivationError("panel keys must not be null")
    if frame.duplicated(subset=[geo_key, time_key]).any():
        raise DerivationError("panel key (geo_key, time_key) must be unique")
    years = frame[time_key].to_numpy(dtype=object)
    if any(isinstance(year, bool) or not isinstance(year, numbers.Integral) for year in years):
        raise DerivationError("panel time key must contain integer calendar years")


def _active_derivations(
    derivations: Sequence[Mapping[str, Any] | BaseModel], panel_name: str
) -> list[dict[str, Any]]:
    active: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_spec in derivations:
        spec = _as_mapping(raw_spec, context="derivation")
        output = _required_string(spec, "id", context="derivation")
        if output in seen:
            raise DerivationError(f"duplicate derived column {output!r}")
        seen.add(output)
        panels = spec.get("panels", [])
        if panels and panel_name not in panels:
            continue
        op = spec.get("op")
        if op not in {
            "sum",
            "ratio",
            "per_capita",
            "diff",
            "lag",
            "growth",
            "log",
            "indicator",
            "share",
        }:
            raise DerivationError(f"derivation {output!r} has unsupported op {op!r}")
        if panels and (
            not isinstance(panels, Sequence)
            or isinstance(panels, (str, bytes))
            or any(not isinstance(name, str) for name in panels)
        ):
            raise DerivationError(f"derivation {output!r} panels must be a list of strings")
        if op in {"diff", "lag", "growth"}:
            years = spec.get("years")
            if isinstance(years, bool) or not isinstance(years, numbers.Integral) or years < 1:
                raise DerivationError(f"derivation {output!r} years must be a positive integer")
        if op == "growth" and spec.get("kind") not in {"pct", "log"}:
            raise DerivationError(f"growth derivation {output!r} kind must be 'pct' or 'log'")
        spec["panels"] = list(panels)
        active.append(spec)
    return active


def _inputs(spec: Mapping[str, Any]) -> tuple[str, ...]:
    op = spec["op"]
    if op == "sum":
        raw = spec.get("of")
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)) or not raw:
            raise DerivationError(f"sum derivation {spec['id']!r} requires non-empty 'of'")
        inputs = tuple(raw)
    elif op == "ratio":
        inputs = (spec.get("num"), spec.get("den"))
    elif op == "per_capita":
        inputs = (spec.get("of"), spec.get("pop"))
    elif op in {"diff", "lag", "growth", "log", "indicator"}:
        inputs = (spec.get("of"),)
    else:
        inputs = (spec.get("of"), spec.get("total"))
    if any(not isinstance(column, str) or not column for column in inputs):
        raise DerivationError(f"derivation {spec['id']!r} has invalid input columns")
    if len(set(inputs)) != len(inputs) and op != "sum":
        raise DerivationError(f"derivation {spec['id']!r} repeats an input column")
    return inputs


def _topological_order(
    specs: list[dict[str, Any]], source_columns: set[str]
) -> list[dict[str, Any]]:
    by_output = {spec["id"]: spec for spec in specs}
    collisions = sorted(set(by_output) & source_columns)
    if collisions:
        raise DerivationError(f"derived columns collide with existing panel columns: {collisions}")
    dependencies: dict[str, set[str]] = {}
    for spec in specs:
        inputs = _inputs(spec)
        unknown = sorted(set(inputs) - source_columns - set(by_output))
        if unknown:
            raise DerivationError(
                f"derivation {spec['id']!r} references unknown columns: {unknown}"
            )
        dependencies[spec["id"]] = set(inputs) & set(by_output)

    ordered: list[dict[str, Any]] = []
    remaining = {spec["id"]: index for index, spec in enumerate(specs)}
    completed = source_columns.copy()
    while remaining:
        ready = [output for output in remaining if dependencies[output] <= completed]
        if not ready:
            raise DerivationError(f"derive dependency cycle among: {sorted(remaining)}")
        for output in sorted(ready, key=remaining.__getitem__):
            ordered.append(by_output[output])
            completed.add(output)
            del remaining[output]
    return ordered


def _normalize_measure(column: str, measure: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(measure, Mapping):
        raise DerivationError(f"measure metadata for {column!r} must be a mapping")
    units = measure.get("units")
    semantics = measure.get("aggregation_semantics")
    if not isinstance(units, str) or not units.strip():
        raise DerivationError(f"measure metadata for {column!r} requires non-empty units")
    if not isinstance(semantics, str) or semantics not in _SEMANTICS:
        raise DerivationError(
            f"measure metadata for {column!r} aggregation_semantics must be one of "
            f"{sorted(_SEMANTICS)}"
        )
    lineage = measure.get("source_lineage", [column])
    if not isinstance(lineage, Sequence) or isinstance(lineage, (str, bytes)):
        raise DerivationError(f"measure metadata for {column!r} source_lineage must be a list")
    if not lineage or any(not isinstance(item, str) or not item for item in lineage):
        raise DerivationError(
            f"measure metadata for {column!r} source_lineage must be non-empty strings"
        )
    return {
        "units": units,
        "aggregation_semantics": semantics,
        "source_lineage": list(dict.fromkeys(lineage)),
    }


def _derived_measure(
    spec: Mapping[str, Any], inputs: tuple[str, ...], metadata: Mapping[str, Mapping[str, Any]]
) -> dict[str, Any]:
    source_measures = [metadata[column] for column in inputs]
    op = spec["op"]
    if op == "sum":
        if any(measure["aggregation_semantics"] != "extensive" for measure in source_measures):
            raise DerivationError(f"sum derivation {spec['id']!r} requires extensive inputs")
        units = {measure["units"] for measure in source_measures}
        if len(units) != 1:
            raise DerivationError(f"sum derivation {spec['id']!r} requires matching input units")
        output_units, semantics = next(iter(units)), "extensive"
    elif op in {"diff", "lag"}:
        output_units = source_measures[0]["units"]
        semantics = source_measures[0]["aggregation_semantics"]
    elif op in {"ratio", "share", "per_capita"}:
        numerator, denominator = source_measures
        if op == "per_capita" and denominator["aggregation_semantics"] != "extensive":
            raise DerivationError(
                f"per_capita derivation {spec['id']!r} requires an extensive population input"
            )
        output_units = (
            "1"
            if numerator["units"] == denominator["units"]
            else f"{numerator['units']}/{denominator['units']}"
        )
        semantics = "rate"
    elif op == "growth":
        output_units, semantics = "1", "rate"
    else:
        output_units, semantics = "1", "intensive"
    lineage = list(
        dict.fromkeys(item for measure in source_measures for item in measure["source_lineage"])
    )
    return {
        "inputs": list(inputs),
        "units": output_units,
        "aggregation_semantics": semantics,
        "source_lineage": lineage,
    }


def _calendar_index(
    frame: pd.DataFrame, *, geo_key: str, time_key: str
) -> dict[tuple[Any, int], int]:
    return {
        (row[geo_key], int(row[time_key])): position
        for position, row in enumerate(frame[[geo_key, time_key]].to_dict(orient="records"))
    }


def _resolve_numeric(value: Any, parameters: Mapping[str, Any], *, context: str) -> int | float:
    if isinstance(value, Mapping):
        if set(value) != {"param"}:
            raise DerivationError(f"{context} parameter reference must contain only 'param'")
        name = value["param"]
        if not isinstance(name, str) or name not in parameters:
            raise DerivationError(f"{context} references unknown parameter {name!r}")
        value = parameters[name]
    if isinstance(value, Mapping):
        raise DerivationError(f"{context} contains an unresolved marker")
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise DerivationError(f"{context} must be a finite number")
    try:
        finite = math.isfinite(value)
    except (OverflowError, TypeError, ValueError):
        finite = False
    if not finite:
        raise DerivationError(f"{context} must be a finite number")
    return value


def _is_null(value: Any) -> bool:
    return value is None or value is pd.NA


def _is_finite(value: Any) -> bool:
    try:
        return math.isfinite(value)
    except (OverflowError, TypeError, ValueError):
        return False


def _number(value: Any, *, column: str, row_position: int) -> int | float:
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise DerivationError(
            f"column {column!r} contains a non-numeric value at row position {row_position}"
        )
    return value


def _mask_ids(provenance: pd.DataFrame, column: str, position: int) -> tuple[str, ...]:
    value = provenance[column].iat[position]
    if isinstance(value, tuple) and all(isinstance(mask_id, str) for mask_id in value):
        return value
    if value is None or value is pd.NA:
        return ()
    raise DerivationError(f"mask provenance for {column!r} must contain tuples of mask IDs")


def _reason(reasons: pd.DataFrame, column: str, position: int) -> str | None:
    value = reasons[column].iat[position]
    if value is None or value is pd.NA:
        return None
    if not isinstance(value, str):
        raise DerivationError(f"value reason for {column!r} must be a string or null")
    return value


def _null_from_inputs(
    output: str,
    inputs: Sequence[tuple[str, int]],
    provenance: pd.DataFrame,
    reasons: pd.DataFrame,
    *,
    default_reason: str,
) -> tuple[Any, tuple[str, ...], str]:
    mask_ids = tuple(
        dict.fromkeys(
            mask_id
            for column, position in inputs
            for mask_id in _mask_ids(provenance, column, position)
        )
    )
    if mask_ids:
        return pd.NA, mask_ids, f"masked:{output}"
    inherited = next(
        (
            source_reason
            for column, position in inputs
            if (source_reason := _reason(reasons, column, position)) is not None
        ),
        None,
    )
    return pd.NA, (), inherited or default_reason


def _evaluate_cell(
    spec: Mapping[str, Any],
    position: int,
    frame: pd.DataFrame,
    provenance: pd.DataFrame,
    reasons: pd.DataFrame,
    calendar: Mapping[tuple[Any, int], int],
    *,
    geo_key: str,
    time_key: str,
    parameters: Mapping[str, Any],
) -> tuple[Any, tuple[str, ...], str | None]:
    output = spec["id"]
    op = spec["op"]
    row_inputs: list[tuple[str, int]] = [(column, position) for column in _inputs(spec)]
    if op in {"lag", "diff", "growth"}:
        year = int(frame[time_key].iat[position])
        geography = frame[geo_key].iat[position]
        prior_position = calendar.get((geography, year - int(spec["years"])))
        if prior_position is None:
            missing_inputs = [(spec["of"], position)] if op != "lag" else []
            if missing_inputs:
                value, mask_ids, reason = _null_from_inputs(
                    output,
                    missing_inputs,
                    provenance,
                    reasons,
                    default_reason=f"missing_baseline:{spec['of']}",
                )
                if mask_ids or reason != f"missing_baseline:{spec['of']}":
                    return value, mask_ids, reason
            return pd.NA, (), f"missing_baseline:{spec['of']}"
        row_inputs = [(spec["of"], prior_position)]
        if op in {"diff", "growth"}:
            row_inputs.insert(0, (spec["of"], position))

    values = [
        (column, row_position, frame[column].iat[row_position])
        for column, row_position in row_inputs
    ]
    nulls = [(column, row_position) for column, row_position, value in values if _is_null(value)]
    if op == "sum" and not spec.get("require_all", False):
        available = [item for item in values if not _is_null(item[2])]
        if not available:
            return _null_from_inputs(
                output,
                [(column, row_position) for column, row_position, _value in values],
                provenance,
                reasons,
                default_reason=f"missing_input:{values[0][0]}",
            )
        values = available
        nulls = []
    if nulls:
        return _null_from_inputs(
            output,
            nulls,
            provenance,
            reasons,
            default_reason=f"missing_input:{nulls[0][0]}",
        )

    numeric = [
        (column, row_position, _number(value, column=column, row_position=row_position))
        for column, row_position, value in values
    ]
    operands = [value for _column, _row, value in numeric]
    all_finite = all(_is_finite(value) for value in operands)
    try:
        if op == "sum":
            result = sum(operands)
        elif op == "ratio" or op == "share":
            numerator, denominator = operands
            if denominator == 0:
                if spec.get("zero_den", "null") == "error":
                    raise DerivationError(f"derivation {output!r} encountered a zero denominator")
                return pd.NA, (), "domain:zero_denominator"
            result = numerator / denominator
        elif op == "per_capita":
            numerator, population = operands
            if population <= 0:
                return pd.NA, (), "domain:nonpositive_population"
            scale = _resolve_numeric(spec["scale"], parameters, context=f"{output}.scale")
            result = numerator / population * scale
        elif op == "lag":
            result = operands[0]
        elif op == "diff":
            result = operands[0] - operands[1]
        elif op == "growth":
            current, baseline = operands
            if spec["kind"] == "pct":
                if baseline == 0:
                    return pd.NA, (), "domain:zero_growth_baseline"
                result = current / baseline - 1
            else:
                if current <= 0 or baseline <= 0:
                    return pd.NA, (), "domain:nonpositive_growth_endpoint"
                result = math.log(current) - math.log(baseline)
        elif op == "log":
            value = operands[0] + _resolve_numeric(
                spec.get("offset", 0), parameters, context=f"{output}.offset"
            )
            if value <= 0:
                return pd.NA, (), "domain:log_nonpositive"
            result = math.log(value)
        elif op == "indicator":
            value = operands[0]
            if not _is_finite(value):
                return value, (), None
            if "in" in spec:
                # RecipeV2's closed model restricts membership values to finite
                # numeric literals or numeric parameter references.
                choices = [
                    _resolve_numeric(item, parameters, context=f"{output}.in")
                    for item in spec["in"]
                ]
                result = int(value in choices)
            elif "gte" in spec:
                threshold = _resolve_numeric(spec["gte"], parameters, context=f"{output}.gte")
                result = int(value >= threshold)
            elif "lte" in spec:
                threshold = _resolve_numeric(spec["lte"], parameters, context=f"{output}.lte")
                result = int(value <= threshold)
            else:
                raise DerivationError(f"indicator derivation {output!r} requires one condition")
        else:
            raise DerivationError(f"unsupported derivation op {op!r}")
    except OverflowError:
        return pd.NA, (), "domain:nonfinite_arithmetic"

    if all_finite and not _is_finite(result):
        return pd.NA, (), "domain:nonfinite_arithmetic"
    return result, (), None
