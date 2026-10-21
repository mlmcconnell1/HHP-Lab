"""Closed identity diagnostics over explicit result and design evidence."""

from __future__ import annotations

import math
import numbers
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from hhplab.schema.results import DiagnosticRecord, DiagnosticStatus, Tolerance, canonical_bytes

DEFAULT_TOLERANCE = Tolerance()


def _record(identifier, variant, status, value, tolerance, reason, details):
    return DiagnosticRecord(
        identifier, variant, DiagnosticStatus(status), value, tolerance, reason, details
    )


def _finite(value: Any) -> bool:
    return isinstance(value, numbers.Real) and not isinstance(value, bool) and math.isfinite(value)


def _design_signature(evidence: Mapping[str, Any]) -> bytes:
    """Compare actual keyed design values, independent of row ordering."""
    columns = tuple(evidence["regressors"])
    fixed_effects = tuple(evidence["fixed_effects"])
    rows = evidence["rows"]
    keys = [canonical_bytes(row["key"]) for row in rows]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate design evidence keys")
    expected = {"key", "regressors", "fixed_effects", "weight"}
    for row in rows:
        if set(row) != expected:
            raise ValueError("incomplete actual design evidence")
        if len(row["regressors"]) != len(columns) or len(row["fixed_effects"]) != len(
            fixed_effects
        ):
            raise ValueError("design evidence dimensions do not match")
        if not _finite(row["weight"]) or row["weight"] <= 0:
            raise ValueError("invalid design evidence weight")
        if not all(_finite(value) for value in row["regressors"]):
            raise ValueError("nonfinite design evidence regressor")
    return canonical_bytes(
        {
            "sample_group": evidence["sample_group"],
            "regressors": columns,
            "fixed_effects": fixed_effects,
            "rows": sorted(rows, key=lambda row: canonical_bytes(row["key"])),
        }
    )


def linear_identity(
    declaration: Mapping[str, Any],
    *,
    variant: str,
    applicable_analyses: Sequence[str],
    results: Mapping[str, Mapping[str, Any]],
    designs: Mapping[str, Mapping[str, Any]],
    tolerance: Tolerance = DEFAULT_TOLERANCE,
) -> DiagnosticRecord:
    """Evaluate signed coefficient sums using base_term result references.

    Results map ``analysis@variant`` to status and regression estimate rows.
    Designs contain actual keys, regressors, FE values and normalized weights;
    claimed hashes or sample counts alone cannot establish same design.
    """
    identifier = declaration["id"]
    terms = declaration["terms"]
    analyses = {term["analysis"] for term in terms}
    details: dict[str, Any] = {"atol": tolerance.atol, "rtol": tolerance.rtol}
    if not analyses.issubset(applicable_analyses):
        return _record(
            identifier,
            variant,
            "not_applicable",
            None,
            None,
            "variant_does_not_apply_to_all_analyses",
            details,
        )
    keys = sorted(f"{analysis}@{variant}" for analysis in analyses)
    missing = [key for key in keys if results.get(key, {}).get("status") != "complete"]
    if missing:
        details["failed_result_keys"] = missing
        return _record(
            identifier, variant, "not_evaluated", None, None, "missing_or_failed_analysis", details
        )
    if declaration.get("require_same_design", False):
        try:
            signatures = [_design_signature(designs[key]) for key in keys]
        except (KeyError, TypeError, ValueError) as exc:
            details["design_error"] = str(exc)
            return _record(
                identifier,
                variant,
                "not_evaluated",
                None,
                None,
                "missing_or_invalid_design_evidence",
                details,
            )
        if len(set(signatures)) != 1:
            return _record(
                identifier, variant, "not_evaluated", None, None, "design_mismatch", details
            )
    values = []
    for term in terms:
        key = f"{term['analysis']}@{variant}"
        matches = [
            row
            for row in results[key].get("regression_estimates", ())
            if row.get("base_term") == term["term"]
        ]
        if len(matches) != 1 or not _finite(matches[0].get("estimate")):
            details["input"] = {"result_key": key, "base_term": term["term"]}
            return _record(
                identifier,
                variant,
                "not_evaluated",
                None,
                None,
                "missing_ambiguous_or_nonfinite_coefficient",
                details,
            )
        value = term["sign"] * matches[0]["estimate"]
        if not _finite(value):
            return _record(
                identifier,
                variant,
                "not_evaluated",
                None,
                None,
                "nonfinite_signed_coefficient",
                details,
            )
        values.append(value)
    try:
        residual = math.fsum(values)
    except (ValueError, OverflowError):
        residual = float("nan")
    if not math.isfinite(residual):
        return _record(
            identifier, variant, "not_evaluated", None, None, "nonfinite_identity_sum", details
        )
    details["residual"] = residual
    return _record(
        identifier,
        variant,
        "pass" if abs(residual) <= tolerance.atol else "fail",
        residual,
        tolerance.atol,
        None,
        details,
    )


def column_identity(
    declaration: Mapping[str, Any],
    panel: pd.DataFrame,
    *,
    geo_key: str,
    time_key: str,
    mask_provenance: pd.DataFrame | None = None,
    group_keys: Sequence[tuple[Any, Any]] | None = None,
    variant: str = "base",
    tolerance: Tolerance = DEFAULT_TOLERANCE,
) -> DiagnosticRecord:
    """Evaluate row identities over declared unmasked rows or exact group keys."""
    identifier = declaration["id"]
    columns = [declaration["lhs"], *declaration["rhs"]]
    details: dict[str, Any] = {"atol": tolerance.atol, "rtol": tolerance.rtol}
    missing = set([geo_key, time_key, *columns]) - set(panel.columns)
    if missing:
        details["missing_columns"] = sorted(missing)
        return _record(identifier, variant, "not_evaluated", None, None, "missing_columns", details)
    if not panel.columns.is_unique or panel.duplicated([geo_key, time_key]).any():
        return _record(
            identifier,
            variant,
            "not_evaluated",
            None,
            None,
            "invalid_panel_keys_or_columns",
            details,
        )
    if mask_provenance is not None and (
        not mask_provenance.index.equals(panel.index)
        or not mask_provenance.columns.equals(panel.columns)
    ):
        raise ValueError("mask provenance must match panel rows and columns")
    keys = list(zip(panel[geo_key], panel[time_key], strict=True))
    if declaration.get("rows", "unmasked") == "unmasked":
        positions = [
            i
            for i in range(len(panel))
            if mask_provenance is None
            or not any(bool(mask_provenance[column].iat[i]) for column in columns)
        ]
    else:
        if group_keys is None or not set(group_keys).issubset(keys):
            return _record(
                identifier,
                variant,
                "not_evaluated",
                None,
                None,
                "missing_sample_group_membership",
                details,
            )
        selected = set(group_keys)
        positions = [i for i, key in enumerate(keys) if key in selected]
    if not positions:
        return _record(identifier, variant, "not_evaluated", None, None, "empty_scope", details)
    discrepancies = []
    residuals = []
    limits = []
    for i in positions:
        values = [panel[column].iat[i] for column in columns]
        if not all(_finite(value) for value in values):
            return _record(
                identifier,
                variant,
                "not_evaluated",
                None,
                None,
                "nonfinite_or_undefined_column_input",
                details,
            )
        try:
            if all(isinstance(value, numbers.Integral) for value in values):
                rhs = sum(int(value) for value in values[1:])
                residual = int(values[0]) - rhs
                limit = 0.0
            else:
                rhs = math.fsum(values[1:])
                residual = float(values[0]) - rhs
                limit = tolerance.atol + tolerance.rtol * abs(rhs)
        except (ValueError, OverflowError):
            residual = limit = float("nan")
        if not math.isfinite(residual) or not math.isfinite(limit):
            return _record(
                identifier,
                variant,
                "not_evaluated",
                None,
                None,
                "nonfinite_column_operation",
                details,
            )
        if abs(residual) > limit:
            discrepancies.append(keys[i])
        residuals.append(abs(residual))
        limits.append(limit)
    details.update(
        {
            "rows_checked": len(positions),
            "discrepant_rows": len(discrepancies),
            "example_keys": [list(key) for key in discrepancies[:20]],
            "max_absolute_residual": max(residuals),
        }
    )
    return _record(
        identifier,
        variant,
        "fail" if discrepancies else "pass",
        max(residuals),
        max(limits),
        None,
        details,
    )
