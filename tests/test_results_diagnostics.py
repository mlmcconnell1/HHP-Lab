"""Declared identities report discrepancy separately from unavailable inputs."""

from copy import deepcopy

import pandas as pd
import pytest

from hhplab.results.diagnostics import column_identity, linear_identity
from hhplab.schema.results import Tolerance, canonical_bytes

ANALYSES = ("a", "b", "total")
ESTIMATES = {"a": 2.0, "b": 3.0, "total": 5.0}
LINEAR = {
    "id": "additivity",
    "require_same_design": True,
    "terms": [
        {"analysis": name, "term": "x", "sign": -1 if name == "total" else 1} for name in ANALYSES
    ],
}
COLUMN = {"id": "components", "lhs": "total", "rhs": ["a", "b"], "rows": "unmasked"}


def _evidence(variant="base"):
    results = {
        f"{name}@{variant}": {
            "status": "complete",
            "regression_estimates": [{"base_term": "x", "estimate": value}],
        }
        for name, value in ESTIMATES.items()
    }
    design = {
        "sample_group": "shared",
        "regressors": ["x"],
        "fixed_effects": ["geo"],
        "rows": [{"key": ["a", 2020], "regressors": [1.0], "fixed_effects": ["a"], "weight": 1.0}],
    }
    return results, {key: deepcopy(design) for key in results}


@pytest.mark.parametrize("variant", ["base", "lagged", "odd_years"])
def test_additivity_all_applicable_variants(variant):
    results, designs = _evidence(variant)
    record = linear_identity(
        LINEAR, variant=variant, applicable_analyses=ANALYSES, results=results, designs=designs
    )
    assert record.status == "pass"
    assert record.value == 0.0
    canonical_bytes(record.to_dict())


def test_subset_is_not_applicable_before_missing_dependency():
    record = linear_identity(
        LINEAR, variant="hc1", applicable_analyses=["a"], results={}, designs={}
    )
    assert record.status == "not_applicable"


@pytest.mark.parametrize(
    "change,expected",
    [
        ("total", "fail"),
        ("failed", "not_evaluated"),
        ("design", "not_evaluated"),
        ("nan", "not_evaluated"),
    ],
)
def test_identity_failures(change, expected):
    results, designs = _evidence()
    if change == "total":
        results["total@base"]["regression_estimates"][0]["estimate"] += 1
    elif change == "failed":
        results["a@base"]["status"] = "failed"
    elif change == "design":
        designs["a@base"]["rows"][0]["regressors"] = [2.0]
    else:
        results["a@base"]["regression_estimates"][0]["estimate"] = float("nan")
    record = linear_identity(
        LINEAR, variant="base", applicable_analyses=ANALYSES, results=results, designs=designs
    )
    assert record.status == expected
    canonical_bytes(record.to_dict())


@pytest.mark.parametrize("residual,expected", [(5e-11, "pass"), (2e-10, "fail")])
def test_zero_target_uses_absolute_tolerance(residual, expected):
    results, designs = _evidence()
    results["total@base"]["regression_estimates"][0]["estimate"] += residual
    assert (
        linear_identity(
            LINEAR,
            variant="base",
            applicable_analyses=ANALYSES,
            results=results,
            designs=designs,
            tolerance=Tolerance(),
        ).status
        == expected
    )


def _panel():
    return pd.DataFrame(
        {
            "geo": ["a", "b"],
            "year": [2020, 2020],
            "a": [2.0, 1.0],
            "b": [3.0, 2.0],
            "total": [5.0, 9.0],
        }
    )


def test_column_discrepancy_and_group_scope():
    panel = _panel()
    record = column_identity(COLUMN, panel, geo_key="geo", time_key="year")
    assert record.status == "fail"
    assert record.details["discrepant_rows"] == 1
    assert record.details["example_keys"] == [["b", 2020]]
    scoped = column_identity(
        {**COLUMN, "rows": {"group": "g"}},
        panel,
        geo_key="geo",
        time_key="year",
        group_keys=[("a", 2020)],
    )
    assert scoped.status == "pass"


def test_unmasked_scope_and_undefined_values():
    panel = _panel()
    panel.loc[1, "total"] = float("nan")
    assert column_identity(COLUMN, panel, geo_key="geo", time_key="year").status == "not_evaluated"
    masks = pd.DataFrame({col: [(), ()] for col in panel}, index=panel.index)
    masks.at[1, "total"] = ("mask",)
    assert (
        column_identity(COLUMN, panel, geo_key="geo", time_key="year", mask_provenance=masks).status
        == "pass"
    )


def test_integer_identity_is_exact_above_float_precision():
    panel = pd.DataFrame({"geo": ["a"], "year": [2020], "a": [2**54], "b": [1], "total": [2**54]})
    record = column_identity(COLUMN, panel, geo_key="geo", time_key="year")
    assert record.status == "fail"
    assert record.value == 1
    assert record.tolerance == 0


def test_discrepant_keys_are_bounded():
    panel = pd.DataFrame(
        {
            "geo": [str(i) for i in range(25)],
            "year": [2020] * 25,
            "a": [1] * 25,
            "b": [1] * 25,
            "total": [3] * 25,
        }
    )
    record = column_identity(COLUMN, panel, geo_key="geo", time_key="year")
    assert record.details["discrepant_rows"] == 25
    assert len(record.details["example_keys"]) == 20
