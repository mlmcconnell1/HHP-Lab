"""Dispatch preserves result identity and isolates numerical failures."""

import pandas as pd
import pytest

from hhplab.analysis.contracts import ResolvedSampleFrame
from hhplab.analysis.sample import resolve_sample
from hhplab.recipe.executor.analyses import execute_analysis
from hhplab.recipe.loader import load_recipe
from hhplab.recipe.results_plan import plan_results

PANEL = {
    "geo": ["a", "a", "b", "b", "c", "c"],
    "year": [2020, 2021, 2020, 2021, 2020, 2021],
    "x": [1.0, 3.0, 2.0, 6.0, 4.0, 5.0],
    "y": [2.0, 5.0, 3.0, 9.0, 7.0, 6.0],
}
SPECS = {
    "regression": {"outcome": "y", "predictors": ["x"], "inference": {"method": "hc1"}},
    "descriptive": {"columns": ["x", "y"], "stats": ["mean", "sd"]},
    "correlation": {"columns": ["x", "y"], "kind": "spearman"},
}


def _sample():
    frame = pd.DataFrame(PANEL)
    return ResolvedSampleFrame(frame, tuple(zip(frame.geo, frame.year, strict=True)), "geo", "year")


def _plan(method):
    return {
        "analysis_id": "model",
        "variant_id": "renamed",
        "result_key": "model@renamed",
        "sample_source": "shared@base",
        "sample_group_id": "shared",
        "effective_spec": {"method": method, **SPECS.get(method, {})},
        "base_term_by_effective_term": {"x": "original_x", "y": "y"},
    }


@pytest.mark.parametrize("method", tuple(SPECS))
def test_all_phase_one_methods(method):
    result = execute_analysis(_plan(method), _sample())
    assert result["status"] == "complete", result
    assert result["result_key"] == "model@renamed"
    assert result["sample_source"] == "shared@base"
    if method == "regression":
        slope = next(row for row in result["regression_estimates"] if row["term"] == "x")
        assert slope["base_term"] == "original_x"


def test_unsupported_capability_is_explicit():
    result = execute_analysis(_plan("lagged_association"), _sample())
    assert result["status"] == "failed"
    assert result["error"]["code"] == "capability_unavailable"


def test_numerical_failure_is_scoped():
    bad = _plan("regression")
    bad["effective_spec"]["predictors"] = ["missing"]
    assert execute_analysis(bad, _sample())["status"] == "failed"
    assert execute_analysis(_plan("regression"), _sample())["status"] == "complete"


def test_consumed_identity_is_checked(monkeypatch):
    from hhplab.recipe.executor import analyses

    original = analyses.correlate_resolved_sample

    def altered(*args, **kwargs):
        table = original(*args, **kwargs)
        table.attrs["row_keys"] = (("wrong", 2020),) * len(PANEL["geo"])
        return table

    monkeypatch.setattr(analyses, "correlate_resolved_sample", altered)
    result = execute_analysis(_plan("correlation"), _sample())
    assert result["status"] == "failed"
    assert result["error"]["code"] == "sample_contract_violation"


def test_planner_sample_dispatch_integration():
    recipe = load_recipe(
        {
            "version": 2,
            "id": "dispatch-integration",
            "title": "Integration",
            "authors": ["test"],
            "universe": {"range": "2020-2021"},
            "targets": [{"id": "target", "geometry": {"type": "coc", "vintage": 2025}}],
            "datasets": {},
            "pipelines": [{"id": "build", "target": "target", "steps": []}],
            "panels": {
                "panel": {
                    "ref": {"pipeline": "build", "output": "panel"},
                    "geo_key": "geo",
                    "time_key": "year",
                }
            },
            "analyses": [
                {
                    "id": "reg",
                    "method": "regression",
                    "input": "panel",
                    "sample": {},
                    "outcome": "y",
                    "predictors": ["x"],
                }
            ],
            "variants": [
                {
                    "id": "robust",
                    "applies_to": ["reg"],
                    "override": {"inference": {"method": "hc1"}},
                }
            ],
            "results": {
                "bundle_dir": "results/{id}/{run_id}",
                "include_panels": ["panel"],
                "report": {"title": "Integration"},
            },
        }
    )
    plan = plan_results(recipe, original_recipe_bytes=b"synthetic integration fixture")
    samples = {}
    for analysis in plan.analyses:
        normalized = analysis.to_dict()
        resolved = resolve_sample(
            pd.DataFrame(PANEL),
            normalized,
            base_membership=samples.get(analysis.base_membership_key),
        )
        samples[analysis.result_key] = resolved
        result = execute_analysis(normalized, resolved["resolved"])
        assert result["status"] == "complete", result
        assert result["regression_fit"]["sample_source"] == analysis.sample_source
