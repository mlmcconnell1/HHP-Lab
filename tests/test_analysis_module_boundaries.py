"""Focused contracts for the decomposed panel-analysis modules."""

from __future__ import annotations

from importlib import import_module

import pytest

from hhplab.analysis import analyze

PUBLIC_ANALYSIS_OWNERS = [
    pytest.param("describe_panel", "hhplab.analysis.inspection", id="inspection"),
    pytest.param("correlate_panel", "hhplab.analysis.correlation", id="correlation"),
    pytest.param("regress_panel", "hhplab.analysis.regression", id="regression"),
    pytest.param(
        "anderson_rubin_confidence_set",
        "hhplab.analysis.iv",
        id="instrumental-variable",
    ),
    pytest.param("lagged_associations_panel", "hhplab.analysis.lagged", id="lagged"),
    pytest.param("read_analysis_manifest", "hhplab.analysis.persistence", id="persistence"),
]


@pytest.mark.parametrize(("name", "owner"), PUBLIC_ANALYSIS_OWNERS)
def test_analysis_facade_reexports_focused_owner(name: str, owner: str) -> None:
    facade_export = getattr(analyze, name)
    owned_export = getattr(import_module(owner), name)

    assert facade_export is owned_export
    assert facade_export.__module__ == owner


def test_analysis_facade_declares_stable_public_exports() -> None:
    assert analyze.__all__ == [
        "AnalysisError",
        "AnalysisResult",
        "InferenceMethod",
        "anderson_rubin_confidence_set",
        "correlate_panel",
        "describe_panel",
        "lagged_associations_panel",
        "list_analysis_manifests",
        "read_analysis_manifest",
        "regress_panel",
    ]


@pytest.mark.parametrize(
    ("name", "owner"),
    [
        pytest.param("_fit_ols", "hhplab.analysis.estimation", id="ols"),
        pytest.param("_fit_2sls", "hhplab.analysis.estimation", id="two-stage-least-squares"),
        pytest.param(
            "_wild_cluster_bootstrap_p_values",
            "hhplab.analysis.resampling",
            id="wild-cluster",
        ),
    ],
)
def test_legacy_private_estimator_imports_remain_compatible(name: str, owner: str) -> None:
    assert getattr(analyze, name) is getattr(import_module(owner), name)
