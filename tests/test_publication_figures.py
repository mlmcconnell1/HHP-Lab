"""Synthetic tests for deterministic, permission-fed publication figures."""

from __future__ import annotations

import csv
import io
import xml.etree.ElementTree as ET

import pytest

from hhplab.results.figures import FigureError, _render_estimate_svg, render_research_assets


def _render(*, finding_ids=("effect",)):
    estimate_rows = (
        {
            "analysis": "fit",
            "variant": "base",
            "term": "x",
            "base_term": "x",
            "estimate": 1.0,
            "se": 0.2,
            "ci_level": 0.95,
            "ci_lo": 0.6,
            "ci_hi": 1.4,
            "p": 0.01,
        },
    )
    fit_rows = ({"analysis": "fit", "variant": "base", "n": 36, "n_clusters": 12, "r2": 0.8},)
    findings = {
        finding_id: {
            "claim": "Estimate for x",
            "variants": [
                {
                    "id": "base",
                    "description": "=formula text",
                    "evaluation_status": "evaluated",
                    "verdict": "supports_claim",
                    "agrees": True,
                    "unavailable_reason": None,
                }
            ],
        }
        for finding_id in finding_ids
    }
    declarations = {
        finding_id: {
            "reference_values": [0.0],
            "bands": [{"label": "similar", "equivalent_to": {"value": 1.0, "margin": 0.2}}],
        }
        for finding_id in finding_ids
    }
    references = [
        {
            "owner_id": finding_id,
            "result_key": "fit@base",
            "base_term": "x",
            "term": "x",
            "stat": "estimate",
        }
        for finding_id in finding_ids
    ]
    return render_research_assets(
        figure_findings=tuple(finding_ids),
        findings=findings,
        declarations=declarations,
        references=references,
        analyses={"fit@base": {"panel_name": "panel", "effective_spec": {"outcome": "y"}}},
        parameters={},
        rows=lambda table: estimate_rows if table == "regression_estimates" else fit_rows,
        optional_rows=lambda table: estimate_rows if table == "regression_estimates" else fit_rows,
        unit_for=lambda _field, _key, _term: "people",
        escape_text=lambda value: value,
    )


def test_research_assets_are_repeatable_and_tables_have_traceable_csv():
    first = _render()
    second = _render()
    assert first.assets == second.assets
    assert {
        "data/effect-estimates.csv",
        "data/research-coefficients.csv",
        "data/research-fit.csv",
        "data/research-variants.csv",
    } <= set(first.assets)
    coefficient_csv = first.assets["data/research-coefficients.csv"].decode()
    coefficient = list(csv.DictReader(io.StringIO(coefficient_csv)))
    assert coefficient[0]["estimate"] == "1"
    assert coefficient[0]["row_address"] == (
        "results/regression_estimates.parquet?analysis=fit&variant=base&term=x&base_term=x"
    )
    variant = list(csv.DictReader(io.StringIO(first.assets["data/research-variants.csv"].decode())))
    assert variant[0]["description"] == "'=formula text"
    assert variant[0]["row_address"] == "findings.json#finding=effect&variant=base"
    artifact_paths = {artifact["path"] for artifact in first.artifacts}
    assert set(first.assets) <= artifact_paths
    svg = ET.fromstring(first.assets["figures/effect-estimates.svg"])
    assert svg.attrib["role"] == "img"
    assert "1.00" in " ".join(element.text or "" for element in svg.iter())
    assert all(
        not value or value.startswith("#")
        for element in svg.iter()
        for key, value in element.attrib.items()
        if key.endswith("href")
    )


def test_asset_name_collision_fails_closed():
    with pytest.raises(FigureError, match="same public asset name"):
        _render(finding_ids=("a/b", "a b"))


def test_variant_substituting_a_term_uses_a_separate_axis():
    base = {
        "panel_name": "panel", "outcome": "y", "base_term": "x", "term": "x", "variant": "base",
        "units": "people", "estimate": 1.0, "ci_level": 0.95, "ci_lo": 0.6, "ci_hi": 1.4,
    }
    substituted = {**base, "term": "lag_x", "variant": "lag"}

    svg, description = _render_estimate_svg("effect", {}, {}, (base, substituted), (), ())

    assert b' id="axes_2"' in svg
    assert "Separate panels preserve different outcomes, terms, or effect scales." in description


def test_publication_callback_errors_keep_their_original_type():
    class PublicationDenied(Exception):
        pass

    with pytest.raises(PublicationDenied, match="table denied"):
        render_research_assets(
            figure_findings=(),
            findings={},
            declarations={},
            references=(),
            analyses={},
            parameters={},
            rows=lambda _table: (),
            optional_rows=lambda _table: (_ for _ in ()).throw(PublicationDenied("table denied")),
            unit_for=lambda _field, _key, _term: None,
            escape_text=lambda value: value,
        )
