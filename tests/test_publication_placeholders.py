"""Strict, permission-scoped narrative placeholder rendering."""

from collections.abc import Mapping, Sequence
from typing import Any

import pytest

from hhplab.results.placeholders import (
    PlaceholderError,
    escape_markdown_text,
    render_narrative,
)

RESULT_KEY = "substitution@base"
ESTIMATE_ROWS = (
    {
        "analysis": "substitution",
        "variant": "base",
        "term": "beds",
        "base_term": "beds",
        "estimate": -0.43,
        "ci_lo": -0.61,
        "ci_hi": -0.25,
        "se": 0.09,
        "p": 0.02,
        "p_status": "evaluated",
        "p_reason": None,
    },
)
FIT_ROWS = (
    {
        "analysis": "substitution",
        "variant": "base",
        "n": 120,
        "n_clusters": 15,
        "r2": 0.42,
        "r2_status": "evaluated",
        "r2_reason": None,
        "r2_within": 0.27,
        "r2_within_status": "evaluated",
        "r2_within_reason": None,
    },
)
CORRELATION_ROWS = (
    {
        "analysis": "substitution",
        "variant": "base",
        "col_a": "beds",
        "col_b": "unsheltered",
        "r": -0.43,
        "status": "evaluated",
        "reason": None,
    },
)
TABLES: Mapping[str, Sequence[Mapping[str, Any]]] = {
    "regression_estimates": ESTIMATE_ROWS,
    "regression_fit": FIT_ROWS,
    "correlations": CORRELATION_ROWS,
}
EVALUATIONS = {
    ("substitution", "base"): {
        "verdict": "partly_supports_claim",
        "verdict_text": "The evidence partly supports the claim.",
        "label": "partial_substitution",
        "compatible_with": [-1.0, 0.0],
        "compatible_text": "Compatible with full or no substitution.",
    },
    ("substitution", "lagged_beds"): {
        "verdict": None,
        "verdict_text": None,
        "label": None,
        "compatible_with": [],
        "compatible_text": None,
        "status": "not_evaluated",
        "reason": "raw internal failure detail must stay private",
    },
}
ALLOWED_ESTIMATES = frozenset({(RESULT_KEY, "beds")})


def _rows(table: str) -> Sequence[Mapping[str, Any]]:
    return TABLES[table]


def _unit(field: str, result_key: str, base_term: str | None) -> str | None:
    if field in {"p", "r2", "r2_within"}:
        return "proportion"
    if field == "r":
        return "correlation"
    return "persons per bed" if base_term == "beds" else None


def _render(narrative: str, **changes: Any):
    arguments = {
        "rows": _rows,
        "allowed_estimates": ALLOWED_ESTIMATES,
        "evaluations": EVALUATIONS,
        "parameters": {"delta_theta": 0.25},
        "unit_for": _unit,
    }
    arguments.update(changes)
    return render_narrative(narrative, **arguments)


def test_render_resolves_estimate_fit_finding_and_parameter_with_provenance():
    result = _render(
        "Estimate {{ estimate: substitution.beds }}, interval "
        "{{ estimate: substitution.beds | ci }}; "
        "R-squared {{ r2: substitution }}. {{ verdict_text: substitution }} "
        "Margin {{ margin: param.delta_theta }}"
    )

    assert "-0.43" in result.markdown
    assert r"\-0.43 \[\-0.61, \-0.25\]" in result.markdown
    assert "0.42" in result.markdown
    assert "The evidence partly supports the claim." in result.markdown
    assert "0.25" in result.markdown
    assert len(result.placeholders) == 5
    assert result.placeholders[3]["source_address"] == (
        "findings.json#finding=substitution&variant=base"
    )
    assert result.placeholders[4]["source_address"] == (
        "recipe/resolved.json#/plan/parameters/delta_theta"
    )
    assert result.placeholders[0] == {
        "placeholder": "{{ estimate: substitution.beds }}",
        "field": "estimate",
        "address": "substitution.beds",
        "source_address": (
            "results/regression_estimates.parquet#analysis=substitution"
            "&variant=base&base_term=beds"
        ),
        "format": "round:2",
        "raw_value": -0.43,
        "rendered": r"\-0.43",
    }


@pytest.mark.parametrize(
    "placeholder,expected",
    [
        ("{{ p: substitution.beds | pct }}", "2.00%"),
        ("{{ estimate: substitution.beds | abs }}", "0.43"),
        ("{{ estimate: substitution.beds | signed }}", "-0.43"),
        ("{{ estimate: substitution.beds | round:1 }}", "-0.4"),
    ],
)
def test_numeric_formats(placeholder, expected):
    result = _render(placeholder)
    assert expected in result.markdown


def test_int_format_rejects_non_integral_value():
    result = _render("{{ n: substitution | int }}")
    assert result.markdown == "120"


def test_percentage_format_requires_proportion_units():
    with pytest.raises(PlaceholderError) as error:
        _render("{{ r: substitution.beds~unsheltered | pct }}")
    assert error.value.code == "invalid_units"


def test_compatible_with_renders_safe_resolved_text_and_variant_address():
    result = _render("{{ compatible_with: substitution@base }}")
    assert result.markdown == "Compatible with full or no substitution."

    with pytest.raises(PlaceholderError) as error:
        _render("{{ verdict: substitution@lagged_beds }}")
    assert error.value.code == "missing_result"
    assert error.value.reason == "finding_status: not_evaluated"
    assert "raw internal failure" not in str(error.value)


def test_exact_result_rows_and_cited_dependencies_are_required():
    with pytest.raises(PlaceholderError) as missing:
        _render("{{ estimate: other_model.beds }}")
    assert missing.value.code == "disallowed_reference"

    with pytest.raises(PlaceholderError) as ambiguous:
        _render(
            "{{ estimate: substitution.beds }}",
            rows=lambda table: (
                ESTIMATE_ROWS + ESTIMATE_ROWS
                if table == "regression_estimates"
                else TABLES[table]
            ),
        )
    assert ambiguous.value.code == "ambiguous_result"


def test_stats_only_references_use_cited_result_keys_without_estimate_references():
    result = _render(
        "N={{ n: substitution }} r={{ r: substitution.beds~unsheltered }}",
        allowed_estimates=frozenset(),
        allowed_result_keys=frozenset({RESULT_KEY}),
    )
    assert "N=120" in result.markdown
    assert result.placeholders[0]["source_address"] == (
        "results/regression_fit.parquet#analysis=substitution&variant=base"
    )
    assert result.placeholders[1]["source_address"] == (
        "results/correlations.parquet#analysis=substitution&variant=base"
        "&col_a=beds&col_b=unsheltered"
    )


def test_not_evaluated_numeric_values_use_only_safe_recorded_status():
    row = {**ESTIMATE_ROWS[0], "p": None, "p_status": "not_evaluated", "p_reason": "private detail"}
    with pytest.raises(PlaceholderError) as error:
        _render(
            "{{ p: substitution.beds }}",
            rows=lambda table: (row,) if table == "regression_estimates" else TABLES[table],
        )
    assert error.value.reason == "p_status: not_evaluated"
    assert "private detail" not in str(error.value)


@pytest.mark.parametrize(
    "narrative",
    [
        "{{ estimate substitution.beds }}",
        "{{ estimate: substitution.beds | html }}",
        "{{ verdict: substitution@base.extra }}",
        "Text {{ estimate: substitution.beds",
        "Text }}",
    ],
)
def test_malformed_or_unsupported_placeholders_fail_stably(narrative):
    with pytest.raises(PlaceholderError) as error:
        _render(narrative)
    assert error.value.code in {"malformed_placeholder", "invalid_format", "malformed_address"}


@pytest.mark.parametrize(
    "narrative",
    [
        "<script>alert(1)</script>",
        "![plot](https://example.org/plot.svg)",
        "[unsafe](javascript:alert(1))",
        "[escape](../private.csv)",
        "[network path](//example.org/path)",
    ],
)
def test_active_html_images_and_unsafe_links_are_rejected(narrative):
    with pytest.raises(PlaceholderError) as error:
        _render(narrative)
    assert error.value.code == "unsafe_markdown"


def test_safe_web_and_contained_relative_links_are_allowed():
    result = _render("[source](https://example.org/policy) and [details](notes/results.md#scope)")
    assert result.markdown.startswith("[source]")


def test_substituted_values_are_escaped_as_plain_markdown_text():
    evaluations = {
        ("substitution", "base"): {
            "verdict": "supports_claim",
            "verdict_text": "**bold** [click](javascript:alert(1)) <script>x</script>",
            "label": "x",
            "compatible_with": [],
            "compatible_text": None,
        }
    }
    result = _render("{{ verdict_text: substitution }}", evaluations=evaluations)
    assert "\\*\\*bold\\*\\*" in result.markdown
    assert "\\[click\\]\\(javascript:alert\\(1\\)\\)" in result.markdown
    assert "&lt;script&gt;" in result.markdown
    with pytest.raises(PlaceholderError):
        _render("<script>x</script>")


def test_number_lint_warns_and_strict_mode_rejects_unmarked_result_numbers():
    result = _render("In 2020 the estimate was 0.43 [12].")
    assert len(result.warnings) == 1
    assert "line 1" in result.warnings[0]
    with pytest.raises(PlaceholderError) as error:
        _render("In 2020 the estimate was 0.43 [12].", strict=True)
    assert error.value.code == "number_lint"


def test_number_lint_allows_years_citations_and_clearly_marked_illustrations():
    result = _render("From 2019 to 2024, see [4, 5]. Illustrative example: 0.43.")
    assert result.warnings == ()


@pytest.mark.parametrize(
    "narrative",
    ["The count was 1,234.", "The percentage was 2,020%.", "See [estimate 0.43](notes.md)."],
)
def test_number_lint_checks_counts_percentages_and_link_labels(narrative):
    result = _render(narrative)
    assert len(result.warnings) == 1


def test_escape_markdown_text_escapes_all_markdown_delimiters():
    assert escape_markdown_text("[a](b) *c* <tag>") == "\\[a\\]\\(b\\) \\*c\\* &lt;tag&gt;"
