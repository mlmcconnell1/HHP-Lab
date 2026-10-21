"""Offline reports keep statuses, failures and withholding visible."""

from copy import deepcopy

import pytest

from hhplab.results.report import render_readme, render_report, render_tables

BASE = {
    "title": "Synthetic result",
    "recipe_id": "example",
    "lock_status": "unlocked",
    "statuses": {
        "execution_status": "complete",
        "diagnostics_status": "pass",
        "findings_status": "evaluated",
        "distribution_status": "full",
    },
    "analyses": {"fit@base": {"status": "complete"}},
    "findings": {"bundle_format_version": "1", "findings": []},
    "finding_claims": {"finding": "An association, not a causal effect."},
}


@pytest.mark.parametrize(
    "execution,diagnostics,distribution,private",
    [
        ("complete", "pass", "full", False),
        ("partial", "incomplete", "full", False),
        ("complete", "fail", "full", False),
        ("complete", "pass", "private", True),
        ("complete", "incomplete", "withheld", False),
    ],
)
def test_statuses_precede_claims(execution, diagnostics, distribution, private):
    payload = deepcopy(BASE)
    payload["statuses"].update(
        execution_status=execution, diagnostics_status=diagnostics, distribution_status=distribution
    )
    payload["private"] = private
    rendered = render_report(payload)
    assert rendered.index("Run status") < rendered.index("An association")
    assert f"<td>{execution}</td>" in rendered
    assert f"<td>{diagnostics}</td>" in rendered
    assert f"<td>{distribution}</td>" in rendered
    assert ("Private bundle:" in rendered) is private
    assert "<script" not in rendered
    assert "src=" not in rendered
    assert "href=" not in rendered


def test_every_variant_error_and_local_path_redaction():
    payload = deepcopy(BASE)
    payload["analyses"]["fit@variant"] = {
        "status": "failed",
        "error": "Missing /home/researcher/data/input.parquet",
    }
    rendered = render_report(payload)
    assert "fit@base" in rendered and "fit@variant" in rendered
    assert "Missing [local path]" in rendered
    assert "/home/researcher" not in rendered


def test_recipe_text_is_escaped_and_withheld_values_not_rendered():
    payload = deepcopy(BASE)
    payload["title"] = "<script>alert('bad')</script>"
    payload["analyses"]["fit@base"].update(withheld=True, protected="SECRET_VALUE")
    rendered = render_report(payload)
    assert "<script>" not in rendered
    assert "&lt;script&gt;" in rendered
    assert "SECRET_VALUE" not in rendered
    assert "Withheld under source redistribution terms" in rendered


def test_undefined_cells_remain_explicit_in_both_table_formats():
    row = {
        "analysis": "a",
        "variant": "base",
        "col_a": "x",
        "col_b": "y",
        "kind": "pearson",
        "r": None,
        "n": 2,
        "status": "not_evaluated",
        "reason": "constant_input",
    }
    files = render_tables({"correlations": [row]})
    assert files["correlations.csv"].decode() == (
        "analysis,variant,col_a,col_b,kind,r,n,status,reason\n"
        "a,base,x,y,pearson,Not evaluated,2,not_evaluated,constant_input\n"
    )
    assert b"Not evaluated" in files["correlations.html"]
    assert b"constant_input" in files["correlations.html"]


def test_readme_snapshot():
    assert render_readme(BASE) == (
        "Synthetic result\n\nexecution_status: complete\ndiagnostics_status: pass\n"
        "findings_status: evaluated\ndistribution_status: full\n\nLock: unlocked\n"
        "A ledger lock establishes event order, not absence of prior data inspection.\n"
        "Publish the exported lock independently for an external timestamp.\n\n"
        "Open report/index.html offline. Keep the complete directory together when moving it.\n"
        "Verify from the parent directory: hhplab results verify BUNDLE --json\n"
        "Withheld inputs prevent full replay; inspect withheld.json for rebuild instructions.\n"
    )
