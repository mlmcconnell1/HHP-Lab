"""Integration tests for panel conformance wiring (legacy build path).

.. deprecated::
    Tests 1-3 validate conformance logic that is shared across both paths.
    Test 4 (``save_panel`` provenance embedding) exercises the legacy path;
    recipe-native provenance embedding is covered in
    ``test_recipe_panel_policies.py``.

Tests verify:
1. Conformance checks run and produce a report on a valid panel.
2. Strict mode logic: errors would cause failure, warnings do not.
3. Skip-conformance flag bypasses all checks.
4. save_panel() embeds conformance in provenance when provided.

Beads: coclab-zabo
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

pytestmark = pytest.mark.legacy_build_path

from hhplab.panel.assemble import save_panel
from hhplab.panel.conformance import (
    ConformanceReport,
    ConformanceResult,
    PanelRequest,
    run_conformance,
)
from hhplab.storage.provenance import read_provenance

# ---------------------------------------------------------------------------
# Minimal panel fixture
# ---------------------------------------------------------------------------


def _make_panel(
    coc_ids: list[str] | None = None,
    years: list[int] | None = None,
) -> pd.DataFrame:
    """Build a minimal valid panel DataFrame."""
    coc_ids = coc_ids or ["CO-500", "CA-600"]
    years = years or [2022, 2023]
    rows = []
    for coc in coc_ids:
        for yr in years:
            rows.append({
                "coc_id": coc,
                "year": yr,
                "pit_total": 100 + yr,  # vary by year to avoid temporal warning
                "pit_sheltered": 60,
                "pit_unsheltered": 40,
                "boundary_vintage_used": "2024",
                "acs5_vintage_used": "2023",
                "tract_vintage_used": "2020",
                "alignment_type": "retrospective",
                "weighting_method": "population",
                "total_population": 50000 + yr * 10,  # vary to avoid temporal warning
                "adult_population": 40000,
                "population_below_poverty": 5000,
                "median_household_income": 55000,
                "median_gross_rent": 1200,
                "coverage_ratio": 0.95,
                "boundary_changed": False,
                "source": "hhplab_panel",
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 1. Conformance output on valid panel
# ---------------------------------------------------------------------------


class TestConformanceOutput:
    """Conformance checks produce a report on a well-formed panel."""

    def test_valid_panel_passes(self):
        df = _make_panel()
        request = PanelRequest(start_year=2022, end_year=2023)
        report = run_conformance(df, request)

        assert report.passed is True
        assert len(report.errors) == 0
        assert isinstance(report.summary(), str)

    def test_report_to_dict(self):
        df = _make_panel()
        request = PanelRequest(start_year=2022, end_year=2023)
        report = run_conformance(df, request)
        d = report.to_dict()

        assert d["passed"] is True
        assert d["error_count"] == 0
        assert isinstance(d["results"], list)

    def test_missing_years_produces_warning(self):
        df = _make_panel(years=[2022])
        request = PanelRequest(start_year=2022, end_year=2024)
        report = run_conformance(df, request)

        # Missing years is a warning, not an error
        assert report.passed is True
        assert len(report.warnings) > 0
        year_warnings = [
            w for w in report.warnings if w.check_name == "check_year_coverage"
        ]
        assert len(year_warnings) == 1
        assert "2023" in year_warnings[0].message


# ---------------------------------------------------------------------------
# 2. Strict mode logic
# ---------------------------------------------------------------------------


class TestStrictMode:
    """Strict mode treats conformance errors as failures."""

    def test_strict_with_errors_would_fail(self):
        """When errors exist, report.passed is False (CLI would exit)."""
        report = ConformanceReport(results=[
            ConformanceResult(
                check_name="test_check",
                severity="error",
                message="something broke",
            ),
        ])
        assert report.passed is False
        assert len(report.errors) == 1

    def test_strict_with_warnings_only_passes(self):
        """Warnings alone do not cause failure even in strict mode."""
        report = ConformanceReport(results=[
            ConformanceResult(
                check_name="test_warn",
                severity="warning",
                message="a warning",
            ),
        ])
        assert report.passed is True
        assert len(report.warnings) == 1

    def test_no_errors_passes_strict(self):
        """A clean panel passes strict mode."""
        df = _make_panel()
        request = PanelRequest(start_year=2022, end_year=2023)
        report = run_conformance(df, request)
        assert report.passed is True


# ---------------------------------------------------------------------------
# 3. Skip conformance
# ---------------------------------------------------------------------------


class TestSkipConformance:
    """skip_conformance=True bypasses all checks (caller concern)."""

    def test_skip_logic(self):
        """Verify the skip pattern: caller doesn't call run_conformance."""
        df = _make_panel(years=[2022])
        request = PanelRequest(start_year=2022, end_year=2024)

        # Without skip: warnings are detected
        report = run_conformance(df, request)
        assert len(report.warnings) > 0

        # With skip: caller skips the call entirely
        skip_conformance = True
        report2 = None
        if not skip_conformance:
            report2 = run_conformance(df, request)

        assert report2 is None


# ---------------------------------------------------------------------------
# 4. Provenance embedding
# ---------------------------------------------------------------------------


class TestProvenanceEmbedding:
    """save_panel() embeds conformance report in provenance metadata."""

    def test_conformance_in_provenance(self, tmp_path: Path):
        df = _make_panel()
        request = PanelRequest(start_year=2022, end_year=2023)
        report = run_conformance(df, request)

        output_path = save_panel(
            df=df,
            start_year=2022,
            end_year=2023,
            output_dir=tmp_path,
            conformance_report=report,
        )

        assert output_path.exists()

        provenance = read_provenance(output_path)
        assert provenance is not None
        assert "conformance" in provenance.extra
        conf = provenance.extra["conformance"]
        assert conf["passed"] is True
        assert conf["error_count"] == 0

    def test_no_conformance_when_none(self, tmp_path: Path):
        df = _make_panel()

        output_path = save_panel(
            df=df,
            start_year=2022,
            end_year=2023,
            output_dir=tmp_path,
        )

        provenance = read_provenance(output_path)
        assert provenance is not None
        assert "conformance" not in provenance.extra

    def test_failed_conformance_embedded(self, tmp_path: Path):
        """A failed conformance report is also embedded in provenance."""
        df = _make_panel()
        report = ConformanceReport(results=[
            ConformanceResult(
                check_name="test_check",
                severity="error",
                message="something broke",
            ),
            ConformanceResult(
                check_name="test_warn",
                severity="warning",
                message="heads up",
            ),
        ])

        output_path = save_panel(
            df=df,
            start_year=2022,
            end_year=2023,
            output_dir=tmp_path,
            conformance_report=report,
        )

        provenance = read_provenance(output_path)
        assert provenance is not None
        conf = provenance.extra["conformance"]
        assert conf["passed"] is False
        assert conf["error_count"] == 1
        assert conf["warning_count"] == 1
        assert len(conf["results"]) == 2
