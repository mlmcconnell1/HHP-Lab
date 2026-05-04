"""Tests for conformance checks with metro_id panels (coclab-djrh.7).

Verifies that all conformance checks work correctly when the panel
uses metro_id instead of coc_id, proving metro is a first-class target.
"""

import pandas as pd
import pytest

from hhplab.metro.metro_definitions import METRO_COUNT
from hhplab.panel.conformance import (
    PanelRequest,
    check_coc_count,
    check_coc_year_gaps,
    check_panel_balance,
    check_pit_exceeds_population,
    check_schema_measures,
    check_temporal_variation,
    check_year_coverage,
    run_conformance,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def metro_request():
    """PanelRequest configured for metro targets."""
    return PanelRequest(
        start_year=2020,
        end_year=2022,
        expected_geo_count=METRO_COUNT,
        geo_type="metro",
    )


@pytest.fixture
def metro_panel():
    """Minimal metro panel with 3 years and 25 metros."""
    metros = [f"GF{i:02d}" for i in range(1, 26)]
    rows = []
    for metro_id in metros:
        for year in [2020, 2021, 2022]:
            rows.append(
                {
                    "metro_id": metro_id,
                    "year": year,
                    "pit_total": 1000 + year,
                    "total_population": 500000 + year * 100,
                    "median_household_income": 60000.0,
                }
            )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# resolve_geo_col recognizes metro_id
# ---------------------------------------------------------------------------


class TestResolveGeoCol:
    def test_metro_id_resolved(self):
        """resolve_geo_col should recognize metro_id."""
        from hhplab.analysis_geo import resolve_geo_col

        df = pd.DataFrame({"metro_id": ["GF01"], "year": [2020]})
        assert resolve_geo_col(df) == "metro_id"

    def test_coc_id_preferred_over_metro_id(self):
        """If both coc_id and metro_id exist, coc_id wins (backward compat)."""
        from hhplab.analysis_geo import resolve_geo_col

        df = pd.DataFrame(
            {
                "coc_id": ["NY-600"],
                "metro_id": ["GF01"],
                "year": [2020],
            }
        )
        assert resolve_geo_col(df) == "coc_id"


# ---------------------------------------------------------------------------
# Individual check tests with metro_id
# ---------------------------------------------------------------------------


class TestYearCoverageWithMetro:
    def test_full_coverage(self, metro_panel, metro_request):
        results = check_year_coverage(metro_panel, metro_request)
        assert len(results) == 0

    def test_missing_years(self, metro_panel, metro_request):
        partial = metro_panel[metro_panel["year"] != 2021]
        results = check_year_coverage(partial, metro_request)
        assert len(results) == 1
        assert results[0].severity == "warning"
        assert "2021" in results[0].message


class TestTemporalVariationWithMetro:
    def test_varying_values_pass(self, metro_panel, metro_request):
        results = check_temporal_variation(metro_panel, metro_request)
        # pit_total varies (1000+year), so should pass
        pit_results = [r for r in results if "pit_total" in r.message]
        assert len(pit_results) == 0

    def test_static_values_flagged(self, metro_request):
        """Uniform total_population triggers temporal variation warning."""
        metros = [f"GF{i:02d}" for i in range(1, 6)]
        rows = []
        for m in metros:
            for year in [2020, 2021, 2022]:
                rows.append(
                    {
                        "metro_id": m,
                        "year": year,
                        "total_population": 500000,
                        "pit_total": 1000 + year,
                    }
                )
        df = pd.DataFrame(rows)
        results = check_temporal_variation(df, metro_request)
        pop_results = [r for r in results if "total_population" in r.message]
        assert len(pop_results) > 0


class TestPitExceedsPopulationWithMetro:
    def test_plausible_values_pass(self, metro_panel, metro_request):
        results = check_pit_exceeds_population(metro_panel, metro_request)
        assert len(results) == 0

    def test_impossible_values_flagged(self, metro_request):
        df = pd.DataFrame(
            {
                "metro_id": ["GF01", "GF02"],
                "year": [2020, 2020],
                "pit_total": [99999, 100],
                "total_population": [100, 500000],
            }
        )
        results = check_pit_exceeds_population(df, metro_request)
        assert len(results) == 1
        assert results[0].severity == "error"
        assert results[0].details["bad_row_count"] == 1
        # Example should contain metro_id key
        assert "metro_id" in results[0].details["examples"][0]


class TestGeoCocCountWithMetro:
    def test_correct_count_passes(self, metro_panel, metro_request):
        results = check_coc_count(metro_panel, metro_request)
        assert len(results) == 0

    def test_missing_metros_flagged(self, metro_request):
        df = pd.DataFrame(
            {
                "metro_id": ["GF01", "GF02"],
                "year": [2020, 2020],
            }
        )
        results = check_coc_count(df, metro_request)
        assert len(results) == 1
        assert "2/25" in results[0].message


class TestPanelBalanceWithMetro:
    def test_balanced_panel_passes(self, metro_panel, metro_request):
        results = check_panel_balance(metro_panel, metro_request)
        assert len(results) == 0

    def test_unbalanced_panel_flagged(self, metro_request):
        df = pd.DataFrame(
            {
                "metro_id": ["GF01", "GF01", "GF02"],
                "year": [2020, 2021, 2020],
            }
        )
        results = check_panel_balance(df, metro_request)
        assert len(results) == 1
        assert "geo units" in results[0].message


class TestYearGapsWithMetro:
    def test_contiguous_years_pass(self, metro_panel, metro_request):
        results = check_coc_year_gaps(metro_panel, metro_request)
        assert len(results) == 0

    def test_gap_flagged(self, metro_request):
        df = pd.DataFrame(
            {
                "metro_id": ["GF01", "GF01", "GF01"],
                "year": [2020, 2022, 2023],
            }
        )
        results = check_coc_year_gaps(df, metro_request)
        assert len(results) == 1
        assert "geo units" in results[0].message
        assert results[0].details["examples"][0]["geo_id"] == "GF01"


# ---------------------------------------------------------------------------
# Full conformance run with metro panel
# ---------------------------------------------------------------------------


class TestSchemaCheckWithPepMetro:
    """Schema check with PEP-based metro panels (coclab-d0qm)."""

    def test_pep_panel_passes_with_measure_columns(self):
        """A PEP metro panel with population passes when measure_columns is set."""
        metros = [f"GF{i:02d}" for i in range(1, 6)]
        rows = []
        for m in metros:
            for year in [2020, 2021]:
                rows.append(
                    {
                        "metro_id": m,
                        "year": year,
                        "pit_total": 1000 + year,
                        "population": 500000 + year * 100,
                    }
                )
        df = pd.DataFrame(rows)
        request = PanelRequest(
            start_year=2020,
            end_year=2021,
            geo_type="metro",
            measure_columns=["population"],
        )
        results = check_schema_measures(df, request)
        assert len(results) == 0

    def test_pep_panel_errors_without_measure_columns(self):
        """A PEP metro panel with no ACS columns errors with default settings."""
        df = pd.DataFrame(
            {
                "metro_id": ["GF01"],
                "year": [2020],
                "pit_total": [1000],
                "population": [500000],
            }
        )
        request = PanelRequest(
            start_year=2020,
            end_year=2020,
            geo_type="metro",
        )
        results = check_schema_measures(df, request)
        assert len(results) == 1
        assert results[0].severity == "error"

    def test_pep_panel_full_conformance_passes(self):
        """Full conformance run passes for PEP metro panel."""
        metros = [f"GF{i:02d}" for i in range(1, 26)]
        rows = []
        for m in metros:
            for year in [2020, 2021, 2022]:
                rows.append(
                    {
                        "metro_id": m,
                        "year": year,
                        "pit_total": 1000 + year,
                        "population": 500000 + year * 100,
                    }
                )
        df = pd.DataFrame(rows)
        request = PanelRequest(
            start_year=2020,
            end_year=2022,
            expected_geo_count=25,
            geo_type="metro",
            measure_columns=["population"],
        )
        report = run_conformance(df, request)
        assert report.passed, f"PEP metro panel failed conformance:\n{report.summary()}"


class TestFullConformanceWithMetro:
    def test_clean_metro_panel_passes(self, metro_panel, metro_request):
        """A clean metro panel should pass all conformance checks."""
        report = run_conformance(metro_panel, metro_request)
        # May have warnings (e.g., static population) but no errors
        assert report.passed, f"Clean metro panel failed conformance:\n{report.summary()}"

    def test_conformance_report_serializes(self, metro_panel, metro_request):
        report = run_conformance(metro_panel, metro_request)
        d = report.to_dict()
        assert "passed" in d
        assert "results" in d
        assert isinstance(d["results"], list)
