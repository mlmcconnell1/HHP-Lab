"""Tests for panel diagnostics module (WP-3G).

Tests cover:
- Coverage ratio summary statistics
- Boundary change detection and summarization
- Weighting sensitivity analysis
- Missingness reporting
- DiagnosticsReport class methods
- Edge cases and error handling
"""

from __future__ import annotations

import pandas as pd
import pytest

from coclab.panel.diagnostics import (
    DiagnosticsReport,
    boundary_change_summary,
    coverage_summary,
    generate_diagnostics_report,
    missingness_report,
    weighting_sensitivity,
)


class TestCoverageSummary:
    """Tests for coverage_summary function."""

    @pytest.fixture
    def sample_panel(self):
        """Create a sample panel with coverage ratios."""
        return pd.DataFrame(
            {
                "coc_id": ["CO-500", "CA-600", "NY-501", "CO-500", "CA-600", "NY-501"],
                "year": [2023, 2023, 2023, 2024, 2024, 2024],
                "coverage_ratio": [0.95, 0.88, 0.99, 0.92, 0.85, 0.98],
            }
        )

    def test_basic_summary(self, sample_panel):
        """Test basic coverage summary computation."""
        result = coverage_summary(sample_panel)

        assert len(result) == 2  # 2 years
        assert set(result["year"]) == {2023, 2024}
        assert "mean" in result.columns
        assert "std" in result.columns
        assert "min" in result.columns
        assert "max" in result.columns
        assert "low_coverage_count" in result.columns

    def test_statistics_correctness(self, sample_panel):
        """Test that statistics are computed correctly."""
        result = coverage_summary(sample_panel)

        # 2023 has coverage [0.95, 0.88, 0.99]
        row_2023 = result[result["year"] == 2023].iloc[0]
        assert row_2023["count"] == 3
        assert abs(row_2023["mean"] - 0.94) < 0.01
        assert row_2023["min"] == 0.88
        assert row_2023["max"] == 0.99
        assert row_2023["low_coverage_count"] == 1  # 0.88 < 0.9

    def test_low_coverage_threshold(self, sample_panel):
        """Test that low coverage count uses 0.9 threshold."""
        result = coverage_summary(sample_panel)

        # 2023: 0.88 < 0.9, so 1 low coverage
        # 2024: 0.85 < 0.9, so 1 low coverage
        row_2023 = result[result["year"] == 2023].iloc[0]
        row_2024 = result[result["year"] == 2024].iloc[0]

        assert row_2023["low_coverage_count"] == 1
        assert row_2024["low_coverage_count"] == 1

    def test_empty_panel(self):
        """Test handling of empty panel."""
        result = coverage_summary(pd.DataFrame())

        assert result.empty
        assert "year" in result.columns
        assert "mean" in result.columns

    def test_none_panel(self):
        """Test handling of None input."""
        result = coverage_summary(None)

        assert result.empty
        assert "year" in result.columns

    def test_missing_columns(self):
        """Test handling of missing required columns."""
        df = pd.DataFrame({"coc_id": ["CO-500"], "year": [2024]})
        result = coverage_summary(df)

        assert result.empty

    def test_all_null_coverage(self):
        """Test handling when all coverage values are null."""
        df = pd.DataFrame(
            {
                "year": [2023, 2024],
                "coverage_ratio": [None, None],
            }
        )
        result = coverage_summary(df)

        assert result.empty

    def test_single_year(self):
        """Test summary with single year."""
        df = pd.DataFrame(
            {
                "year": [2024, 2024, 2024],
                "coverage_ratio": [0.95, 0.96, 0.97],
            }
        )
        result = coverage_summary(df)

        assert len(result) == 1
        assert result["year"].iloc[0] == 2024
        assert result["count"].iloc[0] == 3

    def test_single_observation_per_year(self):
        """Test that std is 0 for single observation years."""
        df = pd.DataFrame(
            {
                "year": [2023],
                "coverage_ratio": [0.95],
            }
        )
        result = coverage_summary(df)

        assert result["std"].iloc[0] == 0.0

    def test_sorted_by_year(self):
        """Test that result is sorted by year."""
        df = pd.DataFrame(
            {
                "year": [2024, 2022, 2023],
                "coverage_ratio": [0.95, 0.96, 0.97],
            }
        )
        result = coverage_summary(df)

        assert list(result["year"]) == [2022, 2023, 2024]


class TestBoundaryChangeSummary:
    """Tests for boundary_change_summary function."""

    @pytest.fixture
    def sample_panel_with_changes(self):
        """Create a panel with boundary changes."""
        return pd.DataFrame(
            {
                "coc_id": ["CO-500", "CO-500", "CO-500", "CA-600", "CA-600", "CA-600"],
                "year": [2022, 2023, 2024, 2022, 2023, 2024],
                "boundary_changed": [False, True, True, False, False, True],
            }
        )

    def test_detects_changes(self, sample_panel_with_changes):
        """Test that boundary changes are detected."""
        result = boundary_change_summary(sample_panel_with_changes)

        assert len(result) == 2  # Both CoCs have changes
        assert set(result["coc_id"]) == {"CO-500", "CA-600"}

    def test_change_years_correct(self, sample_panel_with_changes):
        """Test that change years are correctly recorded."""
        result = boundary_change_summary(sample_panel_with_changes)

        co500 = result[result["coc_id"] == "CO-500"].iloc[0]
        ca600 = result[result["coc_id"] == "CA-600"].iloc[0]

        assert set(co500["change_years"]) == {2023, 2024}
        assert co500["change_count"] == 2
        assert set(ca600["change_years"]) == {2024}
        assert ca600["change_count"] == 1

    def test_no_changes(self):
        """Test handling when no boundary changes exist."""
        df = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CO-500"],
                "year": [2023, 2024],
                "boundary_changed": [False, False],
            }
        )
        result = boundary_change_summary(df)

        assert result.empty

    def test_empty_panel(self):
        """Test handling of empty panel."""
        result = boundary_change_summary(pd.DataFrame())

        assert result.empty
        assert "coc_id" in result.columns
        assert "change_years" in result.columns

    def test_none_panel(self):
        """Test handling of None input."""
        result = boundary_change_summary(None)

        assert result.empty

    def test_missing_columns(self):
        """Test handling of missing required columns."""
        df = pd.DataFrame({"coc_id": ["CO-500"], "year": [2024]})
        result = boundary_change_summary(df)

        assert result.empty

    def test_sorted_by_coc_id(self):
        """Test that result is sorted by coc_id."""
        df = pd.DataFrame(
            {
                "coc_id": ["NY-501", "CA-600", "CO-500"],
                "year": [2024, 2024, 2024],
                "boundary_changed": [True, True, True],
            }
        )
        result = boundary_change_summary(df)

        assert list(result["coc_id"]) == ["CA-600", "CO-500", "NY-501"]


class TestWeightingSensitivity:
    """Tests for weighting_sensitivity function."""

    @pytest.fixture
    def panel_area(self):
        """Create a panel with area weighting."""
        return pd.DataFrame(
            {
                "coc_id": ["CO-500", "CA-600"],
                "year": [2024, 2024],
                "pit_total": [1200, 45000],
                "total_population": [500000, 9000000],
                "weighting_method": ["area", "area"],
            }
        )

    @pytest.fixture
    def panel_pop(self):
        """Create a panel with population weighting."""
        return pd.DataFrame(
            {
                "coc_id": ["CO-500", "CA-600"],
                "year": [2024, 2024],
                "pit_total": [1200, 45000],
                "total_population": [480000, 10000000],
                "weighting_method": ["population", "population"],
            }
        )

    def test_basic_comparison(self, panel_area, panel_pop):
        """Test basic weighting sensitivity computation."""
        result = weighting_sensitivity(panel_area, panel_pop)

        assert len(result) == 2
        assert "rate_area" in result.columns
        assert "rate_pop" in result.columns
        assert "rate_diff" in result.columns
        assert "rate_pct_diff" in result.columns

    def test_rate_calculation(self, panel_area, panel_pop):
        """Test that rates are calculated correctly per 10k."""
        result = weighting_sensitivity(panel_area, panel_pop)

        co500 = result[result["coc_id"] == "CO-500"].iloc[0]

        # Area rate: 1200 / 500000 * 10000 = 24
        expected_area_rate = 1200 / 500000 * 10000
        assert abs(co500["rate_area"] - expected_area_rate) < 0.001

        # Pop rate: 1200 / 480000 * 10000 = 25
        expected_pop_rate = 1200 / 480000 * 10000
        assert abs(co500["rate_pop"] - expected_pop_rate) < 0.001

    def test_difference_calculation(self, panel_area, panel_pop):
        """Test that rate differences are calculated correctly."""
        result = weighting_sensitivity(panel_area, panel_pop)

        co500 = result[result["coc_id"] == "CO-500"].iloc[0]

        # Verify rate_diff is absolute difference
        expected_diff = abs(co500["rate_area"] - co500["rate_pop"])
        assert abs(co500["rate_diff"] - expected_diff) < 0.001

    def test_empty_panel(self):
        """Test handling of empty panels."""
        empty_df = pd.DataFrame()
        other_df = pd.DataFrame(
            {
                "coc_id": ["CO-500"],
                "year": [2024],
                "pit_total": [1200],
                "total_population": [500000],
            }
        )

        result = weighting_sensitivity(empty_df, other_df)
        assert result.empty

        result = weighting_sensitivity(other_df, empty_df)
        assert result.empty

    def test_none_panel(self):
        """Test handling of None input."""
        df = pd.DataFrame(
            {
                "coc_id": ["CO-500"],
                "year": [2024],
                "pit_total": [1200],
                "total_population": [500000],
            }
        )

        result = weighting_sensitivity(None, df)
        assert result.empty

        result = weighting_sensitivity(df, None)
        assert result.empty

    def test_missing_columns(self):
        """Test handling of missing required columns."""
        df1 = pd.DataFrame({"coc_id": ["CO-500"], "year": [2024]})
        df2 = pd.DataFrame(
            {
                "coc_id": ["CO-500"],
                "year": [2024],
                "pit_total": [1200],
                "total_population": [500000],
            }
        )

        result = weighting_sensitivity(df1, df2)
        assert result.empty

    def test_sorted_by_coc_year(self, panel_area, panel_pop):
        """Test that result is sorted by coc_id and year."""
        # Create multi-year panels
        area = pd.DataFrame(
            {
                "coc_id": ["NY-501", "CO-500", "NY-501", "CO-500"],
                "year": [2024, 2023, 2023, 2024],
                "pit_total": [1000, 1200, 1100, 1300],
                "total_population": [5000000, 500000, 5100000, 510000],
            }
        )
        pop = pd.DataFrame(
            {
                "coc_id": ["NY-501", "CO-500", "NY-501", "CO-500"],
                "year": [2024, 2023, 2023, 2024],
                "pit_total": [1000, 1200, 1100, 1300],
                "total_population": [5200000, 480000, 5000000, 490000],
            }
        )

        result = weighting_sensitivity(area, pop)

        # Should be sorted by coc_id, then year
        expected_order = [
            ("CO-500", 2023),
            ("CO-500", 2024),
            ("NY-501", 2023),
            ("NY-501", 2024),
        ]
        actual_order = list(zip(result["coc_id"], result["year"], strict=True))
        assert actual_order == expected_order


class TestMissingnessReport:
    """Tests for missingness_report function."""

    @pytest.fixture
    def sample_panel(self):
        """Create a panel with some missing values."""
        return pd.DataFrame(
            {
                "coc_id": ["CO-500", "CA-600", "NY-501", "CO-500", "CA-600", "NY-501"],
                "year": [2023, 2023, 2023, 2024, 2024, 2024],
                "pit_total": [1200, 45000, 75000, 1300, 48000, None],
                "total_population": [500000, None, 8000000, 510000, 10000000, 8100000],
                "coverage_ratio": [0.95, 0.98, 0.99, None, None, 0.98],
            }
        )

    def test_basic_report(self, sample_panel):
        """Test basic missingness report generation."""
        result = missingness_report(sample_panel)

        assert not result.empty
        assert "column" in result.columns
        assert "year" in result.columns
        assert "missing_count" in result.columns
        assert "total_count" in result.columns
        assert "missing_pct" in result.columns

    def test_per_year_stats(self, sample_panel):
        """Test that per-year statistics are computed."""
        result = missingness_report(sample_panel)

        # Check 2023 pit_total (no missing values)
        pit_2023 = result[(result["column"] == "pit_total") & (result["year"] == 2023)]
        assert len(pit_2023) == 1
        assert pit_2023.iloc[0]["missing_count"] == 0

        # Check 2024 pit_total (1 missing value)
        pit_2024 = result[(result["column"] == "pit_total") & (result["year"] == 2024)]
        assert len(pit_2024) == 1
        assert pit_2024.iloc[0]["missing_count"] == 1

    def test_overall_stats(self, sample_panel):
        """Test that overall statistics are computed."""
        result = missingness_report(sample_panel)

        # Check overall pit_total
        pit_all = result[(result["column"] == "pit_total") & (result["year"] == "all")]
        assert len(pit_all) == 1
        assert pit_all.iloc[0]["missing_count"] == 1
        assert pit_all.iloc[0]["total_count"] == 6

    def test_percentage_calculation(self, sample_panel):
        """Test that percentages are calculated correctly."""
        result = missingness_report(sample_panel)

        # Check overall coverage_ratio (2 missing out of 6)
        coverage_all = result[(result["column"] == "coverage_ratio") & (result["year"] == "all")]
        assert coverage_all.iloc[0]["missing_count"] == 2
        expected_pct = 2 / 6 * 100
        assert abs(coverage_all.iloc[0]["missing_pct"] - expected_pct) < 0.01

    def test_empty_panel(self):
        """Test handling of empty panel."""
        result = missingness_report(pd.DataFrame())

        assert result.empty
        assert "column" in result.columns

    def test_none_panel(self):
        """Test handling of None input."""
        result = missingness_report(None)

        assert result.empty

    def test_missing_year_column(self):
        """Test handling when year column is missing."""
        df = pd.DataFrame({"coc_id": ["CO-500"], "pit_total": [1200]})
        result = missingness_report(df)

        assert result.empty

    def test_no_missing_values(self):
        """Test handling when no values are missing."""
        df = pd.DataFrame(
            {
                "year": [2023, 2024],
                "pit_total": [1200, 1300],
            }
        )
        result = missingness_report(df)

        # All entries should have 0 missing
        assert all(result["missing_count"] == 0)
        assert all(result["complete_pct"] == 100.0)

    def test_sorted_output(self, sample_panel):
        """Test that output is sorted by column and year."""
        result = missingness_report(sample_panel)

        # Check that results are sorted
        prev_col = None
        prev_year = None
        for _, row in result.iterrows():
            if prev_col is not None:
                if row["column"] == prev_col:
                    # Same column, year should be increasing or "all"
                    if prev_year != "all" and row["year"] != "all":
                        assert row["year"] >= prev_year
                else:
                    # Different column, should be alphabetically later
                    assert row["column"] >= prev_col
            prev_col = row["column"]
            prev_year = row["year"]


class TestDiagnosticsReport:
    """Tests for DiagnosticsReport class."""

    @pytest.fixture
    def sample_report(self):
        """Create a sample diagnostics report."""
        coverage = pd.DataFrame(
            {
                "year": [2023, 2024],
                "count": [100, 105],
                "mean": [0.94, 0.95],
                "std": [0.05, 0.04],
                "min": [0.75, 0.78],
                "q25": [0.92, 0.93],
                "median": [0.95, 0.96],
                "q75": [0.97, 0.98],
                "max": [0.99, 0.99],
                "low_coverage_count": [5, 3],
            }
        )

        boundary_changes = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CA-600"],
                "change_years": [[2023, 2024], [2024]],
                "change_count": [2, 1],
            }
        )

        missingness = pd.DataFrame(
            {
                "column": ["pit_total", "pit_total", "coverage_ratio", "coverage_ratio"],
                "year": [2023, "all", 2023, "all"],
                "missing_count": [0, 0, 2, 5],
                "total_count": [100, 205, 100, 205],
                "missing_pct": [0.0, 0.0, 2.0, 2.4],
                "complete_pct": [100.0, 100.0, 98.0, 97.6],
            }
        )

        panel_info = {
            "row_count": 205,
            "coc_count": 100,
            "year_count": 2,
            "year_min": 2023,
            "year_max": 2024,
        }

        return DiagnosticsReport(
            coverage=coverage,
            boundary_changes=boundary_changes,
            missingness=missingness,
            panel_info=panel_info,
        )

    def test_to_dict(self, sample_report):
        """Test serialization to dictionary."""
        result = sample_report.to_dict()

        assert isinstance(result, dict)
        assert "coverage" in result
        assert "boundary_changes" in result
        assert "missingness" in result
        assert "weighting" in result
        assert "panel_info" in result

        # Check that DataFrames were converted to records
        assert isinstance(result["coverage"], list)
        assert len(result["coverage"]) == 2

    def test_to_csv(self, sample_report, tmp_path):
        """Test export to CSV files."""
        paths = sample_report.to_csv(tmp_path)

        assert "coverage" in paths
        assert "boundary_changes" in paths
        assert "missingness" in paths

        # Verify files exist
        assert paths["coverage"].exists()
        assert paths["boundary_changes"].exists()
        assert paths["missingness"].exists()

        # Verify content can be read
        coverage_df = pd.read_csv(paths["coverage"])
        assert len(coverage_df) == 2

    def test_to_csv_creates_directory(self, sample_report, tmp_path):
        """Test that to_csv creates output directory if needed."""
        nested_dir = tmp_path / "nested" / "diagnostics"
        paths = sample_report.to_csv(nested_dir)

        assert nested_dir.exists()
        assert paths["coverage"].exists()

    def test_to_csv_with_weighting(self, sample_report, tmp_path):
        """Test CSV export when weighting sensitivity is present."""
        sample_report.weighting = pd.DataFrame(
            {
                "coc_id": ["CO-500"],
                "year": [2024],
                "pit_total": [1200],
                "pop_area": [500000],
                "pop_pop": [480000],
                "rate_area": [24.0],
                "rate_pop": [25.0],
                "rate_diff": [1.0],
                "rate_pct_diff": [4.08],
            }
        )

        paths = sample_report.to_csv(tmp_path)

        assert "weighting" in paths
        assert paths["weighting"].exists()

    def test_to_csv_empty_dataframes(self, tmp_path):
        """Test CSV export with empty DataFrames."""
        report = DiagnosticsReport()
        paths = report.to_csv(tmp_path)

        # Should not create files for empty DataFrames
        assert "coverage" not in paths
        assert "boundary_changes" not in paths
        assert "missingness" not in paths

    def test_summary(self, sample_report):
        """Test text summary generation."""
        result = sample_report.summary()

        assert isinstance(result, str)
        assert "PANEL DIAGNOSTICS REPORT" in result
        assert "COVERAGE SUMMARY" in result
        assert "BOUNDARY CHANGES" in result
        assert "MISSINGNESS" in result
        assert "WEIGHTING SENSITIVITY" in result

        # Check panel info is included
        assert "205" in result  # row_count

    def test_summary_empty_report(self):
        """Test summary for empty report."""
        report = DiagnosticsReport()
        result = report.summary()

        assert isinstance(result, str)
        assert "No coverage data available" in result
        assert "No boundary changes detected" in result

    def test_summary_with_weighting(self, sample_report):
        """Test summary includes weighting info when present."""
        sample_report.weighting = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CA-600"],
                "year": [2024, 2024],
                "rate_pct_diff": [4.08, 2.5],
            }
        )

        result = sample_report.summary()

        assert "Mean rate difference" in result
        assert "Max rate difference" in result


class TestGenerateDiagnosticsReport:
    """Tests for generate_diagnostics_report function."""

    @pytest.fixture
    def sample_panel(self):
        """Create a comprehensive sample panel."""
        return pd.DataFrame(
            {
                "coc_id": ["CO-500", "CA-600", "NY-501"] * 3,
                "year": [2022] * 3 + [2023] * 3 + [2024] * 3,
                "pit_total": [1200, 45000, 75000, 1250, 46000, 76000, 1300, 48000, 78000],
                "pit_sheltered": [800, 30000, 55000, 820, 31000, 56000, 850, 32000, 58000],
                "pit_unsheltered": [400, 15000, 20000, 430, 15000, 20000, 450, 16000, 20000],
                "boundary_vintage_used": ["2022"] * 3 + ["2023"] * 3 + ["2024"] * 3,
                "acs_vintage_used": ["2021"] * 3 + ["2022"] * 3 + ["2023"] * 3,
                "weighting_method": ["population"] * 9,
                "total_population": [500000, 10000000, 8000000] * 3,
                "adult_population": [400000, 8000000, 6400000] * 3,
                "population_below_poverty": [50000, 1500000, 1200000] * 3,
                "median_household_income": [65000, 75000, 85000] * 3,
                "median_gross_rent": [1200, 1800, 2200] * 3,
                "coverage_ratio": [0.95, 0.98, 0.99, 0.92, 0.85, 0.98, 0.96, 0.97, None],
                "boundary_changed": [False] * 3 + [True] * 3 + [True] * 3,
                "source": ["coclab_panel"] * 9,
            }
        )

    def test_generates_all_diagnostics(self, sample_panel):
        """Test that all diagnostics are generated."""
        report = generate_diagnostics_report(sample_panel)

        assert isinstance(report, DiagnosticsReport)
        assert not report.coverage.empty
        assert not report.boundary_changes.empty
        assert not report.missingness.empty
        assert report.weighting is None  # No alternative panel provided

    def test_panel_info_populated(self, sample_panel):
        """Test that panel_info is populated correctly."""
        report = generate_diagnostics_report(sample_panel)

        assert report.panel_info["row_count"] == 9
        assert report.panel_info["coc_count"] == 3
        assert report.panel_info["year_count"] == 3
        assert report.panel_info["year_min"] == 2022
        assert report.panel_info["year_max"] == 2024

    def test_with_alternative_panel(self, sample_panel):
        """Test weighting sensitivity with alternative panel."""
        # Create alternative panel with different populations
        alt_panel = sample_panel.copy()
        alt_panel["total_population"] = alt_panel["total_population"] * 0.9
        alt_panel["weighting_method"] = "area"

        report = generate_diagnostics_report(sample_panel, alt_panel)

        assert report.weighting is not None
        assert not report.weighting.empty

    def test_empty_panel(self):
        """Test handling of empty panel."""
        report = generate_diagnostics_report(pd.DataFrame())

        assert isinstance(report, DiagnosticsReport)
        assert report.coverage.empty
        assert report.boundary_changes.empty
        assert report.missingness.empty

    def test_none_panel(self):
        """Test handling of None input."""
        report = generate_diagnostics_report(None)

        assert isinstance(report, DiagnosticsReport)
        assert report.coverage.empty

    def test_report_is_complete(self, sample_panel):
        """Test that report can be serialized and summarized."""
        report = generate_diagnostics_report(sample_panel)

        # Should be able to serialize
        data = report.to_dict()
        assert isinstance(data, dict)

        # Should be able to summarize
        summary = report.summary()
        assert isinstance(summary, str)
        assert len(summary) > 0


class TestEdgeCases:
    """Tests for edge cases across all diagnostic functions."""

    def test_single_coc_single_year(self):
        """Test diagnostics with minimal panel (1 CoC, 1 year)."""
        df = pd.DataFrame(
            {
                "coc_id": ["CO-500"],
                "year": [2024],
                "pit_total": [1200],
                "total_population": [500000],
                "coverage_ratio": [0.95],
                "boundary_changed": [False],
            }
        )

        report = generate_diagnostics_report(df)

        assert len(report.coverage) == 1
        assert report.boundary_changes.empty  # No changes
        assert not report.missingness.empty

    def test_all_missing_optional_columns(self):
        """Test diagnostics when optional columns are all null."""
        df = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CA-600"],
                "year": [2024, 2024],
                "pit_total": [1200, 45000],
                "total_population": [None, None],
                "coverage_ratio": [None, None],
                "boundary_changed": [False, False],
            }
        )

        report = generate_diagnostics_report(df)

        # Should still generate report
        assert isinstance(report, DiagnosticsReport)

        # Missingness should reflect the nulls
        overall = report.missingness[report.missingness["year"] == "all"]
        coverage_missing = overall[overall["column"] == "coverage_ratio"]
        assert coverage_missing.iloc[0]["missing_count"] == 2

    def test_large_number_of_cocs(self):
        """Test diagnostics with many CoCs."""
        n_cocs = 500
        df = pd.DataFrame(
            {
                "coc_id": [f"ST-{i:03d}" for i in range(n_cocs)],
                "year": [2024] * n_cocs,
                "pit_total": list(range(100, 100 + n_cocs)),
                "total_population": [500000 + i * 10000 for i in range(n_cocs)],
                "coverage_ratio": [0.9 + (i % 10) * 0.01 for i in range(n_cocs)],
                "boundary_changed": [i % 5 == 0 for i in range(n_cocs)],
            }
        )

        report = generate_diagnostics_report(df)

        assert report.panel_info["coc_count"] == n_cocs
        assert len(report.boundary_changes) == n_cocs // 5  # 20% have changes

    def test_many_years(self):
        """Test diagnostics with many years."""
        years = list(range(2010, 2025))
        df = pd.DataFrame(
            {
                "coc_id": ["CO-500"] * len(years),
                "year": years,
                "pit_total": [1000 + i * 50 for i in range(len(years))],
                "total_population": [500000] * len(years),
                "coverage_ratio": [0.9 + i * 0.005 for i in range(len(years))],
                "boundary_changed": [i % 3 == 0 for i in range(len(years))],
            }
        )

        report = generate_diagnostics_report(df)

        assert report.panel_info["year_count"] == len(years)
        assert len(report.coverage) == len(years)

    def test_mixed_types_in_columns(self):
        """Test that functions handle mixed types gracefully."""
        df = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CA-600"],
                "year": [2024, 2024],
                "pit_total": [1200, 45000],
                "total_population": [500000.0, 10000000.0],  # Floats
                "coverage_ratio": [0.95, 0.98],
                "boundary_changed": [False, True],
            }
        )

        # Should not raise
        report = generate_diagnostics_report(df)
        assert isinstance(report, DiagnosticsReport)


class TestIntegration:
    """Integration tests for complete diagnostic workflow."""

    @pytest.fixture
    def full_panel_setup(self, tmp_path):
        """Create a complete panel for integration testing."""
        cocs = ["CO-500", "CA-600", "NY-501", "TX-500", "FL-501"]
        years = [2020, 2021, 2022, 2023, 2024]

        rows = []
        for i, year in enumerate(years):
            for j, coc in enumerate(cocs):
                rows.append(
                    {
                        "coc_id": coc,
                        "year": year,
                        "pit_total": 1000 + i * 100 + j * 500,
                        "pit_sheltered": 700 + i * 50 + j * 300,
                        "pit_unsheltered": 300 + i * 50 + j * 200,
                        "boundary_vintage_used": str(year),
                        "acs_vintage_used": str(year - 1),
                        "weighting_method": "population",
                        "total_population": 500000 + j * 1000000,
                        "adult_population": 400000 + j * 800000,
                        "population_below_poverty": 50000 + j * 100000
                        if (i + j) % 3 != 0
                        else None,
                        "median_household_income": 60000 + j * 5000,
                        "median_gross_rent": 1000 + j * 200,
                        "coverage_ratio": 0.90 + (i + j) % 10 * 0.01,
                        "boundary_changed": i > 0 and (i + j) % 2 == 0,
                        "source": "coclab_panel",
                    }
                )

        return pd.DataFrame(rows)

    def test_full_workflow(self, full_panel_setup, tmp_path):
        """Test complete diagnostic workflow."""
        # Generate report
        report = generate_diagnostics_report(full_panel_setup)

        # Verify all components
        assert report.panel_info["row_count"] == 25
        assert report.panel_info["coc_count"] == 5
        assert report.panel_info["year_count"] == 5

        # Export to CSV
        paths = report.to_csv(tmp_path)
        assert len(paths) >= 3

        # Generate summary
        summary = report.summary()
        assert "25" in summary  # row_count
        assert "COVERAGE SUMMARY" in summary

        # Serialize
        data = report.to_dict()
        assert len(data["coverage"]) == 5  # 5 years

    def test_workflow_with_two_panels(self, full_panel_setup, tmp_path):
        """Test workflow comparing two panels with different weighting."""
        panel_area = full_panel_setup.copy()
        panel_area["weighting_method"] = "area"
        panel_area["total_population"] = panel_area["total_population"] * 0.95

        panel_pop = full_panel_setup.copy()

        report = generate_diagnostics_report(panel_area, panel_pop)

        # Should have weighting sensitivity
        assert report.weighting is not None
        assert len(report.weighting) == 25

        # Export should include weighting
        paths = report.to_csv(tmp_path)
        assert "weighting" in paths

        # Summary should mention weighting
        summary = report.summary()
        assert "Mean rate difference" in summary
