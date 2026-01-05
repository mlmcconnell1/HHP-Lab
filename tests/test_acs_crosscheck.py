"""Tests for ACS population crosscheck validator."""

from __future__ import annotations

from pathlib import Path
from io import StringIO
import sys

import pandas as pd
import pytest

from coclab.acs.crosscheck import (
    CrosscheckResult,
    crosscheck_population,
    get_crosscheck_output_path,
    get_measures_path,
    get_rollup_path,
    print_crosscheck_report,
    run_crosscheck,
)
from coclab.provenance import read_provenance


# Test fixtures
@pytest.fixture
def sample_rollup():
    """Sample rollup data for testing."""
    return pd.DataFrame({
        "coc_id": ["CO-500", "CO-501", "CO-502", "CO-503", "CO-504"],
        "coc_population": [100000.0, 50000.0, 75000.0, 25000.0, 10000.0],
        "coverage_ratio": [1.0, 0.98, 0.90, 1.05, 0.80],
        "boundary_vintage": ["2025"] * 5,
        "acs_vintage": ["2019-2023"] * 5,
        "tract_vintage": ["2023"] * 5,
        "weighting_method": ["area"] * 5,
    })


@pytest.fixture
def sample_measures():
    """Sample measures data for testing."""
    return pd.DataFrame({
        "coc_id": ["CO-500", "CO-501", "CO-502", "CO-503", "CO-505"],
        "total_population": [99000.0, 51000.0, 70000.0, 24000.0, 30000.0],
        "boundary_vintage": ["2025"] * 5,
        "acs_vintage": ["2019-2023"] * 5,
    })


class TestCrosscheckPopulation:
    """Tests for crosscheck_population function."""

    def test_delta_computation(self, sample_rollup, sample_measures):
        """Test that delta is computed correctly."""
        result = crosscheck_population(sample_rollup, sample_measures)

        # CO-500: 100000 - 99000 = 1000
        co500 = result.report_df[result.report_df["coc_id"] == "CO-500"].iloc[0]
        assert co500["delta"] == pytest.approx(1000.0)

        # CO-501: 50000 - 51000 = -1000
        co501 = result.report_df[result.report_df["coc_id"] == "CO-501"].iloc[0]
        assert co501["delta"] == pytest.approx(-1000.0)

    def test_pct_delta_computation(self, sample_rollup, sample_measures):
        """Test that pct_delta is computed correctly."""
        result = crosscheck_population(sample_rollup, sample_measures)

        # CO-500: 1000 / 99000 = ~0.0101
        co500 = result.report_df[result.report_df["coc_id"] == "CO-500"].iloc[0]
        assert co500["pct_delta"] == pytest.approx(1000 / 99000)

        # CO-501: -1000 / 51000 = ~-0.0196
        co501 = result.report_df[result.report_df["coc_id"] == "CO-501"].iloc[0]
        assert co501["pct_delta"] == pytest.approx(-1000 / 51000)

    def test_missing_coc_detection(self, sample_rollup, sample_measures):
        """Test detection of missing CoCs in both datasets."""
        result = crosscheck_population(sample_rollup, sample_measures)

        # CO-504 is in rollup but not measures
        assert "CO-504" in result.missing_in_measures

        # CO-505 is in measures but not rollup
        assert "CO-505" in result.missing_in_rollup

    def test_warning_threshold(self):
        """Test that warning threshold is applied correctly."""
        rollup = pd.DataFrame({
            "coc_id": ["A", "B"],
            "coc_population": [101.5, 100.0],  # 1.5% and 0% delta
            "coverage_ratio": [1.0, 1.0],
        })
        measures = pd.DataFrame({
            "coc_id": ["A", "B"],
            "total_population": [100.0, 100.0],
        })

        result = crosscheck_population(rollup, measures, warn_pct=0.01, error_pct=0.05)

        # A has 1.5% delta > 1% warn threshold
        row_a = result.report_df[result.report_df["coc_id"] == "A"].iloc[0]
        assert row_a["status"] == "warning"
        assert "pct_delta" in row_a["issues"]

        # B has 0% delta, should be ok
        row_b = result.report_df[result.report_df["coc_id"] == "B"].iloc[0]
        assert row_b["status"] == "ok"

    def test_error_threshold(self):
        """Test that error threshold is applied correctly."""
        rollup = pd.DataFrame({
            "coc_id": ["A", "B"],
            "coc_population": [106.0, 103.0],  # 6% and 3% delta
            "coverage_ratio": [1.0, 1.0],
        })
        measures = pd.DataFrame({
            "coc_id": ["A", "B"],
            "total_population": [100.0, 100.0],
        })

        result = crosscheck_population(rollup, measures, warn_pct=0.01, error_pct=0.05)

        # A has 6% delta > 5% error threshold
        row_a = result.report_df[result.report_df["coc_id"] == "A"].iloc[0]
        assert row_a["status"] == "error"
        assert "exceeds 5%" in row_a["issues"]

        # B has 3% delta > 1% warn but < 5% error
        row_b = result.report_df[result.report_df["coc_id"] == "B"].iloc[0]
        assert row_b["status"] == "warning"

    def test_coverage_ratio_high_error(self):
        """Test that coverage_ratio > 1.01 triggers error."""
        rollup = pd.DataFrame({
            "coc_id": ["A"],
            "coc_population": [100.0],
            "coverage_ratio": [1.05],  # > 1.01 threshold
        })
        measures = pd.DataFrame({
            "coc_id": ["A"],
            "total_population": [100.0],
        })

        result = crosscheck_population(rollup, measures)

        row = result.report_df.iloc[0]
        assert row["status"] == "error"
        assert "coverage_ratio" in row["issues"]
        assert "> 1.01" in row["issues"]

    def test_coverage_ratio_low_warning(self):
        """Test that coverage_ratio < min_coverage triggers warning."""
        rollup = pd.DataFrame({
            "coc_id": ["A"],
            "coc_population": [100.0],
            "coverage_ratio": [0.90],  # < 0.95 default min_coverage
        })
        measures = pd.DataFrame({
            "coc_id": ["A"],
            "total_population": [100.0],
        })

        result = crosscheck_population(rollup, measures, min_coverage=0.95)

        row = result.report_df.iloc[0]
        assert row["status"] == "warning"
        assert "coverage_ratio" in row["issues"]
        assert "< 0.95" in row["issues"]

    def test_error_count_and_warning_count(self, sample_rollup, sample_measures):
        """Test that error_count and warning_count are computed correctly."""
        result = crosscheck_population(
            sample_rollup, sample_measures,
            warn_pct=0.01, error_pct=0.05, min_coverage=0.95
        )

        # Count should match status values in report_df
        error_rows = result.report_df[result.report_df["status"] == "error"]
        warning_rows = result.report_df[result.report_df["status"] == "warning"]

        assert result.error_count == len(error_rows)
        assert result.warning_count == len(warning_rows)

    def test_passed_property(self):
        """Test that passed property reflects error count."""
        rollup = pd.DataFrame({
            "coc_id": ["A"],
            "coc_population": [100.0],
            "coverage_ratio": [1.0],
        })
        measures = pd.DataFrame({
            "coc_id": ["A"],
            "total_population": [100.0],
        })

        # No errors - should pass
        result = crosscheck_population(rollup, measures)
        assert result.passed is True

        # Add error condition
        rollup_bad = pd.DataFrame({
            "coc_id": ["A"],
            "coc_population": [200.0],  # 100% delta > 5% threshold
            "coverage_ratio": [1.0],
        })
        result_bad = crosscheck_population(rollup_bad, measures)
        assert result_bad.passed is False

    def test_output_schema(self, sample_rollup, sample_measures):
        """Test that output DataFrame has correct schema."""
        result = crosscheck_population(sample_rollup, sample_measures)

        expected_columns = [
            "coc_id",
            "rollup_population",
            "measures_population",
            "delta",
            "pct_delta",
            "coverage_ratio",
            "status",
            "issues",
        ]
        assert list(result.report_df.columns) == expected_columns

    def test_summary_statistics(self, sample_rollup, sample_measures):
        """Test that summary statistics are computed."""
        result = crosscheck_population(sample_rollup, sample_measures)

        assert "total_cocs_rollup" in result.summary
        assert "total_cocs_measures" in result.summary
        assert "matched_cocs" in result.summary
        assert "mean_abs_pct_delta" in result.summary
        assert "total_rollup_population" in result.summary


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_measures_population_zero(self):
        """Test handling when measures population is zero."""
        rollup = pd.DataFrame({
            "coc_id": ["A"],
            "coc_population": [100.0],
            "coverage_ratio": [1.0],
        })
        measures = pd.DataFrame({
            "coc_id": ["A"],
            "total_population": [0.0],
        })

        result = crosscheck_population(rollup, measures)

        row = result.report_df.iloc[0]
        assert row["pct_delta"] == float("inf")
        assert row["status"] == "error"
        assert "zero" in row["issues"].lower()

    def test_missing_rollup_column_raises(self):
        """Test that missing coc_population column raises ValueError."""
        rollup = pd.DataFrame({
            "coc_id": ["A"],
            "population": [100.0],  # Wrong column name
        })
        measures = pd.DataFrame({
            "coc_id": ["A"],
            "total_population": [100.0],
        })

        with pytest.raises(ValueError, match="coc_population"):
            crosscheck_population(rollup, measures)

    def test_missing_measures_column_raises(self):
        """Test that missing total_population column raises ValueError."""
        rollup = pd.DataFrame({
            "coc_id": ["A"],
            "coc_population": [100.0],
        })
        measures = pd.DataFrame({
            "coc_id": ["A"],
            "population": [100.0],  # Wrong column name
        })

        with pytest.raises(ValueError, match="total_population"):
            crosscheck_population(rollup, measures)

    def test_empty_dataframes(self):
        """Test handling of empty DataFrames."""
        rollup = pd.DataFrame({"coc_id": [], "coc_population": []})
        measures = pd.DataFrame({"coc_id": [], "total_population": []})

        result = crosscheck_population(rollup, measures)

        assert len(result.report_df) == 0
        assert result.error_count == 0
        assert result.warning_count == 0
        assert result.passed is True

    def test_no_overlap_between_datasets(self):
        """Test when rollup and measures have completely different CoCs."""
        rollup = pd.DataFrame({
            "coc_id": ["A", "B"],
            "coc_population": [100.0, 200.0],
            "coverage_ratio": [1.0, 1.0],
        })
        measures = pd.DataFrame({
            "coc_id": ["C", "D"],
            "total_population": [150.0, 250.0],
        })

        result = crosscheck_population(rollup, measures)

        assert set(result.missing_in_rollup) == {"C", "D"}
        assert set(result.missing_in_measures) == {"A", "B"}

        # All rows should have issues
        assert all(result.report_df["status"] != "ok")

    def test_negative_delta(self):
        """Test that negative deltas are handled correctly."""
        rollup = pd.DataFrame({
            "coc_id": ["A"],
            "coc_population": [90.0],  # Less than measures
            "coverage_ratio": [1.0],
        })
        measures = pd.DataFrame({
            "coc_id": ["A"],
            "total_population": [100.0],
        })

        result = crosscheck_population(rollup, measures, warn_pct=0.05, error_pct=0.15)

        row = result.report_df.iloc[0]
        assert row["delta"] == pytest.approx(-10.0)
        assert row["pct_delta"] == pytest.approx(-0.10)
        # 10% exceeds 5% warn threshold
        assert row["status"] == "warning"

    def test_missing_coverage_ratio_column(self):
        """Test handling when coverage_ratio is missing from rollup."""
        rollup = pd.DataFrame({
            "coc_id": ["A"],
            "coc_population": [100.0],
            # No coverage_ratio column
        })
        measures = pd.DataFrame({
            "coc_id": ["A"],
            "total_population": [100.0],
        })

        # Should not raise, coverage_ratio should be NA
        result = crosscheck_population(rollup, measures)

        row = result.report_df.iloc[0]
        assert pd.isna(row["coverage_ratio"])
        # Should still evaluate delta correctly
        assert row["status"] == "ok"


class TestPathHelpers:
    """Tests for path helper functions."""

    def test_get_rollup_path_default(self):
        """Test default rollup path."""
        path = get_rollup_path("2025", "2019-2023", "2023", "area")
        expected = Path(
            "data/curated/acs/coc_population_rollup__2025__2019-2023__2023__area.parquet"
        )
        assert path == expected

    def test_get_rollup_path_custom(self):
        """Test custom rollup path."""
        path = get_rollup_path("2025", "2019-2023", "2023", "area", base_dir="/tmp/acs")
        expected = Path(
            "/tmp/acs/coc_population_rollup__2025__2019-2023__2023__area.parquet"
        )
        assert path == expected

    def test_get_measures_path_default(self):
        """Test default measures path."""
        path = get_measures_path("2025", "2019-2023")
        expected = Path("data/curated/measures/coc_measures__2025__2019-2023.parquet")
        assert path == expected

    def test_get_measures_path_custom(self):
        """Test custom measures path."""
        path = get_measures_path("2025", "2019-2023", base_dir="/tmp/measures")
        expected = Path("/tmp/measures/coc_measures__2025__2019-2023.parquet")
        assert path == expected

    def test_get_crosscheck_output_path_default(self):
        """Test default crosscheck output path."""
        path = get_crosscheck_output_path("2025", "2019-2023", "2023", "area")
        expected = Path(
            "data/curated/acs/acs_population_crosscheck__2025__2019-2023__2023__area.parquet"
        )
        assert path == expected

    def test_get_crosscheck_output_path_population_mass(self):
        """Test crosscheck output path with population_mass weighting."""
        path = get_crosscheck_output_path("2025", "2019-2023", "2023", "population_mass")
        expected = Path(
            "data/curated/acs/acs_population_crosscheck__2025__2019-2023__2023__population_mass.parquet"
        )
        assert path == expected


class TestRunCrosscheck:
    """Tests for run_crosscheck function."""

    def test_creates_output_file(self, sample_rollup, sample_measures, tmp_path):
        """Test that run_crosscheck creates the output parquet file."""
        # Setup input files
        acs_dir = tmp_path / "acs"
        measures_dir = tmp_path / "measures"
        acs_dir.mkdir()
        measures_dir.mkdir()

        rollup_path = acs_dir / "coc_population_rollup__2025__2019-2023__2023__area.parquet"
        measures_path = measures_dir / "coc_measures__2025__2019-2023.parquet"

        sample_rollup.to_parquet(rollup_path)
        sample_measures.to_parquet(measures_path)

        # Run crosscheck
        result = run_crosscheck(
            boundary_vintage="2025",
            acs_vintage="2019-2023",
            tract_vintage="2023",
            weighting="area",
            acs_dir=acs_dir,
            measures_dir=measures_dir,
            output_dir=acs_dir,
        )

        # Check output file exists
        output_path = acs_dir / "acs_population_crosscheck__2025__2019-2023__2023__area.parquet"
        assert output_path.exists()

    def test_output_matches_crosscheck_result(self, sample_rollup, sample_measures, tmp_path):
        """Test that saved file matches the returned result."""
        # Setup input files
        acs_dir = tmp_path / "acs"
        measures_dir = tmp_path / "measures"
        acs_dir.mkdir()
        measures_dir.mkdir()

        rollup_path = acs_dir / "coc_population_rollup__2025__2019-2023__2023__area.parquet"
        measures_path = measures_dir / "coc_measures__2025__2019-2023.parquet"

        sample_rollup.to_parquet(rollup_path)
        sample_measures.to_parquet(measures_path)

        # Run crosscheck
        result = run_crosscheck(
            boundary_vintage="2025",
            acs_vintage="2019-2023",
            tract_vintage="2023",
            weighting="area",
            acs_dir=acs_dir,
            measures_dir=measures_dir,
            output_dir=acs_dir,
        )

        # Read saved file
        output_path = acs_dir / "acs_population_crosscheck__2025__2019-2023__2023__area.parquet"
        saved_df = pd.read_parquet(output_path)

        # Compare
        pd.testing.assert_frame_equal(result.report_df, saved_df)

    def test_includes_provenance(self, sample_rollup, sample_measures, tmp_path):
        """Test that output file includes provenance metadata."""
        # Setup input files
        acs_dir = tmp_path / "acs"
        measures_dir = tmp_path / "measures"
        acs_dir.mkdir()
        measures_dir.mkdir()

        rollup_path = acs_dir / "coc_population_rollup__2025__2019-2023__2023__area.parquet"
        measures_path = measures_dir / "coc_measures__2025__2019-2023.parquet"

        sample_rollup.to_parquet(rollup_path)
        sample_measures.to_parquet(measures_path)

        # Run crosscheck
        run_crosscheck(
            boundary_vintage="2025",
            acs_vintage="2019-2023",
            tract_vintage="2023",
            weighting="area",
            warn_pct=0.02,
            error_pct=0.10,
            acs_dir=acs_dir,
            measures_dir=measures_dir,
            output_dir=acs_dir,
        )

        # Read provenance
        output_path = acs_dir / "acs_population_crosscheck__2025__2019-2023__2023__area.parquet"
        provenance = read_provenance(output_path)

        assert provenance is not None
        assert provenance.boundary_vintage == "2025"
        assert provenance.acs_vintage == "2019-2023"
        assert provenance.tract_vintage == "2023"
        assert provenance.weighting == "area"
        assert provenance.extra.get("dataset") == "acs_population_crosscheck"
        assert provenance.extra.get("warn_pct") == 0.02
        assert provenance.extra.get("error_pct") == 0.10

    def test_missing_rollup_file_raises(self, sample_measures, tmp_path):
        """Test that missing rollup file raises FileNotFoundError."""
        measures_dir = tmp_path / "measures"
        measures_dir.mkdir()

        measures_path = measures_dir / "coc_measures__2025__2019-2023.parquet"
        sample_measures.to_parquet(measures_path)

        with pytest.raises(FileNotFoundError, match="Rollup file not found"):
            run_crosscheck(
                boundary_vintage="2025",
                acs_vintage="2019-2023",
                tract_vintage="2023",
                acs_dir=tmp_path / "acs",
                measures_dir=measures_dir,
            )

    def test_missing_measures_file_raises(self, sample_rollup, tmp_path):
        """Test that missing measures file raises FileNotFoundError."""
        acs_dir = tmp_path / "acs"
        acs_dir.mkdir()

        rollup_path = acs_dir / "coc_population_rollup__2025__2019-2023__2023__area.parquet"
        sample_rollup.to_parquet(rollup_path)

        with pytest.raises(FileNotFoundError, match="Measures file not found"):
            run_crosscheck(
                boundary_vintage="2025",
                acs_vintage="2019-2023",
                tract_vintage="2023",
                acs_dir=acs_dir,
                measures_dir=tmp_path / "measures",
            )

    def test_save_report_false(self, sample_rollup, sample_measures, tmp_path):
        """Test that save_report=False skips file creation."""
        # Setup input files
        acs_dir = tmp_path / "acs"
        measures_dir = tmp_path / "measures"
        acs_dir.mkdir()
        measures_dir.mkdir()

        rollup_path = acs_dir / "coc_population_rollup__2025__2019-2023__2023__area.parquet"
        measures_path = measures_dir / "coc_measures__2025__2019-2023.parquet"

        sample_rollup.to_parquet(rollup_path)
        sample_measures.to_parquet(measures_path)

        # Run crosscheck without saving
        result = run_crosscheck(
            boundary_vintage="2025",
            acs_vintage="2019-2023",
            tract_vintage="2023",
            weighting="area",
            acs_dir=acs_dir,
            measures_dir=measures_dir,
            output_dir=acs_dir,
            save_report=False,
        )

        # Output file should not exist
        output_path = acs_dir / "acs_population_crosscheck__2025__2019-2023__2023__area.parquet"
        assert not output_path.exists()

        # But result should still be valid
        assert result.report_df is not None
        assert len(result.report_df) > 0


class TestPrintCrosscheckReport:
    """Tests for print_crosscheck_report function."""

    def test_returns_zero_on_pass(self, sample_rollup, sample_measures):
        """Test that print_crosscheck_report returns 0 when no errors."""
        # Create perfect match
        rollup = pd.DataFrame({
            "coc_id": ["A"],
            "coc_population": [100.0],
            "coverage_ratio": [1.0],
        })
        measures = pd.DataFrame({
            "coc_id": ["A"],
            "total_population": [100.0],
        })

        result = crosscheck_population(rollup, measures)

        # Capture stdout
        old_stdout = sys.stdout
        sys.stdout = StringIO()

        exit_code = print_crosscheck_report(result)

        sys.stdout = old_stdout

        assert exit_code == 0

    def test_returns_two_on_error(self):
        """Test that print_crosscheck_report returns 2 when errors found."""
        # Create error condition
        rollup = pd.DataFrame({
            "coc_id": ["A"],
            "coc_population": [200.0],  # 100% delta > 5% threshold
            "coverage_ratio": [1.0],
        })
        measures = pd.DataFrame({
            "coc_id": ["A"],
            "total_population": [100.0],
        })

        result = crosscheck_population(rollup, measures)

        # Capture stdout
        old_stdout = sys.stdout
        sys.stdout = StringIO()

        exit_code = print_crosscheck_report(result)

        sys.stdout = old_stdout

        assert exit_code == 2

    def test_prints_summary_section(self, sample_rollup, sample_measures):
        """Test that report includes summary section."""
        result = crosscheck_population(sample_rollup, sample_measures)

        # Capture stdout
        old_stdout = sys.stdout
        sys.stdout = buffer = StringIO()

        print_crosscheck_report(result)

        output = buffer.getvalue()
        sys.stdout = old_stdout

        assert "Summary:" in output
        assert "CoCs in rollup:" in output
        assert "CoCs in measures:" in output

    def test_prints_top_worst_deltas(self, sample_rollup, sample_measures):
        """Test that report includes top worst deltas section."""
        result = crosscheck_population(sample_rollup, sample_measures)

        # Capture stdout
        old_stdout = sys.stdout
        sys.stdout = buffer = StringIO()

        print_crosscheck_report(result, top_n=5)

        output = buffer.getvalue()
        sys.stdout = old_stdout

        assert "Worst Deltas" in output

    def test_prints_missing_cocs(self, sample_rollup, sample_measures):
        """Test that report includes missing CoCs sections."""
        result = crosscheck_population(sample_rollup, sample_measures)

        # Capture stdout
        old_stdout = sys.stdout
        sys.stdout = buffer = StringIO()

        print_crosscheck_report(result)

        output = buffer.getvalue()
        sys.stdout = old_stdout

        # Should mention missing CoCs
        if result.missing_in_rollup:
            assert "Missing in Rollup" in output
        if result.missing_in_measures:
            assert "Missing in Measures" in output


class TestCrosscheckResultDataclass:
    """Tests for CrosscheckResult dataclass."""

    def test_default_values(self):
        """Test default values of CrosscheckResult."""
        result = CrosscheckResult()

        assert result.error_count == 0
        assert result.warning_count == 0
        assert isinstance(result.report_df, pd.DataFrame)
        assert len(result.report_df) == 0
        assert result.missing_in_rollup == []
        assert result.missing_in_measures == []
        assert result.summary == {}
        assert result.passed is True

    def test_passed_reflects_error_count(self):
        """Test that passed property reflects error_count."""
        result = CrosscheckResult(error_count=0, warning_count=5)
        assert result.passed is True

        result = CrosscheckResult(error_count=1, warning_count=0)
        assert result.passed is False


class TestThresholdClassification:
    """Tests for threshold-based status classification."""

    def test_classification_ok(self):
        """Test that values within thresholds are classified as OK."""
        rollup = pd.DataFrame({
            "coc_id": ["A"],
            "coc_population": [100.5],  # 0.5% delta
            "coverage_ratio": [0.98],   # Above 0.95 min_coverage
        })
        measures = pd.DataFrame({
            "coc_id": ["A"],
            "total_population": [100.0],
        })

        result = crosscheck_population(
            rollup, measures, warn_pct=0.01, error_pct=0.05, min_coverage=0.95
        )

        row = result.report_df.iloc[0]
        assert row["status"] == "ok"

    def test_classification_warning_from_delta(self):
        """Test warning classification from delta threshold."""
        rollup = pd.DataFrame({
            "coc_id": ["A"],
            "coc_population": [102.0],  # 2% delta
            "coverage_ratio": [1.0],
        })
        measures = pd.DataFrame({
            "coc_id": ["A"],
            "total_population": [100.0],
        })

        result = crosscheck_population(
            rollup, measures, warn_pct=0.01, error_pct=0.05
        )

        row = result.report_df.iloc[0]
        assert row["status"] == "warning"

    def test_classification_warning_from_coverage(self):
        """Test warning classification from low coverage."""
        rollup = pd.DataFrame({
            "coc_id": ["A"],
            "coc_population": [100.0],  # 0% delta
            "coverage_ratio": [0.90],   # Below 0.95
        })
        measures = pd.DataFrame({
            "coc_id": ["A"],
            "total_population": [100.0],
        })

        result = crosscheck_population(
            rollup, measures, warn_pct=0.01, error_pct=0.05, min_coverage=0.95
        )

        row = result.report_df.iloc[0]
        assert row["status"] == "warning"
        assert "coverage_ratio" in row["issues"]

    def test_classification_error_from_delta(self):
        """Test error classification from delta threshold."""
        rollup = pd.DataFrame({
            "coc_id": ["A"],
            "coc_population": [110.0],  # 10% delta
            "coverage_ratio": [1.0],
        })
        measures = pd.DataFrame({
            "coc_id": ["A"],
            "total_population": [100.0],
        })

        result = crosscheck_population(
            rollup, measures, warn_pct=0.01, error_pct=0.05
        )

        row = result.report_df.iloc[0]
        assert row["status"] == "error"

    def test_classification_error_from_high_coverage(self):
        """Test error classification from high coverage ratio."""
        rollup = pd.DataFrame({
            "coc_id": ["A"],
            "coc_population": [100.0],
            "coverage_ratio": [1.10],  # > 1.01
        })
        measures = pd.DataFrame({
            "coc_id": ["A"],
            "total_population": [100.0],
        })

        result = crosscheck_population(rollup, measures)

        row = result.report_df.iloc[0]
        assert row["status"] == "error"
        assert "coverage_ratio" in row["issues"]

    def test_error_takes_precedence_over_warning(self):
        """Test that error status takes precedence over warning."""
        rollup = pd.DataFrame({
            "coc_id": ["A"],
            "coc_population": [110.0],  # 10% delta -> error
            "coverage_ratio": [0.90],   # Low coverage -> warning
        })
        measures = pd.DataFrame({
            "coc_id": ["A"],
            "total_population": [100.0],
        })

        result = crosscheck_population(
            rollup, measures, warn_pct=0.01, error_pct=0.05, min_coverage=0.95
        )

        row = result.report_df.iloc[0]
        # Should be error, not warning
        assert row["status"] == "error"
        # Both issues should be listed
        assert "pct_delta" in row["issues"]
        assert "coverage_ratio" in row["issues"]
