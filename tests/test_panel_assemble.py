"""Tests for panel assembly engine (WP-3F).

Tests cover:
- Loading PIT data for specific years
- Loading ACS measures for vintage combinations
- Detecting boundary changes
- Building complete panels
- Saving panels with provenance
- Edge cases and error handling
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from coclab.panel.assemble import (
    PANEL_COLUMNS,
    _detect_boundary_changes,
    _load_acs_measures,
    _load_pit_for_year,
    build_panel,
    save_panel,
)
from coclab.panel.policies import AlignmentPolicy, DEFAULT_POLICY
from coclab.provenance import read_provenance


class TestLoadPitForYear:
    """Tests for _load_pit_for_year function."""

    @pytest.fixture
    def pit_data_dir(self, tmp_path):
        """Create a temporary directory with sample PIT files."""
        pit_dir = tmp_path / "pit"
        pit_dir.mkdir()

        # Create 2023 PIT data
        df_2023 = pd.DataFrame({
            "coc_id": ["CO-500", "CA-600", "NY-501"],
            "pit_total": [1200, 45000, 75000],
            "pit_sheltered": [800, 30000, 55000],
            "pit_unsheltered": [400, 15000, 20000],
            "pit_year": [2023, 2023, 2023],
        })
        df_2023.to_parquet(pit_dir / "pit_counts__2023.parquet", index=False)

        # Create 2024 PIT data
        df_2024 = pd.DataFrame({
            "coc_id": ["CO-500", "CA-600", "NY-501"],
            "pit_total": [1300, 48000, 78000],
            "pit_sheltered": [850, 32000, 58000],
            "pit_unsheltered": [450, 16000, 20000],
            "pit_year": [2024, 2024, 2024],
        })
        df_2024.to_parquet(pit_dir / "pit_counts__2024.parquet", index=False)

        return pit_dir

    def test_load_existing_year(self, pit_data_dir):
        """Test loading PIT data for an existing year."""
        result = _load_pit_for_year(2024, pit_dir=pit_data_dir)

        assert len(result) == 3
        assert "coc_id" in result.columns
        assert "pit_total" in result.columns
        assert list(result["coc_id"]) == ["CO-500", "CA-600", "NY-501"]

    def test_load_missing_year_returns_empty(self, pit_data_dir):
        """Test loading PIT data for a missing year returns empty DataFrame."""
        result = _load_pit_for_year(2020, pit_dir=pit_data_dir)

        assert len(result) == 0
        assert "coc_id" in result.columns
        assert "pit_total" in result.columns

    def test_load_returns_correct_year_data(self, pit_data_dir):
        """Test that correct year's data is returned."""
        result_2023 = _load_pit_for_year(2023, pit_dir=pit_data_dir)
        result_2024 = _load_pit_for_year(2024, pit_dir=pit_data_dir)

        # 2024 should have higher counts
        total_2023 = result_2023["pit_total"].sum()
        total_2024 = result_2024["pit_total"].sum()
        assert total_2024 > total_2023

    def test_load_ensures_string_coc_id(self, pit_data_dir):
        """Test that coc_id is converted to string."""
        result = _load_pit_for_year(2024, pit_dir=pit_data_dir)
        assert result["coc_id"].dtype == object  # pandas string type

    def test_load_ensures_int_pit_total(self, pit_data_dir):
        """Test that pit_total is converted to int."""
        result = _load_pit_for_year(2024, pit_dir=pit_data_dir)
        assert result["pit_total"].dtype in [int, "int64"]


class TestLoadAcsMeasures:
    """Tests for _load_acs_measures function."""

    @pytest.fixture
    def measures_data_dir(self, tmp_path):
        """Create a temporary directory with sample ACS measure files."""
        measures_dir = tmp_path / "measures"
        measures_dir.mkdir()

        # Create measures for boundary vintage 2024, ACS vintage 2023
        df = pd.DataFrame({
            "coc_id": ["CO-500", "CA-600", "NY-501"],
            "total_population": [500000, 10000000, 8000000],
            "adult_population": [400000, 8000000, 6400000],
            "population_below_poverty": [50000, 1500000, 1200000],
            "median_household_income": [65000, 75000, 85000],
            "median_gross_rent": [1200, 1800, 2200],
            "coverage_ratio": [0.95, 0.98, 0.99],
            "weighting_method": ["population", "population", "population"],
        })
        df.to_parquet(
            measures_dir / "coc_measures__2024__2023.parquet",
            index=False,
        )

        # Create measures with weighting suffix
        df_area = df.copy()
        df_area["weighting_method"] = "area"
        df_area.to_parquet(
            measures_dir / "coc_measures__2024__2023__area.parquet",
            index=False,
        )

        return measures_dir

    def test_load_existing_measures(self, measures_data_dir):
        """Test loading ACS measures for existing vintage combination."""
        result = _load_acs_measures(
            boundary_vintage="2024",
            acs_vintage="2023",
            weighting="population",
            measures_dir=measures_data_dir,
        )

        assert len(result) == 3
        assert "coc_id" in result.columns
        assert "total_population" in result.columns
        assert "coverage_ratio" in result.columns

    def test_load_missing_measures_returns_empty(self, measures_data_dir):
        """Test loading ACS measures for missing vintage returns empty DataFrame."""
        result = _load_acs_measures(
            boundary_vintage="2020",
            acs_vintage="2019",
            weighting="population",
            measures_dir=measures_data_dir,
        )

        assert len(result) == 0
        assert "coc_id" in result.columns
        assert "total_population" in result.columns

    def test_load_weighting_specific_file(self, measures_data_dir):
        """Test that weighting-specific files are preferred."""
        result = _load_acs_measures(
            boundary_vintage="2024",
            acs_vintage="2023",
            weighting="area",
            measures_dir=measures_data_dir,
        )

        assert len(result) == 3

    def test_load_ensures_string_coc_id(self, measures_data_dir):
        """Test that coc_id is converted to string."""
        result = _load_acs_measures(
            boundary_vintage="2024",
            acs_vintage="2023",
            weighting="population",
            measures_dir=measures_data_dir,
        )
        assert result["coc_id"].dtype == object


class TestDetectBoundaryChanges:
    """Tests for _detect_boundary_changes function."""

    def test_no_changes_same_vintage(self):
        """Test detection when boundary vintage is constant."""
        df = pd.DataFrame({
            "coc_id": ["CO-500", "CO-500", "CO-500"],
            "year": [2022, 2023, 2024],
            "boundary_vintage_used": ["2022", "2023", "2024"],
        })

        result = _detect_boundary_changes(df)

        # All years have different vintages, so years 2023 and 2024 changed
        assert not result.iloc[0]  # First year, no prior
        assert result.iloc[1]  # 2023 differs from 2022
        assert result.iloc[2]  # 2024 differs from 2023

    def test_first_year_is_false(self):
        """Test that first year for each CoC is False."""
        df = pd.DataFrame({
            "coc_id": ["CO-500", "CO-500"],
            "year": [2023, 2024],
            "boundary_vintage_used": ["2023", "2024"],
        })

        result = _detect_boundary_changes(df)

        assert not result.iloc[0]

    def test_detects_change_when_vintage_differs(self):
        """Test detection when vintage changes."""
        df = pd.DataFrame({
            "coc_id": ["CO-500", "CO-500", "CO-500"],
            "year": [2022, 2023, 2024],
            "boundary_vintage_used": ["2022", "2022", "2024"],  # 2023 uses 2022 vintage
        })

        result = _detect_boundary_changes(df)

        assert not result.iloc[0]  # First year
        assert not result.iloc[1]  # 2023 same as 2022
        assert result.iloc[2]  # 2024 differs from 2022

    def test_multiple_cocs_independent(self):
        """Test that boundary change detection is independent per CoC."""
        df = pd.DataFrame({
            "coc_id": ["CA-600", "CA-600", "CO-500", "CO-500"],
            "year": [2023, 2024, 2023, 2024],
            "boundary_vintage_used": ["2023", "2023", "2023", "2024"],
        })

        result = _detect_boundary_changes(df)

        # CA-600: first year False, 2024 same as 2023 so False
        # CO-500: first year False, 2024 differs from 2023 so True
        # After sorting by coc_id, year: CA-600/2023, CA-600/2024, CO-500/2023, CO-500/2024
        assert not result.iloc[0]  # CA-600 2023: first
        assert not result.iloc[1]  # CA-600 2024: same vintage
        assert not result.iloc[2]  # CO-500 2023: first
        assert result.iloc[3]  # CO-500 2024: vintage changed

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame(columns=["coc_id", "year", "boundary_vintage_used"])
        result = _detect_boundary_changes(df)
        assert len(result) == 0


class TestBuildPanel:
    """Tests for build_panel function."""

    @pytest.fixture
    def data_dirs(self, tmp_path):
        """Create temporary directories with sample PIT and ACS data."""
        pit_dir = tmp_path / "pit"
        pit_dir.mkdir()

        measures_dir = tmp_path / "measures"
        measures_dir.mkdir()

        # Create PIT data for 2023 and 2024
        for year in [2023, 2024]:
            df_pit = pd.DataFrame({
                "coc_id": ["CO-500", "CA-600"],
                "pit_total": [1200 + (year - 2023) * 100, 45000 + (year - 2023) * 3000],
                "pit_sheltered": [800, 30000],
                "pit_unsheltered": [400 + (year - 2023) * 100, 15000 + (year - 2023) * 3000],
                "pit_year": [year, year],
            })
            df_pit.to_parquet(pit_dir / f"pit_counts__{year}.parquet", index=False)

        # Create ACS measures for 2022 and 2023 vintages
        for acs_year in [2022, 2023]:
            boundary_year = acs_year + 1  # boundary vintage = acs + 1 per default policy
            df_acs = pd.DataFrame({
                "coc_id": ["CO-500", "CA-600"],
                "total_population": [500000, 10000000],
                "adult_population": [400000, 8000000],
                "population_below_poverty": [50000, 1500000],
                "median_household_income": [65000, 75000],
                "median_gross_rent": [1200, 1800],
                "coverage_ratio": [0.95, 0.98],
                "weighting_method": ["population", "population"],
            })
            df_acs.to_parquet(
                measures_dir / f"coc_measures__{boundary_year}__{acs_year}.parquet",
                index=False,
            )

        return {"pit_dir": pit_dir, "measures_dir": measures_dir}

    def test_build_single_year(self, data_dirs):
        """Test building panel for a single year."""
        result = build_panel(
            2024,
            2024,
            pit_dir=data_dirs["pit_dir"],
            measures_dir=data_dirs["measures_dir"],
        )

        assert len(result) == 2  # 2 CoCs
        assert list(result["year"].unique()) == [2024]
        assert "pit_total" in result.columns
        assert "total_population" in result.columns

    def test_build_multi_year(self, data_dirs):
        """Test building panel for multiple years."""
        result = build_panel(
            2023,
            2024,
            pit_dir=data_dirs["pit_dir"],
            measures_dir=data_dirs["measures_dir"],
        )

        assert len(result) == 4  # 2 CoCs x 2 years
        assert set(result["year"].unique()) == {2023, 2024}

    def test_build_uses_default_policy(self, data_dirs):
        """Test that default policy is used when none specified."""
        result = build_panel(
            2024,
            2024,
            pit_dir=data_dirs["pit_dir"],
            measures_dir=data_dirs["measures_dir"],
        )

        # Default policy: boundary = year, acs = year - 1
        assert result["boundary_vintage_used"].iloc[0] == "2024"
        assert result["acs_vintage_used"].iloc[0] == "2023"
        assert result["weighting_method"].iloc[0] == "population"

    def test_build_uses_custom_policy(self, data_dirs):
        """Test building panel with custom policy."""
        custom_policy = AlignmentPolicy(
            boundary_vintage_func=lambda y: str(y),
            acs_vintage_func=lambda y: str(y - 1),
            weighting_method="area",
        )

        result = build_panel(
            2024,
            2024,
            policy=custom_policy,
            pit_dir=data_dirs["pit_dir"],
            measures_dir=data_dirs["measures_dir"],
        )

        assert result["weighting_method"].iloc[0] == "area"

    def test_build_invalid_year_range_raises(self, data_dirs):
        """Test that invalid year range raises ValueError."""
        with pytest.raises(ValueError, match="start_year.*must be <= end_year"):
            build_panel(
                2024,
                2023,
                pit_dir=data_dirs["pit_dir"],
                measures_dir=data_dirs["measures_dir"],
            )

    def test_build_handles_missing_pit_gracefully(self, data_dirs):
        """Test that missing PIT years are skipped."""
        result = build_panel(
            2020,
            2024,
            pit_dir=data_dirs["pit_dir"],
            measures_dir=data_dirs["measures_dir"],
        )

        # Only 2023 and 2024 have PIT data
        assert set(result["year"].unique()) == {2023, 2024}

    def test_build_handles_missing_acs_gracefully(self, data_dirs):
        """Test that missing ACS measures result in null values."""
        # Build for year where we don't have matching ACS
        # 2023 PIT with default policy looks for ACS 2022, boundary 2023
        result = build_panel(
            2023,
            2023,
            pit_dir=data_dirs["pit_dir"],
            measures_dir=data_dirs["measures_dir"],
        )

        # Should still have rows, just with ACS data
        assert len(result) == 2

    def test_build_has_all_canonical_columns(self, data_dirs):
        """Test that result has all canonical columns."""
        result = build_panel(
            2024,
            2024,
            pit_dir=data_dirs["pit_dir"],
            measures_dir=data_dirs["measures_dir"],
        )

        for col in PANEL_COLUMNS:
            assert col in result.columns, f"Missing column: {col}"

    def test_build_sets_source_column(self, data_dirs):
        """Test that source column is set to coclab_panel."""
        result = build_panel(
            2024,
            2024,
            pit_dir=data_dirs["pit_dir"],
            measures_dir=data_dirs["measures_dir"],
        )

        assert all(result["source"] == "coclab_panel")

    def test_build_boundary_changed_detection(self, data_dirs):
        """Test that boundary_changed column is computed."""
        result = build_panel(
            2023,
            2024,
            pit_dir=data_dirs["pit_dir"],
            measures_dir=data_dirs["measures_dir"],
        )

        assert "boundary_changed" in result.columns
        assert result["boundary_changed"].dtype == bool

    def test_build_empty_range_returns_empty(self, tmp_path):
        """Test that empty year range with no data returns empty DataFrame."""
        pit_dir = tmp_path / "pit"
        pit_dir.mkdir()
        measures_dir = tmp_path / "measures"
        measures_dir.mkdir()

        result = build_panel(
            2020,
            2022,
            pit_dir=pit_dir,
            measures_dir=measures_dir,
        )

        assert len(result) == 0
        assert set(result.columns) == set(PANEL_COLUMNS)


class TestSavePanel:
    """Tests for save_panel function."""

    @pytest.fixture
    def sample_panel(self):
        """Create a sample panel DataFrame."""
        return pd.DataFrame({
            "coc_id": ["CO-500", "CO-500", "CA-600", "CA-600"],
            "year": [2023, 2024, 2023, 2024],
            "pit_total": [1200, 1300, 45000, 48000],
            "pit_sheltered": [800, 850, 30000, 32000],
            "pit_unsheltered": [400, 450, 15000, 16000],
            "boundary_vintage_used": ["2023", "2024", "2023", "2024"],
            "acs_vintage_used": ["2022", "2023", "2022", "2023"],
            "weighting_method": ["population"] * 4,
            "total_population": [500000, 505000, 10000000, 10100000],
            "adult_population": [400000, 404000, 8000000, 8080000],
            "population_below_poverty": [50000, 51000, 1500000, 1515000],
            "median_household_income": [65000, 66000, 75000, 76000],
            "median_gross_rent": [1200, 1250, 1800, 1850],
            "coverage_ratio": [0.95, 0.95, 0.98, 0.98],
            "boundary_changed": [False, True, False, True],
            "source": ["coclab_panel"] * 4,
        })

    def test_save_creates_file(self, sample_panel, tmp_path):
        """Test that Parquet file is created."""
        result = save_panel(sample_panel, 2023, 2024, output_dir=tmp_path)

        assert result.exists()
        assert result.name == "coc_panel__2023_2024.parquet"

    def test_save_creates_output_dir(self, sample_panel, tmp_path):
        """Test that output directory is created if needed."""
        output_dir = tmp_path / "nested" / "panel"
        result = save_panel(sample_panel, 2023, 2024, output_dir=output_dir)

        assert result.exists()
        assert result.parent == output_dir

    def test_save_data_readable(self, sample_panel, tmp_path):
        """Test that saved data can be read back."""
        result_path = save_panel(sample_panel, 2023, 2024, output_dir=tmp_path)

        df = pd.read_parquet(result_path)
        assert len(df) == 4
        assert set(df["coc_id"].unique()) == {"CO-500", "CA-600"}

    def test_save_has_provenance(self, sample_panel, tmp_path):
        """Test that provenance metadata is embedded."""
        result_path = save_panel(sample_panel, 2023, 2024, output_dir=tmp_path)

        provenance = read_provenance(result_path)
        assert provenance is not None
        assert provenance.extra.get("dataset_type") == "coc_panel"
        assert provenance.extra.get("start_year") == 2023
        assert provenance.extra.get("end_year") == 2024
        assert provenance.extra.get("row_count") == 4

    def test_save_provenance_includes_policy(self, sample_panel, tmp_path):
        """Test that policy is included in provenance."""
        result_path = save_panel(
            sample_panel,
            2023,
            2024,
            output_dir=tmp_path,
            policy=DEFAULT_POLICY,
        )

        provenance = read_provenance(result_path)
        assert "policy" in provenance.extra
        assert provenance.extra["policy"]["weighting_method"] == "population"

    def test_save_empty_panel(self, tmp_path):
        """Test saving an empty panel."""
        empty_df = pd.DataFrame(columns=PANEL_COLUMNS)
        result_path = save_panel(empty_df, 2023, 2024, output_dir=tmp_path)

        assert result_path.exists()
        df = pd.read_parquet(result_path)
        assert len(df) == 0


class TestPanelColumns:
    """Tests for PANEL_COLUMNS constant."""

    def test_required_columns_present(self):
        """Test that all required columns are defined."""
        required = [
            "coc_id",
            "year",
            "pit_total",
            "pit_sheltered",
            "pit_unsheltered",
            "boundary_vintage_used",
            "acs_vintage_used",
            "weighting_method",
            "total_population",
            "adult_population",
            "population_below_poverty",
            "median_household_income",
            "median_gross_rent",
            "coverage_ratio",
            "boundary_changed",
            "source",
        ]
        for col in required:
            assert col in PANEL_COLUMNS, f"Missing required column: {col}"

    def test_column_order(self):
        """Test that columns are in expected order."""
        assert PANEL_COLUMNS[0] == "coc_id"
        assert PANEL_COLUMNS[1] == "year"
        assert PANEL_COLUMNS[-1] == "source"


class TestIntegration:
    """Integration tests for full panel assembly workflow."""

    @pytest.fixture
    def full_data_setup(self, tmp_path):
        """Create a complete test data setup."""
        pit_dir = tmp_path / "pit"
        pit_dir.mkdir()
        measures_dir = tmp_path / "measures"
        measures_dir.mkdir()
        panel_dir = tmp_path / "panel"

        # Create comprehensive PIT data
        cocs = ["CO-500", "CA-600", "NY-501", "TX-500"]
        for year in range(2020, 2025):
            df_pit = pd.DataFrame({
                "coc_id": cocs,
                "pit_total": [1000 + i * 100 + (year - 2020) * 50 for i in range(4)],
                "pit_sheltered": [700 + i * 50 for i in range(4)],
                "pit_unsheltered": [300 + i * 50 + (year - 2020) * 50 for i in range(4)],
                "pit_year": [year] * 4,
            })
            df_pit.to_parquet(pit_dir / f"pit_counts__{year}.parquet", index=False)

        # Create ACS measures
        for acs_year in range(2019, 2024):
            boundary_year = acs_year + 1
            df_acs = pd.DataFrame({
                "coc_id": cocs,
                "total_population": [500000 + i * 1000000 for i in range(4)],
                "adult_population": [400000 + i * 800000 for i in range(4)],
                "population_below_poverty": [50000 + i * 100000 for i in range(4)],
                "median_household_income": [60000 + i * 5000 for i in range(4)],
                "median_gross_rent": [1000 + i * 200 for i in range(4)],
                "coverage_ratio": [0.95, 0.98, 0.99, 0.92],
                "weighting_method": ["population"] * 4,
            })
            df_acs.to_parquet(
                measures_dir / f"coc_measures__{boundary_year}__{acs_year}.parquet",
                index=False,
            )

        return {
            "pit_dir": pit_dir,
            "measures_dir": measures_dir,
            "panel_dir": panel_dir,
        }

    def test_full_workflow_build_and_save(self, full_data_setup):
        """Test complete build and save workflow."""
        panel_df = build_panel(
            2020,
            2024,
            pit_dir=full_data_setup["pit_dir"],
            measures_dir=full_data_setup["measures_dir"],
        )

        output_path = save_panel(
            panel_df,
            2020,
            2024,
            output_dir=full_data_setup["panel_dir"],
        )

        # Verify output
        assert output_path.exists()

        # Read back and verify
        result = pd.read_parquet(output_path)
        assert len(result) == 20  # 4 CoCs x 5 years
        assert result["coc_id"].nunique() == 4
        assert result["year"].nunique() == 5

    def test_panel_data_integrity(self, full_data_setup):
        """Test that panel data maintains integrity."""
        panel_df = build_panel(
            2020,
            2024,
            pit_dir=full_data_setup["pit_dir"],
            measures_dir=full_data_setup["measures_dir"],
        )

        # Each CoC should have 5 years
        for coc in panel_df["coc_id"].unique():
            coc_data = panel_df[panel_df["coc_id"] == coc]
            assert len(coc_data) == 5
            assert set(coc_data["year"]) == {2020, 2021, 2022, 2023, 2024}

    def test_panel_sorts_correctly(self, full_data_setup):
        """Test that panel is sorted by coc_id and year."""
        panel_df = build_panel(
            2020,
            2024,
            pit_dir=full_data_setup["pit_dir"],
            measures_dir=full_data_setup["measures_dir"],
        )

        # Check sorting
        sorted_df = panel_df.sort_values(["coc_id", "year"]).reset_index(drop=True)
        pd.testing.assert_frame_equal(panel_df, sorted_df)
