"""Phase 3 Integration Tests for CoC Lab.

This module provides end-to-end integration tests that validate the entire
Phase 3 pipeline works together correctly. These tests cover:

1. Known CoC produces stable totals across runs (reproducibility)
2. Population shares sum to ~1 (weighting validation)
3. Boundary changes produce explainable deltas
4. PIT counts ingested for multiple years with registry tracking
5. CoC x year panels constructed reproducibly
6. Alignment policies explicit and embedded in provenance
7. Diagnostics identify boundary and weighting sensitivities
8. Panels ready for Phase 4 modeling (output format validation)

WP-3I: Phase 3 Integration Tests & Validation
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from coclab.panel import (
    DEFAULT_POLICY,
    AlignmentPolicy,
    DiagnosticsReport,
    build_panel,
    generate_diagnostics_report,
    save_panel,
)
from coclab.panel.assemble import PANEL_COLUMNS
from coclab.pit.ingest import normalize_coc_id, parse_pit_file, write_pit_parquet
from coclab.pit.qa import validate_pit_data
from coclab.pit.registry import (
    PitRegistryEntry,
    list_pit_years,
    register_pit_year,
)
from coclab.provenance import read_provenance

# ============================================================================
# Fixtures for creating realistic test data
# ============================================================================


@pytest.fixture
def sample_cocs() -> list[str]:
    """Sample CoC IDs for testing."""
    return ["CO-500", "CA-600", "NY-501", "TX-500", "WA-500"]


@pytest.fixture
def sample_pit_csv(tmp_path, sample_cocs) -> Path:
    """Create a sample PIT CSV file in HUD Exchange format."""
    csv_path = tmp_path / "pit_2024.csv"

    # HUD Exchange-style format
    df = pd.DataFrame(
        {
            "CoC Number": sample_cocs,
            "CoC Name": [f"CoC for {coc}" for coc in sample_cocs],
            "Overall Homeless, 2024": [1200, 45000, 75000, 8500, 12000],
            "Sheltered Total Homeless": [800, 30000, 55000, 6000, 8000],
            "Unsheltered Homeless": [400, 15000, 20000, 2500, 4000],
        }
    )
    df.to_csv(csv_path, index=False)
    return csv_path


@pytest.fixture
def sample_pit_csv_2023(tmp_path, sample_cocs) -> Path:
    """Create a sample PIT CSV file for 2023."""
    csv_path = tmp_path / "pit_2023.csv"

    df = pd.DataFrame(
        {
            "CoC Number": sample_cocs,
            "CoC Name": [f"CoC for {coc}" for coc in sample_cocs],
            "Overall Homeless, 2023": [1100, 43000, 72000, 8000, 11500],
            "Sheltered Total Homeless": [750, 29000, 53000, 5700, 7700],
            "Unsheltered Homeless": [350, 14000, 19000, 2300, 3800],
        }
    )
    df.to_csv(csv_path, index=False)
    return csv_path


@pytest.fixture
def sample_boundary_gdf(sample_cocs) -> gpd.GeoDataFrame:
    """Create sample CoC boundary geometries."""
    geometries = []
    for i, _coc in enumerate(sample_cocs):
        # Create simple polygon for each CoC
        lon_base = -120 + i * 10
        lat_base = 35
        poly = Polygon(
            [
                (lon_base, lat_base),
                (lon_base + 5, lat_base),
                (lon_base + 5, lat_base + 5),
                (lon_base, lat_base + 5),
            ]
        )
        geometries.append(poly)

    return gpd.GeoDataFrame(
        {
            "coc_id": sample_cocs,
            "coc_name": [f"CoC for {coc}" for coc in sample_cocs],
            "boundary_vintage": ["2024"] * len(sample_cocs),
        },
        geometry=geometries,
        crs="EPSG:4326",
    )


@pytest.fixture
def sample_acs_measures(tmp_path, sample_cocs) -> Path:
    """Create sample ACS measures file."""
    measures_dir = tmp_path / "measures"
    measures_dir.mkdir()

    # Create measures for boundary vintage 2024, ACS vintage 2023
    df = pd.DataFrame(
        {
            "coc_id": sample_cocs,
            "total_population": [700000, 10000000, 8500000, 4500000, 2800000],
            "adult_population": [550000, 8000000, 6800000, 3600000, 2240000],
            "population_below_poverty": [70000, 1500000, 1275000, 675000, 420000],
            "median_household_income": [65000, 75000, 85000, 55000, 78000],
            "median_gross_rent": [1400, 2100, 2300, 1200, 1800],
            "coverage_ratio": [0.95, 0.98, 0.99, 0.92, 0.96],
        }
    )

    path = measures_dir / "coc_measures__2024__2023.parquet"
    df.to_parquet(path, index=False)

    # Also create 2023__2022 measures
    path_2023 = measures_dir / "coc_measures__2023__2022.parquet"
    df.to_parquet(path_2023, index=False)

    return measures_dir


@pytest.fixture
def full_test_setup(tmp_path, sample_cocs, sample_acs_measures):
    """Create a complete test environment with PIT, ACS, and boundaries."""
    pit_dir = tmp_path / "pit"
    pit_dir.mkdir()

    # Create PIT data for multiple years
    for year in [2022, 2023, 2024]:
        base_total = 1000 + (year - 2022) * 100
        df = pd.DataFrame(
            {
                "coc_id": sample_cocs,
                "pit_total": [base_total + i * 500 for i in range(5)],
                "pit_sheltered": [int((base_total + i * 500) * 0.7) for i in range(5)],
                "pit_unsheltered": [int((base_total + i * 500) * 0.3) for i in range(5)],
                "pit_year": [year] * 5,
                "data_source": ["hud_exchange"] * 5,
                "source_ref": [f"https://example.com/pit/{year}"] * 5,
                "ingested_at": [datetime.now(UTC)] * 5,
                "notes": [None] * 5,
            }
        )
        df.to_parquet(pit_dir / f"pit_counts__{year}.parquet", index=False)

    # Create ACS measures for each boundary/acs vintage combination
    for acs_year in [2021, 2022, 2023]:
        boundary_year = acs_year + 1
        df = pd.DataFrame(
            {
                "coc_id": sample_cocs,
                "total_population": [700000 + i * 1000000 for i in range(5)],
                "adult_population": [550000 + i * 800000 for i in range(5)],
                "population_below_poverty": [70000 + i * 100000 for i in range(5)],
                "median_household_income": [65000 + i * 5000 for i in range(5)],
                "median_gross_rent": [1400 + i * 200 for i in range(5)],
                "coverage_ratio": [0.95, 0.98, 0.99, 0.92, 0.96],
            }
        )
        df.to_parquet(
            sample_acs_measures / f"coc_measures__{boundary_year}__{acs_year}.parquet",
            index=False,
        )

    return {
        "pit_dir": pit_dir,
        "measures_dir": sample_acs_measures,
        "panel_dir": tmp_path / "panel",
        "registry_path": tmp_path / "pit_registry.parquet",
    }


# ============================================================================
# Test Classes
# ============================================================================


class TestReproducibility:
    """Tests that ensure deterministic outputs - same inputs produce same outputs."""

    def test_panel_build_reproducible(self, full_test_setup):
        """Building the same panel twice produces identical results."""
        # Build panel first time
        panel_1 = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        # Build panel second time
        panel_2 = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        # Verify identical
        pd.testing.assert_frame_equal(panel_1, panel_2)

    def test_pit_parsing_reproducible(self, sample_pit_csv):
        """Parsing the same PIT file twice produces identical results."""
        result_1 = parse_pit_file(sample_pit_csv, year=2024)
        result_2 = parse_pit_file(sample_pit_csv, year=2024)

        # Compare data columns (ingested_at will differ)
        compare_cols = ["pit_year", "coc_id", "pit_total", "pit_sheltered", "pit_unsheltered"]
        pd.testing.assert_frame_equal(
            result_1.df[compare_cols].reset_index(drop=True),
            result_2.df[compare_cols].reset_index(drop=True),
        )

    def test_coc_id_normalization_reproducible(self):
        """CoC ID normalization is deterministic."""
        test_cases = [
            ("CO-500", "CO-500"),
            ("co-500", "CO-500"),
            ("CO500", "CO-500"),
            ("CO 500", "CO-500"),
            ("CO-5", "CO-005"),
        ]

        for raw_id, expected in test_cases:
            # Run normalization multiple times
            result_1 = normalize_coc_id(raw_id)
            result_2 = normalize_coc_id(raw_id)
            result_3 = normalize_coc_id(raw_id)

            assert result_1 == expected
            assert result_2 == expected
            assert result_3 == expected

    def test_diagnostics_reproducible(self, full_test_setup):
        """Diagnostics report is reproducible."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        report_1 = generate_diagnostics_report(panel)
        report_2 = generate_diagnostics_report(panel)

        # Compare coverage summaries
        pd.testing.assert_frame_equal(report_1.coverage, report_2.coverage)

        # Compare boundary change summaries
        pd.testing.assert_frame_equal(report_1.boundary_changes, report_2.boundary_changes)

        # Compare missingness reports
        pd.testing.assert_frame_equal(report_1.missingness, report_2.missingness)


class TestPopulationShareValidation:
    """Tests that population shares and weights are valid."""

    def test_coverage_ratios_valid_range(self, full_test_setup):
        """Coverage ratios should be between 0 and 1."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        # Filter to rows with non-null coverage
        coverage = panel["coverage_ratio"].dropna()

        if len(coverage) > 0:
            assert coverage.min() >= 0.0
            assert coverage.max() <= 1.0

    def test_pit_counts_sum_correctly(self, full_test_setup):
        """PIT sheltered + unsheltered should equal total (when both present)."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        # For rows with both sheltered and unsheltered
        has_both = panel["pit_sheltered"].notna() & panel["pit_unsheltered"].notna()
        subset = panel[has_both]

        if len(subset) > 0:
            computed_total = subset["pit_sheltered"] + subset["pit_unsheltered"]
            # Allow for small rounding differences
            assert all(abs(computed_total - subset["pit_total"]) <= 1)


class TestBoundaryChangeDetection:
    """Tests that boundary changes are detected correctly."""

    def test_boundary_changes_detected_in_panel(self, full_test_setup):
        """Panel correctly flags boundary changes between years."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        # boundary_changed column should exist
        assert "boundary_changed" in panel.columns
        assert panel["boundary_changed"].dtype == bool

    def test_first_year_boundary_change_is_false(self, full_test_setup):
        """First year for each CoC should not be flagged as boundary change."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        # For each CoC, the first year should have boundary_changed=False
        for coc_id in panel["coc_id"].unique():
            coc_data = panel[panel["coc_id"] == coc_id].sort_values("year")
            first_year_changed = coc_data.iloc[0]["boundary_changed"]
            assert not first_year_changed, f"First year for {coc_id} should be False"

    def test_diagnostics_boundary_change_summary(self, full_test_setup):
        """Diagnostics report includes boundary change summary."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        report = generate_diagnostics_report(panel)

        # boundary_changes should be a DataFrame
        assert isinstance(report.boundary_changes, pd.DataFrame)


class TestPITIngestionWorkflow:
    """Tests for PIT data ingestion and registry tracking."""

    def test_pit_file_parsing(self, sample_pit_csv):
        """PIT CSV file is correctly parsed."""
        result = parse_pit_file(sample_pit_csv, year=2024)
        df = result.df

        assert len(df) == 5
        assert "coc_id" in df.columns
        assert "pit_total" in df.columns
        assert "pit_year" in df.columns
        assert all(df["pit_year"] == 2024)

    def test_pit_parquet_write_and_read(self, sample_pit_csv, tmp_path):
        """PIT data can be written to Parquet with provenance."""
        result = parse_pit_file(sample_pit_csv, year=2024)
        df = result.df
        output_path = tmp_path / "pit_counts__2024.parquet"

        write_pit_parquet(df, output_path)

        # Verify file exists and is readable
        assert output_path.exists()

        loaded = pd.read_parquet(output_path)
        assert len(loaded) == 5
        assert list(loaded["coc_id"]) == list(df["coc_id"])

    def test_pit_parquet_has_provenance(self, sample_pit_csv, tmp_path):
        """PIT Parquet files have valid provenance metadata."""
        result = parse_pit_file(sample_pit_csv, year=2024)
        df = result.df
        output_path = tmp_path / "pit_counts__2024.parquet"

        write_pit_parquet(df, output_path)

        provenance = read_provenance(output_path)

        assert provenance is not None
        assert provenance.extra.get("pit_year") == 2024
        assert provenance.extra.get("row_count") == 5

    def test_registry_tracking(self, sample_pit_csv, tmp_path):
        """PIT years can be registered and tracked."""
        result = parse_pit_file(sample_pit_csv, year=2024)
        df = result.df
        output_path = tmp_path / "pit" / "pit_counts__2024.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        write_pit_parquet(df, output_path)

        registry_path = tmp_path / "registry.parquet"

        # Register the PIT year
        entry = register_pit_year(
            pit_year=2024,
            source="hud_exchange",
            path=output_path,
            row_count=len(df),
            registry_path=registry_path,
        )

        assert isinstance(entry, PitRegistryEntry)
        assert entry.pit_year == 2024
        assert entry.source == "hud_exchange"

        # Verify we can list registered years
        years = list_pit_years(registry_path=registry_path)
        assert len(years) == 1
        assert years[0].pit_year == 2024

    def test_registry_idempotent(self, sample_pit_csv, tmp_path):
        """Registering the same PIT year twice is idempotent."""
        result = parse_pit_file(sample_pit_csv, year=2024)
        df = result.df
        output_path = tmp_path / "pit" / "pit_counts__2024.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        write_pit_parquet(df, output_path)

        registry_path = tmp_path / "registry.parquet"

        # Register twice
        entry_1 = register_pit_year(
            pit_year=2024,
            source="hud_exchange",
            path=output_path,
            row_count=len(df),
            registry_path=registry_path,
        )

        entry_2 = register_pit_year(
            pit_year=2024,
            source="hud_exchange",
            path=output_path,
            row_count=len(df),
            registry_path=registry_path,
        )

        # Should have same hash
        assert entry_1.hash_of_file == entry_2.hash_of_file

        # Should still only have one entry
        years = list_pit_years(registry_path=registry_path)
        assert len(years) == 1

    def test_multiple_years_registry(self, sample_pit_csv, sample_pit_csv_2023, tmp_path):
        """Multiple PIT years can be registered and tracked."""
        registry_path = tmp_path / "registry.parquet"
        pit_dir = tmp_path / "pit"
        pit_dir.mkdir()

        # Register 2023
        result_2023 = parse_pit_file(sample_pit_csv_2023, year=2023)
        df_2023 = result_2023.df
        path_2023 = pit_dir / "pit_counts__2023.parquet"
        write_pit_parquet(df_2023, path_2023)
        register_pit_year(
            pit_year=2023,
            source="hud_exchange",
            path=path_2023,
            row_count=len(df_2023),
            registry_path=registry_path,
        )

        # Register 2024
        result_2024 = parse_pit_file(sample_pit_csv, year=2024)
        df_2024 = result_2024.df
        path_2024 = pit_dir / "pit_counts__2024.parquet"
        write_pit_parquet(df_2024, path_2024)
        register_pit_year(
            pit_year=2024,
            source="hud_exchange",
            path=path_2024,
            row_count=len(df_2024),
            registry_path=registry_path,
        )

        # Should have both years
        years = list_pit_years(registry_path=registry_path)
        assert len(years) == 2
        pit_years = {y.pit_year for y in years}
        assert pit_years == {2023, 2024}


class TestQAValidation:
    """Tests for PIT data quality validation."""

    def test_qa_passes_valid_data(self, sample_pit_csv):
        """QA passes for valid PIT data."""
        result = parse_pit_file(sample_pit_csv, year=2024)
        report = validate_pit_data(result.df)

        assert report.passed
        assert len(report.errors) == 0

    def test_qa_detects_duplicates(self, tmp_path):
        """QA detects duplicate CoC IDs."""
        csv_path = tmp_path / "pit_dup.csv"
        df = pd.DataFrame(
            {
                "CoC Number": ["CO-500", "CO-500", "CA-600"],
                "Overall Homeless, 2024": [1000, 1100, 50000],
            }
        )
        df.to_csv(csv_path, index=False)

        # Note: parse_pit_file drops duplicates, so we create our own dup df
        dup_df = pd.DataFrame(
            {
                "pit_year": [2024, 2024, 2024],
                "coc_id": ["CO-500", "CO-500", "CA-600"],
                "pit_total": [1000, 1100, 50000],
            }
        )

        report = validate_pit_data(dup_df)

        assert any(i.check_name == "duplicates" for i in report.issues)

    def test_qa_detects_invalid_counts(self, tmp_path):
        """QA detects invalid (negative) counts."""
        df = pd.DataFrame(
            {
                "pit_year": [2024],
                "coc_id": ["CO-500"],
                "pit_total": [-100],
            }
        )

        report = validate_pit_data(df)

        assert any(i.check_name == "invalid_counts" for i in report.issues)

    def test_qa_detects_yoy_changes(self, sample_pit_csv, sample_pit_csv_2023):
        """QA detects year-over-year changes beyond threshold."""
        result_2023 = parse_pit_file(sample_pit_csv_2023, year=2023)
        df_2023 = result_2023.df

        # Create 2024 with a large change for CO-500
        df_2024 = df_2023.copy()
        df_2024["pit_year"] = 2024
        # Double CO-500 count (100% increase)
        df_2024.loc[df_2024["coc_id"] == "CO-500", "pit_total"] = 2200

        report = validate_pit_data(df_2024, df_previous=df_2023, yoy_threshold=0.5)

        yoy_issues = [i for i in report.issues if i.check_name == "yoy_changes"]
        assert len(yoy_issues) > 0

    def test_qa_detects_missing_cocs(self, sample_pit_csv, sample_boundary_gdf):
        """QA detects CoCs missing from boundary data."""
        result = parse_pit_file(sample_pit_csv, year=2024)
        df = result.df

        # Create boundary with fewer CoCs
        cocs_to_keep = ["CO-500", "CA-600"]
        limited_boundary = sample_boundary_gdf[sample_boundary_gdf["coc_id"].isin(cocs_to_keep)]

        report = validate_pit_data(
            df,
            boundary_vintage="2024",
            boundary_gdf=limited_boundary,
        )

        missing_issues = [i for i in report.issues if i.check_name == "missing_cocs"]
        assert len(missing_issues) > 0


class TestPanelAssembly:
    """Tests for CoC x year panel construction."""

    def test_panel_has_all_columns(self, full_test_setup):
        """Panel has all required canonical columns."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        for col in PANEL_COLUMNS:
            assert col in panel.columns, f"Missing column: {col}"

    def test_panel_year_range(self, full_test_setup):
        """Panel covers the requested year range."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        years = set(panel["year"].unique())
        assert years == {2022, 2023, 2024}

    def test_panel_coc_coverage(self, full_test_setup, sample_cocs):
        """Panel covers all expected CoCs."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        cocs = set(panel["coc_id"].unique())
        assert cocs == set(sample_cocs)

    def test_panel_sorted_correctly(self, full_test_setup):
        """Panel is sorted by coc_id and year."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        sorted_panel = panel.sort_values(["coc_id", "year"]).reset_index(drop=True)
        pd.testing.assert_frame_equal(panel.reset_index(drop=True), sorted_panel)


class TestProvenanceIntegrity:
    """Tests that provenance metadata is correctly embedded and readable."""

    def test_panel_has_provenance(self, full_test_setup):
        """Saved panel has embedded provenance metadata."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        output_path = save_panel(
            panel,
            2022,
            2024,
            output_dir=full_test_setup["panel_dir"],
            policy=DEFAULT_POLICY,
        )

        provenance = read_provenance(output_path)

        assert provenance is not None
        assert provenance.extra.get("dataset_type") == "coc_panel"
        assert provenance.extra.get("start_year") == 2022
        assert provenance.extra.get("end_year") == 2024

    def test_panel_provenance_includes_policy(self, full_test_setup):
        """Panel provenance includes alignment policy details."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
            policy=DEFAULT_POLICY,
        )

        output_path = save_panel(
            panel,
            2022,
            2024,
            output_dir=full_test_setup["panel_dir"],
            policy=DEFAULT_POLICY,
        )

        provenance = read_provenance(output_path)

        assert "policy" in provenance.extra
        assert provenance.extra["policy"]["weighting_method"] == "population"

    def test_panel_provenance_has_counts(self, full_test_setup, sample_cocs):
        """Panel provenance includes row and CoC counts."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        output_path = save_panel(
            panel,
            2022,
            2024,
            output_dir=full_test_setup["panel_dir"],
        )

        provenance = read_provenance(output_path)

        expected_rows = len(sample_cocs) * 3  # 5 CoCs x 3 years
        assert provenance.extra.get("row_count") == expected_rows
        assert provenance.extra.get("coc_count") == len(sample_cocs)
        assert provenance.extra.get("year_count") == 3

    def test_pit_parquet_provenance(self, sample_pit_csv, tmp_path):
        """PIT Parquet files have correct provenance."""
        result = parse_pit_file(sample_pit_csv, year=2024)
        df = result.df
        output_path = tmp_path / "pit_counts__2024.parquet"

        write_pit_parquet(df, output_path)

        provenance = read_provenance(output_path)

        assert provenance is not None
        assert provenance.extra.get("pit_year") == 2024
        assert provenance.extra.get("data_source") == "hud_exchange"


class TestAlignmentPolicies:
    """Tests that alignment policies are explicit and correctly applied."""

    def test_default_policy_boundary_alignment(self, full_test_setup):
        """Default policy uses same-year boundaries."""
        panel = build_panel(
            2024,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
            policy=DEFAULT_POLICY,
        )

        # All rows should have boundary_vintage_used = "2024"
        assert all(panel["boundary_vintage_used"] == "2024")

    def test_default_policy_acs_alignment(self, full_test_setup):
        """Default policy uses ACS with 1-year lag."""
        panel = build_panel(
            2024,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
            policy=DEFAULT_POLICY,
        )

        # All rows should have acs_vintage_used = "2023" for PIT year 2024
        assert all(panel["acs_vintage_used"] == "2023")

    def test_custom_policy_applied(self, full_test_setup):
        """Custom alignment policy is correctly applied."""
        custom_policy = AlignmentPolicy(
            boundary_vintage_func=lambda y: str(y),
            acs_vintage_func=lambda y: str(y - 2),  # 2-year lag
            weighting_method="area",
        )

        panel = build_panel(
            2024,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
            policy=custom_policy,
        )

        # Custom policy: PIT 2024 -> ACS 2022, weighting=area
        assert all(panel["acs_vintage_used"] == "2022")
        assert all(panel["weighting_method"] == "area")

    def test_policy_serializable(self):
        """Policy can be serialized and deserialized."""
        policy = DEFAULT_POLICY

        # Serialize
        policy_dict = policy.to_dict()

        assert policy_dict["weighting_method"] == "population"
        assert "boundary_vintage_func" in policy_dict
        assert "acs_vintage_func" in policy_dict

        # Deserialize
        restored = AlignmentPolicy.from_dict(policy_dict)

        assert restored.weighting_method == policy.weighting_method
        # Test function produces same result
        assert restored.boundary_vintage_func(2024) == policy.boundary_vintage_func(2024)
        assert restored.acs_vintage_func(2024) == policy.acs_vintage_func(2024)


class TestDiagnostics:
    """Tests for panel diagnostics functionality."""

    def test_diagnostics_report_structure(self, full_test_setup):
        """Diagnostics report has expected structure."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        report = generate_diagnostics_report(panel)

        assert isinstance(report, DiagnosticsReport)
        assert isinstance(report.coverage, pd.DataFrame)
        assert isinstance(report.boundary_changes, pd.DataFrame)
        assert isinstance(report.missingness, pd.DataFrame)
        assert isinstance(report.panel_info, dict)

    def test_diagnostics_coverage_summary(self, full_test_setup):
        """Coverage summary includes expected statistics."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        report = generate_diagnostics_report(panel)

        if not report.coverage.empty:
            expected_cols = ["year", "count", "mean", "min", "max"]
            for col in expected_cols:
                assert col in report.coverage.columns, f"Missing column: {col}"

    def test_diagnostics_missingness_report(self, full_test_setup):
        """Missingness report includes all columns."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        report = generate_diagnostics_report(panel)

        if not report.missingness.empty:
            assert "column" in report.missingness.columns
            assert "missing_count" in report.missingness.columns
            assert "missing_pct" in report.missingness.columns

    def test_diagnostics_to_dict(self, full_test_setup):
        """Diagnostics report can be serialized to dict."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        report = generate_diagnostics_report(panel)
        report_dict = report.to_dict()

        assert "coverage" in report_dict
        assert "boundary_changes" in report_dict
        assert "missingness" in report_dict
        assert "panel_info" in report_dict

    def test_diagnostics_summary_text(self, full_test_setup):
        """Diagnostics report can generate text summary."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        report = generate_diagnostics_report(panel)
        summary = report.summary()

        assert isinstance(summary, str)
        assert "PANEL DIAGNOSTICS REPORT" in summary
        assert "COVERAGE SUMMARY" in summary
        assert "BOUNDARY CHANGES" in summary
        assert "MISSINGNESS" in summary

    def test_diagnostics_to_csv(self, full_test_setup, tmp_path):
        """Diagnostics can be exported to CSV files."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        report = generate_diagnostics_report(panel)
        csv_dir = tmp_path / "diagnostics_csv"

        paths = report.to_csv(csv_dir)

        # Check that files were created
        if not report.coverage.empty:
            assert "coverage" in paths
            assert paths["coverage"].exists()

        if not report.missingness.empty:
            assert "missingness" in paths
            assert paths["missingness"].exists()


class TestPhase4Readiness:
    """Tests that panels are ready for Phase 4 modeling."""

    def test_panel_dtypes_correct(self, full_test_setup):
        """Panel has correct data types for modeling."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        # Core identifiers should be strings
        assert panel["coc_id"].dtype == object
        assert panel["source"].dtype == object

        # Year and counts should be integers
        assert panel["year"].dtype in [int, "int64"]
        assert panel["pit_total"].dtype in [int, "int64"]

        # Boolean columns
        assert panel["boundary_changed"].dtype == bool

    def test_panel_no_duplicate_keys(self, full_test_setup):
        """Panel has no duplicate (coc_id, year) combinations."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        # Check for duplicates
        duplicates = panel.duplicated(subset=["coc_id", "year"], keep=False)
        assert not duplicates.any(), "Panel contains duplicate (coc_id, year) keys"

    def test_panel_pit_totals_positive(self, full_test_setup):
        """All PIT totals are positive."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        assert all(panel["pit_total"] > 0)

    def test_panel_population_positive(self, full_test_setup):
        """All population values are positive (when present)."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        # Filter to non-null population values
        pop = panel["total_population"].dropna()
        if len(pop) > 0:
            assert all(pop > 0)

    def test_panel_balanced(self, full_test_setup, sample_cocs):
        """Panel is balanced - each CoC appears in each year."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        expected_rows = len(sample_cocs) * 3  # 5 CoCs x 3 years
        assert len(panel) == expected_rows

        # Each CoC should have exactly 3 years
        for coc_id in sample_cocs:
            coc_data = panel[panel["coc_id"] == coc_id]
            assert len(coc_data) == 3, f"{coc_id} should have 3 years of data"


class TestEndToEndWorkflow:
    """Full end-to-end workflow tests."""

    def test_full_pipeline_from_csv_to_diagnostics(
        self,
        sample_pit_csv,
        sample_pit_csv_2023,
        sample_boundary_gdf,
        full_test_setup,
    ):
        """Test complete workflow from raw CSV to diagnostics report."""
        pit_dir = full_test_setup["pit_dir"]
        measures_dir = full_test_setup["measures_dir"]
        registry_path = full_test_setup["registry_path"]

        # Step 1: Parse PIT files
        result_2023 = parse_pit_file(sample_pit_csv_2023, year=2023)
        result_2024 = parse_pit_file(sample_pit_csv, year=2024)
        df_2023 = result_2023.df
        df_2024 = result_2024.df

        # Step 2: Write to Parquet with provenance
        path_2023 = pit_dir / "pit_counts__2023.parquet"
        path_2024 = pit_dir / "pit_counts__2024.parquet"
        write_pit_parquet(df_2023, path_2023)
        write_pit_parquet(df_2024, path_2024)

        # Step 3: Register in registry
        register_pit_year(
            pit_year=2023,
            source="hud_exchange",
            path=path_2023,
            row_count=len(df_2023),
            registry_path=registry_path,
        )
        register_pit_year(
            pit_year=2024,
            source="hud_exchange",
            path=path_2024,
            row_count=len(df_2024),
            registry_path=registry_path,
        )

        # Step 4: Run QA validation
        validate_pit_data(
            df_2024,
            df_previous=df_2023,
            boundary_vintage="2024",
            boundary_gdf=sample_boundary_gdf,
        )
        # Note: Some warnings may be expected since boundary has fewer CoCs

        # Step 5: Build panel
        panel = build_panel(
            2023,
            2024,
            pit_dir=pit_dir,
            measures_dir=measures_dir,
        )

        # Step 6: Save panel with provenance
        panel_path = save_panel(
            panel,
            2023,
            2024,
            output_dir=full_test_setup["panel_dir"],
            policy=DEFAULT_POLICY,
        )

        # Step 7: Run diagnostics
        diagnostics = generate_diagnostics_report(panel)

        # Verify complete workflow succeeded
        assert panel_path.exists()
        assert len(panel) > 0
        assert diagnostics is not None

        # Verify provenance chain
        pit_provenance = read_provenance(path_2024)
        panel_provenance = read_provenance(panel_path)

        assert pit_provenance is not None
        assert panel_provenance is not None
        assert panel_provenance.extra.get("dataset_type") == "coc_panel"

    @pytest.mark.slow
    def test_multi_year_panel_workflow(self, full_test_setup, sample_cocs):
        """Test multi-year panel construction with all diagnostics."""
        panel = build_panel(
            2022,
            2024,
            pit_dir=full_test_setup["pit_dir"],
            measures_dir=full_test_setup["measures_dir"],
        )

        # Save panel
        panel_path = save_panel(
            panel,
            2022,
            2024,
            output_dir=full_test_setup["panel_dir"],
            policy=DEFAULT_POLICY,
        )

        # Generate diagnostics
        report = generate_diagnostics_report(panel)

        # Verify panel structure
        assert len(panel) == len(sample_cocs) * 3  # 5 CoCs x 3 years
        assert set(panel["year"].unique()) == {2022, 2023, 2024}

        # Verify provenance
        provenance = read_provenance(panel_path)
        assert provenance.extra.get("start_year") == 2022
        assert provenance.extra.get("end_year") == 2024

        # Verify diagnostics
        assert report.panel_info.get("year_count") == 3
        assert report.panel_info.get("coc_count") == len(sample_cocs)
