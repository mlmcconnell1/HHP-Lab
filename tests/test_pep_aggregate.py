"""Tests for PEP aggregation to CoC boundaries.

Tests the county-to-CoC aggregation pipeline including:
- Weighted aggregation using crosswalk
- Coverage ratio computation
- Handling of missing counties
- Minimum coverage threshold
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hhplab.pep.pep_aggregate import (
    aggregate_pep_counties,
    aggregate_pep_to_coc,
    aggregate_pep_to_coc_many,
)


class TestAggregationIntegration:
    """Integration tests using actual CoC-aggregated data (if available)."""

    @pytest.fixture
    def coc_pep_path(self):
        """Get path to CoC-level PEP data if it exists."""
        # Look for any coc_pep file
        pep_dir = Path("data/curated/pep")
        if not pep_dir.exists():
            pytest.skip("PEP output directory not available")

        files = list(pep_dir.glob("coc_pep__*.parquet"))
        if not files:
            pytest.skip("CoC-level PEP data not available - run 'hhplab aggregate pep' first")
        return files[0]

    def test_has_expected_columns(self, coc_pep_path):
        """Test that CoC PEP data has expected columns."""
        df = pd.read_parquet(coc_pep_path)

        required_cols = {
            "coc_id",
            "year",
            "population",
            "coverage_ratio",
            "county_count",
            "boundary_vintage",
            "weighting_method",
        }
        assert required_cols.issubset(set(df.columns))

    def test_coc_count_reasonable(self, coc_pep_path):
        """Test that CoC count is reasonable (380-400 CoCs)."""
        df = pd.read_parquet(coc_pep_path)

        coc_count = df["coc_id"].nunique()
        # US has ~390 CoCs depending on vintage
        assert coc_count >= 350
        assert coc_count <= 420

    def test_coverage_ratio_in_valid_range(self, coc_pep_path):
        """Test that coverage ratios are between 0 and 1."""
        df = pd.read_parquet(coc_pep_path)

        # All coverage ratios should be between 0 and 1
        assert (df["coverage_ratio"] >= 0).all()
        assert (df["coverage_ratio"] <= 1.0).all()

    def test_most_cocs_have_high_coverage(self, coc_pep_path):
        """Test that most CoC-years have high coverage."""
        df = pd.read_parquet(coc_pep_path)

        # Most should have coverage > 0.9
        high_coverage = df[df["coverage_ratio"] > 0.9]
        assert len(high_coverage) / len(df) > 0.9  # At least 90% have >90% coverage

    def test_population_values_reasonable(self, coc_pep_path):
        """Test that population values are reasonable."""
        df = pd.read_parquet(coc_pep_path)

        # Filter to rows with valid population
        valid = df[df["population"].notna()]

        # Population should be positive
        assert (valid["population"] > 0).all()

        # Most CoCs should have population > 10,000
        large_pop = valid[valid["population"] > 10000]
        assert len(large_pop) / len(valid) > 0.5

    def test_years_cover_expected_range(self, coc_pep_path):
        """Test that years cover expected range."""
        df = pd.read_parquet(coc_pep_path)

        years = sorted(df["year"].unique())
        # Should have at least 10 years
        assert len(years) >= 10
        assert min(years) <= 2015
        assert max(years) >= 2023

    def test_reference_date_is_july_1(self, coc_pep_path):
        """Test that reference dates are July 1."""
        df = pd.read_parquet(coc_pep_path)

        if "reference_date" in df.columns:
            for ref_date in df["reference_date"].dropna():
                assert pd.Timestamp(ref_date).month == 7
                assert pd.Timestamp(ref_date).day == 1


class TestAggregationUnit:
    """Unit tests for aggregation logic (using mock data)."""

    def test_weighted_sum_computation(self):
        """Test that weighted sum is computed correctly."""
        # Create mock crosswalk
        xwalk = pd.DataFrame(
            {
                "coc_id": ["COC-001", "COC-001", "COC-002"],
                "county_fips": ["01001", "01003", "01005"],
                "area_share": [0.6, 0.4, 1.0],
            }
        )

        # Create mock PEP data
        pep = pd.DataFrame(
            {
                "county_fips": ["01001", "01003", "01005"],
                "year": [2020, 2020, 2020],
                "population": [60000, 40000, 25000],
            }
        )

        # Merge
        merged = xwalk.merge(pep, on="county_fips")

        # Compute weighted population for COC-001
        coc_001 = merged[merged["coc_id"] == "COC-001"]
        expected_pop = (60000 * 0.6) + (40000 * 0.4)
        computed = (coc_001["population"] * coc_001["area_share"]).sum()
        assert computed == expected_pop

    def test_coverage_ratio_with_missing_county(self):
        """Test coverage ratio when some counties have no data."""
        # Create mock crosswalk
        xwalk = pd.DataFrame(
            {
                "coc_id": ["COC-001", "COC-001"],
                "county_fips": ["01001", "01003"],
                "area_share": [0.7, 0.3],
            }
        )

        # Create mock PEP data (only one county has data)
        pep = pd.DataFrame(
            {
                "county_fips": ["01001"],
                "year": [2020],
                "population": [70000],
            }
        )

        # Merge
        merged = xwalk.merge(pep, on="county_fips", how="left")

        # Coverage ratio should be 0.7 (only 01001 has data)
        covered_weight = merged[merged["population"].notna()]["area_share"].sum()
        total_weight = merged["area_share"].sum()
        coverage_ratio = covered_weight / total_weight

        assert coverage_ratio == pytest.approx(0.7)

    def test_equal_weighting(self):
        """Test equal weighting gives each county same weight."""
        # Create mock crosswalk with 3 counties for one CoC
        xwalk = pd.DataFrame(
            {
                "coc_id": ["COC-001", "COC-001", "COC-001"],
                "county_fips": ["01001", "01003", "01005"],
                "area_share": [0.5, 0.3, 0.2],  # Unequal area shares
            }
        )

        # Add equal weights
        xwalk["equal_weight"] = 1.0 / len(xwalk)

        # Each county should have equal weight of 1/3
        for weight in xwalk["equal_weight"]:
            assert weight == pytest.approx(1 / 3)

        # Create mock PEP data
        pep = pd.DataFrame(
            {
                "county_fips": ["01001", "01003", "01005"],
                "year": [2020, 2020, 2020],
                "population": [60000, 30000, 10000],
            }
        )

        # With equal weighting, population should be mean
        merged = xwalk.merge(pep, on="county_fips")
        equal_weighted_pop = (merged["population"] * merged["equal_weight"]).sum()
        expected = (60000 + 30000 + 10000) / 3

        assert equal_weighted_pop == pytest.approx(expected)

    def test_missing_county_does_not_renormalize(self, tmp_path):
        """Missing counties should reduce population rather than renormalize."""
        pep = pd.DataFrame(
            {
                "county_fips": ["01001"],
                "year": [2020],
                "population": [100000],
            }
        )
        pep_path = tmp_path / "pep.parquet"
        pep.to_parquet(pep_path, index=False)

        xwalk = pd.DataFrame(
            {
                "coc_id": ["COC-001", "COC-001"],
                "county_fips": ["01001", "01003"],
                "area_share": [0.6, 0.4],
            }
        )
        xwalk_path = tmp_path / "xwalk.parquet"
        xwalk.to_parquet(xwalk_path, index=False)

        result_path = aggregate_pep_to_coc(
            boundary_vintage="2024",
            county_vintage="2024",
            pep_path=pep_path,
            xwalk_path=xwalk_path,
            min_coverage=0.0,
            output_dir=tmp_path,
            force=True,
        )

        df = pd.read_parquet(result_path)
        row = df[df["coc_id"] == "COC-001"].iloc[0]

        assert row["coverage_ratio"] == pytest.approx(0.6)
        assert row["population"] == pytest.approx(60000)

    def test_missing_year_in_pep_data(self):
        """Years absent from PEP data should produce no rows for that year."""
        xwalk = pd.DataFrame(
            {
                "coc_id": ["COC-001"],
                "county_fips": ["01001"],
                "area_share": [1.0],
            }
        )

        # PEP data has 2020 and 2022 but NOT 2021
        pep = pd.DataFrame(
            {
                "county_fips": ["01001", "01001"],
                "year": [2020, 2022],
                "population": [50000, 52000],
            }
        )

        result = aggregate_pep_counties(pep, xwalk, min_coverage=0.0)

        result_years = sorted(result["year"].unique())
        assert result_years == [2020, 2022]
        assert 2021 not in result_years

    def test_one_sided_county_in_crosswalk(self):
        """County in crosswalk but absent from PEP should lower coverage, not crash."""
        xwalk = pd.DataFrame(
            {
                "coc_id": ["COC-001", "COC-001"],
                "county_fips": ["01001", "99999"],
                "area_share": [0.5, 0.5],
            }
        )

        # Only county 01001 has PEP data; 99999 does not exist in PEP
        pep = pd.DataFrame(
            {
                "county_fips": ["01001"],
                "year": [2020],
                "population": [80000],
            }
        )

        result = aggregate_pep_counties(pep, xwalk, min_coverage=0.0)

        row = result[result["coc_id"] == "COC-001"].iloc[0]
        assert row["coverage_ratio"] == pytest.approx(0.5)
        assert row["population"] == pytest.approx(40000)
        assert row["county_count"] == 1

    @pytest.mark.parametrize(
        ("weighting", "expected_population", "expected_coverage", "expected_max"),
        [
            ("area_share", 220.0, 1.0, 120.0 / 220.0),
            ("population_weight", 212.5, 1.0, 150.0 / 212.5),
            ("household_weight", 218.75, 1.0, 125.0 / 218.75),
            ("renter_household_weight", 200.0, 1.0, 1.0),
        ],
        ids=["area", "population", "household", "renter-household"],
    )
    def test_explicit_tract_mediated_weight_columns(
        self,
        weighting,
        expected_population,
        expected_coverage,
        expected_max,
    ):
        """PEP aggregation can select derived tract-mediated weight columns."""
        xwalk = pd.DataFrame(
            {
                "coc_id": ["COC-001", "COC-001"],
                "county_fips": ["01001", "01003"],
                "area_share": [0.6, 0.4],
                "population_weight": [0.75, 0.25],
                "household_weight": [0.625, 0.375],
                "renter_household_weight": [1.0, 0.0],
            }
        )
        pep = pd.DataFrame(
            {
                "county_fips": ["01001", "01003"],
                "year": [2020, 2020],
                "population": [200.0, 250.0],
            }
        )

        result = aggregate_pep_counties(
            pep,
            xwalk,
            weighting=weighting,
            min_coverage=0.0,
        )

        row = result[result["coc_id"] == "COC-001"].iloc[0]
        assert row["population"] == pytest.approx(expected_population)
        assert row["coverage_ratio"] == pytest.approx(expected_coverage)
        assert row["max_county_contribution"] == pytest.approx(expected_max)
        assert row["weighting_method"] == weighting

    def test_multiple_weightings_write_comparable_outputs(self, tmp_path):
        """One workflow can materialize multiple weighting-specific PEP outputs."""
        pep_path = tmp_path / "pep.parquet"
        pd.DataFrame(
            {
                "county_fips": ["01001", "01003"],
                "year": [2020, 2020],
                "population": [200.0, 250.0],
            }
        ).to_parquet(pep_path, index=False)

        xwalk_path = tmp_path / "xwalk.parquet"
        pd.DataFrame(
            {
                "coc_id": ["COC-001", "COC-001"],
                "county_fips": ["01001", "01003"],
                "area_share": [0.6, 0.4],
                "population_weight": [0.75, 0.25],
            }
        ).to_parquet(xwalk_path, index=False)

        outputs = aggregate_pep_to_coc_many(
            boundary_vintage="2024",
            county_vintage="2020",
            weightings=["area_share", "population_weight"],
            pep_path=pep_path,
            xwalk_path=xwalk_path,
            start_year=2020,
            end_year=2020,
            min_coverage=0.0,
            output_dir=tmp_path,
            force=True,
        )

        assert set(outputs) == {"area_share", "population_weight"}
        area = pd.read_parquet(outputs["area_share"])
        population = pd.read_parquet(outputs["population_weight"])
        assert list(area.columns) == list(population.columns)
        assert area.iloc[0]["population"] == pytest.approx(220.0)
        assert population.iloc[0]["population"] == pytest.approx(212.5)
        assert area.iloc[0]["weighting_method"] == "area_share"
        assert population.iloc[0]["weighting_method"] == "population_weight"

    def test_year_filter_without_matching_rows_raises_clear_error(self, tmp_path):
        """Year filters outside available coverage should fail with guidance."""
        pep_path = tmp_path / "pep_county__v2024.parquet"
        pd.DataFrame(
            {
                "county_fips": ["01001"],
                "year": [2024],
                "population": [100000],
            }
        ).to_parquet(pep_path, index=False)

        xwalk_path = tmp_path / "xwalk.parquet"
        pd.DataFrame(
            {
                "coc_id": ["COC-001"],
                "county_fips": ["01001"],
                "area_share": [1.0],
            }
        ).to_parquet(xwalk_path, index=False)

        msg = "No PEP data remains after applying the requested year filter"
        with pytest.raises(ValueError, match=msg):
            aggregate_pep_to_coc(
                boundary_vintage="2024",
                county_vintage="2024",
                pep_path=pep_path,
                xwalk_path=xwalk_path,
                start_year=2018,
                end_year=2018,
                output_dir=tmp_path,
                force=True,
            )


class TestPepDiagnosticsProvenance:
    """Regression test for coclab-8t4l: diagnostics parquet must embed provenance."""

    def test_run_pep_diagnostics_embeds_provenance(self, tmp_path):
        """Output parquet from run_pep_diagnostics must have provenance."""
        from hhplab.pep.pep_diagnostics import run_pep_diagnostics
        from hhplab.provenance import has_provenance

        # Create a minimal PEP CoC parquet as input.
        pep_df = pd.DataFrame(
            {
                "coc_id": ["A", "A"],
                "year": [2020, 2021],
                "population": [1000, 1010],
                "coverage_ratio": [1.0, 0.98],
            }
        )
        src = tmp_path / "pep_coc.parquet"
        pep_df.to_parquet(src, index=False)

        out = tmp_path / "diag.parquet"
        run_pep_diagnostics(src, output_path=out)
        assert has_provenance(out)
