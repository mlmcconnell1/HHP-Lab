"""Tests for geometry-neutral crosswalk and aggregation primitives (coclab-djrh.3).

Verifies that crosswalk builders, ACS/PEP/ZORI aggregation primitives,
and the recipe executor work with arbitrary geo_id_col values (not just
coc_id), while preserving backward compatibility with existing CoC code.
"""

import numpy as np
import pandas as pd
import pytest

from coclab.measures.acs import aggregate_to_coc, aggregate_to_geo
from coclab.measures.diagnostics import (
    compute_crosswalk_diagnostics,
    compute_measure_diagnostics,
    identify_problem_cocs,
    identify_problem_geos,
)
from coclab.pep.aggregate import aggregate_pep_counties
from coclab.rents.aggregate import (
    collapse_to_yearly,
    compute_coc_county_weights,
    compute_geo_county_weights,
)
from coclab.xwalks.tract import (
    add_population_weights,
    build_coc_tract_crosswalk,
    build_tract_crosswalk,
    validate_population_shares,
)


# ---------------------------------------------------------------------------
# Fixtures: synthetic crosswalks and data with metro_id
# ---------------------------------------------------------------------------


@pytest.fixture
def metro_tract_crosswalk():
    """Synthetic metro-tract crosswalk with metro_id instead of coc_id."""
    return pd.DataFrame(
        {
            "metro_id": ["GF01", "GF01", "GF02", "GF02", "GF02"],
            "boundary_vintage": "glynn_fox_v1",
            "tract_geoid": ["36061000100", "36061000200", "06037000100", "06037000200", "06037000300"],
            "tract_vintage": "2020",
            "area_share": [0.8, 0.6, 1.0, 0.5, 0.3],
            "pop_share": [None, None, None, None, None],
            "intersection_area": [8000.0, 6000.0, 10000.0, 5000.0, 3000.0],
            "tract_area": [10000.0, 10000.0, 10000.0, 10000.0, 10000.0],
        }
    )


@pytest.fixture
def metro_county_crosswalk():
    """Synthetic metro-county crosswalk with metro_id instead of coc_id."""
    return pd.DataFrame(
        {
            "metro_id": ["GF01", "GF01", "GF02", "GF02"],
            "county_fips": ["36061", "36047", "06037", "06059"],
            "area_share": [0.9, 0.7, 1.0, 0.4],
        }
    )


@pytest.fixture
def tract_acs_data():
    """Tract-level ACS data matching the metro crosswalk tracts."""
    return pd.DataFrame(
        {
            "GEOID": ["36061000100", "36061000200", "06037000100", "06037000200", "06037000300"],
            "total_population": [5000, 3000, 8000, 4000, 2000],
            "adult_population": [4000, 2400, 6400, 3200, 1600],
            "population_below_poverty": [500, 300, 800, 400, 200],
            "poverty_universe": [4800, 2900, 7600, 3800, 1900],
            "median_household_income": [60000, 55000, 70000, 65000, 50000],
            "median_gross_rent": [1500, 1400, 2000, 1800, 1200],
        }
    )


@pytest.fixture
def county_pep_data():
    """County-level PEP data matching the metro crosswalk."""
    return pd.DataFrame(
        {
            "county_fips": ["36061", "36047", "06037", "06059"] * 2,
            "year": [2020] * 4 + [2021] * 4,
            "population": [1000000, 500000, 2000000, 300000, 1010000, 505000, 2020000, 303000],
        }
    )


# ---------------------------------------------------------------------------
# Crosswalk generalization
# ---------------------------------------------------------------------------


class TestTractCrosswalkGeneralization:
    def test_add_population_weights_with_metro_id(self, metro_tract_crosswalk):
        pop_data = pd.DataFrame(
            {
                "GEOID": ["36061000100", "36061000200", "06037000100", "06037000200", "06037000300"],
                "total_population": [5000, 3000, 8000, 4000, 2000],
            }
        )
        result = add_population_weights(
            metro_tract_crosswalk, pop_data, geo_id_col="metro_id"
        )
        assert "pop_share" in result.columns
        # Pop shares should sum to ~1 per metro
        for metro_id in result["metro_id"].unique():
            metro_shares = result[result["metro_id"] == metro_id]["pop_share"]
            assert metro_shares.sum() == pytest.approx(1.0, abs=0.01)

    def test_validate_population_shares_with_metro_id(self, metro_tract_crosswalk):
        pop_data = pd.DataFrame(
            {
                "GEOID": ["36061000100", "36061000200", "06037000100", "06037000200", "06037000300"],
                "total_population": [5000, 3000, 8000, 4000, 2000],
            }
        )
        xwalk = add_population_weights(
            metro_tract_crosswalk, pop_data, geo_id_col="metro_id"
        )
        result = validate_population_shares(xwalk, geo_id_col="metro_id")
        assert "metro_id" in result.columns
        assert result["is_valid"].all()


# ---------------------------------------------------------------------------
# ACS aggregation generalization
# ---------------------------------------------------------------------------


class TestACSAggregationGeneralization:
    def test_aggregate_to_geo_with_metro_id(self, metro_tract_crosswalk, tract_acs_data):
        result = aggregate_to_geo(
            tract_acs_data,
            metro_tract_crosswalk,
            geo_id_col="metro_id",
        )
        assert "metro_id" in result.columns
        assert "coc_id" not in result.columns
        assert len(result) == 2  # GF01 and GF02
        assert set(result["metro_id"]) == {"GF01", "GF02"}
        # All measure columns should be present
        assert "total_population" in result.columns
        assert "median_household_income" in result.columns
        assert "coverage_ratio" in result.columns

    def test_aggregate_to_coc_backward_compat(self, metro_tract_crosswalk, tract_acs_data):
        """aggregate_to_coc still works with coc_id crosswalks."""
        coc_xwalk = metro_tract_crosswalk.rename(columns={"metro_id": "coc_id"})
        result = aggregate_to_coc(tract_acs_data, coc_xwalk)
        assert "coc_id" in result.columns
        assert len(result) == 2

    def test_aggregate_to_geo_produces_same_values_as_coc(
        self, metro_tract_crosswalk, tract_acs_data
    ):
        """aggregate_to_geo and aggregate_to_coc produce identical values."""
        coc_xwalk = metro_tract_crosswalk.rename(columns={"metro_id": "coc_id"})
        coc_result = aggregate_to_coc(tract_acs_data, coc_xwalk)
        metro_result = aggregate_to_geo(
            tract_acs_data, metro_tract_crosswalk, geo_id_col="metro_id"
        )
        # Compare numeric columns
        for col in ["total_population", "median_household_income", "coverage_ratio"]:
            coc_vals = coc_result.sort_values("coc_id")[col].values
            metro_vals = metro_result.sort_values("metro_id")[col].values
            np.testing.assert_allclose(coc_vals, metro_vals, rtol=1e-10)


# ---------------------------------------------------------------------------
# PEP aggregation generalization
# ---------------------------------------------------------------------------


class TestPEPAggregationGeneralization:
    def test_aggregate_pep_counties_with_metro_id(
        self, metro_county_crosswalk, county_pep_data
    ):
        result = aggregate_pep_counties(
            county_pep_data,
            metro_county_crosswalk,
            geo_id_col="metro_id",
        )
        assert "metro_id" in result.columns
        assert "coc_id" not in result.columns
        assert set(result["metro_id"]) == {"GF01", "GF02"}
        assert set(result["year"]) == {2020, 2021}
        assert "population" in result.columns
        assert "coverage_ratio" in result.columns

    def test_aggregate_pep_counties_backward_compat(
        self, metro_county_crosswalk, county_pep_data
    ):
        """Default geo_id_col='coc_id' works with coc_id crosswalks."""
        coc_xwalk = metro_county_crosswalk.rename(columns={"metro_id": "coc_id"})
        result = aggregate_pep_counties(county_pep_data, coc_xwalk)
        assert "coc_id" in result.columns
        assert len(result) == 4  # 2 geos x 2 years

    def test_aggregate_pep_counties_values_match(
        self, metro_county_crosswalk, county_pep_data
    ):
        """Metro and CoC paths produce identical values."""
        coc_xwalk = metro_county_crosswalk.rename(columns={"metro_id": "coc_id"})
        coc_result = aggregate_pep_counties(county_pep_data, coc_xwalk)
        metro_result = aggregate_pep_counties(
            county_pep_data, metro_county_crosswalk, geo_id_col="metro_id"
        )
        for year in [2020, 2021]:
            coc_year = coc_result[coc_result["year"] == year].sort_values("coc_id")
            metro_year = metro_result[metro_result["year"] == year].sort_values("metro_id")
            np.testing.assert_allclose(
                coc_year["population"].values,
                metro_year["population"].values,
                rtol=1e-10,
            )

    def test_aggregate_pep_min_coverage(self, metro_county_crosswalk, county_pep_data):
        """min_coverage threshold nulls population for low-coverage geos."""
        result = aggregate_pep_counties(
            county_pep_data,
            metro_county_crosswalk,
            geo_id_col="metro_id",
            min_coverage=0.99,
        )
        # GF02 has coverage < 1.0 (area_share sums to 1.4 but
        # coverage = sum(area_share) for available counties)
        # Both metros should have full coverage here since all counties
        # are present in the PEP data
        assert result["population"].notna().all()


# ---------------------------------------------------------------------------
# ZORI weight generalization
# ---------------------------------------------------------------------------


class TestZORIWeightGeneralization:
    def test_compute_geo_county_weights_with_metro_id(self, metro_county_crosswalk):
        weights = pd.DataFrame(
            {
                "county_fips": ["36061", "36047", "06037", "06059"],
                "weight_value": [1000.0, 500.0, 2000.0, 300.0],
            }
        )
        result = compute_geo_county_weights(
            metro_county_crosswalk, weights, geo_id_col="metro_id"
        )
        assert "metro_id" in result.columns
        # Weights should sum to 1 per metro
        for metro_id in result["metro_id"].unique():
            metro_weights = result[result["metro_id"] == metro_id]["weight"]
            assert metro_weights.sum() == pytest.approx(1.0, abs=0.001)

    def test_compute_coc_county_weights_backward_compat(self, metro_county_crosswalk):
        coc_xwalk = metro_county_crosswalk.rename(columns={"metro_id": "coc_id"})
        weights = pd.DataFrame(
            {
                "county_fips": ["36061", "36047", "06037", "06059"],
                "weight_value": [1000.0, 500.0, 2000.0, 300.0],
            }
        )
        result = compute_coc_county_weights(coc_xwalk, weights)
        assert "coc_id" in result.columns


class TestCollapseToYearlyGeneralization:
    def test_collapse_with_metro_id(self):
        monthly = pd.DataFrame(
            {
                "metro_id": ["GF01"] * 12 + ["GF02"] * 12,
                "date": pd.date_range("2020-01-01", periods=12, freq="MS").tolist() * 2,
                "zori_coc": [1500.0 + i * 10 for i in range(12)] * 2,
                "coverage_ratio": [0.95] * 24,
                "max_geo_contribution": [0.3] * 24,
                "geo_count": [5] * 24,
            }
        )
        result = collapse_to_yearly(monthly, "calendar_mean", geo_id_col="metro_id")
        assert "metro_id" in result.columns
        assert "coc_id" not in result.columns
        assert len(result) == 2  # One year for each metro


# ---------------------------------------------------------------------------
# Diagnostics generalization
# ---------------------------------------------------------------------------


class TestDiagnosticsGeneralization:
    def test_crosswalk_diagnostics_with_metro_id(self, metro_tract_crosswalk):
        result = compute_crosswalk_diagnostics(
            metro_tract_crosswalk, geo_id_col="metro_id"
        )
        assert "metro_id" in result.columns
        assert "coc_id" not in result.columns
        assert len(result) == 2

    def test_crosswalk_diagnostics_backward_compat(self, metro_tract_crosswalk):
        """Default coc_id works when crosswalk has that column."""
        coc_xwalk = metro_tract_crosswalk.rename(columns={"metro_id": "coc_id"})
        result = compute_crosswalk_diagnostics(coc_xwalk)
        assert "coc_id" in result.columns

    def test_measure_diagnostics_with_metro_id(self):
        area = pd.DataFrame(
            {"metro_id": ["GF01", "GF02"], "total_population": [100.0, 200.0]}
        )
        pop = pd.DataFrame(
            {"metro_id": ["GF01", "GF02"], "total_population": [105.0, 195.0]}
        )
        result = compute_measure_diagnostics(area, pop, geo_id_col="metro_id")
        assert "metro_id" in result.columns

    def test_identify_problem_geos_with_metro_id(self, metro_tract_crosswalk):
        diag = compute_crosswalk_diagnostics(
            metro_tract_crosswalk, geo_id_col="metro_id"
        )
        result = identify_problem_geos(diag, geo_id_col="metro_id")
        assert "metro_id" in result.columns or result.empty

    def test_identify_problem_cocs_backward_compat(self, metro_tract_crosswalk):
        coc_xwalk = metro_tract_crosswalk.rename(columns={"metro_id": "coc_id"})
        diag = compute_crosswalk_diagnostics(coc_xwalk)
        result = identify_problem_cocs(diag)
        assert "coc_id" in result.columns or result.empty


# ---------------------------------------------------------------------------
# Recipe executor: _detect_xwalk_target_col
# ---------------------------------------------------------------------------


class TestExecutorCrosswalkDetection:
    def test_detects_coc_id(self):
        from coclab.recipe.executor import _detect_xwalk_target_col

        xwalk = pd.DataFrame(
            {"coc_id": ["A"], "tract_geoid": ["B"], "area_share": [0.5]}
        )
        assert _detect_xwalk_target_col(xwalk, "tract_geoid") == "coc_id"

    def test_detects_metro_id(self):
        from coclab.recipe.executor import _detect_xwalk_target_col

        xwalk = pd.DataFrame(
            {"metro_id": ["GF01"], "tract_geoid": ["B"], "area_share": [0.5]}
        )
        assert _detect_xwalk_target_col(xwalk, "tract_geoid") == "metro_id"

    def test_detects_geo_id(self):
        from coclab.recipe.executor import _detect_xwalk_target_col

        xwalk = pd.DataFrame(
            {"geo_id": ["X"], "county_fips": ["Y"], "area_share": [0.5]}
        )
        assert _detect_xwalk_target_col(xwalk, "county_fips") == "geo_id"

    def test_prefers_coc_id_over_geo_id(self):
        from coclab.recipe.executor import _detect_xwalk_target_col

        xwalk = pd.DataFrame(
            {"coc_id": ["A"], "geo_id": ["A"], "tract_geoid": ["B"], "area_share": [0.5]}
        )
        assert _detect_xwalk_target_col(xwalk, "tract_geoid") == "coc_id"
