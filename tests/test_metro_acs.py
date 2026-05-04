"""Tests for metro ACS aggregation (coclab-djrh.6).

Verifies ACS aggregation from tract-native inputs to Glynn/Fox metros,
covering crosswalk construction, single-county, multi-county, and
coverage/weighting cases.
"""

import pandas as pd
import pytest

from hhplab.acs import aggregate_acs_to_metro, build_metro_tract_crosswalk
from hhplab.metro.metro_definitions import (
    METRO_COUNT,
    METRO_COUNTY_MEMBERSHIP,
    build_county_membership_df,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def all_county_fips():
    """All county FIPS from metro membership table."""
    return sorted({fips for _, fips in METRO_COUNTY_MEMBERSHIP})


@pytest.fixture
def synthetic_tracts(all_county_fips):
    """Synthetic ACS tract data: 2 tracts per member county."""
    rows = []
    for fips in all_county_fips:
        for suffix in ["000100", "000200"]:
            geoid = fips + suffix
            rows.append(
                {
                    "GEOID": geoid,
                    "total_population": 5000,
                    "adult_population": 4000,
                    "population_below_poverty": 500,
                    "poverty_universe": 4800,
                    "median_household_income": 60000.0,
                    "median_gross_rent": 1500.0,
                }
            )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Crosswalk construction
# ---------------------------------------------------------------------------


class TestMetroTractCrosswalk:
    def test_crosswalk_has_all_metros(self, synthetic_tracts):
        xwalk = build_metro_tract_crosswalk(synthetic_tracts)
        assert xwalk["metro_id"].nunique() == METRO_COUNT

    def test_crosswalk_area_share_is_one(self, synthetic_tracts):
        xwalk = build_metro_tract_crosswalk(synthetic_tracts)
        assert (xwalk["area_share"] == 1.0).all()

    def test_crosswalk_has_definition_version(self, synthetic_tracts):
        xwalk = build_metro_tract_crosswalk(synthetic_tracts)
        assert "definition_version" in xwalk.columns
        assert (xwalk["definition_version"] == "glynn_fox_v1").all()

    def test_crosswalk_columns(self, synthetic_tracts):
        xwalk = build_metro_tract_crosswalk(synthetic_tracts)
        expected = {"metro_id", "tract_geoid", "area_share", "definition_version"}
        assert expected == set(xwalk.columns)

    def test_single_county_metro_gets_its_tracts(self, synthetic_tracts):
        """GF04 (Dallas, county 48113) should get tracts 48113000100, 48113000200."""
        xwalk = build_metro_tract_crosswalk(synthetic_tracts)
        gf04 = xwalk[xwalk["metro_id"] == "GF04"]
        assert len(gf04) == 2
        assert set(gf04["tract_geoid"]) == {"48113000100", "48113000200"}

    def test_multi_county_metro_gets_all_tracts(self, synthetic_tracts):
        """GF01 (NYC, 5 boroughs) should get 10 tracts (2 per borough)."""
        xwalk = build_metro_tract_crosswalk(synthetic_tracts)
        gf01 = xwalk[xwalk["metro_id"] == "GF01"]
        assert len(gf01) == 10
        # Check tracts come from all 5 counties
        county_fips = {t[:5] for t in gf01["tract_geoid"]}
        assert county_fips == {"36061", "36005", "36081", "36047", "36085"}

    def test_gf21_denver_seven_counties(self, synthetic_tracts):
        """GF21 (Denver, 7 counties) should get 14 tracts."""
        xwalk = build_metro_tract_crosswalk(synthetic_tracts)
        gf21 = xwalk[xwalk["metro_id"] == "GF21"]
        assert len(gf21) == 14

    def test_no_coc_id_column(self, synthetic_tracts):
        xwalk = build_metro_tract_crosswalk(synthetic_tracts)
        assert "coc_id" not in xwalk.columns

    def test_custom_membership(self, synthetic_tracts):
        """Custom membership table overrides built-in."""
        custom = pd.DataFrame(
            {
                "metro_id": ["TEST01"],
                "county_fips": ["36061"],
                "definition_version": "test_v1",
            }
        )
        xwalk = build_metro_tract_crosswalk(synthetic_tracts, county_membership_df=custom)
        assert xwalk["metro_id"].unique().tolist() == ["TEST01"]
        assert len(xwalk) == 2


# ---------------------------------------------------------------------------
# Basic ACS aggregation
# ---------------------------------------------------------------------------


class TestBasicAggregation:
    def test_output_has_all_metros(self, synthetic_tracts):
        result = aggregate_acs_to_metro(synthetic_tracts)
        assert result["metro_id"].nunique() == METRO_COUNT

    def test_output_columns(self, synthetic_tracts):
        result = aggregate_acs_to_metro(synthetic_tracts)
        expected_cols = {
            "metro_id",
            "total_population",
            "adult_population",
            "median_household_income",
            "median_gross_rent",
            "coverage_ratio",
            "weighting_method",
            "source",
            "definition_version",
        }
        assert expected_cols.issubset(set(result.columns))

    def test_no_coc_id_column(self, synthetic_tracts):
        result = aggregate_acs_to_metro(synthetic_tracts)
        assert "coc_id" not in result.columns

    def test_definition_version(self, synthetic_tracts):
        result = aggregate_acs_to_metro(synthetic_tracts)
        assert (result["definition_version"] == "glynn_fox_v1").all()

    def test_coverage_ratio_full(self, synthetic_tracts):
        """With data for all tracts, coverage should be 1.0."""
        result = aggregate_acs_to_metro(synthetic_tracts)
        # Coverage should be 1.0 since we have data for all tracts
        assert result["coverage_ratio"].tolist() == pytest.approx([1.0] * len(result))


# ---------------------------------------------------------------------------
# Single-county metro (passthrough)
# ---------------------------------------------------------------------------


class TestSingleCountyMetro:
    def test_gf04_dallas_population(self):
        """GF04 (Dallas, county 48113): sum of tract populations."""
        acs = pd.DataFrame(
            {
                "GEOID": ["48113000100", "48113000200"],
                "total_population": [3000, 2000],
                "adult_population": [2400, 1600],
                "population_below_poverty": [300, 200],
                "poverty_universe": [2800, 1900],
                "median_household_income": [55000.0, 65000.0],
                "median_gross_rent": [1200.0, 1400.0],
            }
        )
        result = aggregate_acs_to_metro(acs)
        gf04 = result[result["metro_id"] == "GF04"]
        assert len(gf04) == 1
        # Population sum: 3000 + 2000 = 5000
        assert gf04.iloc[0]["total_population"] == pytest.approx(5000)
        assert gf04.iloc[0]["adult_population"] == pytest.approx(4000)

    def test_gf04_median_income_weighted(self):
        """Median income should be population-weighted average of tract medians."""
        acs = pd.DataFrame(
            {
                "GEOID": ["48113000100", "48113000200"],
                "total_population": [3000, 2000],
                "adult_population": [2400, 1600],
                "population_below_poverty": [300, 200],
                "poverty_universe": [2800, 1900],
                "median_household_income": [50000.0, 70000.0],
                "median_gross_rent": [1200.0, 1800.0],
            }
        )
        result = aggregate_acs_to_metro(acs)
        gf04 = result[result["metro_id"] == "GF04"]
        # Weighted income: (3000*50000 + 2000*70000) / (3000+2000) = 58000
        assert gf04.iloc[0]["median_household_income"] == pytest.approx(58000.0)
        # Weighted rent: (3000*1200 + 2000*1800) / (3000+2000) = 1440
        assert gf04.iloc[0]["median_gross_rent"] == pytest.approx(1440.0)


# ---------------------------------------------------------------------------
# Multi-county metro (aggregation across counties)
# ---------------------------------------------------------------------------


class TestMultiCountyMetro:
    def test_gf01_nyc_population_sum(self):
        """GF01 (NYC) sums population across all 5 boroughs' tracts."""
        boroughs = ["36061", "36005", "36081", "36047", "36085"]
        rows = []
        for fips in boroughs:
            rows.append(
                {
                    "GEOID": fips + "000100",
                    "total_population": 10000,
                    "adult_population": 8000,
                    "population_below_poverty": 1000,
                    "poverty_universe": 9500,
                    "median_household_income": 60000.0,
                    "median_gross_rent": 1500.0,
                }
            )
        acs = pd.DataFrame(rows)
        result = aggregate_acs_to_metro(acs)
        gf01 = result[result["metro_id"] == "GF01"]
        # 5 boroughs × 10000 = 50000
        assert gf01.iloc[0]["total_population"] == pytest.approx(50000)

    def test_gf06_houston_two_counties(self):
        """GF06 (Houston): Harris + Fort Bend."""
        acs = pd.DataFrame(
            {
                "GEOID": ["48201000100", "48157000100"],
                "total_population": [20000, 5000],
                "adult_population": [16000, 4000],
                "population_below_poverty": [2000, 500],
                "poverty_universe": [19000, 4800],
                "median_household_income": [50000.0, 80000.0],
                "median_gross_rent": [1200.0, 1800.0],
            }
        )
        result = aggregate_acs_to_metro(acs)
        gf06 = result[result["metro_id"] == "GF06"]
        assert gf06.iloc[0]["total_population"] == pytest.approx(25000)
        # Weighted income: (20000*50000 + 5000*80000) / 25000 = 56000
        assert gf06.iloc[0]["median_household_income"] == pytest.approx(56000.0)


# ---------------------------------------------------------------------------
# Coverage and missing data
# ---------------------------------------------------------------------------


class TestCoverageHandling:
    def test_metro_with_no_tracts_has_zero_population(self):
        """Metro whose counties have no tracts in ACS gets no data."""
        # Supply only GF04's tracts; GF01 gets nothing
        acs = pd.DataFrame(
            {
                "GEOID": ["48113000100"],
                "total_population": [5000],
                "adult_population": [4000],
                "population_below_poverty": [500],
                "poverty_universe": [4800],
                "median_household_income": [60000.0],
                "median_gross_rent": [1500.0],
            }
        )
        result = aggregate_acs_to_metro(acs)
        # GF01 should not appear (no tracts matched)
        # or have zero/missing values
        gf01 = result[result["metro_id"] == "GF01"]
        if len(gf01) > 0:
            assert gf01.iloc[0]["total_population"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Truth table: tract counts per metro
# ---------------------------------------------------------------------------


class TestTruthTable:
    def test_uniform_population_sum(self, synthetic_tracts):
        """With uniform pop=5000 per tract, metro total = n_tracts * 5000."""
        result = aggregate_acs_to_metro(synthetic_tracts)
        membership = build_county_membership_df()
        county_counts = membership.groupby("metro_id").size()

        for metro_id in county_counts.index:
            n_counties = county_counts[metro_id]
            n_tracts = n_counties * 2  # 2 tracts per county in fixture
            expected_pop = n_tracts * 5000
            row = result[result["metro_id"] == metro_id]
            assert row.iloc[0]["total_population"] == pytest.approx(expected_pop), (
                f"{metro_id}: expected pop {expected_pop}, got {row.iloc[0]['total_population']}"
            )

    def test_uniform_median_unchanged(self, synthetic_tracts):
        """With uniform medians, weighted average should equal the uniform value."""
        result = aggregate_acs_to_metro(synthetic_tracts)
        for _, row in result.iterrows():
            assert row["median_household_income"] == pytest.approx(60000.0), (
                f"{row['metro_id']}: expected income 60000, got {row['median_household_income']}"
            )
            assert row["median_gross_rent"] == pytest.approx(1500.0), (
                f"{row['metro_id']}: expected rent 1500, got {row['median_gross_rent']}"
            )
