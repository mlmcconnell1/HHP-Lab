"""Tests for metro PEP aggregation (coclab-djrh.5).

Verifies PEP rollup from county-native counts to Glynn/Fox metros,
covering single-county, multi-county, missing-county, and truth-table cases.
"""

import pandas as pd
import pytest

from hhplab.metro.metro_definitions import (
    METRO_COUNT,
    METRO_COUNTY_MEMBERSHIP,
    build_county_membership_df,
)
from hhplab.pep import aggregate_pep_to_metro

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def all_county_fips():
    """All county FIPS appearing in the metro membership table."""
    return sorted({fips for _, fips in METRO_COUNTY_MEMBERSHIP})


@pytest.fixture
def synthetic_pep(all_county_fips):
    """Synthetic PEP data for all member counties, 2 years."""
    rows = []
    for fips in all_county_fips:
        for year in [2020, 2021]:
            rows.append(
                {
                    "county_fips": fips,
                    "year": year,
                    "population": 100_000,
                }
            )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Basic functionality
# ---------------------------------------------------------------------------


class TestBasicAggregation:
    def test_output_has_all_metros(self, synthetic_pep):
        result = aggregate_pep_to_metro(synthetic_pep)
        assert result["metro_id"].nunique() == METRO_COUNT

    def test_output_has_all_years(self, synthetic_pep):
        result = aggregate_pep_to_metro(synthetic_pep)
        assert set(result["year"]) == {2020, 2021}

    def test_output_row_count(self, synthetic_pep):
        result = aggregate_pep_to_metro(synthetic_pep)
        assert len(result) == METRO_COUNT * 2  # 25 metros x 2 years

    def test_output_columns(self, synthetic_pep):
        result = aggregate_pep_to_metro(synthetic_pep)
        expected_cols = {
            "metro_id",
            "year",
            "population",
            "coverage_ratio",
            "county_count",
            "county_expected",
            "missing_counties",
            "definition_version",
        }
        assert expected_cols.issubset(set(result.columns))

    def test_no_coc_id_column(self, synthetic_pep):
        """Metro outputs must NOT have a coc_id column."""
        result = aggregate_pep_to_metro(synthetic_pep)
        assert "coc_id" not in result.columns

    def test_definition_version(self, synthetic_pep):
        result = aggregate_pep_to_metro(synthetic_pep)
        assert (result["definition_version"] == "glynn_fox_v1").all()

    def test_sorted_output(self, synthetic_pep):
        result = aggregate_pep_to_metro(synthetic_pep)
        pairs = list(zip(result["metro_id"].tolist(), result["year"].tolist(), strict=False))
        assert pairs == sorted(pairs)


# ---------------------------------------------------------------------------
# Single-county metro tests (1:1 passthrough)
# ---------------------------------------------------------------------------


class TestSingleCountyMetro:
    def test_gf04_dallas(self):
        """GF04 (Dallas) has one county: 48113."""
        pep = pd.DataFrame(
            {
                "county_fips": ["48113"],
                "year": [2021],
                "population": [2_600_000],
            }
        )
        result = aggregate_pep_to_metro(pep)
        gf04 = result[result["metro_id"] == "GF04"]
        assert len(gf04) == 1
        assert gf04.iloc[0]["population"] == 2_600_000
        assert gf04.iloc[0]["county_count"] == 1
        assert gf04.iloc[0]["county_expected"] == 1
        assert gf04.iloc[0]["coverage_ratio"] == 1.0
        assert gf04.iloc[0]["missing_counties"] == ""

    def test_gf11_san_francisco(self):
        """GF11 (San Francisco) has one county: 06075."""
        pep = pd.DataFrame(
            {
                "county_fips": ["06075"],
                "year": [2020],
                "population": [880_000],
            }
        )
        result = aggregate_pep_to_metro(pep)
        gf11 = result[result["metro_id"] == "GF11"]
        assert len(gf11) == 1
        assert gf11.iloc[0]["population"] == 880_000


# ---------------------------------------------------------------------------
# Multi-county metro tests (summation)
# ---------------------------------------------------------------------------


class TestMultiCountyMetro:
    def test_gf01_nyc_five_boroughs(self):
        """GF01 (NYC) has 5 boroughs: 36061, 36005, 36081, 36047, 36085."""
        pep = pd.DataFrame(
            {
                "county_fips": ["36061", "36005", "36081", "36047", "36085"],
                "year": [2020] * 5,
                "population": [1_600_000, 1_400_000, 2_300_000, 2_600_000, 500_000],
            }
        )
        result = aggregate_pep_to_metro(pep)
        gf01 = result[result["metro_id"] == "GF01"]
        assert len(gf01) == 1
        # Sum: 1.6M + 1.4M + 2.3M + 2.6M + 0.5M = 8.4M
        assert gf01.iloc[0]["population"] == 8_400_000
        assert gf01.iloc[0]["county_count"] == 5
        assert gf01.iloc[0]["county_expected"] == 5
        assert gf01.iloc[0]["coverage_ratio"] == 1.0
        assert gf01.iloc[0]["missing_counties"] == ""

    def test_gf06_houston_two_counties(self):
        """GF06 (Houston) has 2 counties: 48201, 48157."""
        pep = pd.DataFrame(
            {
                "county_fips": ["48201", "48157"],
                "year": [2020, 2020],
                "population": [4_700_000, 800_000],
            }
        )
        result = aggregate_pep_to_metro(pep)
        gf06 = result[result["metro_id"] == "GF06"]
        assert gf06.iloc[0]["population"] == 5_500_000
        assert gf06.iloc[0]["county_count"] == 2
        assert gf06.iloc[0]["county_expected"] == 2

    def test_gf21_denver_seven_counties(self):
        """GF21 (Denver) has 7 counties."""
        counties = ["08001", "08005", "08013", "08014", "08031", "08035", "08059"]
        pep = pd.DataFrame(
            {
                "county_fips": counties,
                "year": [2020] * 7,
                "population": [500_000, 650_000, 330_000, 70_000, 720_000, 350_000, 580_000],
            }
        )
        result = aggregate_pep_to_metro(pep)
        gf21 = result[result["metro_id"] == "GF21"]
        assert gf21.iloc[0]["population"] == 3_200_000
        assert gf21.iloc[0]["county_count"] == 7
        assert gf21.iloc[0]["county_expected"] == 7


# ---------------------------------------------------------------------------
# Missing county handling
# ---------------------------------------------------------------------------


class TestMissingCountyHandling:
    def test_partial_coverage_multi_county(self):
        """GF01 (NYC) with only 3 of 5 boroughs present."""
        pep = pd.DataFrame(
            {
                "county_fips": ["36061", "36005", "36081"],
                "year": [2020, 2020, 2020],
                "population": [1_600_000, 1_400_000, 2_300_000],
            }
        )
        result = aggregate_pep_to_metro(pep)
        gf01 = result[result["metro_id"] == "GF01"]
        assert gf01.iloc[0]["population"] == 5_300_000
        assert gf01.iloc[0]["county_count"] == 3
        assert gf01.iloc[0]["county_expected"] == 5
        assert gf01.iloc[0]["coverage_ratio"] == pytest.approx(0.6)
        # Missing boroughs should be listed
        missing = gf01.iloc[0]["missing_counties"]
        assert "36047" in missing
        assert "36085" in missing

    def test_zero_coverage(self):
        """Metro with no county data should have null population."""
        pep = pd.DataFrame(
            {
                "county_fips": ["99999"],  # Not a member of any metro
                "year": [2020],
                "population": [100],
            }
        )
        result = aggregate_pep_to_metro(pep)
        gf04 = result[(result["metro_id"] == "GF04") & (result["year"] == 2020)]
        assert len(gf04) == 1
        assert pd.isna(gf04.iloc[0]["population"])
        assert gf04.iloc[0]["coverage_ratio"] == 0.0

    def test_full_coverage_has_no_missing(self):
        """Full coverage should have empty missing_counties."""
        pep = pd.DataFrame(
            {
                "county_fips": ["48113"],
                "year": [2020],
                "population": [2_600_000],
            }
        )
        result = aggregate_pep_to_metro(pep)
        gf04 = result[(result["metro_id"] == "GF04") & (result["year"] == 2020)]
        assert gf04.iloc[0]["missing_counties"] == ""


# ---------------------------------------------------------------------------
# Truth table: expected county counts per metro
# ---------------------------------------------------------------------------


class TestTruthTable:
    """Verify metro-county membership counts match the truth table."""

    @pytest.mark.parametrize(
        "metro_id,expected_county_count",
        [
            ("GF01", 5),  # New York - 5 boroughs
            ("GF02", 1),  # LA - 1 county
            ("GF03", 1),  # Chicago - 1 county
            ("GF04", 1),  # Dallas - 1 county
            ("GF06", 2),  # Houston - 2 counties
            ("GF18", 2),  # St. Louis - county + city
            ("GF20", 2),  # Baltimore - county + city
            ("GF21", 7),  # Denver - 7 counties
        ],
    )
    def test_member_county_count(self, metro_id, expected_county_count, synthetic_pep):
        result = aggregate_pep_to_metro(synthetic_pep)
        metro = result[(result["metro_id"] == metro_id) & (result["year"] == 2020)]
        assert metro.iloc[0]["county_expected"] == expected_county_count

    def test_all_single_county_metros_have_expected_one(self, synthetic_pep):
        """All single-county metros should have county_expected == 1."""
        result = aggregate_pep_to_metro(synthetic_pep)
        year_2020 = result[result["year"] == 2020]
        single_metros = {
            "GF02",
            "GF03",
            "GF04",
            "GF05",
            "GF07",
            "GF08",
            "GF09",
            "GF10",
            "GF11",
            "GF12",
            "GF13",
            "GF14",
            "GF15",
            "GF16",
            "GF17",
            "GF19",
            "GF22",
            "GF23",
            "GF24",
            "GF25",
        }
        for metro_id in single_metros:
            row = year_2020[year_2020["metro_id"] == metro_id]
            assert row.iloc[0]["county_expected"] == 1, f"{metro_id} should have 1 county"

    def test_multi_county_sum_is_correct(self, synthetic_pep):
        """With uniform pop=100k per county, multi-county total should be n*100k."""
        result = aggregate_pep_to_metro(synthetic_pep)
        year_2020 = result[result["year"] == 2020]
        membership = build_county_membership_df()
        county_counts = membership.groupby("metro_id").size()

        for metro_id in county_counts.index:
            n_counties = county_counts[metro_id]
            row = year_2020[year_2020["metro_id"] == metro_id]
            assert row.iloc[0]["population"] == n_counties * 100_000, (
                f"{metro_id}: expected {n_counties * 100_000}, got {row.iloc[0]['population']}"
            )
