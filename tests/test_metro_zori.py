"""Tests for metro ZORI aggregation (coclab-djrh.5).

Verifies ZORI rollup from county-native values to Glynn/Fox metros,
covering single-county, multi-county, coverage tracking, and yearly collapse.
"""

import pandas as pd
import pytest

from coclab.rents import (
    aggregate_yearly_zori_to_metro,
    aggregate_zori_to_metro,
    collapse_zori_to_yearly,
)
from coclab.metro.definitions import (
    METRO_COUNT,
    METRO_COUNTY_MEMBERSHIP,
    build_county_membership_df,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def all_county_fips():
    """All county FIPS appearing in the metro membership table."""
    return sorted({fips for _, fips in METRO_COUNTY_MEMBERSHIP})


@pytest.fixture
def county_weights(all_county_fips):
    """Synthetic ACS weights for all member counties."""
    return pd.DataFrame({
        "county_fips": all_county_fips,
        "weight_value": [1000.0] * len(all_county_fips),
    })


@pytest.fixture
def synthetic_zori(all_county_fips):
    """Synthetic monthly ZORI data for all member counties, 3 months."""
    dates = pd.date_range("2020-01-01", periods=3, freq="MS")
    rows = []
    for fips in all_county_fips:
        for date in dates:
            rows.append({
                "geo_id": fips,
                "date": date,
                "zori": 1500.0,
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Basic functionality
# ---------------------------------------------------------------------------


class TestBasicAggregation:
    def test_output_has_all_metros(self, synthetic_zori, county_weights):
        result = aggregate_zori_to_metro(synthetic_zori, county_weights)
        assert result["metro_id"].nunique() == METRO_COUNT

    def test_output_has_all_months(self, synthetic_zori, county_weights):
        result = aggregate_zori_to_metro(synthetic_zori, county_weights)
        assert result["date"].nunique() == 3

    def test_output_row_count(self, synthetic_zori, county_weights):
        result = aggregate_zori_to_metro(synthetic_zori, county_weights)
        assert len(result) == METRO_COUNT * 3

    def test_output_columns(self, synthetic_zori, county_weights):
        result = aggregate_zori_to_metro(synthetic_zori, county_weights)
        expected_cols = {
            "metro_id", "date", "zori_coc",
            "coverage_ratio", "max_geo_contribution", "geo_count",
            "definition_version",
        }
        assert expected_cols.issubset(set(result.columns))

    def test_no_coc_id_column(self, synthetic_zori, county_weights):
        result = aggregate_zori_to_metro(synthetic_zori, county_weights)
        assert "coc_id" not in result.columns

    def test_definition_version(self, synthetic_zori, county_weights):
        result = aggregate_zori_to_metro(synthetic_zori, county_weights)
        assert (result["definition_version"] == "glynn_fox_v1").all()

    def test_sorted_output(self, synthetic_zori, county_weights):
        result = aggregate_zori_to_metro(synthetic_zori, county_weights)
        pairs = list(zip(
            result["metro_id"].tolist(),
            result["date"].tolist(), strict=False,
        ))
        assert pairs == sorted(pairs)


# ---------------------------------------------------------------------------
# Single-county metro tests (1:1 passthrough)
# ---------------------------------------------------------------------------


class TestSingleCountyMetro:
    def test_gf04_dallas(self, county_weights):
        """GF04 (Dallas) has one county: 48113."""
        zori = pd.DataFrame({
            "geo_id": ["48113"],
            "date": pd.to_datetime(["2020-01-01"]),
            "zori": [1200.0],
        })
        result = aggregate_zori_to_metro(zori, county_weights)
        gf04 = result[result["metro_id"] == "GF04"]
        gf04_jan = gf04[gf04["date"] == "2020-01-01"]
        assert len(gf04_jan) == 1
        assert gf04_jan.iloc[0]["zori_coc"] == pytest.approx(1200.0)
        assert gf04_jan.iloc[0]["coverage_ratio"] == pytest.approx(1.0)
        assert gf04_jan.iloc[0]["geo_count"] == 1

    def test_gf11_san_francisco(self, county_weights):
        """GF11 (San Francisco) has one county: 06075."""
        zori = pd.DataFrame({
            "geo_id": ["06075"],
            "date": pd.to_datetime(["2020-01-01"]),
            "zori": [2800.0],
        })
        result = aggregate_zori_to_metro(zori, county_weights)
        gf11 = result[result["metro_id"] == "GF11"]
        gf11_jan = gf11[gf11["date"] == "2020-01-01"]
        assert gf11_jan.iloc[0]["zori_coc"] == pytest.approx(2800.0)


# ---------------------------------------------------------------------------
# Multi-county metro tests (weighted mean)
# ---------------------------------------------------------------------------


class TestMultiCountyMetro:
    def test_gf01_nyc_equal_weights(self):
        """GF01 (NYC) with equal weights → simple average."""
        boroughs = ["36061", "36005", "36081", "36047", "36085"]
        zori = pd.DataFrame({
            "geo_id": boroughs,
            "date": pd.to_datetime(["2020-01-01"] * 5),
            "zori": [3000.0, 1500.0, 2000.0, 2500.0, 1000.0],
        })
        weights = pd.DataFrame({
            "county_fips": boroughs,
            "weight_value": [1000.0] * 5,  # Equal weights
        })
        result = aggregate_zori_to_metro(zori, weights)
        gf01 = result[
            (result["metro_id"] == "GF01") & (result["date"] == "2020-01-01")
        ]
        # Equal weights → simple mean: (3000+1500+2000+2500+1000)/5 = 2000
        assert gf01.iloc[0]["zori_coc"] == pytest.approx(2000.0)
        assert gf01.iloc[0]["geo_count"] == 5

    def test_gf01_nyc_unequal_weights(self):
        """GF01 (NYC) with unequal weights → weighted mean."""
        boroughs = ["36061", "36005", "36081", "36047", "36085"]
        zori = pd.DataFrame({
            "geo_id": boroughs,
            "date": pd.to_datetime(["2020-01-01"] * 5),
            "zori": [3000.0, 1500.0, 2000.0, 2500.0, 1000.0],
        })
        weights = pd.DataFrame({
            "county_fips": boroughs,
            "weight_value": [100.0, 50.0, 80.0, 120.0, 30.0],
        })
        result = aggregate_zori_to_metro(zori, weights)
        gf01 = result[
            (result["metro_id"] == "GF01") & (result["date"] == "2020-01-01")
        ]
        # Weighted mean: area_share=1.0 for all, so raw_weight = weight_value
        # w_total = 100+50+80+120+30 = 380
        # zori = (100*3000 + 50*1500 + 80*2000 + 120*2500 + 30*1000) / 380
        #      = (300000 + 75000 + 160000 + 300000 + 30000) / 380
        #      = 865000 / 380 ≈ 2276.32
        expected = 865000.0 / 380.0
        assert gf01.iloc[0]["zori_coc"] == pytest.approx(expected, rel=1e-4)

    def test_gf06_houston_two_counties(self):
        """GF06 (Houston) with 2 counties."""
        zori = pd.DataFrame({
            "geo_id": ["48201", "48157"],
            "date": pd.to_datetime(["2020-01-01", "2020-01-01"]),
            "zori": [1400.0, 1800.0],
        })
        weights = pd.DataFrame({
            "county_fips": ["48201", "48157"],
            "weight_value": [4000.0, 1000.0],
        })
        result = aggregate_zori_to_metro(zori, weights)
        gf06 = result[
            (result["metro_id"] == "GF06") & (result["date"] == "2020-01-01")
        ]
        # Weighted: (4000*1400 + 1000*1800) / 5000 = (5600000 + 1800000) / 5000 = 1480
        assert gf06.iloc[0]["zori_coc"] == pytest.approx(1480.0)
        assert gf06.iloc[0]["geo_count"] == 2


# ---------------------------------------------------------------------------
# Missing county / coverage handling
# ---------------------------------------------------------------------------


class TestCoverageHandling:
    def test_partial_coverage_below_threshold(self):
        """Metro with partial county coverage below min_coverage gets null ZORI."""
        # GF01 has 5 boroughs; supply only 1
        zori = pd.DataFrame({
            "geo_id": ["36061"],
            "date": pd.to_datetime(["2020-01-01"]),
            "zori": [3000.0],
        })
        weights = pd.DataFrame({
            "county_fips": ["36061", "36005", "36081", "36047", "36085"],
            "weight_value": [1000.0] * 5,
        })
        result = aggregate_zori_to_metro(zori, weights, min_coverage=0.90)
        gf01 = result[
            (result["metro_id"] == "GF01") & (result["date"] == "2020-01-01")
        ]
        # Coverage = 1/5 = 0.2 < 0.90 → null
        assert pd.isna(gf01.iloc[0]["zori_coc"])
        assert gf01.iloc[0]["coverage_ratio"] == pytest.approx(0.2)

    def test_partial_coverage_above_threshold(self):
        """Metro with coverage above threshold still gets ZORI value."""
        # GF06 has 2 counties; supply both
        zori = pd.DataFrame({
            "geo_id": ["48201", "48157"],
            "date": pd.to_datetime(["2020-01-01", "2020-01-01"]),
            "zori": [1400.0, 1800.0],
        })
        weights = pd.DataFrame({
            "county_fips": ["48201", "48157"],
            "weight_value": [1000.0, 1000.0],
        })
        result = aggregate_zori_to_metro(zori, weights, min_coverage=0.90)
        gf06 = result[
            (result["metro_id"] == "GF06") & (result["date"] == "2020-01-01")
        ]
        assert gf06.iloc[0]["zori_coc"] is not None
        assert gf06.iloc[0]["coverage_ratio"] == pytest.approx(1.0)

    def test_zero_coverage_has_null_zori(self):
        """Metro with no ZORI data should have null zori_coc."""
        # Supply ZORI for GF04's county so the grid is populated,
        # but NOT for GF06's counties — GF06 should have zero coverage
        zori = pd.DataFrame({
            "geo_id": ["48113"],
            "date": pd.to_datetime(["2020-01-01"]),
            "zori": [1000.0],
        })
        weights = pd.DataFrame({
            "county_fips": ["48113", "48201", "48157"],
            "weight_value": [1000.0, 1000.0, 1000.0],
        })
        result = aggregate_zori_to_metro(zori, weights, min_coverage=0.0)
        gf06 = result[
            (result["metro_id"] == "GF06") & (result["date"] == "2020-01-01")
        ]
        assert len(gf06) == 1
        assert pd.isna(gf06.iloc[0]["zori_coc"])
        assert gf06.iloc[0]["coverage_ratio"] == 0.0


# ---------------------------------------------------------------------------
# Yearly collapse
# ---------------------------------------------------------------------------


class TestYearlyCollapse:
    @pytest.fixture
    def monthly_metro_zori(self, synthetic_zori, county_weights):
        return aggregate_zori_to_metro(synthetic_zori, county_weights)

    def test_collapse_pit_january(self, monthly_metro_zori):
        result = collapse_zori_to_yearly(monthly_metro_zori, "pit_january")
        assert "metro_id" in result.columns
        assert "year" in result.columns
        assert "coc_id" not in result.columns
        assert len(result) == METRO_COUNT  # 1 year (2020)

    def test_collapse_calendar_mean(self, monthly_metro_zori):
        result = collapse_zori_to_yearly(monthly_metro_zori, "calendar_mean")
        assert len(result) == METRO_COUNT

    def test_collapse_calendar_median(self, monthly_metro_zori):
        result = collapse_zori_to_yearly(monthly_metro_zori, "calendar_median")
        assert len(result) == METRO_COUNT

    def test_collapse_preserves_metro_id(self, monthly_metro_zori):
        result = collapse_zori_to_yearly(monthly_metro_zori, "pit_january")
        assert result["metro_id"].nunique() == METRO_COUNT

    def test_collapse_uniform_zori(self, monthly_metro_zori):
        """With uniform ZORI=1500, all methods should give 1500."""
        result = collapse_zori_to_yearly(monthly_metro_zori, "calendar_mean")
        valid = result[result["zori_coc"].notna()]
        if len(valid) > 0:
            assert valid["zori_coc"].tolist() == pytest.approx([1500.0] * len(valid))


# ---------------------------------------------------------------------------
# Truth table: expected county counts per metro
# ---------------------------------------------------------------------------


class TestTruthTable:
    """Verify uniform ZORI passthrough for single-county metros."""

    def test_uniform_zori_single_county(self, synthetic_zori, county_weights):
        """Single-county metros with uniform ZORI should pass through unchanged."""
        result = aggregate_zori_to_metro(synthetic_zori, county_weights)
        jan_2020 = result[result["date"] == pd.Timestamp("2020-01-01")]

        single_county_metros = {
            "GF02", "GF03", "GF04", "GF05", "GF07", "GF08", "GF09",
            "GF10", "GF11", "GF12", "GF13", "GF14", "GF15", "GF16",
            "GF17", "GF19", "GF22", "GF23", "GF24", "GF25",
        }
        for metro_id in single_county_metros:
            row = jan_2020[jan_2020["metro_id"] == metro_id]
            assert row.iloc[0]["zori_coc"] == pytest.approx(1500.0), (
                f"{metro_id} should have ZORI=1500"
            )
            assert row.iloc[0]["geo_count"] == 1

    def test_uniform_zori_multi_county(self, synthetic_zori, county_weights):
        """Multi-county metros with uniform ZORI and weights → still 1500."""
        result = aggregate_zori_to_metro(synthetic_zori, county_weights)
        jan_2020 = result[result["date"] == pd.Timestamp("2020-01-01")]

        multi_county_metros = {"GF01", "GF06", "GF18", "GF20", "GF21"}
        membership = build_county_membership_df()
        county_counts = membership.groupby("metro_id").size()

        for metro_id in multi_county_metros:
            row = jan_2020[jan_2020["metro_id"] == metro_id]
            assert row.iloc[0]["zori_coc"] == pytest.approx(1500.0), (
                f"{metro_id} should have ZORI=1500 (uniform)"
            )
            assert row.iloc[0]["geo_count"] == county_counts[metro_id]


# ---------------------------------------------------------------------------
# Yearly population-weighted aggregation
# ---------------------------------------------------------------------------


class TestYearlyPopulationWeighted:
    """Tests for aggregate_yearly_zori_to_metro."""

    def test_single_county_passthrough(self):
        """Single-county metro returns ZORI unchanged."""
        membership = pd.DataFrame({
            "metro_id": ["M1"],
            "county_fips": ["99001"],
        })
        zori = pd.DataFrame({
            "county_fips": ["99001", "99001"],
            "year": [2020, 2021],
            "zori": [1000.0, 1100.0],
        })
        pop = pd.DataFrame({
            "county_fips": ["99001", "99001"],
            "year": [2020, 2021],
            "population": [50000, 51000],
        })
        result = aggregate_yearly_zori_to_metro(
            zori, pop, county_membership_df=membership,
        )
        assert len(result) == 2
        assert result.loc[result["year"] == 2020, "zori"].iloc[0] == pytest.approx(1000.0)
        assert result.loc[result["year"] == 2021, "zori"].iloc[0] == pytest.approx(1100.0)

    def test_multi_county_population_weighted(self):
        """Multi-county metro uses population-weighted mean per year."""
        membership = pd.DataFrame({
            "metro_id": ["M1", "M1"],
            "county_fips": ["99001", "99002"],
        })
        zori = pd.DataFrame({
            "county_fips": ["99001", "99002"],
            "year": [2020, 2020],
            "zori": [1000.0, 2000.0],
        })
        # County 99001 has 3x the population of 99002.
        pop = pd.DataFrame({
            "county_fips": ["99001", "99002"],
            "year": [2020, 2020],
            "population": [75000, 25000],
        })
        result = aggregate_yearly_zori_to_metro(
            zori, pop, county_membership_df=membership,
        )
        expected = 1000.0 * 0.75 + 2000.0 * 0.25
        assert result.loc[0, "zori"] == pytest.approx(expected)

    def test_weights_vary_by_year(self):
        """Year-specific weights produce different results per year."""
        membership = pd.DataFrame({
            "metro_id": ["M1", "M1"],
            "county_fips": ["99001", "99002"],
        })
        zori = pd.DataFrame({
            "county_fips": ["99001", "99002", "99001", "99002"],
            "year": [2020, 2020, 2021, 2021],
            "zori": [1000.0, 2000.0, 1000.0, 2000.0],
        })
        # Year 2020: equal weight → mean 1500
        # Year 2021: 99002 has 3x population → mean 1750
        pop = pd.DataFrame({
            "county_fips": ["99001", "99002", "99001", "99002"],
            "year": [2020, 2020, 2021, 2021],
            "population": [50000, 50000, 25000, 75000],
        })
        result = aggregate_yearly_zori_to_metro(
            zori, pop, county_membership_df=membership,
        )
        r2020 = result.loc[result["year"] == 2020, "zori"].iloc[0]
        r2021 = result.loc[result["year"] == 2021, "zori"].iloc[0]
        assert r2020 == pytest.approx(1500.0)
        assert r2021 == pytest.approx(1750.0)

    def test_all_missing_population_yields_null_zori(self):
        """Regression test for coclab-i2fj.8.4: when all county populations
        are missing for a metro-year, result must be null, not 0.0."""
        membership = pd.DataFrame({
            "metro_id": ["M1", "M1"],
            "county_fips": ["99001", "99002"],
        })
        zori = pd.DataFrame({
            "county_fips": ["99001", "99002"],
            "year": [2020, 2020],
            "zori": [1000.0, 2000.0],
        })
        pop = pd.DataFrame({
            "county_fips": pd.Series(dtype=str),
            "year": pd.Series(dtype=int),
            "population": pd.Series(dtype=float),
        })
        result = aggregate_yearly_zori_to_metro(
            zori, pop, county_membership_df=membership,
        )
        assert len(result) == 1
        assert pd.isna(result.loc[0, "zori"])

    def test_partial_missing_population_yields_null_zori(self):
        """Regression test for coclab-2bj8: when some (not all) county
        populations are missing for a metro-year, result must be null to
        avoid silently renormalizing weights over a subset of counties."""
        membership = pd.DataFrame({
            "metro_id": ["M1", "M1"],
            "county_fips": ["99001", "99002"],
        })
        zori = pd.DataFrame({
            "county_fips": ["99001", "99002"],
            "year": [2020, 2020],
            "zori": [1000.0, 2000.0],
        })
        # Only 99001 has population; 99002 is missing.
        pop = pd.DataFrame({
            "county_fips": ["99001"],
            "year": [2020],
            "population": [50000],
        })
        result = aggregate_yearly_zori_to_metro(
            zori, pop, county_membership_df=membership,
        )
        assert len(result) == 1
        assert pd.isna(result.loc[0, "zori"])

    def test_uses_builtin_membership_by_default(self, all_county_fips):
        """Without county_membership_df, uses built-in Glynn-Fox membership."""
        zori = pd.DataFrame({
            "county_fips": all_county_fips,
            "year": [2020] * len(all_county_fips),
            "zori": [1500.0] * len(all_county_fips),
        })
        pop = pd.DataFrame({
            "county_fips": all_county_fips,
            "year": [2020] * len(all_county_fips),
            "population": [10000] * len(all_county_fips),
        })
        result = aggregate_yearly_zori_to_metro(zori, pop)
        assert result["metro_id"].nunique() == METRO_COUNT
        assert result["zori"].tolist() == pytest.approx([1500.0] * METRO_COUNT)

    def test_incomplete_county_zori_coverage_yields_null(self):
        """Regression test for coclab-n1bp: when a member county has no ZORI
        row at all, the metro-year result must be null — not renormalized
        over the remaining counties."""
        membership = pd.DataFrame({
            "metro_id": ["M1", "M1"],
            "county_fips": ["99001", "99002"],
        })
        # Only county 99001 has ZORI data; 99002 is entirely absent.
        zori = pd.DataFrame({
            "county_fips": ["99001"],
            "year": [2020],
            "zori": [1000.0],
        })
        pop = pd.DataFrame({
            "county_fips": ["99001", "99002"],
            "year": [2020, 2020],
            "population": [100, 300],
        })
        result = aggregate_yearly_zori_to_metro(
            zori, pop, county_membership_df=membership,
        )
        assert len(result) == 1
        assert pd.isna(result.loc[0, "zori"])
