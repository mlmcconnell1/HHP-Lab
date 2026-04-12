"""Tests for metro PIT aggregation (coclab-djrh.4).

Verifies PIT rollup from CoC-native counts to Glynn/Fox metros,
covering single-CoC, multi-CoC, missing-CoC, and truth-table cases.
"""

import pandas as pd
import pytest

from coclab.metro.definitions import (
    METRO_COC_MEMBERSHIP,
    METRO_COUNT,
    build_coc_membership_df,
)
from coclab.metro.pit import aggregate_pit_to_metro

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def all_coc_ids():
    """All CoC IDs appearing in the metro membership table."""
    return sorted({coc_id for _, coc_id in METRO_COC_MEMBERSHIP})


@pytest.fixture
def synthetic_pit(all_coc_ids):
    """Synthetic PIT data for all member CoCs, 2 years."""
    rows = []
    for coc_id in all_coc_ids:
        for year in [2020, 2021]:
            rows.append({
                "coc_id": coc_id,
                "pit_year": year,
                "pit_total": 100,
                "pit_sheltered": 60,
                "pit_unsheltered": 40,
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Basic functionality
# ---------------------------------------------------------------------------


class TestBasicAggregation:
    def test_output_has_all_metros(self, synthetic_pit):
        result = aggregate_pit_to_metro(synthetic_pit)
        assert result["metro_id"].nunique() == METRO_COUNT

    def test_output_has_all_years(self, synthetic_pit):
        result = aggregate_pit_to_metro(synthetic_pit)
        assert set(result["year"]) == {2020, 2021}

    def test_output_row_count(self, synthetic_pit):
        result = aggregate_pit_to_metro(synthetic_pit)
        assert len(result) == METRO_COUNT * 2  # 25 metros x 2 years

    def test_output_columns(self, synthetic_pit):
        result = aggregate_pit_to_metro(synthetic_pit)
        expected_cols = {
            "metro_id", "year", "pit_total", "pit_sheltered",
            "pit_unsheltered", "coc_count", "coc_expected",
            "coc_coverage_ratio", "missing_cocs", "definition_version",
        }
        assert expected_cols.issubset(set(result.columns))

    def test_no_coc_id_column(self, synthetic_pit):
        """Metro outputs must NOT have a coc_id column."""
        result = aggregate_pit_to_metro(synthetic_pit)
        assert "coc_id" not in result.columns

    def test_definition_version(self, synthetic_pit):
        result = aggregate_pit_to_metro(synthetic_pit)
        assert (result["definition_version"] == "glynn_fox_v1").all()

    def test_sorted_output(self, synthetic_pit):
        result = aggregate_pit_to_metro(synthetic_pit)
        # Should be sorted by metro_id, year
        metro_ids = result["metro_id"].tolist()
        years = result["year"].tolist()
        pairs = list(zip(metro_ids, years, strict=False))
        assert pairs == sorted(pairs)


# ---------------------------------------------------------------------------
# Single-CoC metro tests (1:1 passthrough)
# ---------------------------------------------------------------------------


class TestSingleCoCMetro:
    def test_gf01_new_york(self):
        """GF01 (New York) has one CoC: NY-600."""
        pit = pd.DataFrame({
            "coc_id": ["NY-600"],
            "year": [2020],
            "pit_total": [5000],
            "pit_sheltered": [3500],
            "pit_unsheltered": [1500],
        })
        result = aggregate_pit_to_metro(pit)
        gf01 = result[result["metro_id"] == "GF01"]
        assert len(gf01) == 1
        assert gf01.iloc[0]["pit_total"] == 5000
        assert gf01.iloc[0]["pit_sheltered"] == 3500
        assert gf01.iloc[0]["pit_unsheltered"] == 1500
        assert gf01.iloc[0]["coc_count"] == 1
        assert gf01.iloc[0]["coc_expected"] == 1
        assert gf01.iloc[0]["coc_coverage_ratio"] == 1.0

    def test_gf04_dallas(self):
        """GF04 (Dallas) has one CoC: TX-600."""
        pit = pd.DataFrame({
            "coc_id": ["TX-600"],
            "year": [2021],
            "pit_total": [4000],
            "pit_sheltered": [2800],
            "pit_unsheltered": [1200],
        })
        result = aggregate_pit_to_metro(pit)
        gf04 = result[result["metro_id"] == "GF04"]
        assert len(gf04) == 1
        assert gf04.iloc[0]["pit_total"] == 4000


# ---------------------------------------------------------------------------
# Multi-CoC metro tests (summation)
# ---------------------------------------------------------------------------


class TestMultiCoCMetro:
    def test_gf02_la_four_cocs(self):
        """GF02 (LA) has 4 CoCs: CA-600, CA-606, CA-607, CA-612."""
        pit = pd.DataFrame({
            "coc_id": ["CA-600", "CA-606", "CA-607", "CA-612"],
            "year": [2020, 2020, 2020, 2020],
            "pit_total": [10000, 2000, 1500, 500],
            "pit_sheltered": [6000, 1200, 900, 300],
            "pit_unsheltered": [4000, 800, 600, 200],
        })
        result = aggregate_pit_to_metro(pit)
        gf02 = result[result["metro_id"] == "GF02"]
        assert len(gf02) == 1
        assert gf02.iloc[0]["pit_total"] == 14000
        assert gf02.iloc[0]["pit_sheltered"] == 8400
        assert gf02.iloc[0]["pit_unsheltered"] == 5600
        assert gf02.iloc[0]["coc_count"] == 4
        assert gf02.iloc[0]["coc_expected"] == 4
        assert gf02.iloc[0]["coc_coverage_ratio"] == 1.0

    def test_gf03_chicago_two_cocs(self):
        """GF03 (Chicago) has 2 CoCs: IL-510, IL-511."""
        pit = pd.DataFrame({
            "coc_id": ["IL-510", "IL-511"],
            "year": [2020, 2020],
            "pit_total": [3000, 500],
            "pit_sheltered": [2100, 350],
            "pit_unsheltered": [900, 150],
        })
        result = aggregate_pit_to_metro(pit)
        gf03 = result[result["metro_id"] == "GF03"]
        assert gf03.iloc[0]["pit_total"] == 3500
        assert gf03.iloc[0]["pit_sheltered"] == 2450
        assert gf03.iloc[0]["pit_unsheltered"] == 1050
        assert gf03.iloc[0]["coc_count"] == 2

    def test_gf18_stlouis_two_cocs(self):
        """GF18 (St. Louis) has 2 CoCs: MO-500, MO-501."""
        pit = pd.DataFrame({
            "coc_id": ["MO-500", "MO-501"],
            "year": [2020, 2020],
            "pit_total": [800, 200],
        })
        result = aggregate_pit_to_metro(pit)
        gf18 = result[result["metro_id"] == "GF18"]
        assert gf18.iloc[0]["pit_total"] == 1000
        assert gf18.iloc[0]["coc_count"] == 2
        assert gf18.iloc[0]["coc_expected"] == 2


# ---------------------------------------------------------------------------
# Missing CoC handling
# ---------------------------------------------------------------------------


class TestMissingCoCHandling:
    def test_partial_coverage_multi_coc(self):
        """GF02 (LA) with only 2 of 4 CoCs present."""
        pit = pd.DataFrame({
            "coc_id": ["CA-600", "CA-606"],
            "year": [2020, 2020],
            "pit_total": [10000, 2000],
        })
        result = aggregate_pit_to_metro(pit)
        gf02 = result[result["metro_id"] == "GF02"]
        assert gf02.iloc[0]["pit_total"] == 12000
        assert gf02.iloc[0]["coc_count"] == 2
        assert gf02.iloc[0]["coc_expected"] == 4
        assert gf02.iloc[0]["coc_coverage_ratio"] == 0.5
        # Missing CoCs should be listed
        missing = gf02.iloc[0]["missing_cocs"]
        assert "CA-607" in missing
        assert "CA-612" in missing

    def test_zero_coverage(self):
        """Metro with no CoC data should have null PIT."""
        pit = pd.DataFrame({
            "coc_id": ["XX-999"],  # Not a member of any metro
            "year": [2020],
            "pit_total": [100],
        })
        result = aggregate_pit_to_metro(pit)
        gf01 = result[(result["metro_id"] == "GF01") & (result["year"] == 2020)]
        assert len(gf01) == 1
        assert pd.isna(gf01.iloc[0]["pit_total"])
        assert gf01.iloc[0]["coc_coverage_ratio"] == 0.0

    def test_full_coverage_has_no_missing(self):
        """Full coverage should have empty missing_cocs."""
        pit = pd.DataFrame({
            "coc_id": ["NY-600"],
            "year": [2020],
            "pit_total": [5000],
        })
        result = aggregate_pit_to_metro(pit)
        gf01 = result[(result["metro_id"] == "GF01") & (result["year"] == 2020)]
        assert gf01.iloc[0]["missing_cocs"] == ""


# ---------------------------------------------------------------------------
# Nullable sheltered/unsheltered handling
# ---------------------------------------------------------------------------


class TestNullableFields:
    def test_missing_sheltered_column(self):
        """PIT data without sheltered/unsheltered columns."""
        pit = pd.DataFrame({
            "coc_id": ["NY-600"],
            "year": [2020],
            "pit_total": [5000],
        })
        result = aggregate_pit_to_metro(pit)
        gf01 = result[result["metro_id"] == "GF01"]
        assert pd.isna(gf01.iloc[0]["pit_sheltered"])
        assert pd.isna(gf01.iloc[0]["pit_unsheltered"])

    def test_partial_sheltered_data(self):
        """Multi-CoC metro where only some CoCs have sheltered data."""
        pit = pd.DataFrame({
            "coc_id": ["IL-510", "IL-511"],
            "year": [2020, 2020],
            "pit_total": [3000, 500],
            "pit_sheltered": [2100, pd.NA],
        })
        pit["pit_sheltered"] = pit["pit_sheltered"].astype("Int64")
        result = aggregate_pit_to_metro(pit)
        gf03 = result[result["metro_id"] == "GF03"]
        # Should still aggregate the available data
        assert gf03.iloc[0]["pit_sheltered"] == 2100

    def test_pit_total_uses_int64(self):
        """Output should use nullable Int64 dtype."""
        pit = pd.DataFrame({
            "coc_id": ["NY-600"],
            "year": [2020],
            "pit_total": [5000],
        })
        result = aggregate_pit_to_metro(pit)
        assert result["pit_total"].dtype == pd.Int64Dtype()


# ---------------------------------------------------------------------------
# Year column handling
# ---------------------------------------------------------------------------


class TestYearColumnHandling:
    def test_pit_year_column(self):
        """Accepts pit_year (original PIT column name)."""
        pit = pd.DataFrame({
            "coc_id": ["NY-600"],
            "pit_year": [2020],
            "pit_total": [5000],
        })
        result = aggregate_pit_to_metro(pit)
        assert "year" in result.columns
        assert result.iloc[0]["year"] == 2020

    def test_year_column(self):
        """Accepts year (panel convention)."""
        pit = pd.DataFrame({
            "coc_id": ["NY-600"],
            "year": [2020],
            "pit_total": [5000],
        })
        result = aggregate_pit_to_metro(pit)
        assert result.iloc[0]["year"] == 2020

    def test_missing_year_raises(self):
        """Missing year column should raise ValueError."""
        pit = pd.DataFrame({
            "coc_id": ["NY-600"],
            "pit_total": [5000],
        })
        with pytest.raises(ValueError, match="year"):
            aggregate_pit_to_metro(pit)


# ---------------------------------------------------------------------------
# Truth table: expected CoC counts per metro
# ---------------------------------------------------------------------------


class TestTruthTable:
    """Verify metro-CoC membership counts match the truth table."""

    @pytest.mark.parametrize(
        "metro_id,expected_coc_count",
        [
            ("GF01", 1),   # New York
            ("GF02", 4),   # LA
            ("GF03", 2),   # Chicago
            ("GF04", 1),   # Dallas
            ("GF09", 2),   # Atlanta
            ("GF12", 2),   # Detroit
            ("GF18", 2),   # St. Louis
            ("GF20", 2),   # Baltimore
        ],
    )
    def test_member_coc_count(self, metro_id, expected_coc_count, synthetic_pit):
        result = aggregate_pit_to_metro(synthetic_pit)
        metro = result[
            (result["metro_id"] == metro_id) & (result["year"] == 2020)
        ]
        assert metro.iloc[0]["coc_expected"] == expected_coc_count

    def test_all_single_coc_metros_have_expected_one(self, synthetic_pit):
        """All single-CoC metros should have coc_expected == 1."""
        result = aggregate_pit_to_metro(synthetic_pit)
        year_2020 = result[result["year"] == 2020]
        single_metros = {"GF04", "GF05", "GF06", "GF07", "GF08", "GF10",
                         "GF11", "GF13", "GF14", "GF15", "GF16", "GF17",
                         "GF19", "GF21", "GF22", "GF23", "GF24", "GF25"}
        # Also GF01 is single-CoC (but multi-county)
        single_metros.add("GF01")
        for metro_id in single_metros:
            row = year_2020[year_2020["metro_id"] == metro_id]
            assert row.iloc[0]["coc_expected"] == 1, f"{metro_id} should have 1 CoC"

    def test_multi_coc_sum_is_correct(self, synthetic_pit):
        """With uniform PIT=100 per CoC, multi-CoC metro total should be n*100."""
        result = aggregate_pit_to_metro(synthetic_pit)
        year_2020 = result[result["year"] == 2020]
        membership = build_coc_membership_df()
        coc_counts = membership.groupby("metro_id").size()

        for metro_id in coc_counts.index:
            n_cocs = coc_counts[metro_id]
            row = year_2020[year_2020["metro_id"] == metro_id]
            assert row.iloc[0]["pit_total"] == n_cocs * 100, (
                f"{metro_id}: expected {n_cocs * 100}, "
                f"got {row.iloc[0]['pit_total']}"
            )
