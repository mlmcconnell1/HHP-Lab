"""Tests for Connecticut planning region helper utilities.

Tests the crosswalk dataclass, FIPS identification helpers,
ZORI legacy-to-planning translation, and weight planning-to-legacy
translation using synthetic data (no geometry files on disk).
"""

from __future__ import annotations

import pandas as pd
import pytest

from coclab.geo.ct_planning_regions import (
    CT_LEGACY_COUNTY_CODES,
    CT_PLANNING_REGION_CODES,
    CT_STATE_FIPS,
    CtPlanningRegionCrosswalk,
    is_ct_legacy_county_fips,
    is_ct_planning_region_fips,
    translate_weights_planning_to_legacy,
    translate_zori_legacy_to_planning,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_crosswalk() -> CtPlanningRegionCrosswalk:
    """Build a synthetic two-row crosswalk for testing.

    Legacy county 09001 maps to:
      - planning region 09110 with legacy_share=0.6, planning_share=0.7
      - planning region 09120 with legacy_share=0.4, planning_share=0.3
    """
    mapping = pd.DataFrame(
        {
            "legacy_county_fips": ["09001", "09001"],
            "planning_region_fips": ["09110", "09120"],
            "legacy_share": [0.6, 0.4],
            "planning_share": [0.7, 0.3],
        }
    )
    return CtPlanningRegionCrosswalk(
        mapping=mapping,
        legacy_vintage=2020,
        planning_vintage=2023,
    )


@pytest.fixture()
def crosswalk() -> CtPlanningRegionCrosswalk:
    return _make_crosswalk()


# ---------------------------------------------------------------------------
# CtPlanningRegionCrosswalk dataclass
# ---------------------------------------------------------------------------

class TestCtPlanningRegionCrosswalk:
    """Tests for CtPlanningRegionCrosswalk construction and field access."""

    def test_construction_and_field_access(self, crosswalk):
        assert crosswalk.legacy_vintage == 2020
        assert crosswalk.planning_vintage == 2023
        assert isinstance(crosswalk.mapping, pd.DataFrame)
        assert len(crosswalk.mapping) == 2

    def test_mapping_columns(self, crosswalk):
        expected_cols = {
            "legacy_county_fips",
            "planning_region_fips",
            "legacy_share",
            "planning_share",
        }
        assert set(crosswalk.mapping.columns) == expected_cols

    def test_frozen(self, crosswalk):
        with pytest.raises(AttributeError):
            crosswalk.legacy_vintage = 2021  # type: ignore[misc]


# ---------------------------------------------------------------------------
# is_ct_legacy_county_fips / is_ct_planning_region_fips
# ---------------------------------------------------------------------------

class TestIsCtLegacyCountyFips:
    """Tests for is_ct_legacy_county_fips."""

    @pytest.mark.parametrize("code", sorted(CT_LEGACY_COUNTY_CODES))
    def test_valid_legacy_codes(self, code):
        assert is_ct_legacy_county_fips(f"{CT_STATE_FIPS}{code}") is True

    @pytest.mark.parametrize(
        "geoid",
        [
            "06001",   # California, not CT
            "36001",   # New York
            "09110",   # CT planning region, not legacy
            "09999",   # CT state but invalid county code
        ],
    )
    def test_non_legacy_returns_false(self, geoid):
        assert is_ct_legacy_county_fips(geoid) is False


class TestIsCtPlanningRegionFips:
    """Tests for is_ct_planning_region_fips."""

    @pytest.mark.parametrize("code", sorted(CT_PLANNING_REGION_CODES))
    def test_valid_planning_codes(self, code):
        assert is_ct_planning_region_fips(f"{CT_STATE_FIPS}{code}") is True

    @pytest.mark.parametrize(
        "geoid",
        [
            "06110",   # California, wrong state
            "09001",   # CT legacy county, not planning region
            "09999",   # CT state but invalid code
            "36110",   # New York
        ],
    )
    def test_non_planning_returns_false(self, geoid):
        assert is_ct_planning_region_fips(geoid) is False


# ---------------------------------------------------------------------------
# translate_zori_legacy_to_planning
# ---------------------------------------------------------------------------

class TestTranslateZoriLegacyToPlanning:
    """Tests for translate_zori_legacy_to_planning."""

    def test_ct_values_translated_via_area_shares(self, crosswalk):
        """Legacy county ZORI values are weighted by planning_share into planning regions."""
        zori_df = pd.DataFrame(
            {
                "geo_id": ["09001", "09001"],
                "date": ["2023-01-31", "2023-02-28"],
                "zori": [1000.0, 2000.0],
            }
        )
        result = translate_zori_legacy_to_planning(zori_df, crosswalk)

        # Two planning regions (09110, 09120) x two dates = 4 rows
        assert len(result) == 4

        # Check planning region 09110 for January: 1000 * 0.7 = 700
        jan_110 = result[
            (result["geo_id"] == "09110") & (result["date"] == "2023-01-31")
        ]
        assert len(jan_110) == 1
        assert jan_110.iloc[0]["zori"] == pytest.approx(700.0)

        # Check planning region 09120 for January: 1000 * 0.3 = 300
        jan_120 = result[
            (result["geo_id"] == "09120") & (result["date"] == "2023-01-31")
        ]
        assert len(jan_120) == 1
        assert jan_120.iloc[0]["zori"] == pytest.approx(300.0)

    def test_non_ct_rows_pass_through(self, crosswalk):
        """Non-CT rows are returned unchanged."""
        zori_df = pd.DataFrame(
            {
                "geo_id": ["06001", "36001"],
                "date": ["2023-01-31", "2023-01-31"],
                "zori": [1500.0, 1800.0],
            }
        )
        result = translate_zori_legacy_to_planning(zori_df, crosswalk)

        assert len(result) == 2
        assert set(result["geo_id"]) == {"06001", "36001"}
        assert result["zori"].tolist() == [1500.0, 1800.0]

    def test_mixed_ct_and_non_ct(self, crosswalk):
        """CT rows translate while non-CT rows pass through."""
        zori_df = pd.DataFrame(
            {
                "geo_id": ["09001", "06001"],
                "date": ["2023-01-31", "2023-01-31"],
                "zori": [1000.0, 1500.0],
            }
        )
        result = translate_zori_legacy_to_planning(zori_df, crosswalk)

        # 1 non-CT row + 2 planning region rows = 3
        assert len(result) == 3
        assert "06001" in result["geo_id"].values
        assert "09110" in result["geo_id"].values
        assert "09120" in result["geo_id"].values

    def test_empty_ct_rows_return_input_unchanged(self, crosswalk):
        """When there are no CT legacy rows, the input is returned unchanged."""
        zori_df = pd.DataFrame(
            {
                "geo_id": ["06001"],
                "date": ["2023-01-31"],
                "zori": [1500.0],
            }
        )
        result = translate_zori_legacy_to_planning(zori_df, crosswalk)

        pd.testing.assert_frame_equal(result, zori_df)

    def test_metadata_columns_preserved(self, crosswalk):
        """Extra metadata columns beyond geo_id/date/zori are preserved."""
        zori_df = pd.DataFrame(
            {
                "geo_id": ["09001"],
                "date": ["2023-01-31"],
                "zori": [1000.0],
                "region_name": ["Fairfield County"],
                "state": ["Connecticut"],
            }
        )
        result = translate_zori_legacy_to_planning(zori_df, crosswalk)

        assert "region_name" in result.columns
        assert "state" in result.columns
        # All translated rows should carry metadata
        assert result["region_name"].notna().all()
        assert result["state"].notna().all()

    def test_raises_on_missing_columns(self, crosswalk):
        """ValueError when required columns are missing."""
        bad_df = pd.DataFrame({"geo_id": ["09001"], "date": ["2023-01-31"]})
        with pytest.raises(ValueError, match="zori"):
            translate_zori_legacy_to_planning(bad_df, crosswalk)


# ---------------------------------------------------------------------------
# translate_weights_planning_to_legacy
# ---------------------------------------------------------------------------

class TestTranslateWeightsPlanningToLegacy:
    """Tests for translate_weights_planning_to_legacy."""

    def test_planning_weights_translated_via_planning_share(self, crosswalk):
        """Planning region weights are distributed to legacy counties using planning_share."""
        weights_df = pd.DataFrame(
            {
                "county_fips": ["09110", "09120"],
                "weight_value": [1000.0, 500.0],
            }
        )
        result = translate_weights_planning_to_legacy(weights_df, crosswalk)

        # Both planning regions map to legacy 09001
        assert len(result) == 1
        assert result.iloc[0]["county_fips"] == "09001"

        # 09110 contributes 1000 * 0.7 = 700, 09120 contributes 500 * 0.3 = 150
        # total = 850
        assert result.iloc[0]["weight_value"] == pytest.approx(850.0)

    def test_non_ct_rows_pass_through(self, crosswalk):
        """Non-CT rows are returned unchanged."""
        weights_df = pd.DataFrame(
            {
                "county_fips": ["06001", "36001"],
                "weight_value": [1000.0, 2000.0],
            }
        )
        result = translate_weights_planning_to_legacy(weights_df, crosswalk)

        assert len(result) == 2
        assert set(result["county_fips"]) == {"06001", "36001"}

    def test_total_weight_mass_preserved(self, crosswalk):
        """Sum of translated weights equals sum of (input * planning_share) per legacy county.

        When a single planning region maps with planning_share < 1, the
        translated weight is the fractional contribution to the legacy county.
        The total weight mass after translation should equal the sum of
        (planning_region_weight * planning_share) across all inputs.
        """
        weights_df = pd.DataFrame(
            {
                "county_fips": ["09110", "09120", "06001"],
                "weight_value": [1000.0, 500.0, 3000.0],
            }
        )
        result = translate_weights_planning_to_legacy(weights_df, crosswalk)

        # Non-CT weight: 3000
        # CT translated weight: 1000*0.7 + 500*0.3 = 850
        expected_total = 3000.0 + 850.0
        assert result["weight_value"].sum() == pytest.approx(expected_total)

    def test_empty_planning_rows_return_input_unchanged(self, crosswalk):
        """When there are no CT planning region rows, the input is returned unchanged."""
        weights_df = pd.DataFrame(
            {
                "county_fips": ["06001"],
                "weight_value": [1000.0],
            }
        )
        result = translate_weights_planning_to_legacy(weights_df, crosswalk)

        pd.testing.assert_frame_equal(result, weights_df)

    def test_uses_planning_share_not_legacy_share(self):
        """Verify that planning_share is used as the weight, not legacy_share.

        This tests the bug fix: the function renames planning_share to weight,
        so the distributed value uses planning_share.
        """
        mapping = pd.DataFrame(
            {
                "legacy_county_fips": ["09001"],
                "planning_region_fips": ["09110"],
                "legacy_share": [0.9],
                "planning_share": [0.5],
            }
        )
        xwalk = CtPlanningRegionCrosswalk(
            mapping=mapping,
            legacy_vintage=2020,
            planning_vintage=2023,
        )
        weights_df = pd.DataFrame(
            {
                "county_fips": ["09110"],
                "weight_value": [1000.0],
            }
        )
        result = translate_weights_planning_to_legacy(weights_df, xwalk)

        # If planning_share is used: 1000 * 0.5 = 500
        # If legacy_share were used: 1000 * 0.9 = 900
        assert result.iloc[0]["weight_value"] == pytest.approx(500.0)

    def test_raises_on_missing_columns(self, crosswalk):
        """ValueError when required columns are missing."""
        bad_df = pd.DataFrame({"county_fips": ["09110"]})
        with pytest.raises(ValueError, match="weight_value"):
            translate_weights_planning_to_legacy(bad_df, crosswalk)
