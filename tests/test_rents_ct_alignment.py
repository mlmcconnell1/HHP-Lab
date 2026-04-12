"""Tests for CT geography alignment in ZORI aggregation.

Tests cover the _align_ct_geographies function which reconciles
Connecticut's legacy county FIPS codes with planning region FIPS codes
when crosswalk, weights, and ZORI data use different CT vintages.
"""

import logging

import pandas as pd

from coclab.geo.ct_planning_regions import CT_STATE_FIPS, CtPlanningRegionCrosswalk
from coclab.rents.aggregate import _align_ct_geographies


def _synthetic_crosswalk() -> CtPlanningRegionCrosswalk:
    """Build a deterministic crosswalk for testing.

    Maps two legacy counties to two planning regions with known shares:
      09001 -> 09110 (80% share) + 09120 (20% share)
      09003 -> 09120 (70% share) + 09130 (30% share)
    """
    mapping = pd.DataFrame(
        {
            "legacy_county_fips": ["09001", "09001", "09003", "09003"],
            "planning_region_fips": ["09110", "09120", "09120", "09130"],
            "legacy_share": [0.80, 0.20, 0.70, 0.30],
            "planning_share": [1.0, 0.30, 0.70, 1.0],
        }
    )
    return CtPlanningRegionCrosswalk(
        mapping=mapping,
        legacy_vintage=2020,
        planning_vintage=2023,
    )


def _patch_build_crosswalk(monkeypatch):
    """Patch build_ct_county_planning_region_crosswalk to return the synthetic crosswalk."""
    monkeypatch.setattr(
        "coclab.rents.aggregate.build_ct_county_planning_region_crosswalk",
        lambda **kwargs: _synthetic_crosswalk(),
    )


class TestAlignCtGeographies:
    """Tests for _align_ct_geographies."""

    def test_weights_translated_when_xwalk_legacy_and_weights_planning(self, monkeypatch):
        """Weights with planning-region codes are translated to legacy codes
        when the crosswalk uses legacy county FIPS."""
        _patch_build_crosswalk(monkeypatch)

        zori_df = pd.DataFrame(
            {
                "geo_id": ["01001", "01002"],
                "date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
                "zori": [1500.0, 1600.0],
            }
        )
        xwalk_df = pd.DataFrame(
            {
                "coc_id": ["CT-500", "CT-500"],
                "county_fips": ["09001", "09003"],
                "area_share": [0.6, 0.4],
            }
        )
        weights_df = pd.DataFrame(
            {
                "county_fips": ["09110", "09120", "09130", "01001"],
                "weight_value": [500.0, 300.0, 200.0, 400.0],
            }
        )

        result_zori, result_weights = _align_ct_geographies(
            zori_df, xwalk_df, weights_df, county_vintage=2020
        )

        # Non-CT weight should survive unchanged
        non_ct = result_weights[result_weights["county_fips"] == "01001"]
        assert len(non_ct) == 1
        assert non_ct.iloc[0]["weight_value"] == 400.0

        # CT weights should now be keyed to legacy county FIPS
        ct_weights = result_weights[
            result_weights["county_fips"].isin(["09001", "09003"])
        ]
        assert not ct_weights.empty
        # Planning-region codes should be gone from the result
        planning_remaining = result_weights[
            result_weights["county_fips"].isin(["09110", "09120", "09130"])
        ]
        assert planning_remaining.empty

        # ZORI should pass through unchanged (no legacy ZORI for CT here)
        pd.testing.assert_frame_equal(result_zori, zori_df)

    def test_zori_translated_when_xwalk_planning_and_zori_legacy(self, monkeypatch):
        """Legacy ZORI data is translated to planning regions when the
        crosswalk uses planning-region FIPS."""
        _patch_build_crosswalk(monkeypatch)

        zori_df = pd.DataFrame(
            {
                "geo_id": ["09001", "09003", "01001"],
                "date": pd.to_datetime(["2024-01-01"] * 3),
                "zori": [1200.0, 1400.0, 900.0],
            }
        )
        xwalk_df = pd.DataFrame(
            {
                "coc_id": ["CT-500", "CT-500"],
                "county_fips": ["09110", "09120"],
                "area_share": [0.5, 0.5],
            }
        )
        weights_df = pd.DataFrame(
            {
                "county_fips": ["01001", "01002"],
                "weight_value": [100.0, 100.0],
            }
        )

        result_zori, result_weights = _align_ct_geographies(
            zori_df, xwalk_df, weights_df, county_vintage=2023
        )

        # Legacy CT geo_ids should be replaced with planning-region codes
        ct_result = result_zori[result_zori["geo_id"].str.startswith(CT_STATE_FIPS)]
        legacy_remaining = result_zori[
            result_zori["geo_id"].isin(["09001", "09003"])
        ]
        assert legacy_remaining.empty
        assert set(ct_result["geo_id"]).issubset({"09110", "09120", "09130"})

        # Non-CT ZORI should survive unchanged
        non_ct = result_zori[result_zori["geo_id"] == "01001"]
        assert len(non_ct) == 1
        assert non_ct.iloc[0]["zori"] == 900.0

        # Weights should pass through unchanged (no planning codes in weights)
        pd.testing.assert_frame_equal(result_weights, weights_df)

    def test_translation_failure_logs_warning_and_falls_back(self, monkeypatch, caplog):
        """When build_ct_county_planning_region_crosswalk raises, the
        function logs a warning and returns the original DataFrames."""

        def _raise(**kwargs):
            raise FileNotFoundError("geometry file missing")

        monkeypatch.setattr(
            "coclab.rents.aggregate.build_ct_county_planning_region_crosswalk",
            _raise,
        )

        zori_df = pd.DataFrame(
            {
                "geo_id": ["09001"],
                "date": pd.to_datetime(["2024-01-01"]),
                "zori": [1200.0],
            }
        )
        xwalk_df = pd.DataFrame(
            {
                "coc_id": ["CT-500"],
                "county_fips": ["09110"],
                "area_share": [1.0],
            }
        )
        weights_df = pd.DataFrame(
            {
                "county_fips": ["09110"],
                "weight_value": [500.0],
            }
        )

        with caplog.at_level(logging.WARNING, logger="coclab.rents.aggregate"):
            result_zori, result_weights = _align_ct_geographies(
                zori_df, xwalk_df, weights_df, county_vintage=2023
            )

        # Inputs returned unchanged
        pd.testing.assert_frame_equal(result_zori, zori_df)
        pd.testing.assert_frame_equal(result_weights, weights_df)

        # Warning was logged
        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("skipped" in m.lower() for m in warning_messages)

    def test_weights_translation_failure_logs_warning(self, monkeypatch, caplog):
        """When weight translation fails, warning is logged and weights pass through."""

        def _raise(**kwargs):
            raise ValueError("empty geometries")

        monkeypatch.setattr(
            "coclab.rents.aggregate.build_ct_county_planning_region_crosswalk",
            _raise,
        )

        zori_df = pd.DataFrame(
            {
                "geo_id": ["01001"],
                "date": pd.to_datetime(["2024-01-01"]),
                "zori": [1000.0],
            }
        )
        xwalk_df = pd.DataFrame(
            {
                "coc_id": ["CT-500"],
                "county_fips": ["09001"],
                "area_share": [1.0],
            }
        )
        weights_df = pd.DataFrame(
            {
                "county_fips": ["09110"],
                "weight_value": [300.0],
            }
        )

        with caplog.at_level(logging.WARNING, logger="coclab.rents.aggregate"):
            result_zori, result_weights = _align_ct_geographies(
                zori_df, xwalk_df, weights_df, county_vintage=2020
            )

        pd.testing.assert_frame_equal(result_weights, weights_df)

        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("skipped" in m.lower() for m in warning_messages)

    def test_no_ct_data_passes_through_unchanged(self):
        """When no CT FIPS codes appear in any input, all DataFrames pass through."""
        zori_df = pd.DataFrame(
            {
                "geo_id": ["01001", "02001"],
                "date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
                "zori": [1000.0, 1200.0],
            }
        )
        xwalk_df = pd.DataFrame(
            {
                "coc_id": ["AL-500", "AK-500"],
                "county_fips": ["01001", "02001"],
                "area_share": [1.0, 1.0],
            }
        )
        weights_df = pd.DataFrame(
            {
                "county_fips": ["01001", "02001"],
                "weight_value": [100.0, 200.0],
            }
        )

        result_zori, result_weights = _align_ct_geographies(
            zori_df, xwalk_df, weights_df, county_vintage=2020
        )

        pd.testing.assert_frame_equal(result_zori, zori_df)
        pd.testing.assert_frame_equal(result_weights, weights_df)

    def test_column_preservation_after_weights_translation(self, monkeypatch):
        """Extra columns on weights_df survive the translation process."""
        _patch_build_crosswalk(monkeypatch)

        zori_df = pd.DataFrame(
            {
                "geo_id": ["01001"],
                "date": pd.to_datetime(["2024-01-01"]),
                "zori": [1000.0],
            }
        )
        xwalk_df = pd.DataFrame(
            {
                "coc_id": ["CT-500"],
                "county_fips": ["09001"],
                "area_share": [1.0],
            }
        )
        weights_df = pd.DataFrame(
            {
                "county_fips": ["09110", "01001"],
                "weight_value": [500.0, 300.0],
            }
        )

        _, result_weights = _align_ct_geographies(
            zori_df, xwalk_df, weights_df, county_vintage=2020
        )

        # Both county_fips and weight_value columns must survive
        assert "county_fips" in result_weights.columns
        assert "weight_value" in result_weights.columns

    def test_column_preservation_after_zori_translation(self, monkeypatch):
        """Extra metadata columns on zori_df survive the translation process."""
        _patch_build_crosswalk(monkeypatch)

        zori_df = pd.DataFrame(
            {
                "geo_id": ["09001", "01001"],
                "date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
                "zori": [1200.0, 900.0],
                "region_name": ["Fairfield", "Autauga"],
            }
        )
        xwalk_df = pd.DataFrame(
            {
                "coc_id": ["CT-500"],
                "county_fips": ["09110"],
                "area_share": [1.0],
            }
        )
        weights_df = pd.DataFrame(
            {
                "county_fips": ["01001"],
                "weight_value": [100.0],
            }
        )

        result_zori, _ = _align_ct_geographies(
            zori_df, xwalk_df, weights_df, county_vintage=2023
        )

        assert "geo_id" in result_zori.columns
        assert "date" in result_zori.columns
        assert "zori" in result_zori.columns
        assert "region_name" in result_zori.columns

        # Non-CT row metadata is untouched
        non_ct = result_zori[result_zori["geo_id"] == "01001"]
        assert non_ct.iloc[0]["region_name"] == "Autauga"
