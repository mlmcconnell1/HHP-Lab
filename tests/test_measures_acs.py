"""Tests for ACS measure builder."""

from __future__ import annotations

import pandas as pd
import pytest

from hhplab.measures.measures_acs import (
    aggregate_to_coc,
)


class TestAggregateToCoC:
    """Tests for aggregate_to_coc function."""

    def test_area_weighted_aggregation(self):
        """Test aggregation with area weighting."""
        # Create mock ACS data
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200", "08001000300"],
                "total_population": [1000, 2000, 3000],
                "adult_population": [800, 1600, 2400],
                "population_below_poverty": [100, 200, 300],
                "median_household_income": [50000, 60000, 70000],
                "median_gross_rent": [1000, 1200, 1400],
            }
        )

        # Create crosswalk - two tracts in CO-500, one in CO-501
        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100", "08001000200", "08001000300"],
                "coc_id": ["CO-500", "CO-500", "CO-501"],
                "area_share": [0.8, 0.5, 1.0],  # tract 1: 80% in CO-500, tract 2: 50% in CO-500
                "pop_share": [0.8, 0.5, 1.0],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk, weighting="area")

        assert len(result) == 2
        assert set(result["coc_id"]) == {"CO-500", "CO-501"}

        # Check CO-500 weighted population
        # 1000 * 0.8 + 2000 * 0.5 = 800 + 1000 = 1800
        co500 = result[result["coc_id"] == "CO-500"].iloc[0]
        assert co500["total_population"] == 1800
        assert co500["weighting_method"] == "area"

        # Check CO-501
        co501 = result[result["coc_id"] == "CO-501"].iloc[0]
        assert co501["total_population"] == 3000  # 3000 * 1.0

    def test_population_weighted_aggregation(self):
        """Test aggregation with population weighting.

        Count variables (total_population, etc.) should ALWAYS use area_share
        to produce actual population totals. The weighting parameter only
        affects median value aggregation.
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200"],
                "total_population": [1000, 2000],
                "adult_population": [800, 1600],
                "population_below_poverty": [100, 200],
                "median_household_income": [50000, 60000],
                "median_gross_rent": [1000, 1200],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100", "08001000200"],
                "coc_id": ["CO-500", "CO-500"],
                "area_share": [1.0, 1.0],
                "pop_share": [0.4, 0.6],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk, weighting="population")

        assert len(result) == 1
        co500 = result.iloc[0]

        # Count variables use area_share (not pop_share) to get actual totals
        # 1000 * 1.0 + 2000 * 1.0 = 3000
        assert co500["total_population"] == 3000
        assert co500["adult_population"] == 2400  # 800 + 1600
        assert co500["population_below_poverty"] == 300  # 100 + 200

        # Median values use pop_share for weighting
        # Income: (50000 * 1000 * 0.4 + 60000 * 2000 * 0.6) / (1000 * 0.4 + 2000 * 0.6)
        #       = (20M + 72M) / (400 + 1200) = 92M / 1600 = 57500
        expected_income = (50000 * 1000 * 0.4 + 60000 * 2000 * 0.6) / (1000 * 0.4 + 2000 * 0.6)
        assert abs(co500["median_household_income"] - expected_income) < 0.01

        assert co500["weighting_method"] == "population"

    def test_count_vars_always_use_area_share(self):
        """Test that count variables use area_share regardless of weighting parameter.

        This is a regression test for the bug where pop_share was used for count
        variables when weighting='population', which produced weighted averages
        instead of actual population totals.
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200"],
                "total_population": [1000, 2000],
                "adult_population": [800, 1600],
                "population_below_poverty": [100, 200],
                "median_household_income": [50000, 60000],
                "median_gross_rent": [1000, 1200],
            }
        )

        # Different area_share and pop_share to verify correct one is used
        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100", "08001000200"],
                "coc_id": ["CO-500", "CO-500"],
                "area_share": [0.5, 0.5],  # 50% of each tract
                "pop_share": [0.33, 0.67],  # Normalized to sum to 1.0
            }
        )

        # With area weighting
        result_area = aggregate_to_coc(acs_data, crosswalk, weighting="area")
        # With population weighting
        result_pop = aggregate_to_coc(acs_data, crosswalk, weighting="population")

        # Both should give same count totals (using area_share)
        # 1000 * 0.5 + 2000 * 0.5 = 1500
        assert result_area.iloc[0]["total_population"] == 1500
        assert result_pop.iloc[0]["total_population"] == 1500

        # Adult population: 800 * 0.5 + 1600 * 0.5 = 1200
        assert result_area.iloc[0]["adult_population"] == 1200
        assert result_pop.iloc[0]["adult_population"] == 1200

        # If pop_share were used (the old bug), we'd get:
        # 1000 * 0.33 + 2000 * 0.67 = 330 + 1340 = 1670 (different!)
        # Verify we're NOT getting this buggy value
        assert result_pop.iloc[0]["total_population"] != 1670

    def test_coverage_ratio_calculation(self):
        """Test that coverage_ratio correctly computes area-weighted coverage."""
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200"],
                "total_population": [1000, pd.NA],  # Second tract has no data
                "adult_population": [800, pd.NA],
                "population_below_poverty": [100, pd.NA],
                "median_household_income": [50000, pd.NA],
                "median_gross_rent": [1000, pd.NA],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100", "08001000200"],
                "coc_id": ["CO-500", "CO-500"],
                "area_share": [0.6, 0.4],
                "pop_share": [0.6, 0.4],
                "intersection_area": [600.0, 400.0],  # Areas in arbitrary units
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk, weighting="area")

        # Only first tract has data (intersection_area=600)
        # Total area = 600 + 400 = 1000
        # Coverage = 600 / 1000 = 0.6
        assert result.iloc[0]["coverage_ratio"] == 0.6

    def test_missing_weight_column_raises(self):
        """Test that missing weight column raises ValueError."""
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100"],
                "total_population": [1000],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100"],
                "coc_id": ["CO-500"],
                # Missing area_share column
            }
        )

        with pytest.raises(ValueError, match="missing required column"):
            aggregate_to_coc(acs_data, crosswalk, weighting="area")

    def test_adds_metadata_columns(self):
        """Test that result includes source metadata."""
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100"],
                "total_population": [1000],
                "adult_population": [800],
                "population_below_poverty": [100],
                "median_household_income": [50000],
                "median_gross_rent": [1000],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100"],
                "coc_id": ["CO-500"],
                "area_share": [1.0],
                "pop_share": [1.0],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk, weighting="area")

        assert "source" in result.columns
        assert result.iloc[0]["source"] == "acs_5yr"
        assert result.iloc[0]["weighting_method"] == "area"


class TestACSSchemaMeasures:
    """Tests to ensure the output schema matches requirements."""

    def test_output_schema_columns(self):
        """Test that aggregate output has all required schema columns."""
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100"],
                "total_population": [1000],
                "adult_population": [800],
                "population_below_poverty": [100],
                "median_household_income": [50000],
                "median_gross_rent": [1000],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100"],
                "coc_id": ["CO-500"],
                "area_share": [1.0],
                "pop_share": [1.0],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk, weighting="area")

        # Required columns per schema
        required_columns = [
            "coc_id",
            "weighting_method",
            "total_population",
            "adult_population",
            "population_below_poverty",
            "median_household_income",
            "median_gross_rent",
            "coverage_ratio",
            "source",
        ]

        for col in required_columns:
            assert col in result.columns, f"Missing required column: {col}"


class TestGEOIDValidation:
    """Tests for GEOID overlap validation between crosswalk and ACS data."""

    def test_warns_on_low_geoid_overlap(self):
        """Test that warning is raised when crosswalk and ACS GEOIDs don't match."""
        import warnings

        # ACS data with Connecticut old-style GEOIDs (county-based: 09001)
        acs_data = pd.DataFrame(
            {
                "GEOID": ["09001000100", "09001000200"],  # Old CT format
                "total_population": [1000, 2000],
                "adult_population": [800, 1600],
                "population_below_poverty": [100, 200],
                "median_household_income": [50000, 60000],
                "median_gross_rent": [1000, 1200],
            }
        )

        # Crosswalk with Connecticut new-style GEOIDs (planning region: 09110)
        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["09110000100", "09110000200"],  # New CT format
                "coc_id": ["CT-500", "CT-500"],
                "area_share": [1.0, 1.0],
                "pop_share": [0.5, 0.5],
            }
        )

        # Should warn about low GEOID overlap
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = aggregate_to_coc(acs_data, crosswalk, weighting="area")

            # Check that a warning was raised
            assert len(w) == 1
            assert "Low GEOID overlap" in str(w[0].message)
            assert "State 09" in str(w[0].message)

        # Result should still be computed, but with zero coverage
        assert len(result) == 1
        assert result.iloc[0]["coverage_ratio"] == 0.0
        # Population should be 0 since no tracts matched
        assert result.iloc[0]["total_population"] == 0

    def test_no_warning_when_geoids_match(self):
        """Test that no warning is raised when GEOIDs match correctly."""
        import warnings

        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200"],
                "total_population": [1000, 2000],
                "adult_population": [800, 1600],
                "population_below_poverty": [100, 200],
                "median_household_income": [50000, 60000],
                "median_gross_rent": [1000, 1200],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100", "08001000200"],
                "coc_id": ["CO-500", "CO-500"],
                "area_share": [1.0, 1.0],
                "pop_share": [0.5, 0.5],
            }
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = aggregate_to_coc(acs_data, crosswalk, weighting="area")

            # No GEOID overlap warnings should be raised
            geoid_warnings = [x for x in w if "GEOID overlap" in str(x.message)]
            assert len(geoid_warnings) == 0

        # Verify correct results
        assert result.iloc[0]["total_population"] == 3000


class TestCtPlanningRegionRemap:
    """Tests for CT planning region GEOID remapping."""

    def test_remaps_ct_planning_region_geoids(self):
        from hhplab.geo.ct_planning_regions import remap_ct_planning_region_geoids

        acs_data = pd.DataFrame(
            {
                "GEOID": ["09110000100", "08001000100"],
                "total_population": [1000, 2000],
            }
        )

        mapping = pd.DataFrame(
            {
                "planning_geoid": ["09110000100"],
                "legacy_geoid": ["09001000100"],
            }
        )

        result = remap_ct_planning_region_geoids(acs_data, mapping)

        assert result.loc[0, "GEOID"] == "09001000100"
        assert result.loc[1, "GEOID"] == "08001000100"


class TestCtRemapFailurePaths:
    """Integration tests: CT remap failure paths through the aggregation pipeline.

    ``_maybe_remap_ct_planning_regions`` catches several failure classes and
    returns the original ACS data so that aggregation can continue.  These
    tests verify that each failure path emits a warning *and* that the
    downstream ``aggregate_to_coc`` call still produces valid results.
    """

    # -- shared helpers -------------------------------------------------------

    @staticmethod
    def _ct_acs_data() -> pd.DataFrame:
        """ACS data with CT planning-region GEOIDs (post-2022 format)."""
        return pd.DataFrame(
            {
                "GEOID": ["09110000100", "09110000200"],
                "total_population": [1000, 2000],
                "adult_population": [800, 1600],
                "population_below_poverty": [100, 200],
                "median_household_income": [50000, 60000],
                "median_gross_rent": [1000, 1200],
            }
        )

    @staticmethod
    def _ct_crosswalk() -> pd.DataFrame:
        """Crosswalk with CT GEOIDs and a tract_vintage column."""
        return pd.DataFrame(
            {
                "tract_geoid": ["09110000100", "09110000200"],
                "coc_id": ["CT-503", "CT-503"],
                "area_share": [1.0, 1.0],
                "pop_share": [0.5, 0.5],
                "tract_vintage": ["2020", "2020"],
            }
        )

    # -- import error ---------------------------------------------------------

    def test_import_error_continues_with_warning(self, monkeypatch):
        """When the geo module import fails, aggregation continues with a warning."""
        import builtins
        import sys
        import warnings

        from hhplab.measures.measures_acs import _maybe_remap_ct_planning_regions

        real_import = builtins.__import__

        def _fail_ct_import(name, *args, **kwargs):
            if name == "hhplab.geo.ct_planning_regions":
                raise ImportError("simulated missing module")
            return real_import(name, *args, **kwargs)

        # Remove the module from sys.modules so the deferred import inside
        # _maybe_remap_ct_planning_regions actually goes through __import__
        # rather than finding the already-cached module.
        mod_key = "hhplab.geo.ct_planning_regions"
        saved_mod = sys.modules.pop(mod_key, None)
        monkeypatch.setattr(builtins, "__import__", _fail_ct_import)

        acs_data = self._ct_acs_data()
        crosswalk = self._ct_crosswalk()

        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                result = _maybe_remap_ct_planning_regions(acs_data, crosswalk, "2022")
        finally:
            # Restore the module so other tests are not affected
            if saved_mod is not None:
                sys.modules[mod_key] = saved_mod

        # A warning about import failure should be emitted
        import_warnings = [
            w for w in caught if "Unable to load CT planning-region helpers" in str(w.message)
        ]
        assert len(import_warnings) == 1

        # Original data returned unchanged
        assert result.equals(acs_data)

        # Aggregation still works on the (un-remapped) data
        coc_result = aggregate_to_coc(result, crosswalk)
        assert len(coc_result) == 1
        assert coc_result.iloc[0]["total_population"] == 3000

    # -- FileNotFoundError from build_ct_tract_planning_region_map -----------

    def test_file_not_found_continues_with_warning(self, monkeypatch):
        """When geometry files are missing, aggregation continues with a warning."""
        import warnings

        from hhplab.measures.measures_acs import _maybe_remap_ct_planning_regions

        monkeypatch.setattr(
            "hhplab.geo.ct_planning_regions.build_ct_tract_planning_region_map",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                FileNotFoundError("Required geometry file not found: /fake/path.parquet")
            ),
        )

        acs_data = self._ct_acs_data()
        crosswalk = self._ct_crosswalk()

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _maybe_remap_ct_planning_regions(acs_data, crosswalk, "2022")

        remap_warnings = [
            w for w in caught if "CT planning-region GEOID remap skipped" in str(w.message)
        ]
        assert len(remap_warnings) == 1
        assert "geometry file not found" in str(remap_warnings[0].message).lower()

        # Original data returned unchanged
        assert result.equals(acs_data)

        # Aggregation still succeeds
        coc_result = aggregate_to_coc(result, crosswalk)
        assert len(coc_result) == 1
        assert coc_result.iloc[0]["total_population"] == 3000

    # -- ValueError from build_ct_tract_planning_region_map ------------------

    def test_value_error_continues_with_warning(self, monkeypatch):
        """When mapping raises ValueError, aggregation continues with a warning."""
        import warnings

        from hhplab.measures.measures_acs import _maybe_remap_ct_planning_regions

        monkeypatch.setattr(
            "hhplab.geo.ct_planning_regions.build_ct_tract_planning_region_map",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                ValueError("CT tract or planning region geometries are empty")
            ),
        )

        acs_data = self._ct_acs_data()
        crosswalk = self._ct_crosswalk()

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _maybe_remap_ct_planning_regions(acs_data, crosswalk, "2022")

        remap_warnings = [
            w for w in caught if "CT planning-region GEOID remap skipped" in str(w.message)
        ]
        assert len(remap_warnings) == 1
        assert "empty" in str(remap_warnings[0].message).lower()

        # Original data returned unchanged
        assert result.equals(acs_data)

        # Aggregation still produces valid output
        coc_result = aggregate_to_coc(result, crosswalk)
        assert len(coc_result) == 1
        assert coc_result.iloc[0]["total_population"] == 3000


class TestUnemploymentAggregation:
    """Tests for unemployment rate derivation during aggregation."""

    def test_unemployment_rate_derived_from_components(self):
        """unemployment_rate = sum(unemployed) / sum(labor_force) at CoC level."""
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200"],
                "total_population": [1000, 2000],
                "civilian_labor_force": [600, 1200],
                "unemployed_count": [30, 60],
                "median_household_income": [50000, 60000],
                "median_gross_rent": [1000, 1200],
            }
        )
        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100", "08001000200"],
                "coc_id": ["CO-500", "CO-500"],
                "area_share": [1.0, 1.0],
                "pop_share": [1.0, 1.0],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk, weighting="area")

        co500 = result.iloc[0]
        assert co500["civilian_labor_force"] == pytest.approx(1800)
        assert co500["unemployed_count"] == pytest.approx(90)
        assert co500["unemployment_rate"] == pytest.approx(90 / 1800)

    def test_unemployment_rate_with_partial_area_share(self):
        """Unemployment components are area-weighted before rate derivation."""
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200"],
                "total_population": [1000, 2000],
                "civilian_labor_force": [800, 1600],
                "unemployed_count": [40, 80],
                "median_household_income": [50000, 60000],
                "median_gross_rent": [1000, 1200],
            }
        )
        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100", "08001000200"],
                "coc_id": ["CO-500", "CO-500"],
                "area_share": [0.5, 0.5],
                "pop_share": [0.5, 0.5],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk, weighting="area")

        co500 = result.iloc[0]
        # 800*0.5 + 1600*0.5 = 1200
        assert co500["civilian_labor_force"] == pytest.approx(1200)
        # 40*0.5 + 80*0.5 = 60
        assert co500["unemployed_count"] == pytest.approx(60)
        assert co500["unemployment_rate"] == pytest.approx(60 / 1200)

    def test_unemployment_rate_zero_labor_force_is_null(self):
        """unemployment_rate is null when labor force is zero."""
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100"],
                "total_population": [100],
                "civilian_labor_force": [0],
                "unemployed_count": [0],
                "median_household_income": [50000],
                "median_gross_rent": [1000],
            }
        )
        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100"],
                "coc_id": ["CO-500"],
                "area_share": [1.0],
                "pop_share": [1.0],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk, weighting="area")
        assert pd.isna(result.iloc[0]["unemployment_rate"])

    def test_unemployment_columns_in_output(self):
        """Output includes civilian_labor_force, unemployed_count, and unemployment_rate."""
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100"],
                "total_population": [1000],
                "civilian_labor_force": [600],
                "unemployed_count": [30],
                "median_household_income": [50000],
                "median_gross_rent": [1000],
            }
        )
        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["08001000100"],
                "coc_id": ["CO-500"],
                "area_share": [1.0],
                "pop_share": [1.0],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk, weighting="area")
        for col in ("civilian_labor_force", "unemployed_count", "unemployment_rate"):
            assert col in result.columns, f"Missing column: {col}"


# ---------------------------------------------------------------------------
# Fixtures shared by edge-case tests below
# ---------------------------------------------------------------------------


@pytest.fixture()
def _simple_crosswalk():
    """Two tracts fully inside one CoC, with intersection_area for coverage."""
    return pd.DataFrame(
        {
            "tract_geoid": ["08001000100", "08001000200"],
            "coc_id": ["CO-500", "CO-500"],
            "area_share": [1.0, 1.0],
            "pop_share": [0.5, 0.5],
            "intersection_area": [500.0, 500.0],
        }
    )


@pytest.fixture()
def _three_tract_crosswalk():
    """Three tracts fully inside one CoC, with intersection_area."""
    return pd.DataFrame(
        {
            "tract_geoid": ["08001000100", "08001000200", "08001000300"],
            "coc_id": ["CO-500", "CO-500", "CO-500"],
            "area_share": [1.0, 1.0, 1.0],
            "pop_share": [0.33, 0.34, 0.33],
            "intersection_area": [300.0, 400.0, 300.0],
        }
    )


# ---------------------------------------------------------------------------
# Bead 5.9 — Null-heavy ACS inputs
# ---------------------------------------------------------------------------


class TestNullHeavyACSInputs:
    """Edge cases where ACS columns are predominantly NA."""

    def test_coverage_ratio_vs_per_measure_coverage_with_null_median(self, _simple_crosswalk):
        """coverage_ratio=100% but coverage_median_household_income is low
        when median_household_income is NA for most tracts but total_population
        is present everywhere.
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200"],
                "total_population": [1000, 2000],
                "median_household_income": [pd.NA, 60000],
                "median_gross_rent": [1000, 1200],
            }
        )

        result = aggregate_to_coc(acs_data, _simple_crosswalk)

        row = result.iloc[0]
        # total_population present for both tracts -> 100% coverage
        assert row["coverage_ratio"] == 1.0
        # median_household_income present for only 1 of 2 tracts (area 500/1000)
        assert row["coverage_median_household_income"] == pytest.approx(0.5)

    @pytest.mark.parametrize(
        "median_col",
        ["median_household_income", "median_gross_rent"],
        ids=["income", "rent"],
    )
    def test_all_na_median_returns_pd_na(self, _simple_crosswalk, median_col):
        """When every tract has NA for a median column the result is pd.NA,
        not 0 or NaN.
        """
        other_median = (
            "median_gross_rent"
            if median_col == "median_household_income"
            else "median_household_income"
        )
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200"],
                "total_population": [1000, 2000],
                median_col: [pd.NA, pd.NA],
                other_median: [1000, 1200],
            }
        )

        result = aggregate_to_coc(acs_data, _simple_crosswalk)
        assert pd.isna(result.iloc[0][median_col])

    def test_mixed_na_count_vs_median_columns(self, _three_tract_crosswalk):
        """Count columns fill NA->0 but median columns preserve NA semantics.

        Tract 1: has population, no income, has rent
        Tract 2: has population, has income, no rent
        Tract 3: no population, no income, no rent
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200", "08001000300"],
                "total_population": [1000, 2000, pd.NA],
                "median_household_income": [pd.NA, 55000, pd.NA],
                "median_gross_rent": [1100, pd.NA, pd.NA],
            }
        )

        result = aggregate_to_coc(acs_data, _three_tract_crosswalk)
        row = result.iloc[0]

        # Count: NA tracts contribute 0 -> total = 1000 + 2000 + 0
        assert row["total_population"] == 3000

        # Median income: only tract 2 valid -> should equal tract 2's value
        assert row["median_household_income"] == pytest.approx(55000)

        # Median rent: only tract 1 valid -> should equal tract 1's value
        assert row["median_gross_rent"] == pytest.approx(1100)

        # Per-measure coverage differs from coverage_ratio
        # coverage_ratio: tracts with total_pop (tracts 1+2 = area 700/1000)
        assert row["coverage_ratio"] == pytest.approx(0.7)
        # coverage_median_household_income: only tract 2 (area 400/1000)
        assert row["coverage_median_household_income"] == pytest.approx(0.4)
        # coverage_median_gross_rent: only tract 1 (area 300/1000)
        assert row["coverage_median_gross_rent"] == pytest.approx(0.3)


# ---------------------------------------------------------------------------
# Bead 5.15 — Zero population weight median
# ---------------------------------------------------------------------------


class TestZeroPopulationWeightMedian:
    """Edge cases where population weights are zero (water-only tracts)."""

    def test_all_zero_pop_with_median_values_returns_na(self, _simple_crosswalk):
        """When all tracts have zero population, median columns return pd.NA
        even if median values exist (e.g., water-only tracts with Census noise).
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200"],
                "total_population": [0, 0],
                "median_household_income": [45000, 55000],
                "median_gross_rent": [900, 1100],
            }
        )

        result = aggregate_to_coc(acs_data, _simple_crosswalk)
        row = result.iloc[0]

        # pop_weights = total_population * area_share = 0 for both tracts
        # -> valid_mask requires pop_weights > 0 -> no valid rows -> pd.NA
        assert pd.isna(row["median_household_income"])
        assert pd.isna(row["median_gross_rent"])

    def test_mix_of_na_median_and_zero_pop_returns_na(self, _three_tract_crosswalk):
        """When some tracts have NA medians and the rest have zero population,
        the result is pd.NA (no valid weighted contribution).
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200", "08001000300"],
                "total_population": [0, 500, 0],
                "median_household_income": [50000, pd.NA, 60000],
                "median_gross_rent": [pd.NA, pd.NA, 1200],
            }
        )

        result = aggregate_to_coc(acs_data, _three_tract_crosswalk)
        row = result.iloc[0]

        # Tract 1: has median income but pop=0 -> weight=0 -> excluded
        # Tract 2: has pop but NA income -> excluded
        # Tract 3: has median income but pop=0 -> weight=0 -> excluded
        assert pd.isna(row["median_household_income"])

        # All tracts either NA rent or zero pop -> pd.NA
        assert pd.isna(row["median_gross_rent"])

    @pytest.mark.parametrize(
        "populations,expected_income",
        [
            ([0, 2000], 60000),  # Only tract 2 has nonzero pop
            ([1000, 0], 50000),  # Only tract 1 has nonzero pop
        ],
        ids=["only_second_tract", "only_first_tract"],
    )
    def test_zero_pop_tracts_excluded_from_median(
        self, _simple_crosswalk, populations, expected_income
    ):
        """Zero-population tracts are excluded from median calculation;
        the result equals the sole nonzero-pop tract's median.
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200"],
                "total_population": populations,
                "median_household_income": [50000, 60000],
                "median_gross_rent": [1000, 1200],
            }
        )

        result = aggregate_to_coc(acs_data, _simple_crosswalk)
        assert result.iloc[0]["median_household_income"] == pytest.approx(expected_income)


# ---------------------------------------------------------------------------
# Bead 5.4 — Per-measure coverage columns
# ---------------------------------------------------------------------------


class TestPerMeasureCoverage:
    """Verify per-measure coverage columns exist and reflect per-column availability."""

    def test_per_measure_coverage_columns_present(self, _simple_crosswalk):
        """Output contains coverage_median_household_income and
        coverage_median_gross_rent columns.
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200"],
                "total_population": [1000, 2000],
                "median_household_income": [50000, 60000],
                "median_gross_rent": [1000, 1200],
            }
        )

        result = aggregate_to_coc(acs_data, _simple_crosswalk)
        assert "coverage_median_household_income" in result.columns
        assert "coverage_median_gross_rent" in result.columns

    def test_per_measure_coverage_equals_ratio_when_all_present(self, _simple_crosswalk):
        """When every tract has data for all columns, per-measure coverage
        equals coverage_ratio.
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200"],
                "total_population": [1000, 2000],
                "median_household_income": [50000, 60000],
                "median_gross_rent": [1000, 1200],
            }
        )

        result = aggregate_to_coc(acs_data, _simple_crosswalk)
        row = result.iloc[0]
        assert row["coverage_median_household_income"] == pytest.approx(row["coverage_ratio"])
        assert row["coverage_median_gross_rent"] == pytest.approx(row["coverage_ratio"])

    def test_per_measure_coverage_differs_from_ratio(self, _three_tract_crosswalk):
        """Per-measure coverage diverges from coverage_ratio when data
        availability varies by column.

        Tract 1: pop present, income present, rent NA
        Tract 2: pop present, income NA,      rent present
        Tract 3: pop NA,      income NA,      rent NA
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200", "08001000300"],
                "total_population": [1000, 2000, pd.NA],
                "median_household_income": [50000, pd.NA, pd.NA],
                "median_gross_rent": [pd.NA, 1200, pd.NA],
            }
        )

        result = aggregate_to_coc(acs_data, _three_tract_crosswalk)
        row = result.iloc[0]

        # coverage_ratio based on total_population: tracts 1+2 have data
        # area: (300+400)/1000 = 0.7
        assert row["coverage_ratio"] == pytest.approx(0.7)

        # coverage_median_household_income: only tract 1 -> 300/1000 = 0.3
        assert row["coverage_median_household_income"] == pytest.approx(0.3)

        # coverage_median_gross_rent: only tract 2 -> 400/1000 = 0.4
        assert row["coverage_median_gross_rent"] == pytest.approx(0.4)

        # All three values are different
        assert row["coverage_ratio"] != row["coverage_median_household_income"]
        assert row["coverage_ratio"] != row["coverage_median_gross_rent"]
        assert row["coverage_median_household_income"] != row["coverage_median_gross_rent"]

    def test_per_measure_coverage_without_intersection_area(self):
        """Without intersection_area, per-measure coverage falls back to
        fraction of tracts with data.
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["08001000100", "08001000200", "08001000300"],
                "total_population": [1000, 2000, 3000],
                "median_household_income": [50000, pd.NA, pd.NA],
                "median_gross_rent": [1000, 1200, pd.NA],
            }
        )

        crosswalk_no_area = pd.DataFrame(
            {
                "tract_geoid": ["08001000100", "08001000200", "08001000300"],
                "coc_id": ["CO-500", "CO-500", "CO-500"],
                "area_share": [1.0, 1.0, 1.0],
                "pop_share": [0.33, 0.34, 0.33],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk_no_area)
        row = result.iloc[0]

        # Fallback: fraction of tracts with total_population -> 3/3
        assert row["coverage_ratio"] == pytest.approx(1.0)
        # income present in 1/3 tracts
        assert row["coverage_median_household_income"] == pytest.approx(1 / 3)
        # rent present in 2/3 tracts
        assert row["coverage_median_gross_rent"] == pytest.approx(2 / 3)


# ---------------------------------------------------------------------------
# Bead 5.11 — Single-tract CoCs / small-CoC instability
# ---------------------------------------------------------------------------


class TestSmallCoCInstability:
    """Edge cases for CoCs with only 1-2 tracts.

    Small CoCs are sensitive to individual tract values, crosswalk precision,
    and NA data. These tests verify correct behavior at the minimum tract
    count boundary.
    """

    def test_single_tract_coc_full_overlap(self):
        """Single tract fully inside a CoC (area_share=1.0) passes through values."""
        acs_data = pd.DataFrame(
            {
                "GEOID": ["06037000100"],
                "total_population": [4500],
                "adult_population": [3600],
                "population_below_poverty": [450],
                "median_household_income": [72000],
                "median_gross_rent": [1800],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["06037000100"],
                "coc_id": ["CA-600"],
                "area_share": [1.0],
                "pop_share": [1.0],
                "intersection_area": [1000.0],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk)
        row = result.iloc[0]

        assert row["coc_id"] == "CA-600"
        assert row["total_population"] == 4500
        assert row["adult_population"] == 3600
        assert row["population_below_poverty"] == 450
        assert row["median_household_income"] == pytest.approx(72000)
        assert row["median_gross_rent"] == pytest.approx(1800)
        assert row["coverage_ratio"] == 1.0

    def test_single_tract_coc_partial_overlap(self):
        """Single tract with area_share < 1.0: counts are scaled, medians use
        the sole tract's value (since it is the only contributor).
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["06037000100"],
                "total_population": [4000],
                "adult_population": [3200],
                "population_below_poverty": [400],
                "median_household_income": [65000],
                "median_gross_rent": [1500],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["06037000100"],
                "coc_id": ["CA-600"],
                "area_share": [0.6],
                "pop_share": [0.6],
                "intersection_area": [600.0],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk)
        row = result.iloc[0]

        # Count variables scaled by area_share
        assert row["total_population"] == pytest.approx(4000 * 0.6)
        assert row["adult_population"] == pytest.approx(3200 * 0.6)
        assert row["population_below_poverty"] == pytest.approx(400 * 0.6)

        # Median variables: only one tract, so its median is the result
        # pop_weight = 4000 * 0.6 = 2400 > 0 -> valid
        assert row["median_household_income"] == pytest.approx(65000)
        assert row["median_gross_rent"] == pytest.approx(1500)

    def test_single_tract_coc_all_na_medians(self):
        """Single-tract CoC where median values are NA: result is pd.NA,
        not zero or an error.
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["06037000100"],
                "total_population": [3000],
                "adult_population": [2400],
                "population_below_poverty": [300],
                "median_household_income": [pd.NA],
                "median_gross_rent": [pd.NA],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["06037000100"],
                "coc_id": ["CA-600"],
                "area_share": [1.0],
                "pop_share": [1.0],
                "intersection_area": [1000.0],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk)
        row = result.iloc[0]

        # Count variables still aggregated normally
        assert row["total_population"] == 3000
        assert row["adult_population"] == 2400

        # Median columns are pd.NA
        assert pd.isna(row["median_household_income"])
        assert pd.isna(row["median_gross_rent"])

        # Coverage ratio is 1.0 (tract has total_population)
        assert row["coverage_ratio"] == 1.0

        # Per-measure coverage for median columns is 0.0 (no median data)
        assert row["coverage_median_household_income"] == pytest.approx(0.0)
        assert row["coverage_median_gross_rent"] == pytest.approx(0.0)

    def test_single_tract_coc_partial_overlap_na_medians(self):
        """Single-tract CoC with area_share < 1.0 AND NA medians: counts are
        scaled, medians are pd.NA, no division-by-zero errors.
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["06037000100"],
                "total_population": [2000],
                "adult_population": [1600],
                "population_below_poverty": [200],
                "median_household_income": [pd.NA],
                "median_gross_rent": [pd.NA],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["06037000100"],
                "coc_id": ["CA-600"],
                "area_share": [0.3],
                "pop_share": [0.3],
                "intersection_area": [300.0],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk)
        row = result.iloc[0]

        assert row["total_population"] == pytest.approx(2000 * 0.3)
        assert pd.isna(row["median_household_income"])
        assert pd.isna(row["median_gross_rent"])

    def test_two_tract_coc_one_has_zero_population(self):
        """Two-tract CoC where one tract has population=0: no division by zero,
        median comes entirely from the populated tract.
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["06037000100", "06037000200"],
                "total_population": [5000, 0],
                "adult_population": [4000, 0],
                "population_below_poverty": [500, 0],
                "median_household_income": [80000, 45000],
                "median_gross_rent": [2000, 900],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["06037000100", "06037000200"],
                "coc_id": ["CA-600", "CA-600"],
                "area_share": [1.0, 1.0],
                "pop_share": [1.0, 0.0],
                "intersection_area": [800.0, 200.0],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk)
        row = result.iloc[0]

        # Count variables: 5000*1.0 + 0*1.0 = 5000
        assert row["total_population"] == 5000
        assert row["adult_population"] == 4000
        assert row["population_below_poverty"] == 500

        # Median: pop_weight for tract 2 is 0*1.0 = 0 -> excluded
        # Only tract 1 contributes -> result equals tract 1's median
        assert row["median_household_income"] == pytest.approx(80000)
        assert row["median_gross_rent"] == pytest.approx(2000)

        # Coverage ratio: both tracts have total_population data (even if 0)
        assert row["coverage_ratio"] == 1.0

    def test_two_tract_coc_both_zero_population(self):
        """Two-tract CoC where both tracts have population=0: counts are 0,
        medians are pd.NA.
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["06037000100", "06037000200"],
                "total_population": [0, 0],
                "adult_population": [0, 0],
                "population_below_poverty": [0, 0],
                "median_household_income": [50000, 60000],
                "median_gross_rent": [1000, 1200],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["06037000100", "06037000200"],
                "coc_id": ["CA-600", "CA-600"],
                "area_share": [1.0, 1.0],
                "pop_share": [0.0, 0.0],
                "intersection_area": [500.0, 500.0],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk)
        row = result.iloc[0]

        assert row["total_population"] == 0
        assert row["adult_population"] == 0

        # Both tracts have zero pop -> pop_weights are 0 -> medians are pd.NA
        assert pd.isna(row["median_household_income"])
        assert pd.isna(row["median_gross_rent"])

    def test_two_tract_coc_zero_pop_tract_has_na_medians(self):
        """Two-tract CoC: the zero-population tract also has NA medians.
        Verify graceful handling when NA and zero-pop coincide.
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["06037000100", "06037000200"],
                "total_population": [3000, 0],
                "adult_population": [2400, 0],
                "population_below_poverty": [300, 0],
                "median_household_income": [55000, pd.NA],
                "median_gross_rent": [1300, pd.NA],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["06037000100", "06037000200"],
                "coc_id": ["CA-600", "CA-600"],
                "area_share": [1.0, 1.0],
                "pop_share": [1.0, 0.0],
                "intersection_area": [700.0, 300.0],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk)
        row = result.iloc[0]

        assert row["total_population"] == 3000
        # Median comes from tract 1 only (tract 2: pop=0 AND NA median)
        assert row["median_household_income"] == pytest.approx(55000)
        assert row["median_gross_rent"] == pytest.approx(1300)

        # coverage_ratio: both tracts have total_population (even 0 counts as present)
        assert row["coverage_ratio"] == 1.0
        # coverage for median: tract 1 has data (700), tract 2 has NA (300)
        # -> 700/1000 = 0.7
        assert row["coverage_median_household_income"] == pytest.approx(0.7)
        assert row["coverage_median_gross_rent"] == pytest.approx(0.7)

    def test_single_tract_coc_with_population_weighting(self):
        """Single-tract CoC with population weighting: same result as area
        weighting since there is only one tract to weight.
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["06037000100"],
                "total_population": [5000],
                "adult_population": [4000],
                "population_below_poverty": [500],
                "median_household_income": [70000],
                "median_gross_rent": [1600],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["06037000100"],
                "coc_id": ["CA-600"],
                "area_share": [0.75],
                "pop_share": [0.75],
                "intersection_area": [750.0],
            }
        )

        result_area = aggregate_to_coc(acs_data, crosswalk, weighting="area")
        result_pop = aggregate_to_coc(acs_data, crosswalk, weighting="population")

        # Count variables should be identical (both use area_share)
        assert result_area.iloc[0]["total_population"] == result_pop.iloc[0]["total_population"]
        assert result_area.iloc[0]["total_population"] == pytest.approx(5000 * 0.75)

        # Median variables should be identical for a single-tract CoC
        assert result_area.iloc[0]["median_household_income"] == pytest.approx(70000)
        assert result_pop.iloc[0]["median_household_income"] == pytest.approx(70000)

    def test_single_tract_coc_zero_population(self):
        """Single-tract CoC with zero population: counts are 0, medians are
        pd.NA (no population to weight against).
        """
        acs_data = pd.DataFrame(
            {
                "GEOID": ["06037000100"],
                "total_population": [0],
                "adult_population": [0],
                "population_below_poverty": [0],
                "median_household_income": [50000],
                "median_gross_rent": [1000],
            }
        )

        crosswalk = pd.DataFrame(
            {
                "tract_geoid": ["06037000100"],
                "coc_id": ["CA-600"],
                "area_share": [1.0],
                "pop_share": [1.0],
                "intersection_area": [1000.0],
            }
        )

        result = aggregate_to_coc(acs_data, crosswalk)
        row = result.iloc[0]

        assert row["total_population"] == 0
        assert row["adult_population"] == 0

        # pop_weight = 0 * 1.0 = 0 -> median excluded -> pd.NA
        assert pd.isna(row["median_household_income"])
        assert pd.isna(row["median_gross_rent"])
