"""Tests for ACS 1-year metro dual-product panel assembly (coclab-e88y, legacy build path).

.. deprecated::
    These tests exercise ACS1 integration through the legacy
    ``build_panel`` path.  Recipe-native ACS1 coverage lives in
    ``test_recipe_panel_policies.py::TestAcs1PanelPolicy``.

Tests cover:
- Default behavior (include_acs1=False) leaves ACS1 columns as NaN
- ACS1 artifact discovery and merge when include_acs1=True
- Partial ACS1 coverage (some years but not all)
- acs5_vintage_used column values
- acs1_vintage_used column values
- include_acs1=False skips discovery even when artifacts exist
"""

from __future__ import annotations

import pandas as pd
import pytest

pytestmark = pytest.mark.legacy_build_path

from hhplab.panel.assemble import (
    METRO_PANEL_COLUMNS,
    build_panel,
)
from hhplab.panel.policies import AlignmentPolicy

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def metro_base_dir(tmp_path):
    """Create a temporary metro data tree with PIT and ACS5 measures.

    Layout:
        pit/  - metro PIT files for years 2020 and 2021
        measures/ - ACS 5-year metro measures for vintages 2019 and 2020
        acs/ - ACS 1-year metro artifacts for vintage 2019 (only)
    """
    pit_dir = tmp_path / "pit"
    measures_dir = tmp_path / "measures"
    acs_dir = tmp_path / "acs"
    for d in [pit_dir, measures_dir, acs_dir]:
        d.mkdir()

    # Metro PIT data for year 2020
    pd.DataFrame({
        "metro_id": ["GF01", "GF02", "GF03"],
        "year": [2020, 2020, 2020],
        "pit_total": [1000, 2000, 500],
        "pit_sheltered": [700, 1500, 300],
        "pit_unsheltered": [300, 500, 200],
    }).to_parquet(
        pit_dir / "pit__metro__P2020@Dglynnfoxv1.parquet",
        index=False,
    )

    # Metro PIT data for year 2021
    pd.DataFrame({
        "metro_id": ["GF01", "GF02", "GF03"],
        "year": [2021, 2021, 2021],
        "pit_total": [1100, 2100, 550],
        "pit_sheltered": [770, 1575, 330],
        "pit_unsheltered": [330, 525, 220],
    }).to_parquet(
        pit_dir / "pit__metro__P2021@Dglynnfoxv1.parquet",
        index=False,
    )

    # ACS 5-year measures for vintage 2019 (used by PIT year 2020)
    pd.DataFrame({
        "metro_id": ["GF01", "GF02", "GF03"],
        "total_population": [100000, 200000, 50000],
        "adult_population": [80000, 150000, 40000],
        "population_below_poverty": [10000, 25000, 6000],
        "median_household_income": [70000, 80000, 55000],
        "median_gross_rent": [1800, 2200, 1200],
        "coverage_ratio": [1.0, 1.0, 1.0],
        "weighting_method": ["population"] * 3,
    }).to_parquet(
        measures_dir / "measures__metro__A2019@Dglynnfoxv1.parquet",
        index=False,
    )

    # ACS 5-year measures for vintage 2020 (used by PIT year 2021)
    pd.DataFrame({
        "metro_id": ["GF01", "GF02", "GF03"],
        "total_population": [101000, 201000, 50500],
        "adult_population": [80500, 150500, 40200],
        "population_below_poverty": [10100, 25200, 6050],
        "median_household_income": [71000, 81000, 55500],
        "median_gross_rent": [1850, 2250, 1230],
        "coverage_ratio": [1.0, 1.0, 1.0],
        "weighting_method": ["population"] * 3,
    }).to_parquet(
        measures_dir / "measures__metro__A2020@Dglynnfoxv1.parquet",
        index=False,
    )

    # ACS 1-year metro artifact for vintage 2019 ONLY (not 2020)
    pd.DataFrame({
        "metro_id": ["GF01", "GF02", "GF03"],
        "rent_burden_40_plus": [0.22, 0.18, 0.31],
        "rent_burden_50_plus": [0.12, 0.08, 0.19],
        "unemployment_rate_acs1": [0.05, 0.03, 0.08],
        "acs1_vintage": ["2019", "2019", "2019"],
        "metro_name": ["Metro A", "Metro B", "Metro C"],
        "definition_version": ["glynn_fox_v1"] * 3,
        "cbsa_code": ["12345", "23456", "34567"],
        "pop_16_plus": [75000, 140000, 37000],
        "civilian_labor_force": [70000, 130000, 34000],
        "unemployed_count": [3500, 3900, 2720],
    }).to_parquet(
        acs_dir / "acs1_metro__A2019@Dglynnfoxv1.parquet",
        index=False,
    )

    return {
        "pit_dir": pit_dir,
        "measures_dir": measures_dir,
        "acs_dir": acs_dir,
    }


@pytest.fixture
def policy_fixed():
    """Policy that uses fixed ACS vintage = year - 1."""
    return AlignmentPolicy(
        boundary_vintage_func=lambda year: str(year),
        acs_vintage_func=lambda year: str(year - 1),
        weighting_method="population",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestMetroPanelWithoutAcs1:
    """Default behavior (include_acs1=False) leaves ACS1 columns as NaN."""

    def test_metro_panel_without_acs1(self, metro_base_dir, policy_fixed):
        """When include_acs1 is False, ACS1 columns should be NaN."""
        result = build_panel(
            start_year=2020,
            end_year=2020,
            policy=policy_fixed,
            pit_dir=metro_base_dir["pit_dir"],
            measures_dir=metro_base_dir["measures_dir"],
            geo_type="metro",
            definition_version="glynn_fox_v1",
            include_acs1=False,
        )

        assert "unemployment_rate_acs1" in result.columns
        assert "rent_burden_40_plus" in result.columns
        assert "rent_burden_50_plus" in result.columns
        assert result["unemployment_rate_acs1"].isna().all()
        assert result["rent_burden_40_plus"].isna().all()
        assert result["rent_burden_50_plus"].isna().all()
        assert "acs5_vintage_used" in result.columns
        assert (result["acs5_vintage_used"] == "2019").all()
        assert "acs1_vintage_used" in result.columns
        assert result["acs1_vintage_used"].isna().all()


class TestMetroPanelWithAcs1:
    """When ACS1 artifact exists and include_acs1=True, data is populated."""

    def test_metro_panel_with_acs1(self, metro_base_dir, policy_fixed):
        """unemployment_rate_acs1 should be populated when ACS1 artifact exists."""
        result = build_panel(
            start_year=2020,
            end_year=2020,
            policy=policy_fixed,
            pit_dir=metro_base_dir["pit_dir"],
            measures_dir=metro_base_dir["measures_dir"],
            geo_type="metro",
            definition_version="glynn_fox_v1",
            include_acs1=True,
            acs1_dir=metro_base_dir["acs_dir"],
        )

        assert "rent_burden_40_plus" in result.columns
        assert "rent_burden_50_plus" in result.columns
        assert "unemployment_rate_acs1" in result.columns
        assert result["rent_burden_40_plus"].notna().all()
        assert result["rent_burden_50_plus"].notna().all()
        assert result["unemployment_rate_acs1"].notna().all()
        # Check specific values
        gf01 = result[result["metro_id"] == "GF01"]
        assert float(gf01["rent_burden_40_plus"].iloc[0]) == pytest.approx(0.22)
        assert float(gf01["rent_burden_50_plus"].iloc[0]) == pytest.approx(0.12)
        assert float(gf01["unemployment_rate_acs1"].iloc[0]) == pytest.approx(0.05)
        gf02 = result[result["metro_id"] == "GF02"]
        assert float(gf02["unemployment_rate_acs1"].iloc[0]) == pytest.approx(0.03)

    def test_metro_panel_column_order(self, metro_base_dir, policy_fixed):
        """Metro panel columns should match METRO_PANEL_COLUMNS."""
        result = build_panel(
            start_year=2020,
            end_year=2020,
            policy=policy_fixed,
            pit_dir=metro_base_dir["pit_dir"],
            measures_dir=metro_base_dir["measures_dir"],
            geo_type="metro",
            definition_version="glynn_fox_v1",
            include_acs1=True,
            acs1_dir=metro_base_dir["acs_dir"],
        )
        assert set(result.columns) == set(METRO_PANEL_COLUMNS)


class TestMetroPanelPartialAcs1:
    """ACS1 exists for some years but not all; partial coverage works."""

    def test_metro_panel_partial_acs1(self, metro_base_dir, policy_fixed):
        """ACS1 exists for 2019 (PIT 2020) but not 2020 (PIT 2021)."""
        result = build_panel(
            start_year=2020,
            end_year=2021,
            policy=policy_fixed,
            pit_dir=metro_base_dir["pit_dir"],
            measures_dir=metro_base_dir["measures_dir"],
            geo_type="metro",
            definition_version="glynn_fox_v1",
            include_acs1=True,
            acs1_dir=metro_base_dir["acs_dir"],
        )

        # Year 2020 (ACS vintage 2019) should have ACS1 data
        year_2020 = result[result["year"] == 2020]
        assert year_2020["rent_burden_40_plus"].notna().all()
        assert year_2020["rent_burden_50_plus"].notna().all()
        assert year_2020["unemployment_rate_acs1"].notna().all()
        assert (year_2020["acs5_vintage_used"] == "2019").all()

        # Year 2021 (ACS vintage 2020) should NOT have ACS1 data
        year_2021 = result[result["year"] == 2021]
        assert year_2021["rent_burden_40_plus"].isna().all()
        assert year_2021["rent_burden_50_plus"].isna().all()
        assert year_2021["unemployment_rate_acs1"].isna().all()
        assert (year_2021["acs5_vintage_used"] == "2020").all()


class TestAcs5VintageUsedColumn:
    """Verify acs5_vintage_used column values."""

    def test_acs5_vintage_used_with_acs1(self, metro_base_dir, policy_fixed):
        """acs5_vintage_used reflects the ACS5 vintage when ACS1 is merged."""
        result = build_panel(
            start_year=2020,
            end_year=2020,
            policy=policy_fixed,
            pit_dir=metro_base_dir["pit_dir"],
            measures_dir=metro_base_dir["measures_dir"],
            geo_type="metro",
            definition_version="glynn_fox_v1",
            include_acs1=True,
            acs1_dir=metro_base_dir["acs_dir"],
        )
        assert (result["acs5_vintage_used"] == "2019").all()

    def test_acs5_vintage_used_without_acs1(self, metro_base_dir, policy_fixed):
        """acs5_vintage_used is present even when ACS1 is not requested."""
        result = build_panel(
            start_year=2020,
            end_year=2020,
            policy=policy_fixed,
            pit_dir=metro_base_dir["pit_dir"],
            measures_dir=metro_base_dir["measures_dir"],
            geo_type="metro",
            definition_version="glynn_fox_v1",
            include_acs1=False,
        )
        assert (result["acs5_vintage_used"] == "2019").all()

    def test_acs5_vintage_used_missing_acs1_artifact(self, metro_base_dir, policy_fixed):
        """acs5_vintage_used is unaffected when ACS1 artifact is missing."""
        result = build_panel(
            start_year=2021,
            end_year=2021,
            policy=policy_fixed,
            pit_dir=metro_base_dir["pit_dir"],
            measures_dir=metro_base_dir["measures_dir"],
            geo_type="metro",
            definition_version="glynn_fox_v1",
            include_acs1=True,
            acs1_dir=metro_base_dir["acs_dir"],
        )
        assert (result["acs5_vintage_used"] == "2020").all()


class TestAcs1VintageUsedColumn:
    """Verify acs1_vintage_used column values."""

    def test_acs1_vintage_used_populated(self, metro_base_dir, policy_fixed):
        """acs1_vintage_used should be the ACS vintage when data exists."""
        result = build_panel(
            start_year=2020,
            end_year=2020,
            policy=policy_fixed,
            pit_dir=metro_base_dir["pit_dir"],
            measures_dir=metro_base_dir["measures_dir"],
            geo_type="metro",
            definition_version="glynn_fox_v1",
            include_acs1=True,
            acs1_dir=metro_base_dir["acs_dir"],
        )
        assert (result["acs1_vintage_used"] == "2019").all()

    def test_acs1_vintage_used_null_when_missing(self, metro_base_dir, policy_fixed):
        """acs1_vintage_used should be null when no ACS1 artifact exists."""
        result = build_panel(
            start_year=2021,
            end_year=2021,
            policy=policy_fixed,
            pit_dir=metro_base_dir["pit_dir"],
            measures_dir=metro_base_dir["measures_dir"],
            geo_type="metro",
            definition_version="glynn_fox_v1",
            include_acs1=True,
            acs1_dir=metro_base_dir["acs_dir"],
        )
        assert result["acs1_vintage_used"].isna().all()

    def test_acs1_vintage_used_null_when_not_requested(
        self, metro_base_dir, policy_fixed
    ):
        """acs1_vintage_used should be null when include_acs1=False."""
        result = build_panel(
            start_year=2020,
            end_year=2020,
            policy=policy_fixed,
            pit_dir=metro_base_dir["pit_dir"],
            measures_dir=metro_base_dir["measures_dir"],
            geo_type="metro",
            definition_version="glynn_fox_v1",
            include_acs1=False,
        )
        assert result["acs1_vintage_used"].isna().all()


class TestIncludeAcs1FalseSkipsDiscovery:
    """Even with artifacts present, include_acs1=False skips them."""

    def test_include_acs1_false_skips_discovery(
        self, metro_base_dir, policy_fixed
    ):
        """ACS1 artifacts exist but include_acs1=False: columns are NaN."""
        result = build_panel(
            start_year=2020,
            end_year=2020,
            policy=policy_fixed,
            pit_dir=metro_base_dir["pit_dir"],
            measures_dir=metro_base_dir["measures_dir"],
            geo_type="metro",
            definition_version="glynn_fox_v1",
            include_acs1=False,
            acs1_dir=metro_base_dir["acs_dir"],  # Dir provided but flag is False
        )
        assert result["unemployment_rate_acs1"].isna().all()
        assert (result["acs5_vintage_used"] == "2019").all()
        assert result["acs1_vintage_used"].isna().all()
