"""Recipe-native panel policy and parity tests.

Covers:
- coclab-gude.2: ZORI eligibility and provenance in recipe execution
- coclab-gude.3: ACS1 provenance columns in recipe execution
- coclab-gude.5: Recipe-native parity for CoC and metro panel contracts

This is the authoritative test surface for panel semantics through
the recipe executor.  Legacy ``build_panel``/``save_panel`` tests are
in files marked with ``pytest.mark.legacy_build_path``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from coclab.recipe.executor import execute_recipe
from coclab.recipe.loader import load_recipe

# ---------------------------------------------------------------------------
# ZORI recipe and fixture helpers
# ---------------------------------------------------------------------------

def _zori_recipe_dict() -> dict:
    """Recipe with PIT + ZORI datasets and a panel_policy.zori declaration."""
    return {
        "version": 1,
        "name": "zori-policy-test",
        "universe": {"years": [2020, 2021]},
        "targets": [
            {
                "id": "coc_panel",
                "geometry": {"type": "coc", "vintage": 2025, "source": "hud_exchange"},
                "outputs": ["panel"],
                "panel_policy": {
                    "zori": {"min_coverage": 0.80},
                },
            },
        ],
        "datasets": {
            "pit": {
                "provider": "hud",
                "product": "pit",
                "version": 1,
                "native_geometry": {"type": "coc"},
                "years": {"years": [2020, 2021]},
                "path": "data/pit.parquet",
            },
            "zori": {
                "provider": "zillow",
                "product": "zori",
                "version": 1,
                "native_geometry": {"type": "coc"},
                "years": {"years": [2020, 2021]},
                "path": "data/zori.parquet",
            },
        },
        "transforms": [],
        "pipelines": [
            {
                "id": "main",
                "target": "coc_panel",
                "steps": [
                    {
                        "resample": {
                            "dataset": "pit",
                            "to_geometry": {
                                "type": "coc", "vintage": 2025,
                                "source": "hud_exchange",
                            },
                            "method": "identity",
                            "measures": ["pit_total", "median_household_income"],
                        },
                    },
                    {
                        "resample": {
                            "dataset": "zori",
                            "to_geometry": {
                                "type": "coc", "vintage": 2025,
                                "source": "hud_exchange",
                            },
                            "method": "identity",
                            "measures": ["zori_coc", "zori_coverage_ratio"],
                        },
                    },
                    {
                        "join": {
                            "datasets": ["pit", "zori"],
                            "join_on": ["geo_id", "year"],
                        },
                    },
                ],
            },
        ],
    }


def _setup_zori_fixtures(tmp_path: Path) -> None:
    """Create PIT + ZORI dataset files for the ZORI recipe."""
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    # PIT data with median_household_income for rent_to_income calculation
    pd.DataFrame({
        "coc_id": ["COC1", "COC2", "COC1", "COC2"],
        "year": [2020, 2020, 2021, 2021],
        "pit_total": [100, 200, 110, 210],
        "median_household_income": [60000.0, 48000.0, 62000.0, 50000.0],
    }).to_parquet(data_dir / "pit.parquet")

    # ZORI data: COC1 has high coverage (eligible), COC2 has low coverage
    pd.DataFrame({
        "coc_id": ["COC1", "COC2", "COC1", "COC2"],
        "year": [2020, 2020, 2021, 2021],
        "zori_coc": [1500.0, 1200.0, 1550.0, 1250.0],
        "zori_coverage_ratio": [0.95, 0.50, 0.92, 0.60],
    }).to_parquet(data_dir / "zori.parquet")


# ---------------------------------------------------------------------------
# ACS1 recipe and fixture helpers
# ---------------------------------------------------------------------------

def _acs1_policy_recipe_dict() -> dict:
    """Metro recipe with PIT + ACS5 + ACS1 and panel_policy.acs1 declared."""
    return {
        "version": 1,
        "name": "acs1-policy-test",
        "universe": {"years": [2023]},
        "targets": [
            {
                "id": "metro_panel",
                "geometry": {"type": "metro", "source": "glynn_fox_v1"},
                "outputs": ["panel"],
                "panel_policy": {
                    "acs1": {"include": True},
                },
            },
        ],
        "datasets": {
            "pit": {
                "provider": "hud",
                "product": "pit",
                "version": 1,
                "native_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                "years": {"years": [2023]},
                "path": "data/metro_pit.parquet",
            },
            "acs5": {
                "provider": "census",
                "product": "acs5",
                "version": 1,
                "native_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                "years": {"years": [2023]},
                "path": "data/metro_acs5.parquet",
            },
            "acs1": {
                "provider": "census",
                "product": "acs1",
                "version": 1,
                "native_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                "years": {"years": [2023]},
                "year_column": "acs1_vintage",
                "geo_column": "metro_id",
                "path": "data/metro_acs1.parquet",
            },
        },
        "transforms": [],
        "pipelines": [
            {
                "id": "main",
                "target": "metro_panel",
                "steps": [
                    {
                        "resample": {
                            "dataset": "pit",
                            "to_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                            "method": "identity",
                            "measures": ["pit_total"],
                        },
                    },
                    {
                        "resample": {
                            "dataset": "acs5",
                            "to_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                            "method": "identity",
                            "measures": ["total_population", "median_household_income"],
                        },
                    },
                    {
                        "resample": {
                            "dataset": "acs1",
                            "to_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                            "method": "identity",
                            "measures": {"unemployment_rate_acs1": {"aggregation": "mean"}},
                        },
                    },
                    {
                        "join": {
                            "datasets": ["pit", "acs5", "acs1"],
                            "join_on": ["geo_id", "year"],
                        },
                    },
                ],
            },
        ],
    }


def _setup_acs1_policy_fixtures(tmp_path: Path) -> None:
    """Create metro PIT + ACS5 + ACS1 dataset files."""
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame({
        "metro_id": ["GF01", "GF02"],
        "year": [2023, 2023],
        "pit_total": [100, 200],
    }).to_parquet(data_dir / "metro_pit.parquet")

    pd.DataFrame({
        "metro_id": ["GF01", "GF02"],
        "year": [2023, 2023],
        "total_population": [5000000, 3000000],
        "median_household_income": [70000.0, 55000.0],
    }).to_parquet(data_dir / "metro_acs5.parquet")

    pd.DataFrame({
        "metro_id": ["GF01", "GF02"],
        "acs1_vintage": [2023, 2023],
        "unemployment_rate_acs1": [0.045, 0.032],
    }).to_parquet(data_dir / "metro_acs1.parquet")


# ===========================================================================
# ZORI panel policy tests (coclab-gude.2)
# ===========================================================================


class TestZoriPanelPolicy:
    """Recipe executor applies ZORI eligibility and provenance when policy is set."""

    def test_zori_eligibility_columns_present(self, tmp_path: Path):
        """Eligibility columns are added when panel_policy.zori is declared."""
        _setup_zori_fixtures(tmp_path)
        recipe = load_recipe(_zori_recipe_dict())
        results = execute_recipe(recipe, project_root=tmp_path)

        assert results[0].success
        panel_path = _find_panel_output(tmp_path)
        panel = pd.read_parquet(panel_path)

        assert "zori_is_eligible" in panel.columns
        assert "zori_excluded_reason" in panel.columns
        assert "rent_to_income" in panel.columns

    def test_zori_eligibility_applied_correctly(self, tmp_path: Path):
        """COC1 (coverage 0.95) eligible, COC2 (coverage 0.50) ineligible at 0.80 threshold."""
        _setup_zori_fixtures(tmp_path)
        recipe = load_recipe(_zori_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel = pd.read_parquet(_find_panel_output(tmp_path))

        coc1 = panel[panel["coc_id"] == "COC1"]
        coc2 = panel[panel["coc_id"] == "COC2"]

        assert coc1["zori_is_eligible"].all(), (
            "COC1 should be eligible (coverage 0.95 > 0.80)"
        )
        assert not coc2["zori_is_eligible"].any(), (
            "COC2 should be ineligible (coverage 0.50 < 0.80)"
        )

    def test_rent_to_income_computed_for_eligible(self, tmp_path: Path):
        """rent_to_income is computed for eligible rows and null for ineligible."""
        _setup_zori_fixtures(tmp_path)
        recipe = load_recipe(_zori_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel = pd.read_parquet(_find_panel_output(tmp_path))

        coc1 = panel[panel["coc_id"] == "COC1"]
        coc2 = panel[panel["coc_id"] == "COC2"]

        assert coc1["rent_to_income"].notna().all(), (
            "Eligible rows should have rent_to_income"
        )
        assert coc2["rent_to_income"].isna().all(), (
            "Ineligible rows should have null rent_to_income"
        )

        # Verify the formula: zori_coc / (median_household_income / 12)
        row = coc1.iloc[0]
        expected = row["zori_coc"] / (row["median_household_income"] / 12.0)
        assert abs(row["rent_to_income"] - expected) < 1e-6

    def test_zori_provenance_columns_added(self, tmp_path: Path):
        """ZORI provenance columns are embedded in the panel."""
        _setup_zori_fixtures(tmp_path)
        recipe = load_recipe(_zori_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel = pd.read_parquet(_find_panel_output(tmp_path))

        assert "rent_metric" in panel.columns
        assert "rent_alignment" in panel.columns
        assert "zori_min_coverage" in panel.columns
        assert (panel["rent_metric"] == "ZORI").all()
        assert (panel["zori_min_coverage"] == 0.80).all()

    def test_zori_provenance_in_parquet_metadata(self, tmp_path: Path):
        """ZORI provenance and summary are embedded in parquet file metadata."""
        _setup_zori_fixtures(tmp_path)
        recipe = load_recipe(_zori_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel_path = _find_panel_output(tmp_path)
        metadata = pq.read_metadata(panel_path)
        schema_metadata = metadata.schema.to_arrow_schema().metadata

        provenance = json.loads(schema_metadata[b"coclab_provenance"])
        assert "zori" in provenance
        assert provenance["zori"]["rent_metric"] == "ZORI"
        assert provenance["zori"]["zori_min_coverage"] == 0.80

        assert "zori_summary" in provenance
        assert provenance["zori_summary"]["zori_integrated"] is True
        assert provenance["zori_summary"]["zori_eligible_count"] > 0

    def test_zori_ineligible_nulled_out(self, tmp_path: Path):
        """zori_coc is null for ineligible rows after eligibility rules."""
        _setup_zori_fixtures(tmp_path)
        recipe = load_recipe(_zori_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel = pd.read_parquet(_find_panel_output(tmp_path))
        ineligible = panel[~panel["zori_is_eligible"]]
        assert ineligible["zori_coc"].isna().all()


# ---------------------------------------------------------------------------
# County-native ZORI aggregate-path helpers (coclab-scwk regression)
# ---------------------------------------------------------------------------

def _zori_county_native_recipe_dict() -> dict:
    """Recipe where ZORI is aggregated from county-native data as measure ``zori``.

    This mirrors the committed example recipes (e.g.
    ``coc-base-pit-acs-zori-2016-2021.yaml``) where county-level ZORI is
    aggregated to the target geography and the measure column is named
    ``zori`` (not ``zori_coc``).  The executor must canonicalize the
    column before applying eligibility/provenance logic.
    """
    return {
        "version": 1,
        "name": "zori-county-aggregate-test",
        "universe": {"years": [2020]},
        "targets": [
            {
                "id": "coc_panel",
                "geometry": {"type": "coc", "vintage": 2025, "source": "hud_exchange"},
                "outputs": ["panel"],
                "panel_policy": {
                    "zori": {"min_coverage": 0.80},
                },
            },
        ],
        "datasets": {
            "pit": {
                "provider": "hud",
                "product": "pit",
                "version": 1,
                "native_geometry": {"type": "coc"},
                "years": {"years": [2020]},
                "path": "data/pit.parquet",
            },
            "zori_county": {
                "provider": "zillow",
                "product": "zori",
                "version": 1,
                "native_geometry": {"type": "coc"},
                "years": {"years": [2020]},
                "path": "data/zori_county.parquet",
            },
        },
        "transforms": [],
        "pipelines": [
            {
                "id": "main",
                "target": "coc_panel",
                "steps": [
                    {
                        "resample": {
                            "dataset": "pit",
                            "to_geometry": {
                                "type": "coc", "vintage": 2025,
                                "source": "hud_exchange",
                            },
                            "method": "identity",
                            "measures": ["pit_total", "median_household_income"],
                        },
                    },
                    {
                        "resample": {
                            "dataset": "zori_county",
                            "to_geometry": {
                                "type": "coc", "vintage": 2025,
                                "source": "hud_exchange",
                            },
                            "method": "identity",
                            "measures": ["zori", "zori_coverage_ratio"],
                        },
                    },
                    {
                        "join": {
                            "datasets": ["pit", "zori_county"],
                            "join_on": ["geo_id", "year"],
                        },
                    },
                ],
            },
        ],
    }


def _setup_zori_county_native_fixtures(tmp_path: Path) -> None:
    """Create PIT + county-native ZORI fixtures with ``zori`` column name."""
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame({
        "coc_id": ["COC1", "COC2"],
        "year": [2020, 2020],
        "pit_total": [100, 200],
        "median_household_income": [60000.0, 48000.0],
    }).to_parquet(data_dir / "pit.parquet")

    # Column is "zori" (county-native name), not "zori_coc".
    pd.DataFrame({
        "coc_id": ["COC1", "COC2"],
        "year": [2020, 2020],
        "zori": [1500.0, 1200.0],
        "zori_coverage_ratio": [0.95, 0.50],
    }).to_parquet(data_dir / "zori_county.parquet")


class TestZoriCountyNativeAggregatePath:
    """ZORI policy works when the aggregated measure column is ``zori``
    (county-native path), not the canonical ``zori_coc``.

    Regression coverage for coclab-scwk.
    """

    def test_canonical_zori_columns_populated(self, tmp_path: Path):
        """zori_coc, eligibility, and provenance columns are populated."""
        _setup_zori_county_native_fixtures(tmp_path)
        recipe = load_recipe(_zori_county_native_recipe_dict())
        results = execute_recipe(recipe, project_root=tmp_path)

        assert results[0].success
        panel = pd.read_parquet(_find_panel_output(tmp_path))

        assert "zori_coc" in panel.columns
        assert "zori_is_eligible" in panel.columns
        assert "rent_to_income" in panel.columns
        assert "rent_metric" in panel.columns

    def test_eligibility_applied_with_county_native_zori(self, tmp_path: Path):
        """COC1 (0.95) eligible, COC2 (0.50) ineligible at 0.80 threshold."""
        _setup_zori_county_native_fixtures(tmp_path)
        recipe = load_recipe(_zori_county_native_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel = pd.read_parquet(_find_panel_output(tmp_path))

        coc1 = panel[panel["coc_id"] == "COC1"]
        coc2 = panel[panel["coc_id"] == "COC2"]

        assert coc1["zori_is_eligible"].all()
        assert not coc2["zori_is_eligible"].any()

    def test_no_stray_zori_column(self, tmp_path: Path):
        """The raw ``zori`` column is renamed to ``zori_coc``; no stray column."""
        _setup_zori_county_native_fixtures(tmp_path)
        recipe = load_recipe(_zori_county_native_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel = pd.read_parquet(_find_panel_output(tmp_path))
        assert "zori" not in panel.columns, (
            "Stray 'zori' column should not survive; it should be "
            "canonicalized to 'zori_coc'"
        )

    def test_rent_to_income_computed(self, tmp_path: Path):
        """rent_to_income is computed for eligible rows."""
        _setup_zori_county_native_fixtures(tmp_path)
        recipe = load_recipe(_zori_county_native_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel = pd.read_parquet(_find_panel_output(tmp_path))

        coc1 = panel[panel["coc_id"] == "COC1"]
        assert coc1["rent_to_income"].notna().all()
        row = coc1.iloc[0]
        expected = row["zori_coc"] / (row["median_household_income"] / 12.0)
        assert abs(row["rent_to_income"] - expected) < 1e-6


# ===========================================================================
# ACS1 panel policy tests (coclab-gude.3)
# ===========================================================================


class TestAcs1PanelPolicy:
    """Recipe executor populates ACS1 provenance when panel_policy.acs1 is set."""

    def test_acs_products_used_set(self, tmp_path: Path):
        """acs_products_used is 'acs5,acs1' when ACS1 data is present."""
        _setup_acs1_policy_fixtures(tmp_path)
        recipe = load_recipe(_acs1_policy_recipe_dict())
        results = execute_recipe(recipe, project_root=tmp_path)

        assert results[0].success
        panel_path = _find_panel_output(tmp_path)
        panel = pd.read_parquet(panel_path)

        assert "acs_products_used" in panel.columns
        assert (panel["acs_products_used"] == "acs5,acs1").all()

    def test_acs1_vintage_used_set(self, tmp_path: Path):
        """acs1_vintage_used matches the resolved ACS1 input vintage."""
        _setup_acs1_policy_fixtures(tmp_path)
        recipe = load_recipe(_acs1_policy_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel = pd.read_parquet(_find_panel_output(tmp_path))

        assert "acs1_vintage_used" in panel.columns
        # ACS1 fixture has acs1_vintage=2023 → acs1_vintage_used="2023"
        assert (panel["acs1_vintage_used"] == "2023").all()

    def test_unemployment_rate_acs1_present(self, tmp_path: Path):
        """unemployment_rate_acs1 is in the output panel."""
        _setup_acs1_policy_fixtures(tmp_path)
        recipe = load_recipe(_acs1_policy_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel = pd.read_parquet(_find_panel_output(tmp_path))

        assert "unemployment_rate_acs1" in panel.columns
        assert panel["unemployment_rate_acs1"].notna().all()

    def test_acs1_conformance_products(self, tmp_path: Path):
        """Conformance report includes acs1 product when policy is set."""
        _setup_acs1_policy_fixtures(tmp_path)
        recipe = load_recipe(_acs1_policy_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel_path = _find_panel_output(tmp_path)
        metadata = pq.read_metadata(panel_path)
        schema_metadata = metadata.schema.to_arrow_schema().metadata
        provenance = json.loads(schema_metadata[b"coclab_provenance"])

        # The conformance report should exist in provenance
        assert "conformance" in provenance

    def test_acs1_partial_data_vintage_nulled(self, tmp_path: Path):
        """When ACS1 data is present for some metros but not others, vintage is null for missing."""
        _setup_acs1_policy_fixtures(tmp_path)
        # Overwrite ACS1 data with only one metro
        data_dir = tmp_path / "data"
        pd.DataFrame({
            "metro_id": ["GF01"],
            "acs1_vintage": [2023],
            "unemployment_rate_acs1": [0.045],
        }).to_parquet(data_dir / "metro_acs1.parquet")

        recipe = load_recipe(_acs1_policy_recipe_dict())
        results = execute_recipe(recipe, project_root=tmp_path)

        assert results[0].success
        panel = pd.read_parquet(_find_panel_output(tmp_path))

        gf01 = panel[panel["metro_id"] == "GF01"]
        gf02 = panel[panel["metro_id"] == "GF02"]

        # GF01 has ACS1 data, GF02 does not
        assert gf01["acs1_vintage_used"].notna().all()
        assert gf02["acs1_vintage_used"].isna().all()
        assert (panel["acs_products_used"] == "acs5,acs1").all()

    def test_acs1_vintage_matches_input_not_lag(self, tmp_path: Path):
        """acs1_vintage_used reflects the resolved ACS1 input vintage,
        not a hard-coded ``year - 1`` lag heuristic.

        Regression coverage for coclab-fib8: the recipe explicitly loads
        ACS1 vintage 2023 for universe year 2023, so acs1_vintage_used
        must be "2023", not "2022".
        """
        _setup_acs1_policy_fixtures(tmp_path)
        recipe = load_recipe(_acs1_policy_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel = pd.read_parquet(_find_panel_output(tmp_path))

        # The fixture has acs1_vintage=2023, universe year=2023.
        # The old heuristic would produce "2022"; the fix derives "2023".
        assert (panel["acs1_vintage_used"] == "2023").all(), (
            "acs1_vintage_used should match the resolved ACS1 input "
            "vintage (2023), not the year-1 lag heuristic (2022)"
        )


# ===========================================================================
# Recipe-native panel contract parity tests (coclab-gude.5)
# ===========================================================================


def _coc_recipe_dict() -> dict:
    """Minimal CoC recipe with PIT + ACS for parity testing."""
    return {
        "version": 1,
        "name": "coc-parity-test",
        "universe": {"years": [2020, 2021]},
        "targets": [
            {
                "id": "coc_panel",
                "geometry": {
                    "type": "coc", "vintage": 2025,
                    "source": "hud_exchange",
                },
                "outputs": ["panel"],
            },
        ],
        "datasets": {
            "pit": {
                "provider": "hud",
                "product": "pit",
                "version": 1,
                "native_geometry": {"type": "coc"},
                "years": {"years": [2020, 2021]},
                "path": "data/pit.parquet",
            },
            "acs": {
                "provider": "census",
                "product": "acs5",
                "version": 1,
                "native_geometry": {"type": "coc"},
                "years": {"years": [2020, 2021]},
                "path": "data/acs.parquet",
            },
        },
        "transforms": [],
        "pipelines": [
            {
                "id": "main",
                "target": "coc_panel",
                "steps": [
                    {
                        "resample": {
                            "dataset": "pit",
                            "to_geometry": {
                                "type": "coc", "vintage": 2025,
                                "source": "hud_exchange",
                            },
                            "method": "identity",
                            "measures": ["pit_total"],
                        },
                    },
                    {
                        "resample": {
                            "dataset": "acs",
                            "to_geometry": {
                                "type": "coc", "vintage": 2025,
                                "source": "hud_exchange",
                            },
                            "method": "identity",
                            "measures": [
                                "total_population",
                                "median_household_income",
                            ],
                        },
                    },
                    {
                        "join": {
                            "datasets": ["pit", "acs"],
                            "join_on": ["geo_id", "year"],
                        },
                    },
                ],
            },
        ],
    }


def _setup_coc_parity_fixtures(tmp_path: Path) -> None:
    """Create PIT + ACS dataset files for CoC parity testing."""
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame({
        "coc_id": ["COC1", "COC2", "COC1", "COC2"],
        "year": [2020, 2020, 2021, 2021],
        "pit_total": [100, 200, 110, 210],
    }).to_parquet(data_dir / "pit.parquet")

    pd.DataFrame({
        "coc_id": ["COC1", "COC2", "COC1", "COC2"],
        "year": [2020, 2020, 2021, 2021],
        "total_population": [50000, 80000, 51000, 82000],
        "median_household_income": [60000.0, 48000.0, 62000.0, 50000.0],
    }).to_parquet(data_dir / "acs.parquet")


class TestCocPanelParity:
    """Recipe-native CoC panel matches legacy build_panel output contract."""

    def test_coc_panel_columns_present(self, tmp_path: Path):
        """Canonical CoC columns are present in recipe-built panel."""
        from coclab.panel.finalize import COC_PANEL_COLUMNS

        _setup_coc_parity_fixtures(tmp_path)
        recipe = load_recipe(_coc_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel = pd.read_parquet(_find_panel_output(tmp_path))
        for col in COC_PANEL_COLUMNS:
            assert col in panel.columns, f"Missing canonical column: {col}"

    def test_coc_panel_shape(self, tmp_path: Path):
        """2 CoCs x 2 years = 4 rows."""
        _setup_coc_parity_fixtures(tmp_path)
        recipe = load_recipe(_coc_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel = pd.read_parquet(_find_panel_output(tmp_path))
        assert len(panel) == 4
        assert panel["year"].nunique() == 2

    def test_boundary_changed_derived(self, tmp_path: Path):
        """boundary_changed column is present and boolean."""
        _setup_coc_parity_fixtures(tmp_path)
        recipe = load_recipe(_coc_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel = pd.read_parquet(_find_panel_output(tmp_path))
        assert "boundary_changed" in panel.columns
        assert panel["boundary_changed"].dtype == bool

    def test_source_label_set(self, tmp_path: Path):
        """source column has default label."""
        _setup_coc_parity_fixtures(tmp_path)
        recipe = load_recipe(_coc_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel = pd.read_parquet(_find_panel_output(tmp_path))
        assert "source" in panel.columns
        assert panel["source"].notna().all()

    def test_provenance_in_parquet(self, tmp_path: Path):
        """coclab_provenance metadata exists in the output parquet."""
        _setup_coc_parity_fixtures(tmp_path)
        recipe = load_recipe(_coc_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel_path = _find_panel_output(tmp_path)
        metadata = pq.read_metadata(panel_path)
        schema_metadata = metadata.schema.to_arrow_schema().metadata
        assert b"coclab_provenance" in schema_metadata

        provenance = json.loads(schema_metadata[b"coclab_provenance"])
        assert "conformance" in provenance
        assert "target_geometry" in provenance


class TestMetroPanelParity:
    """Recipe-native metro panel matches legacy build_panel output contract."""

    def test_metro_panel_columns_present(self, tmp_path: Path):
        """Canonical metro columns are present in recipe-built panel."""
        from coclab.panel.finalize import METRO_PANEL_COLUMNS

        _setup_acs1_policy_fixtures(tmp_path)
        recipe = load_recipe(_acs1_policy_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel = pd.read_parquet(_find_panel_output(tmp_path))
        for col in METRO_PANEL_COLUMNS:
            assert col in panel.columns, (
                f"Missing canonical metro column: {col}"
            )

    def test_metro_geo_metadata(self, tmp_path: Path):
        """Metro panel has geo_type, metro_id, metro_name columns."""
        _setup_acs1_policy_fixtures(tmp_path)
        recipe = load_recipe(_acs1_policy_recipe_dict())
        execute_recipe(recipe, project_root=tmp_path)

        panel = pd.read_parquet(_find_panel_output(tmp_path))
        assert (panel["geo_type"] == "metro").all()
        assert panel["metro_id"].notna().all()


# ===========================================================================
# Helpers
# ===========================================================================


def _find_panel_output(tmp_path: Path) -> Path:
    """Find the panel parquet file in the curated output directory."""
    panel_dir = tmp_path / "data" / "curated" / "panel"
    if not panel_dir.exists():
        # Fall back to searching recursively
        matches = list(tmp_path.rglob("panel__*.parquet"))
        assert matches, f"No panel output found under {tmp_path}"
        return matches[0]
    matches = list(panel_dir.glob("panel__*.parquet"))
    assert matches, f"No panel output found in {panel_dir}"
    return matches[0]
