"""Tests for Glynn-Fox audit panel preparation."""

import json
from pathlib import Path

import pandas as pd

from coclab.audit_panels import (
    AUDIT_PANEL_SPECS,
    AuditPanelSpec,
    METRO_DEFINITION_VERSION,
    MODELING_READY_COLUMNS,
    RAW_REQUIRED_COLUMNS,
    _derive_modeling_ready,
    _prepare_raw_panel,
    _validate_modeling_ready,
    _validate_raw_panel,
    build_audit_panels,
)
from coclab.naming import (
    metro_coc_membership_filename,
    metro_county_membership_filename,
    metro_definitions_filename,
)


def _raw_fixture() -> pd.DataFrame:
    return pd.DataFrame({
        "geo_id": ["A", "A", "B", "B"],
        "year": [2020, 2021, 2020, 2021],
        "pit_total": [10, 12, 8, 9],
        "pit_sheltered": [6, 7, 5, 5],
        "pit_unsheltered": [4, 5, 3, 4],
        "total_population": [1000, 1010, 800, 810],
        "median_household_income": [50000.0, 51000.0, 45000.0, 45500.0],
        "zori": [1000.0, 1050.0, 900.0, 918.0],
    })


def test_prepare_raw_panel_drops_invalid_units():
    source = pd.concat(
        [
            _raw_fixture(),
            pd.DataFrame(
                {
                    "geo_id": ["C", "C"],
                    "year": [2020, 2021],
                    "pit_total": [5, 5],
                    "pit_sheltered": [3, 3],
                    "pit_unsheltered": [2, 2],
                    "total_population": [4, 4],
                    "median_household_income": [40000.0, 40500.0],
                    "zori": [800.0, 805.0],
                }
            ),
        ],
        ignore_index=True,
    )

    raw_df, drop_reasons, _ = _prepare_raw_panel(source, spec=AUDIT_PANEL_SPECS[2])

    assert set(raw_df["geo_id"]) == {"A", "B"}
    assert drop_reasons["C"] == ["pit_exceeds_population"]


def test_derive_modeling_ready_has_required_columns_and_zero_first_year():
    modeling_df = _derive_modeling_ready(_raw_fixture())

    assert MODELING_READY_COLUMNS == modeling_df.columns.tolist()
    assert (modeling_df.groupby("geo_id")["d_zori"].first() == 0.0).all()


def test_derive_modeling_ready_zero_zori_yields_finite_d_zori():
    """Regression test for coclab-i2fj.8.8: zero prior-year ZORI must not
    produce inf in d_zori."""
    import numpy as np

    df = pd.DataFrame({
        "geo_id": ["X", "X", "X"],
        "year": [2019, 2020, 2021],
        "pit_total": [5, 6, 7],
        "pit_sheltered": [3, 4, 4],
        "pit_unsheltered": [2, 2, 3],
        "total_population": [500, 510, 520],
        "median_household_income": [40000.0, 41000.0, 42000.0],
        "zori": [0.0, 1200.0, 1250.0],
    })
    modeling_df = _derive_modeling_ready(df)
    assert np.isfinite(modeling_df["d_zori"]).all()


def test_validation_detects_globally_missing_years():
    """Regression test for coclab-i2fj.8.6: a panel with years 2015 and 2017
    (gap at 2016) must NOT pass as structurally valid."""
    df = pd.DataFrame({
        "geo_id": ["A", "A", "B", "B"],
        "year": [2015, 2017, 2015, 2017],
        "pit_total": [10, 12, 8, 9],
        "pit_sheltered": [6, 7, 5, 5],
        "pit_unsheltered": [4, 5, 3, 4],
        "total_population": [1000, 1010, 800, 810],
        "median_household_income": [50000.0, 51000.0, 45000.0, 45500.0],
        "zori": [1000.0, 1050.0, 900.0, 918.0],
    })
    validation = _validate_raw_panel(
        df,
        spec=AUDIT_PANEL_SPECS[0],
        drop_reasons={},
    )
    assert not validation["structurally_valid"]
    year_issue = [i for i in validation["issues"] if i["check"] == "year_contiguity"]
    assert year_issue
    assert 2016 in year_issue[0]["missing_years"]


def test_validation_detects_missing_leading_trailing_years():
    """Regression test for coclab-z0xw: a panel covering 2016-2024 must fail
    validation against a spec requiring 2015-2024 (missing leading year 2015)."""
    years = list(range(2016, 2025))  # 2016-2024, missing 2015
    df = pd.DataFrame({
        "geo_id": ["A"] * len(years) + ["B"] * len(years),
        "year": years * 2,
        "pit_total": [10] * len(years) * 2,
        "pit_sheltered": [6] * len(years) * 2,
        "pit_unsheltered": [4] * len(years) * 2,
        "total_population": [1000] * len(years) * 2,
        "median_household_income": [50000.0] * len(years) * 2,
        "zori": [1000.0] * len(years) * 2,
    })
    validation = _validate_raw_panel(
        df,
        spec=AUDIT_PANEL_SPECS[0],  # requires 2015-2024
        drop_reasons={},
    )
    assert not validation["structurally_valid"]
    year_issue = [i for i in validation["issues"] if i["check"] == "year_contiguity"]
    assert year_issue
    assert 2015 in year_issue[0]["missing_years"]


def test_validation_reports_balanced_panel():
    raw_df = _raw_fixture()
    # Use a spec whose year window matches the fixture (2020-2021).
    spec = AuditPanelSpec(
        panel_name="test_panel",
        workload_id="T",
        unit_type="coc",
        source_panel_path="",
        selection_rule="",
        missing_policy="drop",
        rent_proxy="zori_january",
        notes="",
        start_year=2020,
        end_year=2021,
    )
    raw_validation = _validate_raw_panel(
        raw_df,
        spec=spec,
        drop_reasons={},
    )
    modeling_df = _derive_modeling_ready(raw_df)
    modeling_validation = _validate_modeling_ready(
        modeling_df,
        raw_validation=raw_validation,
    )

    assert raw_validation["structurally_valid"]
    assert modeling_validation["structurally_valid"]


def test_build_audit_panels_writes_expected_artifacts(tmp_path: Path):
    broad = pd.DataFrame({
        "geo_id": ["GF01", "GF01", "GF02", "GF02"],
        "year": [2015, 2016, 2015, 2016],
        "pit_total": [10, 11, 20, 21],
        "pit_sheltered": [6, 7, 12, 13],
        "pit_unsheltered": [4, 4, 8, 8],
        "total_population": [1000, 1010, 2000, 2010],
        "median_household_income": [60000.0, 60500.0, 70000.0, 70500.0],
        "zori": [1200.0, 1260.0, 1400.0, 1456.0],
    })
    coc = pd.DataFrame({
        "geo_id": ["C1", "C1", "C2", "C2"],
        "year": [2015, 2016, 2015, 2016],
        "pit_total": [5, 6, 7, 8],
        "pit_sheltered": [3, 4, 4, 5],
        "pit_unsheltered": [2, 2, 3, 3],
        "total_population": [500, 505, 700, 710],
        "median_household_income": [40000.0, 40500.0, 45000.0, 45500.0],
        "zori": [900.0, 945.0, 950.0, 997.5],
    })

    broad_path = tmp_path / "data" / "curated" / "panel" / "panel__metro__Y2015-2024@Dglynnfoxv1.parquet"
    coc_path = tmp_path / "data" / "curated" / "panel" / "panel__Y2015-2024@B2025.parquet"
    broad_path.parent.mkdir(parents=True, exist_ok=True)
    broad.to_parquet(broad_path)
    coc.to_parquet(coc_path)

    manifests = build_audit_panels(tmp_path)

    assert len(manifests) == 4
    for manifest in manifests:
        panel_dir = tmp_path / "outputs" / "audit_panels" / manifest["panel_name"]
        assert (panel_dir / "raw_panel.parquet").exists()
        assert (panel_dir / "modeling_input.parquet").exists()
        assert (panel_dir / "validation_report.json").exists()
        assert (panel_dir / "panel_manifest.json").exists()

    raw_df = pd.read_parquet(
        tmp_path
        / "outputs"
        / "audit_panels"
        / "glynn_fox_broad_metro_v1"
        / "raw_panel.parquet"
    )
    assert raw_df.columns.tolist() == RAW_REQUIRED_COLUMNS


def _setup_metro_curated_artifacts(tmp_path: Path) -> None:
    """Write minimal metro definition parquets so _copy_metro_reference_artifacts finds them."""
    metro_dir = tmp_path / "data" / "curated" / "metro"
    metro_dir.mkdir(parents=True, exist_ok=True)
    v = METRO_DEFINITION_VERSION
    pd.DataFrame({"metro_id": ["GF01"]}).to_parquet(
        metro_dir / metro_definitions_filename(v)
    )
    pd.DataFrame({"metro_id": ["GF01"], "coc_id": ["NY-600"]}).to_parquet(
        metro_dir / metro_coc_membership_filename(v)
    )
    pd.DataFrame({"metro_id": ["GF01"], "county_fips": ["36061"]}).to_parquet(
        metro_dir / metro_county_membership_filename(v)
    )


def test_metro_outputs_include_reference_artifacts(tmp_path: Path):
    """Metro audit outputs must bundle metro definition artifacts."""
    _setup_metro_curated_artifacts(tmp_path)

    broad = pd.DataFrame({
        "geo_id": ["GF01", "GF01", "GF02", "GF02"],
        "year": [2015, 2016, 2015, 2016],
        "pit_total": [10, 11, 20, 21],
        "pit_sheltered": [6, 7, 12, 13],
        "pit_unsheltered": [4, 4, 8, 8],
        "total_population": [1000, 1010, 2000, 2010],
        "median_household_income": [60000.0, 60500.0, 70000.0, 70500.0],
        "zori": [1200.0, 1260.0, 1400.0, 1456.0],
    })
    coc = pd.DataFrame({
        "geo_id": ["C1", "C1", "C2", "C2"],
        "year": [2015, 2016, 2015, 2016],
        "pit_total": [5, 6, 7, 8],
        "pit_sheltered": [3, 4, 4, 5],
        "pit_unsheltered": [2, 2, 3, 3],
        "total_population": [500, 505, 700, 710],
        "median_household_income": [40000.0, 40500.0, 45000.0, 45500.0],
        "zori": [900.0, 945.0, 950.0, 997.5],
    })

    broad_path = tmp_path / "data" / "curated" / "panel" / "panel__metro__Y2015-2024@Dglynnfoxv1.parquet"
    coc_path = tmp_path / "data" / "curated" / "panel" / "panel__Y2015-2024@B2025.parquet"
    broad_path.parent.mkdir(parents=True, exist_ok=True)
    broad.to_parquet(broad_path)
    coc.to_parquet(coc_path)

    manifests = build_audit_panels(tmp_path)

    v = METRO_DEFINITION_VERSION
    expected_filenames = [
        metro_definitions_filename(v),
        metro_coc_membership_filename(v),
        metro_county_membership_filename(v),
    ]

    for manifest in manifests:
        panel_dir = tmp_path / "outputs" / "audit_panels" / manifest["panel_name"]
        if manifest["unit_type"] == "metro":
            # Metro outputs must include reference artifacts.
            assert "metro_reference_artifacts" in manifest
            assert sorted(manifest["metro_reference_artifacts"]) == sorted(expected_filenames)
            for fn in expected_filenames:
                assert (panel_dir / fn).exists()
            # Validation report should list them in artifacts_present.
            report = json.loads((panel_dir / "validation_report.json").read_text())
            for fn in expected_filenames:
                assert report["artifacts_present"].get(fn) is True
        else:
            # CoC outputs must NOT include metro reference artifacts.
            assert "metro_reference_artifacts" not in manifest
            for fn in expected_filenames:
                assert not (panel_dir / fn).exists()


# ---------------------------------------------------------------------------
# Fixtures and helpers for _build_metro_source_panel / _ensure_metro_source_panel
# ---------------------------------------------------------------------------


def _make_pit_df() -> pd.DataFrame:
    """Minimal PIT data for two CoCs across 2015-2024."""
    rows = []
    for year in range(2015, 2025):
        for coc_id in ["NY-600", "CA-600"]:
            rows.append({
                "coc_id": coc_id,
                "pit_year": year,
                "pit_total": 100 + year - 2015,
                "pit_sheltered": 60 + year - 2015,
                "pit_unsheltered": 40,
            })
    return pd.DataFrame(rows)


def _make_coc_membership_df() -> pd.DataFrame:
    return pd.DataFrame({
        "metro_id": ["GF01", "GF02"],
        "coc_id": ["NY-600", "CA-600"],
    })


def _make_county_membership_df() -> pd.DataFrame:
    return pd.DataFrame({
        "metro_id": ["GF01", "GF02"],
        "county_fips": ["36061", "06037"],
        "definition_version": ["glynn_fox_v1", "glynn_fox_v1"],
    })


def _make_acs_df(year: int) -> pd.DataFrame:
    """Minimal tract-level ACS data for one tract per metro county.

    The real ACS parquets use ``tract_geoid``; _build_metro_source_panel
    renames this to ``GEOID`` before calling aggregate_acs_to_metro.
    """
    return pd.DataFrame({
        "tract_geoid": ["36061000100", "06037000100"],
        "total_population": [50000, 80000],
        "adult_population": [35000, 55000],
        "population_below_poverty": [5000, 9000],
        "median_household_income": [60000.0 + (year - 2015) * 500, 55000.0 + (year - 2015) * 500],
        "median_gross_rent": [1500.0, 1800.0],
    })


def _make_zori_df() -> pd.DataFrame:
    """Minimal county-level January ZORI for 2015-2024."""
    rows = []
    for year in range(2015, 2025):
        for county, val in [("36061", 1200.0), ("06037", 1600.0)]:
            rows.append({
                "geo_id": county,
                "year": year,
                "month": 1,
                "zori": val + (year - 2015) * 20,
            })
    return pd.DataFrame(rows)


def _make_metro_acs_result(year: int) -> pd.DataFrame:
    """Pre-aggregated metro-level ACS result matching aggregate_acs_to_metro output."""
    return pd.DataFrame({
        "metro_id": ["GF01", "GF02"],
        "total_population": [50000, 80000],
        "adult_population": [35000, 55000],
        "population_below_poverty": [5000, 9000],
        "median_household_income": [60000.0 + (year - 2015) * 500, 55000.0 + (year - 2015) * 500],
        "median_gross_rent": [1500.0, 1800.0],
        "coverage_ratio": [1.0, 1.0],
        "weighting_method": ["population", "population"],
        "source": ["acs5", "acs5"],
        "definition_version": ["glynn_fox_v1", "glynn_fox_v1"],
    })


def _make_metro_zori_result() -> pd.DataFrame:
    """Pre-aggregated metro-level yearly ZORI matching aggregate_yearly_zori_to_metro output."""
    rows = []
    for year in range(2015, 2025):
        for mid, base in [("GF01", 1200.0), ("GF02", 1600.0)]:
            rows.append({"metro_id": mid, "year": year, "zori": base + (year - 2015) * 20})
    return pd.DataFrame(rows)


class TestBuildMetroSourcePanel:
    """Tests for _build_metro_source_panel."""

    def test_produces_valid_panel_with_mocked_inputs(self, tmp_path, monkeypatch):
        """Rebuild path produces a structurally valid source panel."""
        from coclab import audit_panels

        pit_df = _make_pit_df()
        zori_df = _make_zori_df()

        # Write PIT and ZORI parquets that _build_metro_source_panel reads directly.
        pit_path = tmp_path / "data" / "curated" / "pit" / "pit_vintage__P2024.parquet"
        pit_path.parent.mkdir(parents=True, exist_ok=True)
        pit_df.to_parquet(pit_path)

        zori_path = tmp_path / "data" / "curated" / "zori" / "zori__county__Z2026.parquet"
        zori_path.parent.mkdir(parents=True, exist_ok=True)
        zori_df.to_parquet(zori_path)

        # Write ACS parquets for each year.
        for year in range(2015, 2025):
            acs = _make_acs_df(year)
            acs_path = audit_panels._acs_path_for_year(tmp_path, year)
            acs_path.parent.mkdir(parents=True, exist_ok=True)
            acs.to_parquet(acs_path)

        # Mock membership readers to return our fixtures.
        coc_mem = _make_coc_membership_df()
        county_mem = _make_county_membership_df()
        monkeypatch.setattr(
            audit_panels,
            "read_metro_coc_membership",
            lambda base_dir: coc_mem,
        )
        monkeypatch.setattr(
            audit_panels,
            "read_metro_county_membership",
            lambda base_dir: county_mem,
        )

        # Mock aggregate_acs_to_metro so we don't need full crosswalk infrastructure.
        def mock_aggregate_acs(acs_data, *, weighting="population"):
            # Return a plausible metro-level result from the acs_data.
            county_fips_list = acs_data["GEOID"].str[:5].unique()
            rows = []
            for fips in county_fips_list:
                mask = acs_data["GEOID"].str[:5] == fips
                subset = acs_data.loc[mask]
                mid = county_mem.loc[county_mem["county_fips"] == fips, "metro_id"]
                if mid.empty:
                    continue
                rows.append({
                    "metro_id": mid.iloc[0],
                    "total_population": int(subset["total_population"].sum()),
                    "adult_population": int(subset["adult_population"].sum()),
                    "population_below_poverty": int(subset["population_below_poverty"].sum()),
                    "median_household_income": float(subset["median_household_income"].mean()),
                    "median_gross_rent": float(subset["median_gross_rent"].mean()),
                })
            return pd.DataFrame(rows)

        monkeypatch.setattr(audit_panels, "aggregate_acs_to_metro", mock_aggregate_acs)

        # Mock aggregate_yearly_zori_to_metro to return pre-built results.
        metro_zori = _make_metro_zori_result()
        monkeypatch.setattr(
            audit_panels,
            "aggregate_yearly_zori_to_metro",
            lambda zori_yearly, county_pop, county_membership_df=None: metro_zori,
        )

        result = audit_panels._build_metro_source_panel(tmp_path)

        # Structural checks.
        assert isinstance(result, pd.DataFrame)
        assert "geo_id" in result.columns
        assert "year" in result.columns
        assert "pit_total" in result.columns
        assert "total_population" in result.columns
        assert "zori" in result.columns
        assert "median_household_income" in result.columns

        # Should contain both metros across all years.
        assert set(result["geo_id"]) == {"GF01", "GF02"}
        assert set(result["year"]) == set(range(2015, 2025))

        # Panel should be sorted by geo_id then year.
        assert result.equals(result.sort_values(["geo_id", "year"]).reset_index(drop=True))


class TestEnsureMetroSourcePanel:
    """Tests for _ensure_metro_source_panel rebuild behaviour."""

    def test_rebuilds_when_file_missing(self, tmp_path, monkeypatch):
        """When the source panel parquet does not exist, _ensure_metro_source_panel
        builds it from raw artifacts and writes to disk."""
        from coclab import audit_panels

        # The expected output path does NOT exist, triggering the rebuild.
        expected_path = tmp_path / audit_panels.METRO_SOURCE_PATH
        assert not expected_path.exists()

        # Build a small valid panel to return from _build_metro_source_panel.
        panel_df = pd.DataFrame({
            "geo_id": ["GF01", "GF01", "GF02", "GF02"],
            "metro_id": ["GF01", "GF01", "GF02", "GF02"],
            "year": [2015, 2016, 2015, 2016],
            "pit_total": [100, 101, 200, 201],
            "pit_sheltered": [60, 61, 120, 121],
            "pit_unsheltered": [40, 40, 80, 80],
            "total_population": [50000, 50100, 80000, 80100],
            "median_household_income": [60000.0, 60500.0, 55000.0, 55500.0],
            "zori": [1200.0, 1220.0, 1600.0, 1620.0],
        })

        monkeypatch.setattr(
            audit_panels,
            "_build_metro_source_panel",
            lambda project_root: panel_df,
        )

        result_path = audit_panels._ensure_metro_source_panel(tmp_path)

        # File must now exist.
        assert result_path.exists()
        assert result_path == expected_path

        # Written parquet must be readable and contain the expected data.
        written = pd.read_parquet(result_path)
        assert len(written) == len(panel_df)
        assert set(written["geo_id"]) == {"GF01", "GF02"}

    def test_skips_rebuild_when_file_exists(self, tmp_path, monkeypatch):
        """When the source panel parquet already exists and force=False,
        _ensure_metro_source_panel returns immediately without rebuilding."""
        from coclab import audit_panels

        expected_path = tmp_path / audit_panels.METRO_SOURCE_PATH
        expected_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"geo_id": ["GF01"]}).to_parquet(expected_path)

        build_called = {"count": 0}

        def fail_build(project_root):
            build_called["count"] += 1
            raise AssertionError("_build_metro_source_panel should not be called")

        monkeypatch.setattr(audit_panels, "_build_metro_source_panel", fail_build)

        result_path = audit_panels._ensure_metro_source_panel(tmp_path, force=False)

        assert result_path == expected_path
        assert build_called["count"] == 0

    def test_force_triggers_rebuild_even_when_file_exists(self, tmp_path, monkeypatch):
        """When force=True, _ensure_metro_source_panel rebuilds even if file exists."""
        from coclab import audit_panels

        expected_path = tmp_path / audit_panels.METRO_SOURCE_PATH
        expected_path.parent.mkdir(parents=True, exist_ok=True)
        # Write a dummy file so the path exists.
        pd.DataFrame({"old": [1]}).to_parquet(expected_path)

        new_panel = pd.DataFrame({
            "geo_id": ["GF01"],
            "year": [2020],
            "pit_total": [100],
            "pit_sheltered": [60],
            "pit_unsheltered": [40],
            "total_population": [50000],
            "median_household_income": [60000.0],
            "zori": [1200.0],
        })

        monkeypatch.setattr(
            audit_panels,
            "_build_metro_source_panel",
            lambda project_root: new_panel,
        )

        result_path = audit_panels._ensure_metro_source_panel(tmp_path, force=True)

        assert result_path == expected_path
        written = pd.read_parquet(result_path)
        assert "geo_id" in written.columns
        # Old column should be gone.
        assert "old" not in written.columns


class TestLoadSourcePanelRebuild:
    """Tests for _load_source_panel triggering the rebuild path."""

    def test_load_source_panel_triggers_rebuild_for_missing_metro(
        self, tmp_path, monkeypatch
    ):
        """When spec.source_panel_path does not exist and unit_type is metro,
        _load_source_panel invokes _ensure_metro_source_panel."""
        from coclab import audit_panels

        spec = AuditPanelSpec(
            panel_name="test_rebuild",
            workload_id="T",
            unit_type="metro",
            source_panel_path="data/curated/panel/nonexistent.parquet",
            selection_rule="test",
            missing_policy="drop",
            rent_proxy="zori_january",
            notes="test rebuild path",
            start_year=2020,
            end_year=2021,
        )

        rebuilt_panel = pd.DataFrame({
            "geo_id": ["GF01", "GF01"],
            "year": [2020, 2021],
            "pit_total": [100, 101],
            "pit_sheltered": [60, 61],
            "pit_unsheltered": [40, 40],
            "total_population": [50000, 50100],
            "median_household_income": [60000.0, 60500.0],
            "zori": [1200.0, 1220.0],
        })

        # Write the rebuilt panel to the expected metro source path.
        metro_source_path = tmp_path / audit_panels.METRO_SOURCE_PATH
        metro_source_path.parent.mkdir(parents=True, exist_ok=True)
        rebuilt_panel.to_parquet(metro_source_path)

        ensure_called = {"count": 0}
        original_ensure = audit_panels._ensure_metro_source_panel

        def tracking_ensure(project_root, *, force=False):
            ensure_called["count"] += 1
            return metro_source_path

        monkeypatch.setattr(audit_panels, "_ensure_metro_source_panel", tracking_ensure)

        df, rel_path = audit_panels._load_source_panel(tmp_path, spec)

        assert ensure_called["count"] == 1
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_load_source_panel_does_not_rebuild_for_coc(self, tmp_path, monkeypatch):
        """When unit_type is coc and source panel is missing, _load_source_panel
        does NOT attempt metro rebuild (it raises FileNotFoundError)."""
        import pytest
        from coclab import audit_panels

        spec = AuditPanelSpec(
            panel_name="test_coc_missing",
            workload_id="T",
            unit_type="coc",
            source_panel_path="data/curated/panel/nonexistent_coc.parquet",
            selection_rule="test",
            missing_policy="drop",
            rent_proxy="zori_january",
            notes="test coc no rebuild",
            start_year=2020,
            end_year=2021,
        )

        ensure_called = {"count": 0}

        def fail_ensure(project_root, *, force=False):
            ensure_called["count"] += 1
            raise AssertionError("should not be called for coc unit_type")

        monkeypatch.setattr(audit_panels, "_ensure_metro_source_panel", fail_ensure)

        with pytest.raises((FileNotFoundError, OSError)):
            audit_panels._load_source_panel(tmp_path, spec)

        assert ensure_called["count"] == 0


class TestBuildMetroSourcePanelMissingArtifacts:
    """Tests for rebuild behaviour when some raw artifacts are missing."""

    def test_missing_pit_file_raises(self, tmp_path, monkeypatch):
        """If the PIT parquet is absent, _build_metro_source_panel raises
        FileNotFoundError (graceful failure at first I/O step)."""
        import pytest
        from coclab import audit_panels

        # No PIT file written — the very first pd.read_parquet should fail.
        with pytest.raises((FileNotFoundError, OSError)):
            audit_panels._build_metro_source_panel(tmp_path)

    def test_missing_zori_file_raises(self, tmp_path, monkeypatch):
        """If ZORI parquet is absent (but PIT exists), _build_metro_source_panel
        raises FileNotFoundError."""
        import pytest
        from coclab import audit_panels

        # Write PIT so the first read succeeds.
        pit_df = _make_pit_df()
        pit_path = tmp_path / "data" / "curated" / "pit" / "pit_vintage__P2024.parquet"
        pit_path.parent.mkdir(parents=True, exist_ok=True)
        pit_df.to_parquet(pit_path)

        # Mock membership readers (they run before ZORI read).
        monkeypatch.setattr(
            audit_panels,
            "read_metro_coc_membership",
            lambda base_dir: _make_coc_membership_df(),
        )

        # No ZORI file — should raise.
        with pytest.raises((FileNotFoundError, OSError)):
            audit_panels._build_metro_source_panel(tmp_path)

    def test_missing_acs_file_raises(self, tmp_path, monkeypatch):
        """If an ACS vintage file is absent, _build_metro_source_panel raises
        FileNotFoundError during the year loop."""
        import pytest
        from coclab import audit_panels

        # Write PIT and ZORI so those reads succeed.
        pit_df = _make_pit_df()
        pit_path = tmp_path / "data" / "curated" / "pit" / "pit_vintage__P2024.parquet"
        pit_path.parent.mkdir(parents=True, exist_ok=True)
        pit_df.to_parquet(pit_path)

        zori_df = _make_zori_df()
        zori_path = tmp_path / "data" / "curated" / "zori" / "zori__county__Z2026.parquet"
        zori_path.parent.mkdir(parents=True, exist_ok=True)
        zori_df.to_parquet(zori_path)

        # Mock membership readers.
        monkeypatch.setattr(
            audit_panels,
            "read_metro_coc_membership",
            lambda base_dir: _make_coc_membership_df(),
        )
        monkeypatch.setattr(
            audit_panels,
            "read_metro_county_membership",
            lambda base_dir: _make_county_membership_df(),
        )

        # Do NOT write any ACS files — the year-loop read should fail.
        with pytest.raises((FileNotFoundError, OSError)):
            audit_panels._build_metro_source_panel(tmp_path)

    def test_ensure_propagates_build_error(self, tmp_path, monkeypatch):
        """_ensure_metro_source_panel propagates errors from _build_metro_source_panel
        without leaving a partial file behind."""
        import pytest
        from coclab import audit_panels

        expected_path = tmp_path / audit_panels.METRO_SOURCE_PATH
        assert not expected_path.exists()

        def broken_build(project_root):
            raise FileNotFoundError("simulated missing artifact")

        monkeypatch.setattr(audit_panels, "_build_metro_source_panel", broken_build)

        with pytest.raises(FileNotFoundError, match="simulated missing artifact"):
            audit_panels._ensure_metro_source_panel(tmp_path)

        # No partial output should be left on disk.
        assert not expected_path.exists()
