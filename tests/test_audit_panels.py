"""Tests for Glynn-Fox audit panel preparation."""

import json
from pathlib import Path

import pandas as pd

from coclab.audit_panels import (
    AUDIT_PANEL_SPECS,
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


def test_validation_reports_balanced_panel():
    raw_df = _raw_fixture()
    raw_validation = _validate_raw_panel(
        raw_df,
        spec=AUDIT_PANEL_SPECS[0],
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
