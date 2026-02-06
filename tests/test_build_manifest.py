"""Tests for build manifest, base asset pinning, and aggregate run recording.

Covers:
- ensure_build_dir with year-based asset pinning
- write_build_manifest / read_build_manifest round-trip
- populate_base_assets SHA-256 verification
- get_build_years helper
- record_aggregate_run appending and schema
- Fallback naming resolution for base assets (coc__B, boundaries__B, legacy)
"""

import json
from pathlib import Path

import pytest

from coclab.builds import (
    ensure_build_dir,
    get_build_years,
    populate_base_assets,
    read_build_manifest,
    record_aggregate_run,
    write_build_manifest,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_boundary_files(base: Path, years: list[int], scheme: str = "coc") -> None:
    """Create stub boundary files under base/data/curated/coc_boundaries/."""
    boundaries_dir = base / "data" / "curated" / "coc_boundaries"
    boundaries_dir.mkdir(parents=True, exist_ok=True)
    for year in years:
        if scheme == "coc":
            name = f"coc__B{year}.parquet"
        elif scheme == "boundaries":
            name = f"boundaries__B{year}.parquet"
        else:
            name = f"coc_boundaries__{year}.parquet"
        (boundaries_dir / name).write_bytes(
            b"PAR1stub" + year.to_bytes(2, "big")
        )


# ---------------------------------------------------------------------------
# ensure_build_dir with years
# ---------------------------------------------------------------------------


class TestEnsureBuildDir:
    def test_creates_scaffold_with_years(self, tmp_path):
        _create_boundary_files(tmp_path, [2020, 2021])

        build_dir, assets = ensure_build_dir(
            "test", builds_dir=tmp_path / "builds", years=[2020, 2021], data_dir=tmp_path / "data",
        )

        assert build_dir.exists()
        assert (build_dir / "data" / "curated").exists()
        assert (build_dir / "data" / "raw").exists()
        assert (build_dir / "base").exists()
        assert len(assets) == 2

    def test_pins_boundary_files(self, tmp_path):
        _create_boundary_files(tmp_path, [2020])

        build_dir, assets = ensure_build_dir(
            "test", builds_dir=tmp_path / "builds", years=[2020], data_dir=tmp_path / "data",
        )

        # File should be copied to base/
        pinned = build_dir / "base" / "coc__B2020.parquet"
        assert pinned.exists()

    def test_writes_full_manifest(self, tmp_path):
        _create_boundary_files(tmp_path, [2020, 2021])

        build_dir, _ = ensure_build_dir(
            "test", builds_dir=tmp_path / "builds", years=[2020, 2021], data_dir=tmp_path / "data",
        )

        manifest = json.loads((build_dir / "manifest.json").read_text())
        assert manifest["schema_version"] == 1
        assert manifest["build"]["name"] == "test"
        assert manifest["build"]["years"] == [2020, 2021]
        assert len(manifest["base_assets"]) == 2
        assert manifest["aggregate_runs"] == []

    def test_missing_boundary_raises(self, tmp_path):
        _create_boundary_files(tmp_path, [2020])  # Only 2020

        with pytest.raises(FileNotFoundError, match="2021"):
            ensure_build_dir(
                "test",
                builds_dir=tmp_path / "builds",
                years=[2020, 2021],
                data_dir=tmp_path / "data",
            )

    def test_legacy_minimal_manifest_without_years(self, tmp_path):
        build_dir, assets = ensure_build_dir("test", builds_dir=tmp_path / "builds")

        assert assets == []
        manifest = json.loads((build_dir / "manifest.json").read_text())
        assert manifest == {"schema_version": 1}


# ---------------------------------------------------------------------------
# populate_base_assets
# ---------------------------------------------------------------------------


class TestPopulateBaseAssets:
    def test_sha256_is_64_hex_chars(self, tmp_path):
        _create_boundary_files(tmp_path, [2020])
        build_dir = tmp_path / "builds" / "test"
        (build_dir / "base").mkdir(parents=True)

        assets = populate_base_assets(build_dir, [2020], data_dir=tmp_path / "data")

        assert len(assets) == 1
        assert len(assets[0]["sha256"]) == 64
        assert all(c in "0123456789abcdef" for c in assets[0]["sha256"])

    def test_relative_path_format(self, tmp_path):
        _create_boundary_files(tmp_path, [2020])
        build_dir = tmp_path / "builds" / "test"
        (build_dir / "base").mkdir(parents=True)

        assets = populate_base_assets(build_dir, [2020], data_dir=tmp_path / "data")

        assert assets[0]["relative_path"] == "base/coc__B2020.parquet"

    def test_resolves_boundaries_scheme(self, tmp_path):
        """Should resolve boundaries__B naming as fallback."""
        _create_boundary_files(tmp_path, [2020], scheme="boundaries")
        build_dir = tmp_path / "builds" / "test"
        (build_dir / "base").mkdir(parents=True)

        assets = populate_base_assets(build_dir, [2020], data_dir=tmp_path / "data")

        assert assets[0]["asset_type"] == "coc_boundary"
        assert assets[0]["relative_path"] == "base/boundaries__B2020.parquet"

    def test_resolves_legacy_scheme(self, tmp_path):
        """Should resolve coc_boundaries__ legacy naming as fallback."""
        _create_boundary_files(tmp_path, [2020], scheme="legacy")
        build_dir = tmp_path / "builds" / "test"
        (build_dir / "base").mkdir(parents=True)

        assets = populate_base_assets(build_dir, [2020], data_dir=tmp_path / "data")

        assert assets[0]["asset_type"] == "coc_boundary"
        assert assets[0]["relative_path"] == "base/coc_boundaries__2020.parquet"

    def test_copied_file_content_matches(self, tmp_path):
        _create_boundary_files(tmp_path, [2020])
        build_dir = tmp_path / "builds" / "test"
        (build_dir / "base").mkdir(parents=True)

        assets = populate_base_assets(build_dir, [2020], data_dir=tmp_path / "data")

        source = tmp_path / "data" / "curated" / "coc_boundaries" / "coc__B2020.parquet"
        pinned = build_dir / assets[0]["relative_path"]
        assert source.read_bytes() == pinned.read_bytes()


# ---------------------------------------------------------------------------
# Manifest read/write
# ---------------------------------------------------------------------------


class TestManifestReadWrite:
    def test_roundtrip(self, tmp_path):
        assets = [{"asset_type": "coc_boundary", "year": 2020}]
        write_build_manifest(tmp_path, "test", [2020, 2021], assets)

        manifest = read_build_manifest(tmp_path)
        assert manifest["schema_version"] == 1
        assert manifest["build"]["name"] == "test"
        assert manifest["build"]["years"] == [2020, 2021]
        assert manifest["build"]["created_at"]  # ISO timestamp present
        assert len(manifest["base_assets"]) == 1

    def test_read_missing_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_build_manifest(tmp_path / "nonexistent")


class TestGetBuildYears:
    def test_returns_years(self, tmp_path):
        write_build_manifest(tmp_path, "test", [2020, 2021, 2022], [])
        assert get_build_years(tmp_path) == [2020, 2021, 2022]

    def test_returns_empty_for_minimal_manifest(self, tmp_path):
        (tmp_path / "manifest.json").write_text('{"schema_version": 1}\n')
        assert get_build_years(tmp_path) == []


# ---------------------------------------------------------------------------
# record_aggregate_run
# ---------------------------------------------------------------------------


class TestRecordAggregateRun:
    def test_appends_run_entry(self, tmp_path):
        write_build_manifest(tmp_path, "test", [2020], [])

        entry = record_aggregate_run(
            tmp_path,
            dataset="pep",
            alignment="as_of_july",
            years_requested=[2020],
        )

        assert entry["dataset"] == "pep"
        assert entry["status"] == "success"
        assert len(entry["run_id"]) == 12

        manifest = read_build_manifest(tmp_path)
        assert len(manifest["aggregate_runs"]) == 1
        assert manifest["aggregate_runs"][0]["run_id"] == entry["run_id"]

    def test_multiple_runs_append(self, tmp_path):
        write_build_manifest(tmp_path, "test", [2020, 2021], [])

        record_aggregate_run(
            tmp_path, dataset="pep", alignment="as_of_july", years_requested=[2020],
        )
        record_aggregate_run(
            tmp_path, dataset="acs", alignment="vintage_end_year", years_requested=[2020, 2021],
        )

        manifest = read_build_manifest(tmp_path)
        assert len(manifest["aggregate_runs"]) == 2
        assert manifest["aggregate_runs"][0]["dataset"] == "pep"
        assert manifest["aggregate_runs"][1]["dataset"] == "acs"

    def test_failed_run_records_error(self, tmp_path):
        write_build_manifest(tmp_path, "test", [2020], [])

        entry = record_aggregate_run(
            tmp_path,
            dataset="zori",
            alignment="monthly_native",
            years_requested=[2020],
            status="failed",
            error="Missing ZORI data",
        )

        assert entry["status"] == "failed"
        assert entry["error"] == "Missing ZORI data"

    def test_alignment_params_recorded(self, tmp_path):
        write_build_manifest(tmp_path, "test", [2020], [])

        entry = record_aggregate_run(
            tmp_path,
            dataset="pep",
            alignment="lagged",
            years_requested=[2020],
            alignment_params={"lag_years": 2},
        )

        assert entry["alignment"]["mode"] == "lagged"
        assert entry["alignment"]["lag_years"] == 2

    def test_years_materialized_defaults_to_requested(self, tmp_path):
        write_build_manifest(tmp_path, "test", [2020, 2021], [])

        entry = record_aggregate_run(
            tmp_path, dataset="pit", alignment="point_in_time_jan",
            years_requested=[2020, 2021],
        )

        assert entry["years_materialized"] == [2020, 2021]

    def test_years_materialized_can_differ(self, tmp_path):
        write_build_manifest(tmp_path, "test", [2020, 2021], [])

        entry = record_aggregate_run(
            tmp_path, dataset="pit", alignment="point_in_time_jan",
            years_requested=[2020, 2021],
            years_materialized=[2020],  # 2021 was missing
        )

        assert entry["years_materialized"] == [2020]

    def test_outputs_recorded(self, tmp_path):
        write_build_manifest(tmp_path, "test", [2020], [])

        entry = record_aggregate_run(
            tmp_path, dataset="pep", alignment="as_of_july",
            years_requested=[2020],
            outputs=["data/curated/pep/pep__B2020.parquet"],
        )

        assert entry["outputs"] == ["data/curated/pep/pep__B2020.parquet"]
