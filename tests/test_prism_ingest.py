"""Tests for PRISM monthly temperature raw ingest."""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.sources.prism import materialize_prism_monthly_counties, prism_county_monthly_columns
from hhplab.sources.prism.ingest import (
    default_prism_monthly_filename,
    download_prism_monthly,
    get_prism_monthly_source_url,
)
from hhplab.storage.provenance import read_provenance
from hhplab.storage.raw_snapshot import hash_zip_contents

runner = CliRunner()

PRISM_ZIP_FILENAME = "prism_tmin_us_25m_202401.zip"
PRISM_URL = (
    "https://data.prism.oregonstate.edu/time_series/us/an/4km/"
    "tmin/monthly/2024/prism_tmin_us_25m_202401.zip"
)


def _zip_bytes() -> bytes:
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as zf:
        zf.writestr("prism_tmin_us_25m_202401.bil", b"raster-bytes")
        zf.writestr("prism_tmin_us_25m_202401.hdr", b"header-bytes")
    return payload.getvalue()


def _write_prism_bil_zip(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    workdir = path.parent / "raster_parts"
    workdir.mkdir()
    bil_path = workdir / "prism_tmin_us_25m_202401.bil"
    data = np.array([[1.0, -9999.0], [3.0, 4.0]], dtype="float32")
    with rasterio.open(
        bil_path,
        "w",
        driver="EHdr",
        width=2,
        height=2,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 2, 1, 1),
        nodata=-9999.0,
    ) as dst:
        dst.write(data.astype("float32"), 1)
    with zipfile.ZipFile(path, "w") as zf:
        for part in workdir.iterdir():
            zf.write(part, arcname=part.name)
    return path


def _write_prism_tif_zip(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tif_path = path.parent / "prism_tmin_us_25m_202401.tif"
    data = np.array([[1.0, -9999.0], [3.0, 4.0]], dtype="float32")
    with rasterio.open(
        tif_path,
        "w",
        driver="GTiff",
        width=2,
        height=2,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 2, 1, 1),
        nodata=-9999.0,
    ) as dst:
        dst.write(data.astype("float32"), 1)
    with zipfile.ZipFile(path, "w") as zf:
        zf.write(tif_path, arcname=tif_path.name)
    return path


def _write_counties(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    counties = gpd.GeoDataFrame(
        {"geoid": ["00001", "00002"]},
        geometry=[box(0, 0, 1, 2), box(1, 0, 2, 2)],
        crs="EPSG:4326",
    )
    counties.to_parquet(path, index=False)
    return path


def _write_edge_case_counties(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    counties = gpd.GeoDataFrame(
        {"geoid": ["00003", "00004"]},
        geometry=[
            box(0.05, 1.05, 0.45, 1.45),
            box(3.0, 3.0, 4.0, 4.0),
        ],
        crs="EPSG:4326",
    )
    counties.to_parquet(path, index=False)
    return path


def test_prism_monthly_url_uses_current_web_service_pattern() -> None:
    assert get_prism_monthly_source_url("tmin", 2024, 1) == PRISM_URL
    assert (
        get_prism_monthly_source_url("tmean", 2024, 12)
        == "https://data.prism.oregonstate.edu/time_series/us/an/4km/"
        "tmean/monthly/2024/prism_tmean_us_25m_202412.zip"
    )


@pytest.mark.parametrize(
    ("variable", "year", "month", "match"),
    [
        ("ppt", 2024, 1, "Unsupported PRISM variable"),
        ("tmin", 2024, 13, "Unsupported PRISM month"),
        ("tmax", 1894, 1, "Unsupported PRISM year"),
    ],
)
def test_prism_monthly_request_validation_is_actionable(
    variable: str, year: int, month: int, match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        get_prism_monthly_source_url(variable, year, month)


def test_download_prism_monthly_writes_raw_zip_and_registers_source(
    tmp_path: Path, httpx_mock, monkeypatch
) -> None:
    content = _zip_bytes()
    httpx_mock.add_response(
        url=PRISM_URL,
        content=content,
        headers={
            "content-type": "application/zip",
            "content-disposition": f'filename="{PRISM_ZIP_FILENAME}"',
        },
    )
    monkeypatch.setenv("HHPLAB_ASSET_STORE_ROOT", str(tmp_path / "data"))

    result = download_prism_monthly("tmin", 2024, 1, force=True)

    expected_path = (
        tmp_path / "data" / "raw" / "prism" / "tmin" / "monthly" / "2024" / PRISM_ZIP_FILENAME
    )
    assert result.path == expected_path
    assert result.path.read_bytes() == content
    assert result.file_size == len(content)
    assert result.raw_sha256 == hash_zip_contents(content)
    assert result.cached is False

    registry = pd.read_parquet(tmp_path / "data" / "curated" / "source_registry.parquet")
    row = registry.iloc[0]
    metadata = json.loads(row["metadata"])
    assert row["source_type"] == "prism"
    assert row["source_url"] == PRISM_URL
    assert row["local_path"] == str(expected_path)
    assert metadata["provider"] == "prism"
    assert metadata["product"] == "temperature"
    assert metadata["variable"] == "tmin"
    assert metadata["month"] == 1
    assert metadata["native_geometry"] == "raster"


def test_download_prism_monthly_uses_cached_raw_zip(tmp_path: Path, httpx_mock) -> None:
    content = _zip_bytes()
    raw_dir = tmp_path / "raw"
    cached_path = (
        raw_dir
        / "prism"
        / "tmin"
        / "monthly"
        / "2024"
        / default_prism_monthly_filename("tmin", 2024, 1)
    )
    cached_path.parent.mkdir(parents=True)
    cached_path.write_bytes(content)

    result = download_prism_monthly("tmin", 2024, 1, raw_root=raw_dir)

    assert result.path == cached_path
    assert result.cached is True
    assert result.raw_sha256 == hash_zip_contents(content)
    assert not httpx_mock.get_requests()


def test_prism_cli_json_output_downloads_raw_zip(tmp_path: Path, httpx_mock, monkeypatch) -> None:
    content = _zip_bytes()
    httpx_mock.add_response(
        url=PRISM_URL,
        content=content,
        headers={
            "content-type": "application/zip",
            "content-disposition": f'filename="{PRISM_ZIP_FILENAME}"',
        },
    )
    monkeypatch.setenv("HHPLAB_ASSET_STORE_ROOT", str(tmp_path / "data"))

    result = runner.invoke(
        app,
        ["ingest", "prism", "--variable", "tmin", "--year", "2024", "--month", "1", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["provider"] == "prism"
    assert payload["product"] == "temperature"
    assert payload["variable"] == "tmin"
    assert payload["year"] == 2024
    assert payload["month"] == 1
    assert payload["source_url"] == PRISM_URL
    assert Path(payload["raw_path"]).exists()
    assert payload["raw_sha256"] == hash_zip_contents(content)


def test_prism_cli_json_error_for_unsupported_variable() -> None:
    result = runner.invoke(app, ["ingest", "prism", "--variable", "ppt", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert "Unsupported PRISM variable" in payload["error"]


def test_materialize_prism_monthly_counties_computes_means_and_provenance(
    tmp_path: Path,
) -> None:
    raw_zip = _write_prism_bil_zip(tmp_path / PRISM_ZIP_FILENAME)
    counties = _write_counties(tmp_path / "counties__C2024.parquet")
    output = tmp_path / "prism_county.parquet"

    result = materialize_prism_monthly_counties(
        "tmin",
        2024,
        1,
        2024,
        raw_zip_path=raw_zip,
        county_geometry_path=counties,
        output_path=output,
    )

    assert result == output
    df = pd.read_parquet(output)
    assert list(df.columns) == prism_county_monthly_columns("tmin")
    assert list(df["county_fips"]) == ["00001", "00002"]
    assert list(df["tmin_c"]) == [2.0, 4.0]
    assert list(df["raster_total_cell_count"]) == [2, 2]
    assert list(df["raster_valid_cell_count"]) == [2, 1]
    assert list(df["raster_nodata_cell_count"]) == [0, 1]
    assert list(df["raster_coverage_ratio"]) == [1.0, 0.5]
    assert set(df["raw_sha256"]) == {hash_zip_contents(raw_zip.read_bytes())}

    provenance = read_provenance(output)
    assert provenance is not None
    assert provenance.county_vintage == "2024"
    assert provenance.geo_type == "county"
    assert provenance.extra["dataset"] == "prism_county_monthly"
    assert provenance.extra["variable"] == "tmin"
    assert provenance.extra["row_count"] == 2
    assert provenance.extra["output_columns"] == prism_county_monthly_columns("tmin")


def test_materialize_prism_monthly_counties_accepts_geotiff_zip(tmp_path: Path) -> None:
    raw_zip = _write_prism_tif_zip(tmp_path / PRISM_ZIP_FILENAME)
    counties = _write_counties(tmp_path / "counties__C2024.parquet")
    output = tmp_path / "prism_county.parquet"

    materialize_prism_monthly_counties(
        "tmin",
        2024,
        1,
        2024,
        raw_zip_path=raw_zip,
        county_geometry_path=counties,
        output_path=output,
    )

    df = pd.read_parquet(output)
    assert list(df["county_fips"]) == ["00001", "00002"]
    assert list(df["tmin_c"]) == [2.0, 4.0]


def test_materialize_prism_monthly_counties_falls_back_for_cell_center_miss(
    tmp_path: Path,
) -> None:
    raw_zip = _write_prism_bil_zip(tmp_path / PRISM_ZIP_FILENAME)
    counties = _write_edge_case_counties(tmp_path / "counties__C2024.parquet")
    output = tmp_path / "prism_county.parquet"

    materialize_prism_monthly_counties(
        "tmin",
        2024,
        1,
        2024,
        raw_zip_path=raw_zip,
        county_geometry_path=counties,
        output_path=output,
    )

    df = pd.read_parquet(output).set_index("county_fips")
    tiny_county = df.loc["00003"]
    outside_county = df.loc["00004"]
    assert tiny_county["tmin_c"] == pytest.approx(1.0)
    assert tiny_county["raster_total_cell_count"] == 1
    assert tiny_county["raster_valid_cell_count"] == 1
    assert tiny_county["raster_nodata_cell_count"] == 0
    assert tiny_county["raster_coverage_ratio"] == pytest.approx(1.0)
    assert pd.isna(outside_county["tmin_c"])
    assert outside_county["raster_total_cell_count"] == 0
    assert outside_county["raster_valid_cell_count"] == 0
    assert outside_county["raster_nodata_cell_count"] == 0
    assert pd.isna(outside_county["raster_coverage_ratio"])


def test_build_prism_county_cli_json_output(tmp_path: Path) -> None:
    raw_zip = _write_prism_bil_zip(tmp_path / PRISM_ZIP_FILENAME)
    counties = _write_counties(tmp_path / "counties__C2024.parquet")
    output = tmp_path / "prism_county.parquet"

    result = runner.invoke(
        app,
        [
            "build",
            "prism-county",
            "--variable",
            "tmin",
            "--year",
            "2024",
            "--month",
            "1",
            "--county-vintage",
            "2024",
            "--raw-zip",
            str(raw_zip),
            "--county-geometry",
            str(counties),
            "--output",
            str(output),
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["output_path"] == str(output)
    assert payload["row_count"] == 2
    assert output.exists()


def test_prism_workflow_docs_include_url_pattern_and_stability_caveat() -> None:
    docs = Path("devdocs/prism_county_workflow.md").read_text(encoding="utf-8")

    assert (
        "https://data.prism.oregonstate.edu/time_series/us/an/4km/"
        "{variable}/monthly/{year}/prism_{variable}_us_25m_{YYYYMM}.zip"
    ) in docs
    assert "roughly six months" in docs
    assert "Direct recipe-time raster zonal statistics to CoC/MSA geometry are out of scope" in docs
