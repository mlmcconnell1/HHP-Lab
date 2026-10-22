"""Tests for `hhplab ingest msa-boundaries` and `hhplab validate msa`."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box
from typer.testing import CliRunner

from hhplab.cli.main import app

runner = CliRunner()


def _patch_msa_boundary_download(
    monkeypatch: pytest.MonkeyPatch,
    source_gdf: gpd.GeoDataFrame,
) -> None:
    expected = pd.DataFrame(
        {
            "msa_id": ["17410"],
            "cbsa_code": ["17410"],
            "msa_name": ["Cleveland, OH"],
            "area_type": ["Metropolitan Statistical Area"],
            "definition_version": ["census_msa_2023"],
        }
    )

    monkeypatch.setattr(
        "hhplab.geographies.msa.msa_boundaries._load_expected_definitions",
        lambda definition_version, base_dir=None, raw_root=None: expected,
    )
    monkeypatch.setattr(
        "hhplab.geographies.msa.msa_boundaries.persist_file_snapshot",
        lambda raw_content, *_args, **_kwargs: (Path("/tmp/fake.zip"), "abc", len(raw_content)),
    )
    monkeypatch.setattr(
        "hhplab.geographies.msa.msa_boundaries.check_source_changed",
        lambda **_kwargs: (False, {}),
    )

    class _FakeResponse:
        content = b"zip-bytes"

        def raise_for_status(self) -> None:
            return None

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, url, follow_redirects=True):
            return _FakeResponse()

    class _FakeZipFile:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extractall(self, path):
            (Path(path) / "fake.shp").write_text("", encoding="utf-8")

    monkeypatch.setattr("hhplab.geographies.msa.msa_boundaries.httpx.Client", _FakeClient)
    monkeypatch.setattr("hhplab.geographies.msa.msa_boundaries.zipfile.ZipFile", _FakeZipFile)
    monkeypatch.setattr(
        "hhplab.geographies.msa.msa_boundaries.gpd.read_file", lambda _path: source_gdf
    )


@pytest.mark.parametrize("name_col", ["NAME", "NAMELSAD", "CBSA_NAME"])
def test_download_msa_boundaries_uses_supported_name_column_fallbacks(
    monkeypatch: pytest.MonkeyPatch,
    name_col: str,
):
    source_name = f"{name_col} Cleveland, OH"
    source_gdf = gpd.GeoDataFrame(
        {
            "CBSAFP": ["17410"],
            name_col: [source_name],
        },
        geometry=[box(0, 0, 1, 1)],
        crs="EPSG:4326",
    )
    _patch_msa_boundary_download(monkeypatch, source_gdf)

    from hhplab.geographies.msa.msa_boundaries import download_msa_boundaries

    boundaries, _sha256, _size, _raw_path = download_msa_boundaries("census_msa_2023")

    assert list(boundaries["cbsa_code"]) == ["17410"]
    assert list(boundaries["msa_id"]) == ["17410"]
    assert list(boundaries["cbsa_name_source"]) == [source_name]
    assert "cbsa_code_x" not in boundaries.columns
    assert "cbsa_code_y" not in boundaries.columns


def test_download_msa_boundaries_missing_crs_raises_actionable_error(
    monkeypatch: pytest.MonkeyPatch,
):
    source_gdf = gpd.GeoDataFrame(
        {
            "CBSAFP": ["17410"],
            "NAME": ["Cleveland, OH"],
        },
        geometry=[box(0, 0, 1, 1)],
    )
    _patch_msa_boundary_download(monkeypatch, source_gdf)

    from hhplab.geographies.msa.msa_boundaries import download_msa_boundaries

    with pytest.raises(ValueError, match="source has no CRS; cannot safely ingest"):
        download_msa_boundaries("census_msa_2023")


def test_download_msa_boundaries_reprojects_to_epsg_4326(
    monkeypatch: pytest.MonkeyPatch,
):
    source_gdf = gpd.GeoDataFrame(
        {
            "CBSAFP": ["17410"],
            "NAME": ["Cleveland, OH"],
        },
        geometry=[box(0, 0, 1000, 1000)],
        crs="EPSG:5070",
    )
    _patch_msa_boundary_download(monkeypatch, source_gdf)

    from hhplab.geographies.msa.msa_boundaries import download_msa_boundaries

    boundaries, _sha256, _size, _raw_path = download_msa_boundaries("census_msa_2023")

    assert boundaries.crs.to_epsg() == 4326


def test_download_historical_msa_boundaries_dissolves_2010_counties(
    monkeypatch: pytest.MonkeyPatch,
):
    from hhplab.geographies.msa.msa_boundaries import download_msa_boundaries

    definitions = pd.DataFrame(
        {
            "msa_id": ["17410"],
            "cbsa_code": ["17410"],
            "msa_name": ["Cleveland, OH"],
            "area_type": ["Metropolitan Statistical Area"],
            "definition_version": ["census_msa_2010"],
        }
    )
    membership = pd.DataFrame(
        {
            "msa_id": ["17410"],
            "cbsa_code": ["17410"],
            "county_fips": ["39035"],
        }
    )
    source_gdf = gpd.GeoDataFrame(
        {
            "GEOID10": ["39035", "39049"],
            "CBSAFP10": ["17410", "99999"],
        },
        geometry=[box(0, 0, 1, 1), box(2, 2, 3, 3)],
        crs="EPSG:4326",
    )
    monkeypatch.setattr(
        "hhplab.geographies.msa.msa_boundaries._load_expected_definitions",
        lambda *_args, **_kwargs: definitions,
    )
    monkeypatch.setattr(
        "hhplab.geographies.msa.msa_boundaries._load_expected_membership",
        lambda *_args, **_kwargs: membership,
    )
    monkeypatch.setattr(
        "hhplab.geographies.msa.msa_boundaries.persist_file_snapshot",
        lambda raw_content, *_args, **_kwargs: (
            Path("/tmp/fake-tiger.zip"),
            "abc",
            len(raw_content),
        ),
    )
    monkeypatch.setattr(
        "hhplab.geographies.msa.msa_boundaries.check_source_changed",
        lambda **_kwargs: (False, {}),
    )

    class _FakeResponse:
        content = b"zip-bytes"

        def raise_for_status(self) -> None:
            return None

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, url, follow_redirects=True):
            assert "TIGER2010/COUNTY/2010" in url
            return _FakeResponse()

    class _FakeZipFile:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extractall(self, path):
            (Path(path) / "fake.shp").write_text("", encoding="utf-8")

    monkeypatch.setattr("hhplab.geographies.msa.msa_boundaries.httpx.Client", _FakeClient)
    monkeypatch.setattr("hhplab.geographies.msa.msa_boundaries.zipfile.ZipFile", _FakeZipFile)
    monkeypatch.setattr(
        "hhplab.geographies.msa.msa_boundaries.gpd.read_file", lambda _path: source_gdf
    )

    boundaries, _sha256, _size, _raw_path = download_msa_boundaries("census_msa_2010")

    assert boundaries["msa_id"].tolist() == ["17410"]
    assert boundaries["geometry_vintage"].tolist() == ["2010"]
    assert boundaries["source"].tolist() == ["census_tiger_county10_dissolved_by_msa"]
    assert boundaries.geometry.iloc[0].area == pytest.approx(1.0)


def test_ingest_msa_boundaries_json(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    artifact = tmp_path / "data" / "curated" / "msa" / "msa_boundaries__census_msa_2023.parquet"
    artifact.parent.mkdir(parents=True, exist_ok=True)

    def fake_ingest(definition_version: str, *, tiger_year: int):
        gpd.GeoDataFrame(
            {
                "msa_id": ["35620"],
                "cbsa_code": ["35620"],
                "msa_name": ["New York-Newark-Jersey City, NY-NJ-PA"],
                "area_type": ["Metropolitan Statistical Area"],
                "definition_version": [definition_version],
                "geometry_vintage": [str(tiger_year)],
                "source": ["census_tiger_cbsa"],
                "source_ref": ["https://example.test/cbsa.zip"],
                "ingested_at": [pd.Timestamp("2026-04-30T00:00:00Z")],
            },
            geometry=[box(0, 0, 1, 1)],
            crs="EPSG:4326",
        ).to_parquet(artifact)
        return artifact

    monkeypatch.setattr(
        "hhplab.geographies.msa.msa_boundaries.ingest_msa_boundaries",
        fake_ingest,
    )
    monkeypatch.setattr(
        "hhplab.geographies.msa.msa_boundaries.read_msa_boundaries",
        lambda definition_version: gpd.read_parquet(artifact),
    )

    result = runner.invoke(
        app,
        ["ingest", "msa-boundaries", "--json"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["definition_version"] == "census_msa_2023"
    assert payload["geometry_vintage"] == 2023
    assert payload["msa_count"] == 1
    assert payload["artifact"].endswith("msa_boundaries__census_msa_2023.parquet")


def test_ingest_msa_boundaries_json_surfaces_validation_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    monkeypatch.chdir(tmp_path)

    def fake_ingest(definition_version: str, *, tiger_year: int):
        raise ValueError("MSA boundary validation failed:\nmissing expected MSA ids")

    monkeypatch.setattr(
        "hhplab.geographies.msa.msa_boundaries.ingest_msa_boundaries",
        fake_ingest,
    )

    result = runner.invoke(
        app,
        ["ingest", "msa-boundaries", "--json"],
        catch_exceptions=False,
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload == {
        "status": "error",
        "error": "validation_failed",
        "detail": "MSA boundary validation failed:\nmissing expected MSA ids",
    }


def test_validate_msa_json(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    msadir = tmp_path / "data" / "curated" / "msa"
    msadir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "msa_id": ["35620"],
            "cbsa_code": ["35620"],
            "msa_name": ["New York-Newark-Jersey City, NY-NJ-PA"],
            "area_type": ["Metropolitan Statistical Area"],
            "definition_version": ["census_msa_2023"],
            "source": ["census_msa_delineation_2023"],
            "source_ref": ["https://example.test/list1.xlsx"],
        }
    ).to_parquet(msadir / "msa_definitions__census_msa_2023.parquet")
    pd.DataFrame(
        {
            "msa_id": ["35620"],
            "cbsa_code": ["35620"],
            "county_fips": ["36061"],
            "county_name": ["New York County"],
            "state_name": ["New York"],
            "central_outlying": ["Central"],
            "definition_version": ["census_msa_2023"],
        }
    ).to_parquet(msadir / "msa_county_membership__census_msa_2023.parquet")
    gpd.GeoDataFrame(
        {
            "msa_id": ["35620"],
            "cbsa_code": ["35620"],
            "msa_name": ["New York-Newark-Jersey City, NY-NJ-PA"],
            "area_type": ["Metropolitan Statistical Area"],
            "definition_version": ["census_msa_2023"],
            "geometry_vintage": ["2023"],
            "source": ["census_tiger_cbsa"],
            "source_ref": ["https://example.test/cbsa.zip"],
            "ingested_at": [pd.Timestamp("2026-04-30T00:00:00Z")],
        },
        geometry=[box(0, 0, 1, 1)],
        crs="EPSG:4326",
    ).to_parquet(msadir / "msa_boundaries__census_msa_2023.parquet")

    result = runner.invoke(app, ["validate", "msa", "--json"], catch_exceptions=False)

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["definition_version"] == "census_msa_2023"
    assert payload["errors"] == []
