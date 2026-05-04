"""Tests for metro generation and validation CLIs."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import box
from typer.testing import CliRunner

from hhplab.cli.main import app

runner = CliRunner()


def test_generate_metro_boundaries_json(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    artifact = (
        tmp_path / "data" / "curated" / "metro" / "metro_boundaries__glynn_fox_v1xC2025.parquet"
    )
    artifact.parent.mkdir(parents=True, exist_ok=True)

    def fake_generate(definition_version: str, *, county_vintage: int):
        gpd.GeoDataFrame(
            {
                "metro_id": ["GF21"],
                "metro_name": ["Denver, CO"],
                "definition_version": [definition_version],
                "geometry_vintage": [str(county_vintage)],
                "source": ["derived_metro_county_union"],
                "source_ref": ["https://example.test/metro"],
                "ingested_at": [pd.Timestamp("2026-04-30T00:00:00Z")],
            },
            geometry=[box(0, 0, 1, 1)],
            crs="EPSG:4326",
        ).to_parquet(artifact)
        return artifact

    monkeypatch.setattr(
        "hhplab.metro.metro_boundaries.generate_metro_boundaries",
        fake_generate,
    )
    monkeypatch.setattr(
        "hhplab.metro.metro_boundaries.read_metro_boundaries",
        lambda definition_version, county_vintage: gpd.read_parquet(artifact),
    )

    result = runner.invoke(
        app,
        ["generate", "metro-boundaries", "--counties", "2025", "--json"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["definition_version"] == "glynn_fox_v1"
    assert payload["county_vintage"] == 2025
    assert payload["metro_count"] == 1
    assert payload["artifact"].endswith("metro_boundaries__glynn_fox_v1xC2025.parquet")


def test_validate_metro_json(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    from hhplab.metro.metro_validate import MetroValidationResult

    monkeypatch.setattr(
        "hhplab.metro.metro_io.validate_curated_metro",
        lambda definition_version: MetroValidationResult(
            passed=True,
            errors=[],
            warnings=[],
        ),
    )
    monkeypatch.setattr(
        "hhplab.metro.metro_boundaries.validate_curated_metro_boundaries",
        lambda definition_version, county_vintage: MetroValidationResult(
            passed=True,
            errors=[],
            warnings=[],
        ),
    )

    result = runner.invoke(
        app,
        ["validate", "metro", "--counties", "2025", "--json"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["definition_version"] == "glynn_fox_v1"
    assert payload["county_vintage"] == 2025
    assert payload["errors"] == []


def test_generate_metro_universe_json(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)

    def fake_write(
        metro_definition_version: str,
        profile_definition_version: str,
    ):
        universe = (
            tmp_path / "data" / "curated" / "metro" / "metro_universe__census_msa_2023.parquet"
        )
        subset = (
            tmp_path
            / "data"
            / "curated"
            / "metro"
            / "metro_subset_membership__glynn_fox_v1xMcensus_msa_2023.parquet"
        )
        universe.parent.mkdir(parents=True, exist_ok=True)
        universe.write_text(metro_definition_version)
        subset.write_text(profile_definition_version)
        return universe, subset

    monkeypatch.setattr(
        "hhplab.metro.metro_io.write_metro_universe_artifacts",
        fake_write,
    )

    result = runner.invoke(
        app,
        ["generate", "metro-universe", "--json"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["definition_version"] == "census_msa_2023"
    assert payload["profile_definition_version"] == "glynn_fox_v1"
    assert payload["artifacts"]["metro_universe"].endswith(
        "metro_universe__census_msa_2023.parquet"
    )


def test_validate_metro_universe_json(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    from hhplab.metro.metro_validate import MetroValidationResult

    monkeypatch.setattr(
        "hhplab.metro.metro_io.validate_curated_metro_universe",
        lambda metro_definition_version, profile_definition_version: MetroValidationResult(
            passed=True,
            errors=[],
            warnings=[],
        ),
    )

    result = runner.invoke(
        app,
        ["validate", "metro-universe", "--json"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["definition_version"] == "census_msa_2023"
    assert payload["profile_definition_version"] == "glynn_fox_v1"
    assert payload["errors"] == []
