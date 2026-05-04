"""Tests for `hhplab generate msa`."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from hhplab.cli.main import app

runner = CliRunner()


def test_generate_msa_json(monkeypatch, tmp_path: Path):
    def fake_write_msa_artifacts(definition_version: str):
        defs = tmp_path / "data" / "curated" / "msa" / "msa_definitions__test.parquet"
        county = tmp_path / "data" / "curated" / "msa" / "msa_county_membership__test.parquet"
        defs.parent.mkdir(parents=True, exist_ok=True)
        defs.write_text("defs")
        county.write_text("county")
        return defs, county

    monkeypatch.setattr("hhplab.msa.msa_io.write_msa_artifacts", fake_write_msa_artifacts)

    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(app, ["generate", "msa", "--json"], catch_exceptions=False)

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["definition_version"] == "census_msa_2023"
    assert payload["artifacts"]["definitions"].endswith("msa_definitions__test.parquet")


def test_generate_msa_json_rejects_existing_artifact_without_force(
    monkeypatch,
    tmp_path: Path,
):
    monkeypatch.chdir(tmp_path)
    existing = tmp_path / "data" / "curated" / "msa" / ("msa_definitions__census_msa_2023.parquet")
    existing.parent.mkdir(parents=True, exist_ok=True)
    existing.write_text("existing", encoding="utf-8")

    def fail_write_msa_artifacts(definition_version: str):
        raise AssertionError("write_msa_artifacts should not run without --force")

    monkeypatch.setattr("hhplab.msa.msa_io.write_msa_artifacts", fail_write_msa_artifacts)

    result = runner.invoke(app, ["generate", "msa", "--json"], catch_exceptions=False)

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["error"] == "artifacts_exist"
    assert payload["existing"] == ["data/curated/msa/msa_definitions__census_msa_2023.parquet"]


def test_generate_msa_force_allows_existing_artifact(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    existing_defs = (
        tmp_path / "data" / "curated" / "msa" / ("msa_definitions__census_msa_2023.parquet")
    )
    existing_county = (
        tmp_path / "data" / "curated" / "msa" / ("msa_county_membership__census_msa_2023.parquet")
    )
    existing_defs.parent.mkdir(parents=True, exist_ok=True)
    existing_defs.write_text("existing defs", encoding="utf-8")

    def fake_write_msa_artifacts(definition_version: str):
        existing_defs.write_text("new defs", encoding="utf-8")
        existing_county.write_text("new county", encoding="utf-8")
        return existing_defs, existing_county

    monkeypatch.setattr("hhplab.msa.msa_io.write_msa_artifacts", fake_write_msa_artifacts)

    result = runner.invoke(
        app,
        ["generate", "msa", "--json", "--force"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["artifacts"]["definitions"] == str(existing_defs)
    assert existing_defs.read_text(encoding="utf-8") == "new defs"
