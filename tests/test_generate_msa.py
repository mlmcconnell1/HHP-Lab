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

    monkeypatch.setattr("hhplab.msa.io.write_msa_artifacts", fake_write_msa_artifacts)

    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(app, ["generate", "msa", "--json"], catch_exceptions=False)

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["definition_version"] == "census_msa_2023"
    assert payload["artifacts"]["definitions"].endswith("msa_definitions__test.parquet")
