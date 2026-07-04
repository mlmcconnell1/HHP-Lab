"""Tests for expanded hidden-cause covariate sources."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.covariates.aggregate import aggregate_covariate_source
from hhplab.covariates.catalog import COVARIATE_SOURCE_SPECS
from hhplab.covariates.ingest import ingest_covariate_source
from hhplab.curated_policy import validate_curated_layout
from hhplab.provenance import read_provenance

runner = CliRunner()

EXPECTED_COVARIATE_SOURCES = {
    "eviction_lab": ("county", "eviction_filings"),
    "census_bps": ("county", "permitted_units"),
    "hud_fmr": ("county", "fmr_2br"),
    "hud_psh": ("county", "subsidized_households"),
    "hud_spm": ("coc", "spm_first_time_homeless"),
    "kff_medicaid_expansion": ("state", "medicaid_expansion_adopted"),
}


def test_covariate_catalog_declares_hidden_cause_sources() -> None:
    """The expanded source catalog should cover all bead-requested families."""
    assert set(COVARIATE_SOURCE_SPECS) == set(EXPECTED_COVARIATE_SOURCES)
    for source_id, (native_geo, measure) in EXPECTED_COVARIATE_SOURCES.items():
        spec = COVARIATE_SOURCE_SPECS[source_id]
        assert spec.native_geo == native_geo
        assert measure in spec.measure_columns
        assert spec.source_page.startswith("https://")
        assert spec.recommended_align


def test_ingest_and_aggregate_county_covariate_roundtrip(tmp_path: Path, monkeypatch) -> None:
    """Staged provider CSVs become curated and panel-ready Parquet."""
    monkeypatch.setattr("hhplab.covariates.ingest.register_source", lambda **_: None)
    raw = tmp_path / "evictions.csv"
    pd.DataFrame(
        {
            "fips": ["1001", "01003"],
            "year": [2020, 2021],
            "eviction_filings": [120, 140],
            "eviction_rate": [2.5, 2.7],
        }
    ).to_csv(raw, index=False)

    curated = ingest_covariate_source(
        "eviction_lab",
        raw,
        output_dir=tmp_path,
        force=True,
    )
    curated_df = pd.read_parquet(curated)

    assert curated.name == "covariate__eviction_lab__Y2000-ongoing.parquet"
    assert curated_df["geo_type"].tolist() == ["county", "county"]
    assert curated_df["county_fips"].tolist() == ["01001", "01003"]
    assert curated_df["eviction_filings"].tolist() == [120, 140]
    provenance = read_provenance(curated)
    assert provenance is not None
    assert provenance.extra["dataset_type"] == "expanded_covariate"
    assert provenance.extra["source_id"] == "eviction_lab"

    panel = aggregate_covariate_source(
        "eviction_lab",
        curated_path=curated,
        output_dir=tmp_path,
        years=[2021],
        force=True,
    )
    panel_df = pd.read_parquet(panel)
    assert panel.name == "covariate_panel__eviction_lab__Y2000-ongoing.parquet"
    assert panel_df["geo_id"].tolist() == ["01003"]
    assert panel_df["year"].tolist() == [2021]
    panel_provenance = read_provenance(panel)
    assert panel_provenance is not None
    assert panel_provenance.extra["dataset_type"] == "expanded_covariate_panel"


def test_covariate_outputs_pass_curated_layout_policy(tmp_path: Path, monkeypatch) -> None:
    """Covariate artifacts should use canonical names in a registered curated subdir."""
    monkeypatch.setattr("hhplab.covariates.ingest.register_source", lambda **_: None)
    raw = tmp_path / "fmr.csv"
    pd.DataFrame(
        {
            "county_fips": ["01001"],
            "year": [2024],
            "fmr_0br": [750],
            "fmr_1br": [850],
            "fmr_2br": [1000],
            "fmr_3br": [1250],
            "fmr_4br": [1500],
        }
    ).to_csv(raw, index=False)
    curated_root = tmp_path / "curated"
    covariate_dir = curated_root / "covariates"

    curated = ingest_covariate_source(
        "hud_fmr",
        raw,
        output_dir=covariate_dir,
        force=True,
    )
    panel = aggregate_covariate_source(
        "hud_fmr",
        curated_path=curated,
        output_dir=covariate_dir,
        years=[2024],
        force=True,
    )

    assert curated.name == "covariate__hud_fmr__Y2000-ongoing.parquet"
    assert panel.name == "covariate_panel__hud_fmr__Y2000-ongoing.parquet"
    assert validate_curated_layout(curated_root) == []


def test_cli_lists_covariate_sources_as_json() -> None:
    """Agents can discover expanded covariate support without scraping text."""
    result = runner.invoke(app, ["list", "covariates", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["source_count"] == len(EXPECTED_COVARIATE_SOURCES)
    source_ids = {source["source_id"] for source in payload["sources"]}
    assert source_ids == set(EXPECTED_COVARIATE_SOURCES)


def test_cli_ingest_and_aggregate_covariate(tmp_path: Path, monkeypatch) -> None:
    """CLI commands expose ingest and aggregate with structured output."""
    monkeypatch.setattr("hhplab.covariates.ingest.register_source", lambda **_: None)
    raw = tmp_path / "fmr.csv"
    pd.DataFrame(
        {
            "county_fips": ["01001"],
            "year": [2024],
            "fmr_0br": [750],
            "fmr_1br": [850],
            "fmr_2br": [1000],
            "fmr_3br": [1250],
            "fmr_4br": [1500],
        }
    ).to_csv(raw, index=False)

    ingest_result = runner.invoke(
        app,
        [
            "ingest",
            "covariate",
            "--source",
            "hud_fmr",
            "--raw-path",
            str(raw),
            "--output-dir",
            str(tmp_path),
            "--force",
            "--json",
        ],
    )
    assert ingest_result.exit_code == 0
    ingest_payload = json.loads(ingest_result.output)
    assert ingest_payload["status"] == "ok"
    assert ingest_payload["source_id"] == "hud_fmr"

    aggregate_result = runner.invoke(
        app,
        [
            "aggregate",
            "covariate",
            "--source",
            "hud_fmr",
            "--curated-path",
            ingest_payload["output_path"],
            "--output-dir",
            str(tmp_path),
            "--years",
            "2024",
            "--force",
            "--json",
        ],
    )
    assert aggregate_result.exit_code == 0
    aggregate_payload = json.loads(aggregate_result.output)
    assert aggregate_payload["status"] == "ok"
    assert aggregate_payload["row_count"] == 1
