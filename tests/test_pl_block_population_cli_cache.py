"""CLI cache validation tests for PL block population artifacts."""

from __future__ import annotations

import json

import pandas as pd
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.schema.columns import PL_BLOCK_POPULATION_COLUMNS
from hhplab.sources.census.ingest import pl_block_population as population_ingest
from hhplab.sources.census.ingest.pl_block_population import PL_2010_STATE_FIPS_CODES
from hhplab.storage.provenance import ProvenanceBlock, write_parquet_with_provenance

runner = CliRunner()


def _cached_2010_artifact(path, state_fips_codes: tuple[str, ...]) -> None:
    frame = pd.DataFrame(
        [
            {
                "block_geoid": "080310001000007",
                "state_fips": "08",
                "county_fips": "08031",
                "tract_geoid": "08031000100",
                "block_vintage": "2010",
                "decennial_vintage": "2010",
                "total_population": 37,
                "data_source": "census_pl_94_171",
                "source_ref": "https://www2.census.gov/example/co2010.pl.zip",
                "ingested_at": "2026-05-31T00:00:00Z",
            }
        ],
        columns=PL_BLOCK_POPULATION_COLUMNS,
    )
    write_parquet_with_provenance(
        frame,
        path,
        ProvenanceBlock(
            weighting="denominator",
            extra={
                "dataset_type": "pl_block_population",
                "decennial_vintage": "2010",
                "block_vintage": "2010",
                "state_fips_codes": list(state_fips_codes),
            },
        ),
    )


def _point_cli_and_ingest_at(monkeypatch, cached_path) -> None:
    monkeypatch.setattr(
        "hhplab.cli.ingest.pl_block_population.get_pl_block_population_output_path",
        lambda *args, **kwargs: cached_path,
    )
    monkeypatch.setattr(
        population_ingest,
        "get_pl_block_population_output_path",
        lambda *args, **kwargs: cached_path,
    )


def test_pl_blocks_cli_rejects_cached_2010_artifact_with_incomplete_state_coverage(
    monkeypatch, tmp_path
) -> None:
    """The CLI must run the native 52-FIPS cache provenance check before reporting success."""
    cached_path = tmp_path / "pl_blocks__N2010xK2010.parquet"
    incomplete_states = tuple(state for state in PL_2010_STATE_FIPS_CODES if state != "72")
    _cached_2010_artifact(cached_path, incomplete_states)
    _point_cli_and_ingest_at(monkeypatch, cached_path)

    result = runner.invoke(app, ["ingest", "pl-blocks", "--decennial", "2010", "--json"])

    assert result.exit_code == 1
    assert "missing verified coverage for all 50 states, DC, and Puerto Rico" in result.output


def test_pl_blocks_cli_reports_valid_2010_cache_as_cached_json(monkeypatch, tmp_path) -> None:
    """A cache with all 52 state/territory FIPS codes retains cached JSON reporting."""
    cached_path = tmp_path / "pl_blocks__N2010xK2010.parquet"
    _cached_2010_artifact(cached_path, PL_2010_STATE_FIPS_CODES)
    _point_cli_and_ingest_at(monkeypatch, cached_path)

    result = runner.invoke(app, ["ingest", "pl-blocks", "--decennial", "2010", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["cached"] is True
    assert payload["decennial_vintage"] == "2010"
    assert payload["block_vintage"] == "2010"
    assert payload["total_blocks"] == 1
    assert payload["total_population"] == 37
