"""Tests for PL 94-171 block population ingest."""

from __future__ import annotations

import json

import pandas as pd
import pytest
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.schema.columns import PL_BLOCK_POPULATION_COLUMNS
from hhplab.sources.census.census.ingest.pl_block_population import (
    PL_BLOCK_API_SPECS,
    fetch_pl_block_population,
    get_pl_block_population_output_path,
    ingest_pl_block_population,
)
from hhplab.storage.provenance import read_provenance

pytestmark = pytest.mark.httpx_mock(can_send_already_matched_responses=True)

runner = CliRunner()

BLOCK_RESPONSE_2020 = [
    ["NAME", "P1_001N", "state", "county", "tract", "block"],
    ["Block 7, Census Tract 1, Denver County, Colorado", "42", "8", "31", "1", "7"],
]

BLOCK_RESPONSE_2010 = [
    ["NAME", "P001001", "state", "county", "tract", "block"],
    [
        "Block 0007, Census Tract 000100, Denver County, Colorado",
        "37",
        "08",
        "031",
        "000100",
        "0007",
    ],
]


def test_fetch_pl_block_population_parses_2020_response(httpx_mock) -> None:
    """2020 PL block responses preserve padded block, tract, county, and state IDs."""
    httpx_mock.add_response(json=BLOCK_RESPONSE_2020)

    df, digest, content_size = fetch_pl_block_population("2020", state_fips_codes=("08",))

    assert list(df.columns) == list(PL_BLOCK_POPULATION_COLUMNS)
    assert df.loc[0, "block_geoid"] == "080310000010007"
    assert df.loc[0, "tract_geoid"] == "08031000001"
    assert df.loc[0, "county_fips"] == "08031"
    assert df.loc[0, "state_fips"] == "08"
    assert df.loc[0, "total_population"] == 42
    assert df.loc[0, "block_vintage"] == "2020"
    assert df.loc[0, "decennial_vintage"] == "2020"
    assert df.loc[0, "data_source"] == "census_pl_94_171"
    assert df.loc[0, "source_ref"].endswith(":P1_001N")
    assert digest
    assert content_size > 0


def test_fetch_pl_block_population_uses_2010_pl_variable(httpx_mock) -> None:
    """2010 PL block ingest uses P001001 rather than tract-level SF1 denominators."""
    httpx_mock.add_response(json=BLOCK_RESPONSE_2010)

    df, _digest, _content_size = fetch_pl_block_population("2010", state_fips_codes=("08",))

    assert df.loc[0, "block_geoid"] == "080310001000007"
    assert df.loc[0, "total_population"] == 37
    assert df.loc[0, "source_ref"] == f"{PL_BLOCK_API_SPECS['2010'][0]}:P001001"


def test_fetch_pl_block_population_passes_api_key(httpx_mock, monkeypatch) -> None:
    """PL block requests pass through CENSUS_API_KEY when configured."""
    monkeypatch.setenv("CENSUS_API_KEY", "test-census-key")
    httpx_mock.add_response(json=BLOCK_RESPONSE_2020)

    fetch_pl_block_population("2020", state_fips_codes=("08",))

    request = httpx_mock.get_requests()[0]
    assert request.url.params["key"] == "test-census-key"


def test_fetch_pl_block_population_missing_key_error_is_actionable(httpx_mock) -> None:
    """Census missing-key redirects name the --api-key/CENSUS_API_KEY fix."""
    httpx_mock.add_response(
        status_code=302,
        headers={"location": "https://api.census.gov/data/missing_key.html"},
    )

    with pytest.raises(ValueError, match="CENSUS_API_KEY not set"):
        fetch_pl_block_population("2020", state_fips_codes=("08",))


def test_fetch_pl_block_population_rejects_unsupported_vintage() -> None:
    """Unsupported PL block vintages fail with an actionable message."""
    with pytest.raises(ValueError, match="Supported vintages: 2010, 2020"):
        fetch_pl_block_population("2000", state_fips_codes=("08",))


def test_ingest_pl_block_population_writes_schema_and_provenance(monkeypatch, tmp_path) -> None:
    """Ingest writes the canonical PL block schema with embedded provenance."""
    frame = pd.DataFrame(
        [
            {
                "block_geoid": "080310001000007",
                "state_fips": "08",
                "county_fips": "08031",
                "tract_geoid": "08031000100",
                "block_vintage": "2020",
                "decennial_vintage": "2020",
                "total_population": 42,
                "data_source": "census_pl_94_171",
                "source_ref": "https://api.census.gov/data/2020/dec/pl:P1_001N",
                "ingested_at": "2026-05-31T00:00:00Z",
            }
        ],
        columns=PL_BLOCK_POPULATION_COLUMNS,
    )
    monkeypatch.setattr(
        "hhplab.sources.census.census.ingest.pl_block_population.fetch_pl_block_population",
        lambda decennial_vintage, api_key=None: (frame, "abc123", 123),
    )

    output = ingest_pl_block_population("2020", output_dir=tmp_path)

    assert output == tmp_path / "pl_blocks__N2020xK2020.parquet"
    roundtrip = pd.read_parquet(output)
    assert list(roundtrip.columns) == list(PL_BLOCK_POPULATION_COLUMNS)
    provenance = read_provenance(output)
    assert provenance is not None
    assert provenance.extra["dataset_type"] == "pl_block_population"
    assert provenance.extra["decennial_vintage"] == "2020"
    assert provenance.extra["block_vintage"] == "2020"
    assert provenance.extra["denominator_source"] == "pl_94_171_block_population"
    assert provenance.extra["content_sha256"] == "abc123"


def test_ingest_pl_block_population_rejects_cross_vintage_blocks() -> None:
    """PL block denominators are native to their own decennial block vintage."""
    with pytest.raises(ValueError, match="native to their decennial block era"):
        ingest_pl_block_population("2020", block_vintage="2010")


def test_pl_blocks_cli_cached_json(monkeypatch, tmp_path) -> None:
    """Cached PL block CLI runs emit machine-readable JSON."""
    cached_path = get_pl_block_population_output_path("2020", base_dir=tmp_path)
    cached_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "block_geoid": "080310001000007",
                "state_fips": "08",
                "county_fips": "08031",
                "tract_geoid": "08031000100",
                "block_vintage": "2020",
                "decennial_vintage": "2020",
                "total_population": 42,
                "data_source": "census_pl_94_171",
                "source_ref": "https://api.census.gov/data/2020/dec/pl:P1_001N",
                "ingested_at": "2026-05-31T00:00:00Z",
            }
        ],
        columns=PL_BLOCK_POPULATION_COLUMNS,
    ).to_parquet(cached_path, index=False)
    monkeypatch.setattr(
        "hhplab.cli.ingest.pl_block_population.get_pl_block_population_output_path",
        lambda decennial, blocks=None: cached_path,
    )

    result = runner.invoke(app, ["ingest", "pl-blocks", "--decennial", "2020", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["cached"] is True
    assert payload["total_blocks"] == 1
    assert payload["total_population"] == 42


def test_pl_blocks_cli_fresh_json(monkeypatch, tmp_path) -> None:
    """Fresh PL block CLI runs call ingest and emit machine-readable JSON."""
    output_path = get_pl_block_population_output_path("2010", base_dir=tmp_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
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
                "source_ref": "https://api.census.gov/data/2010/dec/pl:P001001",
                "ingested_at": "2026-05-31T00:00:00Z",
            }
        ],
        columns=PL_BLOCK_POPULATION_COLUMNS,
    ).to_parquet(output_path, index=False)
    monkeypatch.setattr(
        "hhplab.cli.ingest.pl_block_population.get_pl_block_population_output_path",
        lambda decennial, blocks=None: tmp_path / "missing.parquet",
    )
    monkeypatch.setattr(
        "hhplab.sources.census.census.ingest.pl_block_population.ingest_pl_block_population",
        lambda decennial, block_vintage=None, force=False, api_key=None: output_path,
    )

    result = runner.invoke(app, ["ingest", "pl-blocks", "--decennial", "2010", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["cached"] is False
    assert payload["decennial_vintage"] == "2010"
    assert payload["block_vintage"] == "2010"
    assert payload["total_population"] == 37
