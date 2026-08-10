"""Tests for decennial tract population Census API ingest."""

from __future__ import annotations

import pytest

from hhplab.sources.census.ingest.decennial_tract_population import (
    fetch_decennial_tract_population,
)

DECENNIAL_TRACT_RESPONSE_2020 = [
    ["NAME", "P1_001N", "state", "county", "tract"],
    ["Census Tract 1, Test County, Colorado", "37", "08", "031", "000100"],
]


def test_fetch_decennial_tract_population_passes_api_key(httpx_mock, monkeypatch) -> None:
    """Decennial tract requests pass through CENSUS_API_KEY when configured."""
    monkeypatch.setenv("CENSUS_API_KEY", "test-census-key")
    httpx_mock.add_response(json=DECENNIAL_TRACT_RESPONSE_2020)

    df, _digest, _content_size = fetch_decennial_tract_population(
        "2020",
        state_fips_codes=("08",),
    )

    request = httpx_mock.get_requests()[0]
    assert request.url.params["key"] == "test-census-key"
    assert df.loc[0, "tract_geoid"] == "08031000100"
    assert df.loc[0, "total_population"] == 37


def test_fetch_decennial_tract_population_missing_key_error_is_actionable(httpx_mock) -> None:
    """Census missing-key redirects name the CENSUS_API_KEY fix."""
    httpx_mock.add_response(
        status_code=302,
        headers={
            "location": "https://api.census.gov/data/missing_key.html",
            "X-DataWebAPI-KeyError": "1",
        },
    )

    with pytest.raises(ValueError, match="CENSUS_API_KEY not set"):
        fetch_decennial_tract_population("2020", state_fips_codes=("08",))
