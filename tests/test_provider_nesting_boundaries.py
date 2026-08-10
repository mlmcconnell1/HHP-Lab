"""Canonical ownership contracts for formerly doubled provider paths."""

from __future__ import annotations

from importlib import import_module

import pytest

CANONICAL_OWNER_CASES = [
    pytest.param(
        "hhplab.sources.census.api",
        "census_api_credentials_status",
        id="census-api",
    ),
    pytest.param(
        "hhplab.sources.census.ingest.decennial_tract_population",
        "ingest_decennial_tract_population",
        id="census-data-product",
    ),
    pytest.param(
        "hhplab.geographies.boundaries.census.ingest.tiger_tracts",
        "ingest_tiger_tracts",
        id="census-tiger-boundary",
    ),
    pytest.param(
        "hhplab.geographies.boundaries.hud.exchange_gis",
        "ingest_hud_exchange",
        id="hud-boundary",
    ),
    pytest.param(
        "hhplab.sources.medsl.ingest",
        "ingest_county_presidential_returns",
        id="medsl-ingest",
    ),
    pytest.param(
        "hhplab.sources.medsl.materialize",
        "materialize_county_political_leaning",
        id="medsl-materialize",
    ),
]


@pytest.mark.parametrize(("module_name", "symbol"), CANONICAL_OWNER_CASES)
def test_provider_implementation_lives_at_canonical_location(module_name: str, symbol: str) -> None:
    implementation = getattr(import_module(module_name), symbol)

    assert implementation.__module__ == module_name


LEGACY_FACADE_CASES = [
    pytest.param(
        "hhplab.analysis_geo",
        "hhplab.geographies.analysis",
        "resolve_geo_col",
        id="analysis-geography-facade",
    ),
    pytest.param(
        "hhplab.sources.census.census.api",
        "hhplab.sources.census.api",
        "census_api_credentials_status",
        id="census-api-facade",
    ),
    pytest.param(
        "hhplab.sources.hud.hud.exchange_gis",
        "hhplab.geographies.boundaries.hud.exchange_gis",
        "ingest_hud_exchange",
        id="hud-facade",
    ),
    pytest.param(
        "hhplab.sources.medsl.medsl.ingest",
        "hhplab.sources.medsl.ingest",
        "ingest_county_presidential_returns",
        id="medsl-facade",
    ),
    pytest.param(
        "hhplab.sources.census.census.ingest.pl_block_population",
        "hhplab.sources.census.ingest.pl_block_population",
        "ingest_pl_block_population",
        id="census-data-product-facade",
    ),
    pytest.param(
        "hhplab.sources.census.census.ingest.tiger_tracts",
        "hhplab.geographies.boundaries.census.ingest.tiger_tracts",
        "ingest_tiger_tracts",
        id="census-boundary-facade",
    ),
]


@pytest.mark.parametrize(("legacy_module", "canonical_module", "symbol"), LEGACY_FACADE_CASES)
def test_legacy_provider_imports_remain_callable(
    legacy_module: str, canonical_module: str, symbol: str
) -> None:
    legacy = getattr(import_module(legacy_module), symbol)
    canonical = getattr(import_module(canonical_module), symbol)

    assert callable(legacy)
    assert callable(canonical)
