"""Provider URL ownership and compatibility contract tests."""

import pytest

from hhplab.geographies.boundaries.census import urls as census_boundary_urls
from hhplab.geographies.boundaries.hud import urls as hud_boundary_urls
from hhplab.sources import urls as legacy_urls
from hhplab.sources.bls import urls as bls_urls
from hhplab.sources.census import urls as census_urls
from hhplab.sources.hud import urls as hud_urls
from hhplab.sources.medsl import urls as medsl_urls
from hhplab.sources.prism import urls as prism_urls
from hhplab.sources.vera import urls as vera_urls
from hhplab.sources.zori import urls as zori_urls

URL_OWNERS = (
    (census_boundary_urls, "CENSUS_TIGER_BASE"),
    (census_boundary_urls, "CENSUS_TIGER_CBSA_TEMPLATE"),
    (census_boundary_urls, "CENSUS_TRACT_RELATIONSHIP_URL"),
    (census_boundary_urls, "CENSUS_MSA_DELINEATION_FILE_2023"),
    (census_urls, "CENSUS_API_ACS1"),
    (census_urls, "CENSUS_API_ACS5"),
    (census_urls, "CENSUS_PEP_DATASETS_BASE"),
    (hud_boundary_urls, "HUD_ARCGIS_COC_FEATURE_SERVICE"),
    (hud_boundary_urls, "HUD_EXCHANGE_COC_NATIONAL_BOUNDARY_TEMPLATE"),
    (hud_urls, "HUD_USER_PIT_BASE"),
    (hud_urls, "HUD_USER_HIC_COUNTS_BY_STATE_TEMPLATE"),
    (bls_urls, "BLS_API_V2"),
    (zori_urls, "ZILLOW_ZORI_COUNTY"),
    (prism_urls, "PRISM_WEB_SERVICE_TEMPLATE"),
    (medsl_urls, "MEDSL_COUNTY_PRESIDENTIAL_RETURNS_DOI"),
    (vera_urls, "VERA_INCARCERATION_TRENDS_COUNTY_CSV"),
)


@pytest.mark.parametrize(
    "owner, name",
    URL_OWNERS,
    ids=lambda item: getattr(item, "__name__", item),
)
def test_legacy_url_facade_reexports_provider_constant(owner, name):
    assert getattr(legacy_urls, name) == getattr(owner, name)
