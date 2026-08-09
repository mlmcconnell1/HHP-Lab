"""Compatibility facade for provider-owned source URLs.

New code should import URLs from the owning provider or geography boundary
module. This facade preserves the historical ``hhplab.sources.urls`` API.
"""

from hhplab.geographies.boundaries.census.urls import (  # noqa: F401
    CENSUS_MSA_DELINEATION_FILE_2023,
    CENSUS_TIGER_BASE,
    CENSUS_TIGER_CBSA_TEMPLATE,
    CENSUS_TRACT_RELATIONSHIP_URL,
)
from hhplab.geographies.boundaries.hud.urls import (  # noqa: F401
    HUD_ARCGIS_BASE,
    HUD_ARCGIS_COC_FEATURE_SERVICE,
    HUD_ARCGIS_COC_SOURCE_REF,
    HUD_EXCHANGE_COC_GDB_TEMPLATE,
    HUD_EXCHANGE_COC_NATIONAL_BOUNDARY_TEMPLATE,
    HUD_EXCHANGE_COC_STATE_SHAPEFILE_TEMPLATE,
)
from hhplab.sources.bls.urls import (  # noqa: F401
    BLS_API_REGISTRATION_URL,
    BLS_API_V2,
    BLS_CPI_SOURCE_REF,
    BLS_LAUS_SOURCE_REF,
)
from hhplab.sources.census.urls import (  # noqa: F401
    CENSUS_API_ACS1,
    CENSUS_API_ACS5,
    CENSUS_PEP_DATASETS_BASE,
)
from hhplab.sources.hud.urls import (  # noqa: F401
    HUD_USER_HIC_BASE,
    HUD_USER_HIC_COUNTS_BY_STATE_TEMPLATE,
    HUD_USER_PIT_BASE,
)
from hhplab.sources.medsl.urls import (  # noqa: F401
    MEDSL_COUNTY_PRESIDENTIAL_RETURNS_DATAVERSE_API,
    MEDSL_COUNTY_PRESIDENTIAL_RETURNS_DOI,
)
from hhplab.sources.prism.urls import PRISM_WEB_SERVICE_TEMPLATE  # noqa: F401
from hhplab.sources.vera.urls import (  # noqa: F401
    VERA_INCARCERATION_TRENDS_COUNTY_CSV,
    VERA_INCARCERATION_TRENDS_REPO,
)
from hhplab.sources.zori.urls import ZILLOW_ZORI_COUNTY, ZILLOW_ZORI_ZIP  # noqa: F401

__all__ = [name for name in globals() if name.isupper()]
