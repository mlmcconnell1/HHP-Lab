"""Centralized source URLs and templates for ingest modules."""

from __future__ import annotations

from typing import Final

# Census Bureau
CENSUS_API_ACS5: Final = "https://api.census.gov/data/{year}/acs/acs5"
CENSUS_API_ACS1: Final = "https://api.census.gov/data/{year}/acs/acs1"
CENSUS_TIGER_BASE: Final = "https://www2.census.gov/geo/tiger/TIGER{year}/{layer}/"
CENSUS_TRACT_RELATIONSHIP_URL: Final = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/tract/tab20_tract20_tract10_natl.txt"
)

# Census PEP
CENSUS_PEP_DATASETS_BASE: Final = "https://www2.census.gov/programs-surveys/popest/datasets"

# HUD (CoC boundaries + PIT)
HUD_ARCGIS_BASE: Final = "https://services.arcgis.com/VTyQ9soqVukalItT/ArcGIS/rest/services"
HUD_ARCGIS_COC_FEATURE_SERVICE: Final = (
    f"{HUD_ARCGIS_BASE}/Continuum_of_Care_Grantee_Areas/FeatureServer/0/query"
)
HUD_ARCGIS_COC_SOURCE_REF: Final = (
    "https://hudgis-hud.opendata.arcgis.com/datasets/HUD::continuum-of-care-coc-grantee-areas"
)
HUD_EXCHANGE_COC_GDB_TEMPLATE: Final = (
    "https://files.hudexchange.info/resources/documents/CoC_GIS_NatlTerrDC_Shapefile_{vintage}.zip"
)
HUD_EXCHANGE_COC_NATIONAL_BOUNDARY_TEMPLATE: Final = (
    "https://files.hudexchange.info/resources/documents/CoC_GIS_National_Boundary_{vintage}.zip"
)
HUD_EXCHANGE_COC_STATE_SHAPEFILE_TEMPLATE: Final = (
    "https://files.hudexchange.info/reports/published/CoC_GIS_State_Shapefile_{state}_{vintage}.zip"
)
HUD_USER_PIT_BASE: Final = "https://www.huduser.gov/portal/sites/default/files/xls/"

# Zillow
ZILLOW_ZORI_COUNTY: Final = (
    "https://files.zillowstatic.com/research/public_csvs/zori/County_zori_uc_sfrcondomfr_sm_month.csv"
)
ZILLOW_ZORI_ZIP: Final = (
    "https://files.zillowstatic.com/research/public_csvs/zori/Zip_zori_uc_sfrcondomfr_sm_month.csv"
)
