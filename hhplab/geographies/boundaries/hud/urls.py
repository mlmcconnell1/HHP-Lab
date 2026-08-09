"""HUD geometry-provider URLs owned by the geography boundary layer."""

from typing import Final

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

__all__ = [
    "HUD_ARCGIS_BASE",
    "HUD_ARCGIS_COC_FEATURE_SERVICE",
    "HUD_ARCGIS_COC_SOURCE_REF",
    "HUD_EXCHANGE_COC_GDB_TEMPLATE",
    "HUD_EXCHANGE_COC_NATIONAL_BOUNDARY_TEMPLATE",
    "HUD_EXCHANGE_COC_STATE_SHAPEFILE_TEMPLATE",
]
