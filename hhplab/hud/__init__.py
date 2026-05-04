"""HUD boundary ingestion entrypoints."""

from hhplab.hud.exchange_gis import ingest_hud_exchange
from hhplab.hud.opendata_arcgis import ingest_hud_opendata

__all__ = ["ingest_hud_exchange", "ingest_hud_opendata"]
