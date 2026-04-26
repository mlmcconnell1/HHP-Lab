"""HUD boundary ingestion entrypoints.

The concrete implementations still live in the legacy ``coclab.ingest``
modules, but callers should use this source-owned package instead of the
generic ingest namespace.
"""

from coclab.ingest.hud_exchange_gis import ingest_hud_exchange
from coclab.ingest.hud_opendata_arcgis import ingest_hud_opendata

__all__ = ["ingest_hud_exchange", "ingest_hud_opendata"]
