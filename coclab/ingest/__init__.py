"""Compatibility entrypoints for legacy generic ingest imports.

New source-owned public APIs live under packages such as ``coclab.hud`` and
``coclab.bls``. The legacy ``coclab.ingest`` namespace remains available as a
thin compatibility layer for existing callers and tests.
"""

from coclab.ingest.hud_exchange_gis import ingest_hud_exchange
from coclab.ingest.hud_opendata_arcgis import ingest_hud_opendata

__all__ = ["ingest_hud_exchange", "ingest_hud_opendata"]
