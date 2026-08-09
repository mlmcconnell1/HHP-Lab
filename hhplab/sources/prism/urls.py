"""PRISM Climate Group web-service URL templates."""

from typing import Final

PRISM_WEB_SERVICE_TEMPLATE: Final = (
    "https://data.prism.oregonstate.edu/time_series/us/an/4km/"
    "{variable}/monthly/{year}/prism_{variable}_us_25m_{year}{month:02d}.zip"
)

__all__ = ["PRISM_WEB_SERVICE_TEMPLATE"]
