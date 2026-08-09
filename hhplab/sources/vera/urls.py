"""Vera incarceration-trends source URLs."""

from typing import Final

VERA_INCARCERATION_TRENDS_REPO: Final = "https://github.com/vera-institute/incarceration-trends"
VERA_INCARCERATION_TRENDS_COUNTY_CSV: Final = (
    "https://raw.githubusercontent.com/vera-institute/incarceration-trends/"
    "main/incarceration_trends_county.csv"
)

__all__ = [
    "VERA_INCARCERATION_TRENDS_COUNTY_CSV",
    "VERA_INCARCERATION_TRENDS_REPO",
]
