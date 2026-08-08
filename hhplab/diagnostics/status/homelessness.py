"""Scanners for homelessness source assets."""

from __future__ import annotations

import re
from pathlib import Path

__all__ = ["scan_hic", "scan_pit"]


def scan_pit(curated: Path) -> dict:
    """Scan PIT counts, deduplicating base and boundary-scoped vintages."""
    year_set: set[int] = set()
    msa_items: list[dict] = []
    directory = curated / "pit"
    for path in sorted(directory.glob("*.parquet")) if directory.exists() else []:
        match = re.match(r"^pit__P(\d{4})(?:@B\d{4})?\.parquet$", path.name)
        if match:
            year_set.add(int(match.group(1)))
            continue
        match = re.match(r"^pit__msa__P(\d{4})@M(\w+)xB(\d{4})xC(\d{4})\.parquet$", path.name)
        if match:
            msa_items.append(
                {
                    "year": int(match.group(1)),
                    "definition_version": match.group(2),
                    "boundary_vintage": int(match.group(3)),
                    "county_vintage": int(match.group(4)),
                }
            )
    years = sorted(year_set)
    return {
        "count": len(years),
        "years": years,
        "msa_count": len(msa_items),
        "msa_items": msa_items,
    }


def scan_hic(curated: Path) -> dict:
    """Scan HUD HIC count files by inventory year."""
    years: list[int] = []
    items: list[str] = []
    directory = curated / "hic"
    for path in sorted(directory.glob("*.parquet")) if directory.exists() else []:
        match = re.match(r"^hic__H(\d{4})\.parquet$", path.name)
        if match:
            years.append(int(match.group(1)))
            items.append(path.stem)
    return {"count": len(years), "years": years, "items": items}
