"""Scanners for Census and other non-homelessness source assets."""

from __future__ import annotations

import re
from pathlib import Path

from hhplab.artifacts.curated.schema import validate_curated_schemas

__all__ = ["scan_acs", "scan_laus", "scan_medsl", "scan_measures", "scan_zori"]


def scan_measures(curated: Path) -> dict:
    items: list[str] = []
    for path in (
        sorted((curated / "measures").glob("*.parquet")) if (curated / "measures").exists() else []
    ):
        match = re.match(r"^measures__A(\d{4})@B(\d{4})(?:xT\d{4})?\.parquet$", path.name)
        if match:
            items.append(f"A{match.group(1)}@B{match.group(2)}")
            continue
        match = re.match(r"^coc_measures__(.+?)__(.+?)\.parquet$", path.name)
        if match:
            items.append(f"B{match.group(1)}/A{match.group(2)}")
    return {"count": len(items), "items": items}


def scan_acs(curated: Path) -> dict:
    items: list[str] = []
    directory = curated / "acs"
    for path in sorted(directory.glob("*.parquet")) if directory.exists() else []:
        match = re.match(r"^acs5_tracts__A(\d{4})xT(\d{4})\.parquet$", path.name)
        if match:
            items.append(f"A{match.group(1)}xT{match.group(2)}")
    schema_issues = validate_curated_schemas(curated)
    return {
        "count": len(items),
        "items": items,
        "schema_staleness_count": len(schema_issues),
        "schema_staleness": [issue.to_dict() for issue in schema_issues],
    }


def scan_zori(curated: Path) -> dict:
    directory = curated / "zori"
    items = (
        [
            path.stem
            for path in sorted(directory.glob("*.parquet"))
            if re.match(r"^zori__.*\.parquet$", path.name)
        ]
        if directory.exists()
        else []
    )
    return {"count": len(items), "items": items}


def scan_laus(curated: Path) -> dict:
    """Scan canonical BLS LAUS metro yearly files."""
    items: list[dict] = []
    directory = curated / "laus"
    for path in sorted(directory.glob("*.parquet")) if directory.exists() else []:
        match = re.match(r"^laus_metro__A(\d{4})@D(.+)\.parquet$", path.name)
        if match:
            items.append({"year": int(match.group(1)), "definition_version": match.group(2)})
    items.sort(key=lambda item: (item["year"], item["definition_version"]))
    return {"count": len(items), "items": items, "years": sorted({item["year"] for item in items})}


def _file_status(path: Path) -> dict:
    payload = {"path": str(path), "exists": path.exists()}
    if path.exists():
        payload["file_size"] = path.stat().st_size
    return payload


def scan_medsl(data_dir: Path, curated: Path) -> dict:
    """Scan raw and curated MEDSL presidential artifacts."""
    returns_items: list[dict] = []
    president_county_items: list[dict] = []
    directory = curated / "medsl"
    for path in sorted(directory.glob("*.parquet")) if directory.exists() else []:
        match = re.match(
            r"^medsl_county_presidential_returns__Y(\d{4})-(\d{4})\.parquet$", path.name
        )
        if match:
            returns_items.append(
                {
                    "start_year": int(match.group(1)),
                    "end_year": int(match.group(2)),
                    "path": str(path),
                }
            )
            continue
        match = re.match(r"^medsl_president_county__Y(\d{4})-(\d{4})@C(\d{4})\.parquet$", path.name)
        if match:
            president_county_items.append(
                {
                    "start_year": int(match.group(1)),
                    "end_year": int(match.group(2)),
                    "county_vintage": int(match.group(3)),
                    "path": str(path),
                }
            )
    raw_dir = data_dir / "raw" / "medsl"
    return {
        "raw": {
            "county_presidential_returns": _file_status(raw_dir / "countypres_2000-2024.tab"),
            "state_presidential_returns": _file_status(raw_dir / "1976-2024-president.csv"),
        },
        "curated_returns": returns_items,
        "president_county": president_county_items,
        "curated_count": len(returns_items) + len(president_county_items),
    }
