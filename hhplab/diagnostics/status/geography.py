"""Scanners for boundary, Census geometry, and geography assets."""

from __future__ import annotations

import re
from pathlib import Path

__all__ = ["scan_boundaries", "scan_census", "scan_crosswalks", "scan_metro", "scan_msa"]


def _list_parquet_stems(directory: Path, pattern: str = "*.parquet") -> list[str]:
    if not directory.exists():
        return []
    return sorted(p.stem for p in directory.glob(pattern))


def scan_boundaries(curated: Path) -> dict:
    """Scan curated CoC boundary assets."""
    files = _list_parquet_stems(curated / "coc_boundaries", "coc__B*.parquet")
    vintages = [
        int(parts[1])
        for stem in files
        if len(parts := stem.split("__B")) == 2 and parts[1].isdigit()
    ]
    return {"count": len(vintages), "vintages": vintages}


def scan_census(curated: Path) -> dict:
    """Scan TIGER tract and county geometry files."""
    tracts: list[int] = []
    counties: list[int] = []
    for path in (curated / "tiger").glob("*.parquet") if (curated / "tiger").exists() else []:
        match = re.match(r"^tracts__T?(\d{4})\.parquet$", path.name)
        if match:
            tracts.append(int(match.group(1)))
            continue
        match = re.match(r"^counties__C?(\d{4})\.parquet$", path.name)
        if match:
            counties.append(int(match.group(1)))
    return {"tracts": tracts, "counties": counties}


def scan_crosswalks(curated: Path) -> dict:
    """Scan current and legacy crosswalk filenames."""
    tract: list[str] = []
    county: list[str] = []
    msa: list[str] = []
    directory = curated / "xwalks"
    for path in sorted(directory.glob("*.parquet")) if directory.exists() else []:
        name = path.name
        match = re.match(r"^xwalk__B(\d{4})xT(\d{4})\.parquet$", name)
        if match:
            tract.append(f"B{match.group(1)}xT{match.group(2)}")
            continue
        match = re.match(r"^xwalk__B(\d{4})xC(\d{4})\.parquet$", name)
        if match:
            county.append(f"B{match.group(1)}xC{match.group(2)}")
            continue
        match = re.match(r"^coc_tract_xwalk__(.+?)__(.+?)\.parquet$", name)
        if match:
            tract.append(f"B{match.group(1)}xT{match.group(2)}")
            continue
        match = re.match(r"^coc_county_xwalk__(.+?)\.parquet$", name)
        if match:
            county.append(f"B{match.group(1)}")
            continue
        match = re.match(r"^msa_coc_xwalk__B(\d{4})xM(\w+)xC(\d{4})\.parquet$", name)
        if match:
            msa.append(f"B{match.group(1)}xM{match.group(2)}xC{match.group(3)}")
            continue
        match = re.match(
            r"^msa_coc_xwalk__N(\d{4})@B(\d{4})xM(\w+)xC(\d{4})xK(\d{4})"
            r"__basis-block_population\.parquet$",
            name,
        )
        if match:
            msa.append(
                f"N{match.group(1)}@B{match.group(2)}xM{match.group(3)}"
                f"xC{match.group(4)}xK{match.group(5)}:block_population"
            )
    return {"tract": tract, "county": county, "msa": msa}


def scan_msa(curated: Path) -> dict:
    """Scan curated MSA definitions, memberships, boundaries, and coverage."""
    definitions: list[str] = []
    county_memberships: list[str] = []
    boundaries: list[str] = []
    coverage: list[dict] = []
    directory = curated / "msa"
    for path in sorted(directory.glob("*.parquet")) if directory.exists() else []:
        name = path.name
        match = re.match(r"^msa_definitions__(\w+)\.parquet$", name)
        if match:
            definitions.append(match.group(1))
            continue
        match = re.match(r"^msa_county_membership__(\w+)\.parquet$", name)
        if match:
            county_memberships.append(match.group(1))
            continue
        match = re.match(r"^msa_boundaries__(\w+)\.parquet$", name)
        if match:
            boundaries.append(match.group(1))
            continue
        match = re.match(
            r"^msa_coc_coverage__Y(\d{4})@B(\d{4})xM(\w+)xC(\d{4})__top(\d+)"
            r"__basis-(area|population|area-population)\.parquet$",
            name,
        )
        if match:
            coverage.append(
                {
                    "year": int(match.group(1)),
                    "boundary_vintage": int(match.group(2)),
                    "definition_version": match.group(3),
                    "county_vintage": int(match.group(4)),
                    "top_n": int(match.group(5)),
                    "overlap_bases": match.group(6).split("-"),
                    "path": str(path),
                }
            )
    complete = sorted(set(definitions) & set(county_memberships))
    return {
        "definitions": definitions,
        "county_memberships": county_memberships,
        "boundaries": boundaries,
        "coverage": coverage,
        "coverage_count": len(coverage),
        "complete_versions": complete,
        "fully_materialized_versions": sorted(set(complete) & set(boundaries)),
    }


def scan_metro(curated: Path) -> dict:
    """Scan curated metro definition, membership, and boundary artifacts."""
    definitions: list[str] = []
    coc_memberships: list[str] = []
    county_memberships: list[str] = []
    universes: list[str] = []
    subset_memberships: list[dict] = []
    boundaries: list[dict] = []
    directory = curated / "metro"
    for path in sorted(directory.glob("*.parquet")) if directory.exists() else []:
        name = path.name
        match = re.match(r"^metro_definitions__(\w+)\.parquet$", name)
        if match:
            definitions.append(match.group(1))
            continue
        match = re.match(r"^metro_coc_membership__(\w+)\.parquet$", name)
        if match:
            coc_memberships.append(match.group(1))
            continue
        match = re.match(r"^metro_county_membership__(\w+)\.parquet$", name)
        if match:
            county_memberships.append(match.group(1))
            continue
        match = re.match(r"^metro_universe__(\w+)\.parquet$", name)
        if match:
            universes.append(match.group(1))
            continue
        match = re.match(r"^metro_subset_membership__(\w+)xM(\w+)\.parquet$", name)
        if match:
            subset_memberships.append(
                {
                    "profile_definition_version": match.group(1),
                    "metro_definition_version": match.group(2),
                }
            )
            continue
        match = re.match(r"^metro_boundaries__(\w+)xC(\d{4})\.parquet$", name)
        if match:
            boundaries.append(
                {
                    "definition_version": match.group(1),
                    "county_vintage": int(match.group(2)),
                }
            )
    complete = sorted(set(definitions) & set(coc_memberships) & set(county_memberships))
    return {
        "definitions": definitions,
        "coc_memberships": coc_memberships,
        "county_memberships": county_memberships,
        "universes": universes,
        "subset_memberships": subset_memberships,
        "boundaries": boundaries,
        "boundary_versions": sorted({item["definition_version"] for item in boundaries}),
        "complete_versions": complete,
    }
