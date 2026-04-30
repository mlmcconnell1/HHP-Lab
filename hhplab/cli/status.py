"""CLI command for one-shot environment readiness report."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from hhplab.builds import (
    DEFAULT_BUILDS_DIR,
    list_builds,
    read_build_manifest,
)


def _count_parquet(directory: Path, pattern: str = "*.parquet") -> int:
    """Count parquet files matching a glob pattern in a directory."""
    if not directory.exists():
        return 0
    return len(list(directory.glob(pattern)))


def _list_parquet_stems(directory: Path, pattern: str = "*.parquet") -> list[str]:
    """Return sorted stem names of parquet files matching pattern."""
    if not directory.exists():
        return []
    return sorted(p.stem for p in directory.glob(pattern))


def _scan_boundaries(curated: Path) -> dict:
    """Scan curated boundary assets."""
    bdir = curated / "coc_boundaries"
    files = _list_parquet_stems(bdir, "coc__B*.parquet")
    vintages = []
    for stem in files:
        # coc__B2024 -> 2024
        parts = stem.split("__B")
        if len(parts) == 2 and parts[1].isdigit():
            vintages.append(int(parts[1]))
    return {"count": len(vintages), "vintages": vintages}


def _scan_census(curated: Path) -> dict:
    """Scan TIGER census geometry files."""
    import re

    tdir = curated / "tiger"
    tracts: list[int] = []
    counties: list[int] = []
    if tdir.exists():
        for p in sorted(tdir.glob("*.parquet")):
            m = re.match(r"^tracts__T?(\d{4})\.parquet$", p.name)
            if m:
                tracts.append(int(m.group(1)))
                continue
            m = re.match(r"^counties__C?(\d{4})\.parquet$", p.name)
            if m:
                counties.append(int(m.group(1)))
    return {"tracts": tracts, "counties": counties}


def _scan_xwalks(curated: Path) -> dict:
    """Scan crosswalk files."""
    import re

    xdir = curated / "xwalks"
    tract_xwalks: list[str] = []
    county_xwalks: list[str] = []
    msa_xwalks: list[str] = []
    if xdir.exists():
        for p in sorted(xdir.glob("*.parquet")):
            m = re.match(r"^xwalk__B(\d{4})xT(\d{4})\.parquet$", p.name)
            if m:
                tract_xwalks.append(f"B{m.group(1)}xT{m.group(2)}")
                continue
            m = re.match(r"^xwalk__B(\d{4})xC(\d{4})\.parquet$", p.name)
            if m:
                county_xwalks.append(f"B{m.group(1)}xC{m.group(2)}")
                continue
            # Legacy formats
            m = re.match(r"^coc_tract_xwalk__(.+?)__(.+?)\.parquet$", p.name)
            if m:
                tract_xwalks.append(f"B{m.group(1)}xT{m.group(2)}")
                continue
            m = re.match(r"^coc_county_xwalk__(.+?)\.parquet$", p.name)
            if m:
                county_xwalks.append(f"B{m.group(1)}")
                continue
            m = re.match(r"^msa_coc_xwalk__B(\d{4})xM(\w+)xC(\d{4})\.parquet$", p.name)
            if m:
                msa_xwalks.append(f"B{m.group(1)}xM{m.group(2)}xC{m.group(3)}")
    return {"tract": tract_xwalks, "county": county_xwalks, "msa": msa_xwalks}


def _scan_pit(curated: Path) -> dict:
    """Scan PIT count files.

    Matches both base (``pit__P2024.parquet``) and boundary-scoped
    (``pit__P2024@B2024.parquet``) files but deduplicates by year so
    the count reflects unique PIT vintages, not file count.
    """
    import re

    pdir = curated / "pit"
    year_set: set[int] = set()
    msa_items: list[dict] = []
    if pdir.exists():
        for p in sorted(pdir.glob("*.parquet")):
            m = re.match(r"^pit__P(\d{4})(?:@B\d{4})?\.parquet$", p.name)
            if m:
                year_set.add(int(m.group(1)))
                continue
            m = re.match(r"^pit__msa__P(\d{4})@M(\w+)xB(\d{4})xC(\d{4})\.parquet$", p.name)
            if m:
                msa_items.append({
                    "year": int(m.group(1)),
                    "definition_version": m.group(2),
                    "boundary_vintage": int(m.group(3)),
                    "county_vintage": int(m.group(4)),
                })
    years = sorted(year_set)
    return {
        "count": len(years),
        "years": years,
        "msa_count": len(msa_items),
        "msa_items": msa_items,
    }


def _scan_msa(curated: Path) -> dict:
    """Scan curated MSA definition and membership artifacts."""
    import re

    mdir = curated / "msa"
    definitions: list[str] = []
    county_memberships: list[str] = []
    if mdir.exists():
        for p in sorted(mdir.glob("*.parquet")):
            m = re.match(r"^msa_definitions__(\w+)\.parquet$", p.name)
            if m:
                definitions.append(m.group(1))
                continue
            m = re.match(r"^msa_county_membership__(\w+)\.parquet$", p.name)
            if m:
                county_memberships.append(m.group(1))
    complete_versions = sorted(set(definitions) & set(county_memberships))
    return {
        "definitions": definitions,
        "county_memberships": county_memberships,
        "complete_versions": complete_versions,
    }


def _scan_measures(curated: Path) -> dict:
    """Scan measures files."""
    import re

    mdir = curated / "measures"
    items: list[str] = []
    if mdir.exists():
        for p in sorted(mdir.glob("*.parquet")):
            m = re.match(r"^measures__A(\d{4})@B(\d{4})(?:xT\d{4})?\.parquet$", p.name)
            if m:
                items.append(f"A{m.group(1)}@B{m.group(2)}")
                continue
            m = re.match(r"^coc_measures__(.+?)__(.+?)\.parquet$", p.name)
            if m:
                items.append(f"B{m.group(1)}/A{m.group(2)}")
    return {"count": len(items), "items": items}


def _scan_acs(curated: Path) -> dict:
    """Scan ACS files."""
    import re

    adir = curated / "acs"
    items: list[str] = []
    if adir.exists():
        for p in sorted(adir.glob("*.parquet")):
            m = re.match(r"^acs5_tracts__A(\d{4})xT(\d{4})\.parquet$", p.name)
            if m:
                items.append(f"A{m.group(1)}xT{m.group(2)}")
    return {"count": len(items), "items": items}


def _scan_zori(curated: Path) -> dict:
    """Scan ZORI files."""
    import re

    zdir = curated / "zori"
    items: list[str] = []
    if zdir.exists():
        for p in sorted(zdir.glob("*.parquet")):
            m = re.match(r"^zori__.*\.parquet$", p.name)
            if m:
                items.append(p.stem)
    return {"count": len(items), "items": items}


def _scan_laus(curated: Path) -> dict:
    """Scan curated BLS LAUS metro yearly files.

    Matches the canonical naming from hhplab.naming.laus_metro_filename
    (``laus_metro__A<year>@D<definition>.parquet``) and reports unique
    (year, definition_version) pairs sorted by year then definition.
    """
    import re

    ldir = curated / "laus"
    items: list[dict] = []
    years_set: set[int] = set()
    if ldir.exists():
        for p in sorted(ldir.glob("*.parquet")):
            m = re.match(r"^laus_metro__A(\d{4})@D(.+)\.parquet$", p.name)
            if m:
                year = int(m.group(1))
                items.append({"year": year, "definition_version": m.group(2)})
                years_set.add(year)
    items.sort(key=lambda i: (i["year"], i["definition_version"]))
    return {"count": len(items), "items": items, "years": sorted(years_set)}


def _scan_builds(builds_dir: Path) -> list[dict]:
    """Scan optional named builds and their manifests."""
    results: list[dict] = []
    for build_path in list_builds(builds_dir):
        entry: dict = {"name": build_path.name, "path": str(build_path)}
        try:
            manifest = read_build_manifest(build_path)
            build_info = manifest.get("build", {})
            entry["years"] = build_info.get("years", [])
            entry["base_assets"] = len(manifest.get("base_assets", []))
            entry["aggregate_runs"] = len(manifest.get("aggregate_runs", []))
            entry["healthy"] = True
        except (FileNotFoundError, json.JSONDecodeError):
            entry["years"] = []
            entry["base_assets"] = 0
            entry["aggregate_runs"] = 0
            entry["healthy"] = False
        results.append(entry)
    return results


def _check_prerequisites(assets: dict) -> list[dict]:
    """Check for common missing prerequisites and return issues."""
    issues: list[dict] = []

    if assets["boundaries"]["count"] == 0:
        issues.append({
            "severity": "error",
            "area": "boundaries",
            "message": "No curated boundary files found.",
            "hint": "Run: hhplab ingest boundaries --source hud_exchange --vintage <YEAR>",
        })

    census = assets["census"]
    if not census["tracts"] and not census["counties"]:
        issues.append({
            "severity": "error",
            "area": "census",
            "message": "No TIGER census geometry files found.",
            "hint": "Run: hhplab ingest tiger --year <YEAR>",
        })

    xwalks = assets["crosswalks"]
    if not xwalks["tract"] and not xwalks["county"]:
        issues.append({
            "severity": "warning",
            "area": "crosswalks",
            "message": "No crosswalk files found.",
            "hint": "Run: hhplab generate xwalks --boundary <YEAR> --census <YEAR>",
        })

    if assets["pit"]["count"] == 0:
        issues.append({
            "severity": "warning",
            "area": "pit",
            "message": "No PIT count files found.",
            "hint": "Run: hhplab ingest pit --year <YEAR>",
        })

    msa = assets["msa"]
    definition_set = set(msa["definitions"])
    membership_set = set(msa["county_memberships"])
    missing_membership = sorted(definition_set - membership_set)
    missing_definitions = sorted(membership_set - definition_set)
    for version in missing_membership:
        issues.append({
            "severity": "warning",
            "area": "msa",
            "message": (
                f"MSA definition version '{version}' is missing county membership artifacts."
            ),
            "hint": f"Run: hhplab generate msa --definition-version {version} --force",
        })
    for version in missing_definitions:
        issues.append({
            "severity": "warning",
            "area": "msa",
            "message": (
                f"MSA county membership version '{version}' is missing definitions artifacts."
            ),
            "hint": f"Run: hhplab generate msa --definition-version {version} --force",
        })

    return issues


def status_cmd(
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output machine-readable JSON instead of human text.",
        ),
    ] = False,
    data_dir: Annotated[
        Path,
        typer.Option(
            "--data-dir",
            help="Root data directory to scan.",
        ),
    ] = Path("data"),
    builds_dir: Annotated[
        Path,
        typer.Option(
            "--builds-dir",
            help="Builds directory to scan.",
        ),
    ] = DEFAULT_BUILDS_DIR,
) -> None:
    """One-shot environment readiness report.

    Scans curated assets, optional named-build inventories, and common
    prerequisites to provide a consolidated view of environment health.
    Returns non-zero exit code when required prerequisites are missing.

    Examples:

        hhplab status

        hhplab status --json

        hhplab status --data-dir /path/to/data
    """
    curated = data_dir / "curated"

    assets = {
        "boundaries": _scan_boundaries(curated),
        "census": _scan_census(curated),
        "crosswalks": _scan_xwalks(curated),
        "pit": _scan_pit(curated),
        "msa": _scan_msa(curated),
        "measures": _scan_measures(curated),
        "acs": _scan_acs(curated),
        "zori": _scan_zori(curated),
        "laus": _scan_laus(curated),
    }

    builds = _scan_builds(builds_dir)
    issues = _check_prerequisites(assets)
    has_errors = any(i["severity"] == "error" for i in issues)
    health = "degraded" if has_errors else ("healthy" if not issues else "ok")

    if json_output:
        payload = {
            "status": health,
            "assets": assets,
            "builds": builds,
            "issues": issues,
        }
        typer.echo(json.dumps(payload, indent=2))
        if has_errors:
            raise typer.Exit(1)
        return

    # Human-readable output
    typer.echo("HHP-Lab Status Report")
    typer.echo("=" * 50)

    # Boundaries
    b = assets["boundaries"]
    typer.echo(f"\nBoundaries: {b['count']} vintage(s)")
    if b["vintages"]:
        typer.echo(f"  Vintages: {', '.join(str(v) for v in b['vintages'])}")

    # Census
    c = assets["census"]
    typer.echo("\nCensus Geometries:")
    typer.echo(f"  Tracts:   {len(c['tracts'])} vintage(s)  {_fmt_years(c['tracts'])}")
    typer.echo(f"  Counties: {len(c['counties'])} vintage(s)  {_fmt_years(c['counties'])}")

    # Crosswalks
    x = assets["crosswalks"]
    typer.echo("\nCrosswalks:")
    tract_list = ", ".join(x["tract"]) if x["tract"] else "-"
    county_list = ", ".join(x["county"]) if x["county"] else "-"
    msa_list = ", ".join(x["msa"]) if x["msa"] else "-"
    typer.echo(f"  Tract:  {len(x['tract'])} file(s)  {tract_list}")
    typer.echo(f"  County: {len(x['county'])} file(s)  {county_list}")
    typer.echo(f"  MSA:    {len(x['msa'])} file(s)  {msa_list}")

    # PIT
    p = assets["pit"]
    typer.echo(f"\nPIT Counts: {p['count']} year(s)  {_fmt_years(p['years'])}")
    msa_pit_versions = ", ".join(
        f"A{item['year']}@M{item['definition_version']}xB{item['boundary_vintage']}xC{item['county_vintage']}"
        for item in p["msa_items"]
    ) if p["msa_items"] else "-"
    typer.echo(f"MSA PIT:    {p['msa_count']} file(s)  {msa_pit_versions}")

    msa = assets["msa"]
    typer.echo(
        "MSA Artifacts: "
        f"{len(msa['complete_versions'])} complete version(s)  "
        f"{', '.join(msa['complete_versions']) if msa['complete_versions'] else '-'}"
    )

    # ACS
    a = assets["acs"]
    typer.echo(f"ACS Tracts: {a['count']} file(s)  {', '.join(a['items']) if a['items'] else '-'}")

    # Measures
    m = assets["measures"]
    typer.echo(f"Measures:   {m['count']} file(s)  {', '.join(m['items']) if m['items'] else '-'}")

    # ZORI
    z = assets["zori"]
    typer.echo(f"ZORI:       {z['count']} file(s)")

    # LAUS
    laus = assets["laus"]
    typer.echo(f"LAUS:       {laus['count']} file(s)  {_fmt_years(laus['years'])}")

    # Builds
    typer.echo(f"\nNamed builds (optional): {len(builds)}")
    for bld in builds:
        health_str = "OK" if bld["healthy"] else "UNHEALTHY"
        years_str = f"years={bld['years']}" if bld["years"] else "no years"
        typer.echo(
            f"  {bld['name']}: {health_str}, {years_str}, "
            f"{bld['base_assets']} base asset(s), "
            f"{bld['aggregate_runs']} aggregate run(s)"
        )

    # Issues
    if issues:
        typer.echo(f"\nIssues ({len(issues)}):")
        for issue in issues:
            marker = "ERROR" if issue["severity"] == "error" else "WARN"
            typer.echo(f"  [{marker}] {issue['message']}")
            typer.echo(f"         {issue['hint']}")
    else:
        typer.echo("\nNo issues found.")

    if has_errors:
        raise typer.Exit(1)


def _fmt_years(years: list[int]) -> str:
    """Format a list of years for display."""
    if not years:
        return "-"
    return ", ".join(str(y) for y in years)
