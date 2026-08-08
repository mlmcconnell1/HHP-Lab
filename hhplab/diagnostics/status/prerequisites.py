"""Actionable prerequisite checks for the status report."""

from __future__ import annotations

__all__ = ["check_prerequisites"]


def check_prerequisites(assets: dict) -> list[dict]:
    issues: list[dict] = []

    def add(severity: str, area: str, message: str, hint: str) -> None:
        issues.append({"severity": severity, "area": area, "message": message, "hint": hint})

    if assets["boundaries"]["count"] == 0:
        add(
            "error",
            "boundaries",
            "No curated boundary files found.",
            "Run: hhplab ingest boundaries --source hud_exchange --vintage <YEAR>",
        )
    census = assets["census"]
    if not census["tracts"] and not census["counties"]:
        add(
            "error",
            "census",
            "No TIGER census geometry files found.",
            "Run: hhplab ingest tiger --year <YEAR>",
        )
    crosswalks = assets["crosswalks"]
    if not crosswalks["tract"] and not crosswalks["county"]:
        add(
            "warning",
            "crosswalks",
            "No crosswalk files found.",
            "Run: hhplab generate xwalks --boundary <YEAR> --tracts <YEAR>",
        )
    if assets["pit"]["count"] == 0:
        add("warning", "pit", "No PIT count files found.", "Run: hhplab ingest pit --year <YEAR>")
    if assets["hic"]["count"] == 0:
        add(
            "warning",
            "hic",
            "No HIC count files found.",
            (
                "Place HUD HIC files under data/raw/hic/<YEAR>/, then run "
                "hhplab ingest hic --year <YEAR> --parse-only."
            ),
        )

    msa = assets["msa"]
    definitions = set(msa["definitions"])
    memberships = set(msa["county_memberships"])
    boundaries = set(msa["boundaries"])
    for version in sorted(definitions - memberships):
        add(
            "warning",
            "msa",
            f"MSA definition version '{version}' is missing county membership artifacts.",
            f"Run: hhplab generate msa --definition-version {version} --force",
        )
    for version in sorted(memberships - definitions):
        add(
            "warning",
            "msa",
            f"MSA county membership version '{version}' is missing definitions artifacts.",
            f"Run: hhplab generate msa --definition-version {version} --force",
        )
    for version in sorted(definitions - boundaries):
        add(
            "warning",
            "msa",
            f"MSA definition version '{version}' is missing boundary polygon artifacts.",
            f"Run: hhplab ingest msa-boundaries --definition-version {version} --force",
        )
    for version in sorted(boundaries - definitions):
        add(
            "warning",
            "msa",
            f"MSA boundary version '{version}' is missing definitions artifacts.",
            f"Run: hhplab generate msa --definition-version {version} --force",
        )

    metro = assets["metro"]
    definitions = set(metro["definitions"])
    coc = set(metro["coc_memberships"])
    counties = set(metro["county_memberships"])
    boundaries = set(metro["boundary_versions"])
    for version in sorted(definitions - coc):
        add(
            "warning",
            "metro",
            f"Metro definition version '{version}' is missing CoC membership artifacts.",
            f"Run: hhplab generate metro --definition-version {version} --force",
        )
    for version in sorted(coc - definitions):
        add(
            "warning",
            "metro",
            f"Metro CoC membership version '{version}' is missing definitions artifacts.",
            f"Run: hhplab generate metro --definition-version {version} --force",
        )
    for version in sorted(counties - definitions):
        add(
            "warning",
            "metro",
            f"Metro county membership version '{version}' is missing definitions artifacts.",
            f"Run: hhplab generate metro --definition-version {version} --force",
        )
    for version in sorted(boundaries - definitions):
        add(
            "warning",
            "metro",
            f"Metro boundary version '{version}' is missing definitions artifacts.",
            f"Run: hhplab generate metro --definition-version {version} --force",
        )
    for version in sorted(definitions - counties):
        add(
            "warning",
            "metro",
            f"Metro definition version '{version}' is missing county membership artifacts.",
            f"Run: hhplab generate metro --definition-version {version} --force",
        )
    for version in sorted(definitions - boundaries):
        add(
            "warning",
            "metro",
            f"Metro definition version '{version}' is missing boundary polygon artifacts.",
            (
                f"Run: hhplab generate metro-boundaries --definition-version {version} "
                "--counties <YEAR>"
            ),
        )
    return issues
