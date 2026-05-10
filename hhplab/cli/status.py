"""CLI command for one-shot environment readiness report."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from hhplab.status import collect_status_report


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
            help="Asset store root directory to scan for curated assets.",
        ),
    ] = Path("data"),
    output_root: Annotated[
        Path | None,
        typer.Option(
            "--output-root",
            help="Recipe output root directory to scan.",
        ),
    ] = None,
) -> None:
    """One-shot environment readiness report.

    Scans curated assets, recipe output namespaces, and common
    prerequisites to provide a consolidated view of environment health.
    Returns non-zero exit code when required prerequisites are missing.

    Examples:

        hhplab status

        hhplab status --json

        hhplab status --data-dir /path/to/data
    """
    payload = collect_status_report(
        data_dir=data_dir,
        output_root=output_root,
        project_root=Path.cwd(),
    )
    assets = payload["assets"]
    recipe_outputs = payload["recipe_outputs"]
    guidance = payload["guidance"]
    issues = payload["issues"]
    has_errors = any(i["severity"] == "error" for i in issues)

    if json_output:
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

    metro = assets["metro"]
    typer.echo(
        "Metro Artifacts: "
        f"{len(metro['complete_versions'])} complete version(s)  "
        f"{', '.join(metro['complete_versions']) if metro['complete_versions'] else '-'}"
    )
    metro_boundary_versions = ", ".join(
        f"D{item['definition_version']}xC{item['county_vintage']}"
        for item in metro["boundaries"]
    ) if metro["boundaries"] else "-"
    typer.echo(
        f"Metro Boundaries: {len(metro['boundaries'])} file(s)  {metro_boundary_versions}"
    )

    msa = assets["msa"]
    typer.echo(
        "MSA Artifacts: "
        f"{len(msa['complete_versions'])} complete version(s)  "
        f"{', '.join(msa['complete_versions']) if msa['complete_versions'] else '-'}"
    )
    typer.echo(
        "MSA Boundaries: "
        f"{len(msa['boundaries'])} version(s)  "
        f"{', '.join(msa['boundaries']) if msa['boundaries'] else '-'}"
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

    # Recipe outputs
    typer.echo(
        f"\nRecipe Outputs: {recipe_outputs['count']} namespace(s)  root={recipe_outputs['root']}"
    )
    if recipe_outputs["recipes"]:
        for entry in recipe_outputs["recipes"]:
            typer.echo(
                f"  {entry['name']}: "
                f"{len(entry['panel_files'])} panel(s), "
                f"{len(entry['manifest_files'])} manifest(s), "
                f"{len(entry['diagnostics_files'])} diagnostics file(s), "
                f"{len(entry['map_files'])} map(s)"
            )
    else:
        typer.echo(
            "  No recipe outputs found. "
            "Use 'hhplab build recipe-preflight --recipe <file> --json' "
            "to inspect a recipe, then "
            "'hhplab build recipe --recipe <file> --json' to materialize outputs."
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

    typer.echo("\nRecipe Workflow:")
    typer.echo(f"  Preflight: {guidance['recipe_preflight']}")
    typer.echo(f"  Execute:   {guidance['recipe_execute']}")

    if has_errors:
        raise typer.Exit(1)


def _fmt_years(years: list[int]) -> str:
    """Format a list of years for display."""
    if not years:
        return "-"
    return ", ".join(str(y) for y in years)
