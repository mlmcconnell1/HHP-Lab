"""CLI commands for CoC Lab using Typer.

Provides commands for ingesting CoC boundary data, building crosswalks,
computing measures, and visualizing boundaries.
"""

import os
import sys
import warnings
from pathlib import Path
from typing import Annotated

import typer

from coclab.cli.aggregate import aggregate_app
from coclab.cli.build_xwalks import build_xwalks
from coclab.cli.compare_vintages import compare_vintages
from coclab.cli.crosscheck_pit_vintages import validate_pit_vintages
from coclab.cli.crosscheck_population import validate_population
from coclab.cli.diagnostics import diagnostics
from coclab.cli.generate_metro import generate_metro
from coclab.cli.ingest_acs1_metro import ingest_acs1_metro
from coclab.cli.ingest_laus_metro import ingest_laus_metro
from coclab.cli.ingest_acs_population import ingest_acs_population
from coclab.cli.ingest_census import ingest_tiger
from coclab.cli.ingest_nhgis import ingest_nhgis
from coclab.cli.ingest_pit import ingest_pit
from coclab.cli.ingest_pit_vintage import ingest_pit_vintage
from coclab.cli.ingest_tract_relationship import ingest_tract_relationship
from coclab.cli.list_census import list_census
from coclab.cli.list_curated import list_curated
from coclab.cli.list_measures import list_measures
from coclab.cli.list_xwalks import list_xwalks
from coclab.cli.migrate_curated import migrate_curated_cmd
from coclab.cli.panel_diagnostics import panel_diagnostics
from coclab.cli.pep import ingest_pep
from coclab.cli.recipe import (
    recipe_cmd,
    recipe_export_cmd,
    recipe_plan_cmd,
    recipe_preflight_cmd,
    recipe_provenance_cmd,
)
from coclab.cli.registry_rebuild import registry_rebuild
from coclab.cli.show_measures import show_measures
from coclab.cli.status import status_cmd
from coclab.cli.validate_curated import validate_curated_layout_cmd
from coclab.cli.zori import (
    ingest_zori,
    zori_diagnostics,
)

# Suppress known PyArrow warnings on macOS (sysctlbyname failures in sandboxed environments)
# These are harmless warnings about CPU cache detection that don't affect functionality.
warnings.filterwarnings(
    "ignore",
    message=".*sysctlbyname failed.*",
    category=UserWarning,
)


def _is_non_interactive(ctx: typer.Context | None = None) -> bool:
    """Return True when CLI should avoid all interactive prompts."""
    env = os.getenv("COCLAB_NON_INTERACTIVE", "").strip().lower()
    env_true = env in {"1", "true", "yes", "on"}
    argv_flag = "--non-interactive" in sys.argv[1:]

    if ctx is None:
        return bool(env_true or argv_flag)
    obj = ctx.obj if isinstance(ctx.obj, dict) else {}
    return bool(obj.get("non_interactive", False) or env_true or argv_flag)


def _check_working_directory(*, non_interactive: bool = False) -> None:
    """Warn if the current directory doesn't look like the CoC Lab project root."""
    cwd = Path.cwd()
    expected_markers = [
        cwd / "pyproject.toml",
        cwd / "coclab",
        cwd / "data",
    ]
    missing = [p for p in expected_markers if not p.exists()]

    if missing:
        missing_names = ", ".join(p.name for p in missing)
        typer.echo(
            f"Warning: Current directory may not be the CoC Lab project root. "
            f"Missing: {missing_names}",
            err=True,
        )
        if sys.stdin.isatty() and not non_interactive:
            if not typer.confirm("Do you still want to continue?", default=False):
                raise typer.Exit(0)


app = typer.Typer(
    name="coclab",
    help="CoC Lab - Continuum of Care boundary data infrastructure CLI",
    no_args_is_help=True,
)

ingest_app = typer.Typer(
    name="ingest",
    help="Ingest raw and curated datasets",
    no_args_is_help=True,
)
list_app = typer.Typer(
    name="list",
    help="List available datasets",
    no_args_is_help=True,
)
validate_app = typer.Typer(
    name="validate",
    help="Validate datasets and registries",
    no_args_is_help=True,
)
diagnostics_app = typer.Typer(
    name="diagnostics",
    help="Run diagnostics on datasets",
    no_args_is_help=True,
)
migrate_app = typer.Typer(
    name="migrate",
    help="Run data migration utilities",
    no_args_is_help=True,
)
generate_app = typer.Typer(
    name="generate",
    help="Generate crosswalks and metro definitions",
    no_args_is_help=True,
)
build_app = typer.Typer(
    name="build",
    help="Run recipe builds and bundle utilities",
    no_args_is_help=True,
)
show_app = typer.Typer(
    name="show",
    help="Display and visualize data",
    no_args_is_help=True,
)
registry_app = typer.Typer(
    name="registry",
    help="Manage boundary and source registries",
    no_args_is_help=True,
)

AGENTS_INFO_TEXT = """# CoC-Lab Agent Quick Reference

## Automation Defaults

- Prefer machine-readable JSON output when available:
  - `coclab status --json`
  - `coclab build recipe-preflight --recipe <file> --json`
  - `coclab build recipe --recipe <file> --json`
- Use `coclab build recipe-plan --recipe <file> --json` when you need the
  resolved task graph while authoring or debugging a recipe.
- Run non-interactively for automation:
  - `coclab --non-interactive ...`
  - or set `COCLAB_NON_INTERACTIVE=1`
- Validate curated layout policy before/after writes:
  - `coclab validate curated-layout`
- Preview curated migration changes before applying:
  - `coclab migrate curated-layout`
  - `coclab migrate curated-layout --apply`

## Crosswalk Rules: Geography-to-Year Matching

## Core Principle

Every dataset must be matched to the correct geographic vintage on both sides
of the crosswalk. The rules below govern which vintage to use for each source.

## Rules by Data Source

| Data Source | Geography | Crosswalk Rule |
|---|---|---|
| **PIT Counts** | CoC | Direct match; no crosswalk needed. |
| **ACS Estimates** | Census Tracts -> CoC | Use ACS tract vintage, then map to CoC boundary year. |
| **PEP Estimates** | Counties -> CoC | Use the county-to-CoC crosswalk for the PEP estimate year. |
| **ZORI (Zillow)** | Counties -> CoC | Use the county-to-CoC crosswalk for the CoC boundary year. |
| **CHAS** | Census Tracts -> CoC | Follow ACS tract-vintage rule, not CHAS release year. |

## Important Notes

- **CoC boundary reuse:** HUD does not publish new CoC boundaries every year.
  Track which boundary file is *effective* for a given program year, not when
  it was published.
- **ACS decennial transitions:** The tract vintage flips at decennial census
  boundaries with a lag. Hardcode or look up transition years rather than
  assuming the last year of the ACS range equals the tract vintage.
- **Crosswalk weights:** When using areal or population-weighted interpolation
  (tracts -> CoCs), use weights (e.g., decennial block populations) that are
  temporally consistent with the tract vintage, not the data year.
"""


@app.callback()
def main_callback(
    ctx: typer.Context,
    non_interactive: Annotated[
        bool,
        typer.Option(
            "--non-interactive",
            help=(
                "Disable interactive prompts. Can also be enabled with "
                "COCLAB_NON_INTERACTIVE=1."
            ),
        ),
    ] = False,
) -> None:
    """Check working directory before running any command."""
    if not isinstance(ctx.obj, dict):
        ctx.obj = {}
    ctx.obj["non_interactive"] = bool(non_interactive)
    _check_working_directory(non_interactive=_is_non_interactive(ctx))


# -----------------------------------------------------------------------------
# Inline command functions (defined here, registered alphabetically below)
# -----------------------------------------------------------------------------


@app.command(
    "agents",
    help="Information for agents who are using the coclab package.",
)
def agents() -> None:
    """Display automation and crosswalk guidance for agents."""
    typer.echo(AGENTS_INFO_TEXT)


def ingest_boundaries(
    source: Annotated[
        str,
        typer.Option(
            "--source",
            "-s",
            help="Data source: 'hud_exchange' or 'hud_opendata'",
        ),
    ],
    vintage: Annotated[
        str | None,
        typer.Option(
            "--vintage",
            "-v",
            help="Boundary vintage year (e.g., '2025') for hud_exchange source",
        ),
    ] = None,
    snapshot: Annotated[
        str,
        typer.Option(
            "--snapshot",
            help="Snapshot tag for hud_opendata source (default: 'latest')",
        ),
    ] = "latest",
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Force re-ingest even if vintage already exists",
        ),
    ] = False,
) -> None:
    """Ingest CoC boundary data from HUD sources.

    Examples:

        coclab ingest boundaries --source hud_exchange --vintage 2025

        coclab ingest boundaries --source hud_opendata --snapshot latest
    """
    if source == "hud_exchange":
        if vintage is None:
            typer.echo("Error: --vintage is required for hud_exchange source", err=True)
            raise typer.Exit(1)

        from coclab.geo.io import curated_boundary_path
        from coclab.ingest.hud_exchange_gis import ingest_hud_exchange
        from coclab.registry.registry import list_boundaries

        output_path = curated_boundary_path(vintage)
        registered_vintages = [v.boundary_vintage for v in list_boundaries()]
        file_exists = output_path.exists()
        in_registry = vintage in registered_vintages

        if file_exists and in_registry and not force:
            typer.echo(f"Vintage {vintage} already exists at: {output_path}")
            typer.echo("Use --force to re-ingest.")
            raise typer.Exit(0)
        if file_exists and not in_registry and not force:
            typer.echo(
                f"Warning: File exists at {output_path} but not in registry.",
                err=True,
            )
            typer.echo("Re-ingesting to ensure proper registration...")
        if not file_exists and in_registry:
            typer.echo(
                f"Warning: Vintage {vintage} is in registry but file is missing.",
                err=True,
            )
            typer.echo("Re-ingesting...")

        typer.echo(f"Ingesting HUD Exchange CoC boundaries for vintage {vintage}...")
        try:
            output_path = ingest_hud_exchange(vintage, show_progress=True)
            typer.echo(f"Successfully ingested to: {output_path}")
        except Exception as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1) from e

    elif source == "hud_opendata":
        from coclab.ingest.hud_opendata_arcgis import ingest_hud_opendata

        typer.echo(f"Ingesting HUD Open Data CoC boundaries (snapshot: {snapshot})...")
        try:
            output_path = ingest_hud_opendata(snapshot_tag=snapshot)
            typer.echo(f"Successfully ingested to: {output_path}")
        except Exception as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1) from e

    else:
        typer.echo(
            f"Error: Unknown source '{source}'. Use 'hud_exchange' or 'hud_opendata'.",
            err=True,
        )
        raise typer.Exit(1)


def delete_boundaries(
    ctx: typer.Context,
    vintage: Annotated[
        str,
        typer.Argument(help="Boundary vintage year to delete (e.g., '2024')"),
    ],
    source: Annotated[
        str,
        typer.Argument(help="Data source (e.g., 'hud_exchange', 'hud_opendata')"),
    ],
    yes: Annotated[
        bool,
        typer.Option(
            "--yes",
            "-y",
            help="Skip confirmation prompt",
        ),
    ] = False,
) -> None:
    """Delete a boundary vintage from the registry.

    This removes the registry entry and any corresponding source registry entries,
    but does not delete the underlying data file.

    Examples:

        coclab registry delete-entry 2024 hud_exchange

        coclab registry delete-entry 2024 hud_exchange --yes
    """
    from coclab.registry.registry import delete_vintage, list_boundaries
    from coclab.source_registry import delete_by_local_path

    # Check if the entry exists first
    vintages = list_boundaries()
    matching = [v for v in vintages if v.boundary_vintage == vintage and v.source == source]

    if not matching:
        typer.echo(f"No entry found for vintage '{vintage}' with source '{source}'", err=True)
        raise typer.Exit(1)

    entry = matching[0]
    typer.echo(f"Found entry: vintage={vintage}, source={source}, features={entry.feature_count}")

    if not yes:
        if _is_non_interactive(ctx):
            typer.echo(
                "Error: Non-interactive mode requires '--yes' for delete-entry.",
                err=True,
            )
            raise typer.Exit(2)
        confirm = typer.confirm("Are you sure you want to delete this registry entry?")
        if not confirm:
            typer.echo("Aborted.")
            raise typer.Exit(0)

    if delete_vintage(vintage, source):
        typer.echo(f"Deleted registry entry for vintage '{vintage}' from source '{source}'")
        # Clean up source_registry entries by both local_path and curated_path
        source_deleted = delete_by_local_path(str(entry.path))
        from coclab.source_registry import delete_by_curated_path

        source_deleted += delete_by_curated_path(str(entry.path))
        if source_deleted > 0:
            typer.echo(f"Deleted {source_deleted} source registry entry(s) for path '{entry.path}'")
    else:
        typer.echo("Failed to delete entry", err=True)
        raise typer.Exit(1)


def list_boundaries_cmd(
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output structured JSON instead of human-readable text.",
        ),
    ] = False,
) -> None:
    """List all available boundary vintages in the registry."""
    import json

    from coclab.registry.registry import list_boundaries

    vintages = list_boundaries()

    if json_output:
        typer.echo(
            json.dumps(
                {
                    "status": "ok",
                    "vintages": [e.to_dict() for e in vintages],
                },
            )
        )
        return

    if not vintages:
        typer.echo("No vintages registered yet.")
        return

    typer.echo("Available boundary vintages:\n")
    typer.echo(f"{'Vintage':<30} {'Source':<25} {'Features':<10} {'Ingested At'}")
    typer.echo("-" * 85)

    for entry in vintages:
        ingested_str = entry.ingested_at.strftime("%Y-%m-%d %H:%M")
        typer.echo(
            f"{entry.boundary_vintage:<30} {entry.source:<25} "
            f"{entry.feature_count:<10} {ingested_str}"
        )


def check_boundaries(*, json_output: bool = False) -> None:
    """Validate boundary registry health for issues.

    Scans all registry entries for:
    - Paths in temporary directories (may disappear after process exit)
    - Missing boundary files
    - Empty or invalid paths

    Examples:

        coclab validate boundaries
    """
    import json

    from coclab.registry import check_registry_health

    report = check_registry_health()

    if json_output:
        if report.is_healthy:
            typer.echo(json.dumps({"status": "ok", "issues": []}))
        else:
            typer.echo(
                json.dumps(
                    {
                        "status": "error",
                        "message": f"Registry health check found {len(report.issues)} issue(s)",
                        "issues": [
                            {
                                "vintage": i.vintage,
                                "source": i.source,
                                "issue_type": i.issue_type,
                                "message": i.message,
                                "path": i.path,
                            }
                            for i in report.issues
                        ],
                    },
                )
            )
            raise typer.Exit(1)
        return

    typer.echo("Checking boundary registry health...\n")
    typer.echo(str(report))

    if not report.is_healthy:
        typer.echo(
            "\nTo fix issues, use 'coclab registry delete-entry <vintage> <source>' "
            "and re-ingest the boundaries.",
            err=True,
        )
        raise typer.Exit(1)


def validate_boundaries(
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output structured JSON instead of human-readable text.",
        ),
    ] = False,
) -> None:
    """Validate boundary registry health for issues."""
    check_boundaries(json_output=json_output)


def show(
    coc: Annotated[
        str,
        typer.Option(
            "--coc",
            "-c",
            help="CoC identifier (e.g., 'CO-500')",
        ),
    ],
    vintage: Annotated[
        str | None,
        typer.Option(
            "--vintage",
            "-v",
            help="Boundary vintage to use. If not specified, uses the latest.",
        ),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Output path for the HTML map file",
        ),
    ] = None,
) -> None:
    """Render an interactive map for a CoC boundary.

    Examples:

        coclab show map --coc CO-500

        coclab show map --coc CO-500 --vintage 2025
    """
    from coclab.viz.map_folium import render_coc_map

    vintage_display = vintage or "latest"
    typer.echo(f"Rendering map for CoC {coc} (vintage: {vintage_display})...")

    try:
        output_path = render_coc_map(coc_id=coc, vintage=vintage, out_html=output)
        typer.echo(f"Map saved to: {output_path}")
    except FileNotFoundError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e
    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e


def source_status(
    source_type: Annotated[
        str | None,
        typer.Option(
            "--type",
            "-t",
            help="Filter to specific source type (zori, boundary, census_tract, etc.)",
        ),
    ] = None,
    check_changes: Annotated[
        bool,
        typer.Option(
            "--check-changes",
            "-c",
            help="Highlight sources that have changed over time",
        ),
    ] = False,
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output structured JSON instead of human-readable text.",
        ),
    ] = False,
) -> None:
    """Show status of tracked external data sources.

    Displays all registered data source ingestions with their hashes,
    timestamps, and change detection information.

    Examples:

        coclab show sources

        coclab show sources --type zori

        coclab show sources --check-changes
    """
    import json

    from coclab.source_registry import (
        _load_registry,
        detect_upstream_changes,
        summarize_registry,
    )

    if json_output:
        if check_changes:
            changes = detect_upstream_changes()
            if changes.empty:
                typer.echo(json.dumps({"status": "ok", "changes": []}))
            else:
                typer.echo(
                    json.dumps(
                        {
                            "status": "ok",
                            "changes": json.loads(
                                changes.to_json(orient="records", date_format="iso")
                            ),
                        },
                    )
                )
        else:
            df = _load_registry()
            if source_type:
                df = df[df["source_type"] == source_type]
            if df.empty:
                typer.echo(json.dumps({"status": "ok", "sources": []}))
            else:
                typer.echo(
                    json.dumps(
                        {
                            "status": "ok",
                            "sources": json.loads(
                                df.to_json(orient="records", date_format="iso")
                            ),
                        },
                    )
                )
        return

    if check_changes:
        changes = detect_upstream_changes()
        if changes.empty:
            typer.echo("No upstream changes detected. All sources have consistent hashes.")
        else:
            typer.echo("⚠️  UPSTREAM DATA CHANGES DETECTED:\n")
            for _, row in changes.iterrows():
                typer.echo(f"  {row['source_type']}: {row['source_url'][:60]}...")
                typer.echo(f"    Versions seen: {row['hash_count']}")
                typer.echo(f"    First: {row['first_seen']} (hash: {row['first_hash'][:12]}...)")
                typer.echo(f"    Last:  {row['last_seen']} (hash: {row['last_hash'][:12]}...)")
                typer.echo("")
        return

    # Show full summary
    summary = summarize_registry()
    typer.echo(summary)


# -----------------------------------------------------------------------------
# Register all commands alphabetically for consistent help output
# -----------------------------------------------------------------------------

app.command("status")(status_cmd)
app.add_typer(ingest_app, name="ingest")
app.add_typer(list_app, name="list")
app.add_typer(validate_app, name="validate")
app.add_typer(diagnostics_app, name="diagnostics")
app.add_typer(generate_app, name="generate")
app.add_typer(build_app, name="build")
app.add_typer(aggregate_app, name="aggregate")
app.add_typer(show_app, name="show")
app.add_typer(registry_app, name="registry")
app.add_typer(migrate_app, name="migrate")

ingest_app.command("acs1-metro")(ingest_acs1_metro)
ingest_app.command("laus-metro")(ingest_laus_metro)
ingest_app.command("acs5-tract")(ingest_acs_population)
ingest_app.command("boundaries")(ingest_boundaries)
ingest_app.command("tiger")(ingest_tiger)
ingest_app.command("nhgis")(ingest_nhgis)
ingest_app.command("pit")(ingest_pit)
ingest_app.command("pit-vintage")(ingest_pit_vintage)
ingest_app.command("tract-relationship")(ingest_tract_relationship)
ingest_app.command("zori")(ingest_zori)
ingest_app.command("pep")(ingest_pep)
list_app.command("boundaries")(list_boundaries_cmd)
list_app.command("census")(list_census)
list_app.command("curated")(list_curated)
list_app.command("measures")(list_measures)
list_app.command("xwalks")(list_xwalks)
validate_app.command("boundaries")(validate_boundaries)
validate_app.command("pit-vintages")(validate_pit_vintages)
validate_app.command("population")(validate_population)
validate_app.command("curated-layout")(validate_curated_layout_cmd)
diagnostics_app.command("panel")(panel_diagnostics)
diagnostics_app.command("xwalk")(diagnostics)
diagnostics_app.command("zori")(zori_diagnostics)
generate_app.command("xwalks")(build_xwalks)
generate_app.command("metro")(generate_metro)
build_app.command("recipe")(recipe_cmd)
build_app.command("recipe-plan")(recipe_plan_cmd)
build_app.command("recipe-provenance")(recipe_provenance_cmd)
build_app.command("recipe-export")(recipe_export_cmd)
build_app.command("recipe-preflight")(recipe_preflight_cmd)
show_app.command("vintage-diffs")(compare_vintages)
show_app.command("map")(show)
show_app.command("measures")(show_measures)
show_app.command("sources")(source_status)
registry_app.command("delete-entry")(delete_boundaries)
registry_app.command("rebuild")(registry_rebuild)
migrate_app.command("curated-layout")(migrate_curated_cmd)


if __name__ == "__main__":
    app()
