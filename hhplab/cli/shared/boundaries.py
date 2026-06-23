"""CLI commands for CoC boundary registry and ingest workflows."""

import json
import os
import sys
from pathlib import Path
from typing import Annotated

import typer


def _is_non_interactive(ctx: typer.Context | None = None) -> bool:
    """Return True when CLI should avoid all interactive prompts."""
    env = os.getenv("HHPLAB_NON_INTERACTIVE", "").strip().lower()
    env_true = env in {"1", "true", "yes", "on"}
    argv_flag = "--non-interactive" in sys.argv[1:]

    if ctx is None:
        return bool(env_true or argv_flag)
    obj = ctx.obj if isinstance(ctx.obj, dict) else {}
    return bool(obj.get("non_interactive", False) or env_true or argv_flag)


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
    """Ingest CoC boundary data from HUD sources."""
    if source == "hud_exchange":
        if vintage is None:
            typer.echo("Error: --vintage is required for hud_exchange source", err=True)
            raise typer.Exit(1)

        from hhplab.geo.geo_io import curated_boundary_path
        from hhplab.hud import ingest_hud_exchange
        from hhplab.registry.boundary_registry import list_boundaries

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
        from hhplab.hud import ingest_hud_opendata

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
    """Delete a boundary vintage from the registry."""
    from hhplab.registry.boundary_registry import delete_vintage, list_boundaries
    from hhplab.source_registry import delete_by_local_path

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
        source_deleted = delete_by_local_path(str(entry.path))
        from hhplab.source_registry import delete_by_curated_path

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
    from hhplab.registry.boundary_registry import list_boundaries

    vintages = list_boundaries()

    if json_output:
        typer.echo(json.dumps({"status": "ok", "vintages": [e.to_dict() for e in vintages]}))
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
    """Validate boundary registry health for issues."""
    from hhplab.registry import check_registry_health

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
            "\nTo fix issues, use 'hhplab registry delete-entry <vintage> <source>' "
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
    """Render an interactive map for a CoC boundary."""
    from hhplab.viz.map_folium import render_coc_map

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
