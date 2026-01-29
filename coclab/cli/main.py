"""CLI commands for CoC Lab using Typer.

Provides commands for ingesting CoC boundary data, building crosswalks,
computing measures, and visualizing boundaries.
"""

import warnings
from functools import wraps
from pathlib import Path
from typing import Annotated

import typer

from coclab.cli.build_measures import build_measures
from coclab.cli.build_panel import DEFAULT_ZORI_MIN_COVERAGE, build_panel_cmd
from coclab.cli.build_xwalks import build_xwalks
from coclab.cli.compare_vintages import compare_vintages
from coclab.cli.crosscheck_pit_vintages import crosscheck_pit_vintages, validate_pit_vintages
from coclab.cli.crosscheck_population import crosscheck_population, validate_population
from coclab.cli.diagnostics import diagnostics
from coclab.cli.export_bundle import export_bundle
from coclab.cli.ingest_acs_population import ingest_acs_population
from coclab.cli.ingest_census import ingest_census
from coclab.cli.ingest_nhgis import ingest_nhgis
from coclab.cli.ingest_pit import ingest_pit
from coclab.cli.ingest_pit_vintage import ingest_pit_vintage
from coclab.cli.ingest_tract_relationship import ingest_tract_relationship
from coclab.cli.list_census import list_census
from coclab.cli.list_measures import list_measures
from coclab.cli.list_xwalks import list_xwalks
from coclab.cli.panel_diagnostics import panel_diagnostics
from coclab.cli.registry_rebuild import registry_rebuild
from coclab.cli.show_measures import show_measures
from coclab.cli.zori import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_RAW_DIR,
    aggregate_zori,
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


def _check_working_directory() -> None:
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
ingest_app = typer.Typer(
    name="ingest",
    help="Ingest raw and curated datasets",
    no_args_is_help=True,
)
list_app = typer.Typer(
    name="list",
    help="List available datasets and artifacts",
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
build_app = typer.Typer(
    name="build",
    help="Build datasets and bundles",
    no_args_is_help=True,
)


@app.callback()
def main_callback() -> None:
    """Check working directory before running any command."""
    _check_working_directory()


# -----------------------------------------------------------------------------
# Inline command functions (defined here, registered alphabetically below)
# -----------------------------------------------------------------------------


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

        coclab ingest-boundaries --source hud_exchange --vintage 2025

        coclab ingest-boundaries --source hud_opendata --snapshot latest
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


@wraps(ingest_boundaries)
def ingest_boundaries_deprecated(
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
    """Deprecated: use `coclab ingest boundaries`."""
    typer.echo(
        "Warning: 'coclab ingest-boundaries' is deprecated; "
        "use 'coclab ingest boundaries' instead.",
        err=True,
    )
    ingest_boundaries(
        source=source,
        vintage=vintage,
        snapshot=snapshot,
        force=force,
    )


@wraps(ingest_acs_population)
def ingest_acs_population_deprecated(
    acs: Annotated[
        str,
        typer.Option(
            "--acs",
            "-a",
            help="ACS 5-year estimate vintage (e.g., '2019-2023').",
        ),
    ],
    tracts: Annotated[
        str,
        typer.Option(
            "--tracts",
            "-t",
            help="Census tract vintage (e.g., '2023').",
        ),
    ],
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Re-ingest even if cached file exists.",
        ),
    ] = False,
    translate: Annotated[
        bool,
        typer.Option(
            "--translate/--no-translate",
            help="Auto-translate from 2010 to 2020 tract geography if needed.",
        ),
    ] = True,
) -> None:
    """Deprecated: use `coclab ingest acs-population`."""
    typer.echo(
        "Warning: 'coclab ingest-acs-population' is deprecated; "
        "use 'coclab ingest acs-population' instead.",
        err=True,
    )
    ingest_acs_population(
        acs=acs,
        tracts=tracts,
        force=force,
        translate=translate,
    )


@wraps(ingest_census)
def ingest_census_deprecated(
    year: Annotated[
        int,
        typer.Option(
            "--year",
            "-y",
            help="TIGER vintage year (e.g., 2023).",
        ),
    ] = 2023,
    type_: Annotated[
        str,
        typer.Option(
            "--type",
            "-t",
            help="What to download: 'tracts', 'counties', or 'all'.",
        ),
    ] = "all",
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Re-download even if file already exists.",
        ),
    ] = False,
) -> None:
    """Deprecated: use `coclab ingest census`."""
    typer.echo(
        "Warning: 'coclab ingest-census' is deprecated; use 'coclab ingest census' instead.",
        err=True,
    )
    ingest_census(
        year=year,
        type_=type_,
        force=force,
    )


@wraps(ingest_nhgis)
def ingest_nhgis_deprecated(
    years: Annotated[
        list[int],
        typer.Option(
            "--year",
            "-y",
            help="Census year(s) to download (2010, 2020). Can specify multiple.",
        ),
    ],
    geo_type: Annotated[
        str,
        typer.Option(
            "--type",
            "-t",
            help="Geography type(s) to download: 'tracts', 'counties', or 'all'.",
        ),
    ] = "all",
    api_key: Annotated[
        str | None,
        typer.Option(
            "--api-key",
            envvar="IPUMS_API_KEY",
            help="IPUMS API key. Can also set IPUMS_API_KEY environment variable.",
        ),
    ] = None,
    poll_interval: Annotated[
        int,
        typer.Option(
            "--poll-interval",
            help="Minutes between status checks while waiting for extract.",
        ),
    ] = 2,
    max_wait: Annotated[
        int,
        typer.Option(
            "--max-wait",
            help="Maximum minutes to wait for extract completion.",
        ),
    ] = 60,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Re-download even if file already exists.",
        ),
    ] = False,
) -> None:
    """Deprecated: use `coclab ingest nhgis`."""
    typer.echo(
        "Warning: 'coclab ingest-nhgis' is deprecated; use 'coclab ingest nhgis' instead.",
        err=True,
    )
    ingest_nhgis(
        years=years,
        geo_type=geo_type,  # type: ignore[arg-type]
        api_key=api_key,
        poll_interval=poll_interval,
        max_wait=max_wait,
        force=force,
    )


@wraps(ingest_pit)
def ingest_pit_deprecated(
    year: Annotated[
        int,
        typer.Option(
            "--year",
            "-y",
            help="PIT survey year to ingest (e.g., 2024).",
        ),
    ],
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Re-download and re-process even if files exist.",
        ),
    ] = False,
    parse_only: Annotated[
        bool,
        typer.Option(
            "--parse-only",
            help="Skip download if file exists, only parse and process.",
        ),
    ] = False,
) -> None:
    """Deprecated: use `coclab ingest pit`."""
    typer.echo(
        "Warning: 'coclab ingest-pit' is deprecated; use 'coclab ingest pit' instead.",
        err=True,
    )
    ingest_pit(
        year=year,
        force=force,
        parse_only=parse_only,
    )


@wraps(ingest_pit_vintage)
def ingest_pit_vintage_deprecated(
    vintage: Annotated[
        int,
        typer.Option(
            "--vintage",
            "-v",
            help="PIT vintage year to ingest (e.g., 2024). This is the release year.",
        ),
    ],
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Re-download and re-process even if files exist.",
        ),
    ] = False,
    parse_only: Annotated[
        bool,
        typer.Option(
            "--parse-only",
            help="Skip download if file exists, only parse and process.",
        ),
    ] = False,
) -> None:
    """Deprecated: use `coclab ingest pit-vintage`."""
    typer.echo(
        "Warning: 'coclab ingest-pit-vintage' is deprecated; "
        "use 'coclab ingest pit-vintage' instead.",
        err=True,
    )
    ingest_pit_vintage(
        vintage=vintage,
        force=force,
        parse_only=parse_only,
    )


@wraps(ingest_tract_relationship)
def ingest_tract_relationship_deprecated(
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Re-download even if file already exists.",
        ),
    ] = False,
) -> None:
    """Deprecated: use `coclab ingest tract-relationship`."""
    typer.echo(
        "Warning: 'coclab ingest-tract-relationship' is deprecated; "
        "use 'coclab ingest tract-relationship' instead.",
        err=True,
    )
    ingest_tract_relationship(force=force)


@wraps(ingest_zori)
def ingest_zori_deprecated(
    geography: Annotated[
        str,
        typer.Option(
            "--geography",
            "-g",
            help="Geography level: 'county' or 'zip' (county recommended for v1).",
        ),
    ] = "county",
    url: Annotated[
        str | None,
        typer.Option(
            "--url",
            help="Override download URL for ZORI data.",
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Re-download and reprocess even if cached.",
        ),
    ] = False,
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            "-o",
            help="Output directory for curated parquet.",
        ),
    ] = DEFAULT_OUTPUT_DIR,
    raw_dir: Annotated[
        Path,
        typer.Option(
            "--raw-dir",
            help="Directory for raw downloads.",
        ),
    ] = DEFAULT_RAW_DIR,
    start: Annotated[
        str | None,
        typer.Option(
            "--start",
            help="Filter to dates >= start (YYYY-MM-DD). Does not truncate raw archive.",
        ),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option(
            "--end",
            help="Filter to dates <= end (YYYY-MM-DD). Does not truncate raw archive.",
        ),
    ] = None,
) -> None:
    """Deprecated: use `coclab ingest zori`."""
    typer.echo(
        "Warning: 'coclab ingest-zori' is deprecated; use 'coclab ingest zori' instead.",
        err=True,
    )
    ingest_zori(
        geography=geography,
        url=url,
        force=force,
        output_dir=output_dir,
        raw_dir=raw_dir,
        start=start,
        end=end,
    )


def delete_boundaries(
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

        coclab delete-boundaries 2024 hud_exchange

        coclab delete-boundaries 2024 hud_exchange --yes
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
        confirm = typer.confirm("Are you sure you want to delete this registry entry?")
        if not confirm:
            typer.echo("Aborted.")
            raise typer.Exit(0)

    if delete_vintage(vintage, source):
        typer.echo(f"Deleted registry entry for vintage '{vintage}' from source '{source}'")
        # Also clean up source_registry entries that reference the same path
        source_deleted = delete_by_local_path(str(entry.path))
        if source_deleted > 0:
            typer.echo(f"Deleted {source_deleted} source registry entry(s) for path '{entry.path}'")
    else:
        typer.echo("Failed to delete entry", err=True)
        raise typer.Exit(1)


def list_boundaries_cmd() -> None:
    """List all available boundary vintages in the registry."""
    from coclab.registry.registry import list_boundaries

    vintages = list_boundaries()

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


@wraps(list_boundaries_cmd)
def list_boundaries_deprecated() -> None:
    """Deprecated: use `coclab list boundaries`."""
    typer.echo(
        "Warning: 'coclab list-boundaries' is deprecated; "
        "use 'coclab list boundaries' instead.",
        err=True,
    )
    list_boundaries_cmd()


@wraps(list_census)
def list_census_deprecated(
    census_type: Annotated[
        str | None,
        typer.Option(
            "--type",
            "-t",
            help="Filter by census type: 'counties' or 'tracts'.",
        ),
    ] = None,
    directory: Annotated[
        Path,
        typer.Option(
            "--dir",
            "-d",
            help="Directory to scan for census files.",
        ),
    ] = Path("data/curated/census"),
) -> None:
    """Deprecated: use `coclab list census`."""
    typer.echo(
        "Warning: 'coclab list-census' is deprecated; "
        "use 'coclab list census' instead.",
        err=True,
    )
    list_census(census_type=census_type, directory=directory)


@wraps(list_measures)
def list_measures_deprecated(
    dir: Annotated[
        Path,
        typer.Option(
            "--dir",
            "-d",
            help="Directory to scan for measure files.",
        ),
    ] = Path("data/curated/measures"),
) -> None:
    """Deprecated: use `coclab list measures`."""
    typer.echo(
        "Warning: 'coclab list-measures' is deprecated; "
        "use 'coclab list measures' instead.",
        err=True,
    )
    list_measures(dir=dir)


@wraps(list_xwalks)
def list_xwalks_deprecated(
    xwalk_type: Annotated[
        str,
        typer.Option(
            "--type",
            "-t",
            help="Filter by crosswalk type: 'tract', 'county', or 'all'.",
        ),
    ] = "all",
    directory: Annotated[
        Path,
        typer.Option(
            "--dir",
            "-d",
            help="Directory to scan for crosswalk files.",
        ),
    ] = Path("data/curated/xwalks"),
) -> None:
    """Deprecated: use `coclab list xwalks`."""
    typer.echo(
        "Warning: 'coclab list-xwalks' is deprecated; "
        "use 'coclab list xwalks' instead.",
        err=True,
    )
    list_xwalks(xwalk_type=xwalk_type, directory=directory)


def check_boundaries() -> None:
    """Validate boundary registry health for issues.

    Scans all registry entries for:
    - Paths in temporary directories (may disappear after process exit)
    - Missing boundary files
    - Empty or invalid paths

    Examples:

        coclab validate boundaries
    """
    from coclab.registry import check_registry_health

    typer.echo("Checking boundary registry health...\n")
    report = check_registry_health()
    typer.echo(str(report))

    if not report.is_healthy:
        typer.echo(
            "\nTo fix issues, use 'coclab delete-boundaries <vintage> <source>' "
            "and re-ingest the boundaries.",
            err=True,
        )
        raise typer.Exit(1)


def validate_boundaries() -> None:
    """Validate boundary registry health for issues."""
    check_boundaries()


@wraps(validate_boundaries)
def validate_boundaries_deprecated() -> None:
    """Deprecated: use `coclab validate boundaries`."""
    typer.echo(
        "Warning: 'coclab validate-boundaries' is deprecated; "
        "use 'coclab validate boundaries' instead.",
        err=True,
    )
    validate_boundaries()


def check_boundaries_deprecated() -> None:
    """Deprecated: use validate boundaries."""
    typer.echo(
        "Warning: 'coclab check-boundaries' is deprecated; "
        "use 'coclab validate boundaries' instead.",
        err=True,
    )
    check_boundaries()


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

        coclab show --coc CO-500

        coclab show --coc CO-500 --vintage 2025
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
) -> None:
    """Show status of tracked external data sources.

    Displays all registered data source ingestions with their hashes,
    timestamps, and change detection information.

    Examples:

        coclab source-status

        coclab source-status --type zori

        coclab source-status --check-changes
    """
    from coclab.source_registry import (
        detect_upstream_changes,
        summarize_registry,
    )

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


@wraps(validate_pit_vintages)
def validate_pit_vintages_deprecated(
    vintage1: Annotated[
        str,
        typer.Option(
            "--vintage1",
            "-v1",
            help="First (older) PIT vintage to compare.",
        ),
    ],
    vintage2: Annotated[
        str,
        typer.Option(
            "--vintage2",
            "-v2",
            help="Second (newer) PIT vintage to compare.",
        ),
    ],
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Optional: save detailed comparison to CSV.",
        ),
    ] = None,
    show_unchanged: Annotated[
        bool,
        typer.Option(
            "--show-unchanged",
            help="Also show CoC-years with no changes.",
        ),
    ] = False,
    year: Annotated[
        int | None,
        typer.Option(
            "--year",
            "-y",
            help="Filter to a specific PIT year (e.g., 2020).",
        ),
    ] = None,
) -> None:
    """Deprecated: use `coclab validate pit-vintages`."""
    typer.echo(
        "Warning: 'coclab validate-pit-vintages' is deprecated; "
        "use 'coclab validate pit-vintages' instead.",
        err=True,
    )
    validate_pit_vintages(
        vintage1=vintage1,
        vintage2=vintage2,
        output=output,
        show_unchanged=show_unchanged,
        year=year,
    )


@wraps(validate_population)
def validate_population_deprecated(
    boundary: Annotated[
        str | None,
        typer.Option(
            "--boundary",
            "-b",
            help="CoC boundary vintage (e.g., '2025'). Uses latest if not specified.",
        ),
    ] = None,
    acs: Annotated[
        str | None,
        typer.Option(
            "--acs",
            "-a",
            help="ACS 5-year estimate vintage (e.g., '2019-2023'). Uses latest if not specified.",
        ),
    ] = None,
    tracts: Annotated[
        str | None,
        typer.Option(
            "--tracts",
            "-t",
            help="Census tract vintage (e.g., '2023'). Defaults to ACS year.",
        ),
    ] = None,
    xwalk_dir: Annotated[
        Path,
        typer.Option(
            "--xwalk-dir",
            help="Directory containing crosswalk files.",
        ),
    ] = Path("data/curated/xwalks"),
    acs_dir: Annotated[
        Path,
        typer.Option(
            "--acs-dir",
            help="Directory containing ACS tract population files.",
        ),
    ] = Path("data/curated/acs"),
    by_state: Annotated[
        bool,
        typer.Option(
            "--by-state",
            "-s",
            help="Show detailed state-level comparison.",
        ),
    ] = False,
    warn_threshold: Annotated[
        float,
        typer.Option(
            "--warn-threshold",
            "-w",
            help="Warning threshold for CoC/ACS ratio deviation from 1.0 (default: 0.05 = 5%).",
        ),
    ] = 0.05,
) -> None:
    """Deprecated: use `coclab validate population`."""
    typer.echo(
        "Warning: 'coclab validate-population' is deprecated; "
        "use 'coclab validate population' instead.",
        err=True,
    )
    validate_population(
        boundary=boundary,
        acs=acs,
        tracts=tracts,
        xwalk_dir=xwalk_dir,
        acs_dir=acs_dir,
        by_state=by_state,
        warn_threshold=warn_threshold,
    )


@wraps(panel_diagnostics)
def diagnostics_panel_deprecated(
    panel: Annotated[
        Path,
        typer.Option(
            "--panel",
            "-p",
            help="Path to the panel Parquet file to analyze.",
        ),
    ],
    output_dir: Annotated[
        Path | None,
        typer.Option(
            "--output-dir",
            "-o",
            help="Directory to save diagnostic output files.",
        ),
    ] = None,
    format_: Annotated[
        str,
        typer.Option(
            "--format",
            "-f",
            help="Output format: 'text' (summary only), 'csv' (export CSVs).",
        ),
    ] = "text",
) -> None:
    """Deprecated: use `coclab diagnostics panel`."""
    typer.echo(
        "Warning: 'coclab diagnostics-panel' is deprecated; "
        "use 'coclab diagnostics panel' instead.",
        err=True,
    )
    panel_diagnostics(panel=panel, output_dir=output_dir, format_=format_)


@wraps(diagnostics)
def diagnostics_xwalk_deprecated(
    crosswalk: Annotated[
        Path,
        typer.Option(
            "--crosswalk",
            "-x",
            help="Path to crosswalk parquet file.",
        ),
    ],
    coverage_threshold: Annotated[
        float,
        typer.Option(
            "--coverage-threshold",
            help="Coverage threshold for flagging problem CoCs.",
        ),
    ] = 0.95,
    max_contribution: Annotated[
        float,
        typer.Option(
            "--max-contribution",
            help="Max tract contribution threshold for flagging.",
        ),
    ] = 0.8,
    show_problems: Annotated[
        bool,
        typer.Option(
            "--show-problems",
            help="Only show problem CoCs.",
        ),
    ] = False,
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Optional: save diagnostics to CSV file.",
        ),
    ] = None,
) -> None:
    """Deprecated: use `coclab diagnostics xwalk`."""
    typer.echo(
        "Warning: 'coclab diagnostics-xwalk' is deprecated; "
        "use 'coclab diagnostics xwalk' instead.",
        err=True,
    )
    diagnostics(
        crosswalk=crosswalk,
        coverage_threshold=coverage_threshold,
        max_contribution=max_contribution,
        show_problems=show_problems,
        output=output,
    )


@wraps(zori_diagnostics)
def diagnostics_zori_deprecated(
    coc_zori: Annotated[
        Path,
        typer.Option(
            "--coc-zori",
            help="Path to CoC-level ZORI parquet file.",
        ),
    ],
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Optional: save diagnostics to CSV or parquet file.",
        ),
    ] = None,
    coverage_threshold: Annotated[
        float,
        typer.Option(
            "--coverage-threshold",
            help="Threshold for flagging low coverage (default 0.90).",
        ),
    ] = 0.90,
    dominance_threshold: Annotated[
        float,
        typer.Option(
            "--dominance-threshold",
            help="Threshold for flagging high dominance (default 0.80).",
        ),
    ] = 0.80,
) -> None:
    """Deprecated: use `coclab diagnostics zori`."""
    typer.echo(
        "Warning: 'coclab diagnostics-zori' is deprecated; "
        "use 'coclab diagnostics zori' instead.",
        err=True,
    )
    zori_diagnostics(
        coc_zori=coc_zori,
        output=output,
        coverage_threshold=coverage_threshold,
        dominance_threshold=dominance_threshold,
    )


@wraps(build_measures)
def aggregate_measures_deprecated(
    boundary: Annotated[
        str | None,
        typer.Option(
            "--boundary",
            "-b",
            help="CoC boundary vintage (e.g., '2025'). Uses latest if not specified.",
        ),
    ] = None,
    acs: Annotated[
        str,
        typer.Option(
            "--acs",
            "-a",
            help="ACS 5-year estimate vintage (e.g., '2019-2023').",
        ),
    ] = "2018-2022",
    tracts: Annotated[
        int | None,
        typer.Option(
            "--tracts",
            "-t",
            help="Census tract vintage for crosswalk. Defaults to same as ACS year.",
        ),
    ] = None,
    weighting: Annotated[
        str,
        typer.Option(
            "--weighting",
            "-w",
            help="Weighting method: 'area' or 'population'.",
        ),
    ] = "area",
    xwalk_dir: Annotated[
        Path,
        typer.Option(
            "--xwalk-dir",
            help="Directory containing crosswalk files.",
        ),
    ] = Path("data/curated/xwalks"),
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            "-o",
            help="Output directory for measure files.",
        ),
    ] = Path("data/curated/measures"),
) -> None:
    """Deprecated: use `coclab build measures`."""
    typer.echo(
        "Warning: 'coclab aggregate-measures' is deprecated; "
        "use 'coclab build measures' instead.",
        err=True,
    )
    build_measures(
        boundary=boundary,
        acs=acs,
        tracts=tracts,
        weighting=weighting,
        xwalk_dir=xwalk_dir,
        output_dir=output_dir,
    )


@wraps(aggregate_zori)
def aggregate_zori_deprecated(
    boundary: Annotated[
        str,
        typer.Option(
            "--boundary",
            "-b",
            help="CoC boundary vintage (e.g., '2025').",
        ),
    ],
    counties: Annotated[
        str,
        typer.Option(
            "--counties",
            "-c",
            help="TIGER county vintage year used by the crosswalk (e.g., '2023').",
        ),
    ],
    acs: Annotated[
        str,
        typer.Option(
            "--acs",
            help="ACS 5-year vintage for weights (e.g., '2019-2023').",
        ),
    ],
    geography: Annotated[
        str,
        typer.Option(
            "--geography",
            "-g",
            help="Base geography type. Currently only 'county' is supported.",
        ),
    ] = "county",
    zori_path: Annotated[
        Path | None,
        typer.Option(
            "--zori-path",
            help="Explicit path to curated ZORI parquet file.",
        ),
    ] = None,
    xwalk_path: Annotated[
        Path | None,
        typer.Option(
            "--xwalk-path",
            help="Explicit crosswalk path. If omitted, inferred from boundary and counties.",
        ),
    ] = None,
    weighting: Annotated[
        str,
        typer.Option(
            "--weighting",
            "-w",
            help="Weighting: renter_households, housing_units, population, or equal.",
        ),
    ] = "renter_households",
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            "-o",
            help="Output directory for CoC-level ZORI parquet.",
        ),
    ] = DEFAULT_OUTPUT_DIR,
    to_yearly: Annotated[
        bool,
        typer.Option(
            "--to-yearly",
            help="Also emit a yearly collapsed file.",
        ),
    ] = False,
    yearly_method: Annotated[
        str,
        typer.Option(
            "--yearly-method",
            help="Yearly collapse method: 'pit_january', 'calendar_mean', 'calendar_median'.",
        ),
    ] = "pit_january",
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Recompute outputs even if present.",
        ),
    ] = False,
) -> None:
    """Deprecated: use `coclab build zori`."""
    typer.echo(
        "Warning: 'coclab aggregate-zori' is deprecated; "
        "use 'coclab build zori' instead.",
        err=True,
    )
    aggregate_zori(
        boundary=boundary,
        counties=counties,
        acs=acs,
        geography=geography,
        zori_path=zori_path,
        xwalk_path=xwalk_path,
        weighting=weighting,
        output_dir=output_dir,
        to_yearly=to_yearly,
        yearly_method=yearly_method,
        force=force,
    )


@wraps(build_panel_cmd)
def build_panel_deprecated(
    start: Annotated[
        int,
        typer.Option(
            "--start",
            "-s",
            help="First PIT year to include in the panel (inclusive).",
        ),
    ],
    end: Annotated[
        int,
        typer.Option(
            "--end",
            "-e",
            help="Last PIT year to include in the panel (inclusive).",
        ),
    ],
    weighting: Annotated[
        str,
        typer.Option(
            "--weighting",
            "-w",
            help="Weighting method for ACS measures: 'population' or 'area'.",
        ),
    ] = "population",
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Custom output path for the panel Parquet file.",
        ),
    ] = None,
    include_zori: Annotated[
        bool,
        typer.Option(
            "--include-zori/--no-include-zori",
            help="Include ZORI rent data and compute rent_to_income ratio.",
        ),
    ] = False,
    zori_yearly_path: Annotated[
        Path | None,
        typer.Option(
            "--zori-yearly-path",
            help="Explicit path to yearly ZORI parquet. If omitted, searches defaults.",
        ),
    ] = None,
    zori_min_coverage: Annotated[
        float,
        typer.Option(
            "--zori-min-coverage",
            help="Minimum ZORI coverage ratio for eligibility (0.0-1.0).",
        ),
    ] = DEFAULT_ZORI_MIN_COVERAGE,
) -> None:
    """Deprecated: use `coclab build panel`."""
    typer.echo(
        "Warning: 'coclab build-panel' is deprecated; "
        "use 'coclab build panel' instead.",
        err=True,
    )
    build_panel_cmd(
        start=start,
        end=end,
        weighting=weighting,
        output=output,
        include_zori=include_zori,
        zori_yearly_path=zori_yearly_path,
        zori_min_coverage=zori_min_coverage,
    )


@wraps(build_xwalks)
def build_xwalks_deprecated(
    boundary: Annotated[
        str | None,
        typer.Option(
            "--boundary",
            "-b",
            help="CoC boundary vintage (e.g., '2025'). Uses latest if not specified.",
        ),
    ] = None,
    tracts: Annotated[
        str | None,
        typer.Option(
            "--tracts",
            "-t",
            help="Census tract vintage (e.g., '2023'). Defaults to latest.",
        ),
    ] = None,
    counties: Annotated[
        str | None,
        typer.Option(
            "--counties",
            "-c",
            help="County vintage (e.g., '2023'). Defaults to latest.",
        ),
    ] = None,
    xwalk_type: Annotated[
        str,
        typer.Option(
            "--type",
            help="Crosswalk type to build: 'tracts', 'counties', or 'all'.",
        ),
    ] = "all",
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            "-o",
            help="Output directory for crosswalk files.",
        ),
    ] = Path("data/curated/xwalks"),
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Force rebuild even if outputs already exist.",
        ),
    ] = False,
    population_weights: Annotated[
        bool,
        typer.Option(
            "--population-weights",
            help="Compute population-weighted crosswalks.",
        ),
    ] = False,
    auto_fetch: Annotated[
        bool,
        typer.Option(
            "--auto-fetch",
            help="Auto-fetch missing census/ACS inputs if possible.",
        ),
    ] = False,
) -> None:
    """Deprecated: use `coclab build xwalks`."""
    typer.echo(
        "Warning: 'coclab build-xwalks' is deprecated; "
        "use 'coclab build xwalks' instead.",
        err=True,
    )
    build_xwalks(
        boundary=boundary,
        tracts=tracts,
        counties=counties,
        xwalk_type=xwalk_type,
        output_dir=output_dir,
        force=force,
        population_weights=population_weights,
        auto_fetch=auto_fetch,
    )


@wraps(export_bundle)
def export_bundle_deprecated(
    name: Annotated[
        str,
        typer.Option(
            "--name",
            "-n",
            help="Logical bundle name for metadata and documentation",
        ),
    ],
    out_dir: Annotated[
        Path,
        typer.Option(
            "--out-dir",
            "-o",
            help="Output directory where export-N folders are created",
        ),
    ] = Path("exports"),
    panel: Annotated[
        Path | None,
        typer.Option(
            "--panel",
            "-p",
            help="Explicit panel parquet path (inferred from curated if omitted)",
        ),
    ] = None,
    include: Annotated[
        str,
        typer.Option(
            "--include",
            "-i",
            help="Components to include (comma-separated)",
        ),
    ] = "panel,manifest,codebook,diagnostics",
    boundary_vintage: Annotated[
        str | None,
        typer.Option(
            "--boundary-vintage",
            help="Boundary vintage (e.g., 2025)",
        ),
    ] = None,
    tract_vintage: Annotated[
        str | None,
        typer.Option(
            "--tract-vintage",
            help="Census tract vintage (e.g., 2023)",
        ),
    ] = None,
    county_vintage: Annotated[
        str | None,
        typer.Option(
            "--county-vintage",
            help="County vintage (e.g., 2023)",
        ),
    ] = None,
    acs_vintage: Annotated[
        str | None,
        typer.Option(
            "--acs-vintage",
            help="ACS vintage (e.g., 2019-2023)",
        ),
    ] = None,
    years: Annotated[
        str | None,
        typer.Option(
            "--years",
            help="Year range (e.g., 2011-2024)",
        ),
    ] = None,
    copy_mode: Annotated[
        str,
        typer.Option(
            "--copy-mode",
            help="File copy mode: copy, hardlink, or symlink",
        ),
    ] = "copy",
    compress: Annotated[
        bool,
        typer.Option(
            "--compress",
            help="Create .tar.gz archive of the bundle",
        ),
    ] = False,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Create bundle even if identical manifest exists",
        ),
    ] = False,
) -> None:
    """Deprecated: use `coclab build export`."""
    typer.echo(
        "Warning: 'coclab export-bundle' is deprecated; "
        "use 'coclab build export' instead.",
        err=True,
    )
    export_bundle(
        name=name,
        out_dir=out_dir,
        panel=panel,
        include=include,
        boundary_vintage=boundary_vintage,
        tract_vintage=tract_vintage,
        county_vintage=county_vintage,
        acs_vintage=acs_vintage,
        years=years,
        copy_mode=copy_mode,
        compress=compress,
        force=force,
    )


# -----------------------------------------------------------------------------
# Register all commands alphabetically for consistent help output
# -----------------------------------------------------------------------------

app.command("aggregate-measures", hidden=True)(aggregate_measures_deprecated)
app.command("aggregate-zori", hidden=True)(aggregate_zori_deprecated)
app.command("build-panel", hidden=True)(build_panel_deprecated)
app.command("build-xwalks", hidden=True)(build_xwalks_deprecated)
app.command("check-boundaries", hidden=True)(check_boundaries_deprecated)
app.command("compare-vintages")(compare_vintages)
app.command("crosscheck-pit-vintages", hidden=True)(crosscheck_pit_vintages)
app.command("crosscheck-population", hidden=True)(crosscheck_population)
app.command("delete-boundaries")(delete_boundaries)
app.command("diagnostics-panel", hidden=True)(diagnostics_panel_deprecated)
app.command("diagnostics-xwalk", hidden=True)(diagnostics_xwalk_deprecated)
app.command("diagnostics-zori", hidden=True)(diagnostics_zori_deprecated)
app.command("export-bundle", hidden=True)(export_bundle_deprecated)
app.add_typer(ingest_app, name="ingest")
app.add_typer(list_app, name="list")
app.add_typer(validate_app, name="validate")
app.add_typer(diagnostics_app, name="diagnostics")
app.add_typer(build_app, name="build")
app.command("ingest-acs-population", hidden=True)(ingest_acs_population_deprecated)
app.command("ingest-boundaries", hidden=True)(ingest_boundaries_deprecated)
app.command("ingest-census", hidden=True)(ingest_census_deprecated)
app.command("ingest-nhgis", hidden=True)(ingest_nhgis_deprecated)
app.command("ingest-pit", hidden=True)(ingest_pit_deprecated)
app.command("ingest-pit-vintage", hidden=True)(ingest_pit_vintage_deprecated)
app.command("ingest-tract-relationship", hidden=True)(ingest_tract_relationship_deprecated)
app.command("ingest-zori", hidden=True)(ingest_zori_deprecated)
app.command("list-boundaries", hidden=True)(list_boundaries_deprecated)
app.command("list-census", hidden=True)(list_census_deprecated)
app.command("list-measures", hidden=True)(list_measures_deprecated)
app.command("list-xwalks", hidden=True)(list_xwalks_deprecated)
app.command("registry-rebuild")(registry_rebuild)
app.command("show")(show)
app.command("show-measures")(show_measures)
app.command("source-status")(source_status)
app.command("validate-boundaries", hidden=True)(validate_boundaries_deprecated)
app.command("validate-pit-vintages", hidden=True)(validate_pit_vintages_deprecated)
app.command("validate-population", hidden=True)(validate_population_deprecated)

ingest_app.command("acs-population")(ingest_acs_population)
ingest_app.command("boundaries")(ingest_boundaries)
ingest_app.command("census")(ingest_census)
ingest_app.command("nhgis")(ingest_nhgis)
ingest_app.command("pit")(ingest_pit)
ingest_app.command("pit-vintage")(ingest_pit_vintage)
ingest_app.command("tract-relationship")(ingest_tract_relationship)
ingest_app.command("zori")(ingest_zori)
list_app.command("boundaries")(list_boundaries_cmd)
list_app.command("census")(list_census)
list_app.command("measures")(list_measures)
list_app.command("xwalks")(list_xwalks)
validate_app.command("boundaries")(validate_boundaries)
validate_app.command("pit-vintages")(validate_pit_vintages)
validate_app.command("population")(validate_population)
diagnostics_app.command("panel")(panel_diagnostics)
diagnostics_app.command("xwalk")(diagnostics)
diagnostics_app.command("zori")(zori_diagnostics)
build_app.command("measures")(build_measures)
build_app.command("zori")(aggregate_zori)
build_app.command("panel")(build_panel_cmd)
build_app.command("xwalks")(build_xwalks)
build_app.command("export")(export_bundle)


if __name__ == "__main__":
    app()
