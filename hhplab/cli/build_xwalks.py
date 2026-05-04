"""CLI command for building CoC crosswalks."""

from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal

import click
import geopandas as gpd
import typer

from hhplab.measures.measures_diagnostics import (
    compute_crosswalk_diagnostics,
    summarize_diagnostics,
)
from hhplab.paths import curated_dir
from hhplab.registry.boundary_registry import latest_vintage, list_boundaries
from hhplab.xwalks.county import build_coc_county_crosswalk, save_county_crosswalk
from hhplab.xwalks.tract import (
    add_population_weights,
    build_coc_tract_crosswalk,
    save_crosswalk,
    validate_population_shares,
)

if TYPE_CHECKING:
    import pandas as pd

XwalkType = Literal["tracts", "counties", "all"]


def build_xwalks(
    boundary: Annotated[
        str | None,
        typer.Option(
            "--boundary",
            "-b",
            help="CoC boundary vintage (e.g., '2025'). Uses latest if not specified.",
        ),
    ] = None,
    tracts: Annotated[
        int,
        typer.Option(
            "--tracts",
            "-t",
            help="Census tract vintage year (e.g., 2023).",
        ),
    ] = 2023,
    counties: Annotated[
        int | None,
        typer.Option(
            "--counties",
            "-c",
            help="Census county vintage year. Defaults to same as tracts.",
        ),
    ] = None,
    xwalk_type: Annotated[
        XwalkType,
        typer.Option(
            "--type",
            help="Which crosswalks to build: 'tracts', 'counties', or 'all'.",
        ),
    ] = "all",
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Overwrite existing crosswalk files if they already exist.",
        ),
    ] = False,
    population_weights: Annotated[
        bool,
        typer.Option(
            "--population-weights",
            "-p",
            help="Add population-based weighting to tract crosswalk.",
        ),
    ] = False,
    auto_fetch: Annotated[
        bool,
        typer.Option(
            "--auto-fetch",
            help="Auto-fetch population data if not cached (needs --population-weights).",
        ),
    ] = False,
) -> None:
    """Build tract and county crosswalks for CoC boundaries.

    Creates area-weighted crosswalks mapping census tracts and counties
    to CoC boundaries. The crosswalks enable aggregating tract-level
    statistics (like ACS data) to CoC level.

    Examples:

        # Area-only (default)
        hhplab generate xwalks --boundary 2025 --tracts 2023

        # With population weights
        hhplab generate xwalks --boundary 2025 --tracts 2023 --population-weights

        # Auto-fetch population data if missing
        hhplab generate xwalks --boundary 2025 --tracts 2023 --population-weights --auto-fetch

        # Build only county crosswalk
        hhplab generate xwalks --boundary 2025 --type counties --counties 2020

        # Build only tract crosswalk
        hhplab generate xwalks --boundary 2025 --type tracts --tracts 2023

        # Write crosswalks to the curated xwalk store
        hhplab generate xwalks --boundary 2025 --tracts 2023
    """
    # Determine what to build
    build_tracts = xwalk_type in ("tracts", "all")
    build_counties = xwalk_type in ("counties", "all")

    output_dir = curated_dir("xwalks")

    # Resolve county vintage (defaults to tracts vintage if not specified)
    county_vintage = counties if counties is not None else tracts

    # Resolve boundary vintage from registry
    if boundary is None:
        boundary = latest_vintage()
        if boundary is None:
            typer.echo(
                "Error: No boundary vintages found in registry. Run 'hhplab ingest' first.",
                err=True,
            )
            raise typer.Exit(1)
        typer.echo(f"Using latest boundary vintage: {boundary}")
    else:
        # Verify the boundary vintage exists
        vintages = list_boundaries()
        vintage_ids = [v.boundary_vintage for v in vintages]
        if boundary not in vintage_ids:
            typer.echo(
                f"Error: Boundary vintage '{boundary}' not found in registry. "
                f"Available: {vintage_ids}",
                err=True,
            )
            raise typer.Exit(1)

    # Load CoC boundaries from registry
    typer.echo(f"Loading CoC boundaries (vintage: {boundary})...")
    vintages = list_boundaries()
    boundary_entry = next(v for v in vintages if v.boundary_vintage == boundary)
    boundary_path = Path(boundary_entry.path)
    if not boundary_path.exists():
        typer.echo(
            f"Error: Boundary file not found: {boundary_path}. "
            f"Run 'hhplab ingest boundaries --source hud_exchange --vintage {boundary}' first.",
            err=True,
        )
        raise typer.Exit(1)
    try:
        coc_gdf = gpd.read_parquet(boundary_path)
    except Exception as e:
        typer.echo(
            f"Error: Failed to read boundary file {boundary_path}: {e}",
            err=True,
        )
        raise typer.Exit(1) from e

    # Guard against overwriting existing crosswalks unless --force is provided
    from hhplab.naming import county_xwalk_filename, tract_xwalk_filename

    existing_outputs: list[Path] = []
    if build_tracts:
        tract_output = output_dir / tract_xwalk_filename(boundary, tracts)
        if tract_output.exists():
            existing_outputs.append(tract_output)
    if build_counties:
        county_output = output_dir / county_xwalk_filename(boundary, county_vintage)
        if county_output.exists():
            existing_outputs.append(county_output)

    if existing_outputs and not force:
        paths = "\n".join(f"  - {path}" for path in existing_outputs)
        typer.echo(
            f"Error: Crosswalk already exists:\n{paths}\nUse --force to regenerate the crosswalk.",
            err=True,
        )
        raise typer.Exit(1)

    # Build tract crosswalk if requested
    if build_tracts:
        # Load tract geometries (try new naming, then legacy)
        from hhplab.naming import tract_filename

        tract_path = curated_dir("tiger") / tract_filename(tracts)
        legacy_tract_path = curated_dir("tiger") / f"tracts__{tracts}.parquet"

        if not tract_path.exists():
            if legacy_tract_path.exists():
                tract_path = legacy_tract_path
            else:
                typer.echo(
                    f"Error: Tract file not found: {tract_path}. "
                    f"Run 'hhplab ingest tiger --year {tracts} --type tracts' first.",
                    err=True,
                )
                raise typer.Exit(1)

        typer.echo(f"Loading census tracts (vintage: {tracts})...")
        try:
            tract_gdf = gpd.read_parquet(tract_path)
        except Exception as e:
            typer.echo(
                f"Error: Failed to read tract file {tract_path}: {e}",
                err=True,
            )
            raise typer.Exit(1) from e

        # Standardize column names for tract crosswalk builder
        # The tract module expects 'GEOID', but tiger_tracts saves as 'geoid'
        if "geoid" in tract_gdf.columns and "GEOID" not in tract_gdf.columns:
            tract_gdf = tract_gdf.rename(columns={"geoid": "GEOID"})

        # Build tract crosswalk with progress bar
        n_cocs = len(coc_gdf)
        with click.progressbar(
            length=n_cocs,
            label="Building tract crosswalk",
            show_pos=True,
        ) as progress:

            def update_progress(completed: int, total: int) -> None:
                progress.update(completed - progress.pos)

            tract_xwalk = build_coc_tract_crosswalk(
                coc_gdf=coc_gdf,
                tract_gdf=tract_gdf,
                boundary_vintage=boundary,
                tract_vintage=str(tracts),
                progress_callback=update_progress,
            )

        # Add population weights if requested
        has_pop_weights = False
        if population_weights:
            has_pop_weights = _apply_population_weights(
                tract_xwalk=tract_xwalk,
                tract_vintage=tracts,
                auto_fetch=auto_fetch,
            )

        # Save tract crosswalk
        tract_output = save_crosswalk(
            crosswalk=tract_xwalk,
            boundary_vintage=boundary,
            tract_vintage=str(tracts),
            output_dir=output_dir,
            has_pop_weights=has_pop_weights,
        )
        typer.echo(f"Saved tract crosswalk to: {tract_output}")

        # Compute and display tract crosswalk diagnostics
        typer.echo("")
        tract_diagnostics = compute_crosswalk_diagnostics(tract_xwalk)
        typer.echo(summarize_diagnostics(tract_diagnostics))

    # Build county crosswalk if requested
    if not build_counties:
        typer.echo("\nCrosswalk generation complete!")
        return

    # Load county geometries (try new naming, then legacy)
    from hhplab.naming import county_filename

    county_path = curated_dir("tiger") / county_filename(county_vintage)
    legacy_county_path = curated_dir("tiger") / f"counties__{county_vintage}.parquet"

    if not county_path.exists():
        if legacy_county_path.exists():
            county_path = legacy_county_path
        else:
            # Only warn if user explicitly requested counties via --counties or --type=counties
            # If xwalk_type is "all" (default) and --counties wasn't specified, skip silently
            counties_explicitly_requested = counties is not None or xwalk_type == "counties"
            if counties_explicitly_requested:
                typer.echo(
                    f"Warning: County file not found: {county_path}. Skipping county crosswalk.",
                    err=True,
                )
            typer.echo("\nCrosswalk generation complete!")
            return

    typer.echo(f"\nLoading census counties (vintage: {county_vintage})...")
    try:
        county_gdf = gpd.read_parquet(county_path)
    except Exception as e:
        typer.echo(
            f"Error: Failed to read county file {county_path}: {e}",
            err=True,
        )
        raise typer.Exit(1) from e

    # Standardize column names for county crosswalk builder
    if "geoid" in county_gdf.columns and "GEOID" not in county_gdf.columns:
        county_gdf = county_gdf.rename(columns={"geoid": "GEOID"})

    # Build county crosswalk
    typer.echo("Building county crosswalk...")
    county_xwalk = build_coc_county_crosswalk(
        coc_gdf=coc_gdf,
        county_gdf=county_gdf,
        boundary_vintage=boundary,
    )

    # Save county crosswalk
    county_output = save_county_crosswalk(
        crosswalk=county_xwalk,
        boundary_vintage=boundary,
        county_vintage=county_vintage,
        output_dir=output_dir,
    )
    typer.echo(f"Saved county crosswalk to: {county_output}")

    typer.echo("\nCrosswalk generation complete!")


def _apply_population_weights(
    tract_xwalk: "pd.DataFrame",
    tract_vintage: int,
    auto_fetch: bool,
) -> bool:
    """Load population data and apply population weights to tract crosswalk.

    Modifies tract_xwalk in place.

    Returns True if population weights were successfully applied.
    """
    import pandas as pd

    from hhplab.acs.ingest.tract_population import get_output_path, ingest_tract_data

    # Determine ACS vintage to use (5-year ending in tract_vintage)
    acs_vintage = f"{tract_vintage - 4}-{tract_vintage}"

    # Try to load cached population data
    pop_path = get_output_path(acs_vintage, str(tract_vintage))

    if pop_path.exists():
        typer.echo(f"Loading population data from: {pop_path}")
        pop_df = pd.read_parquet(pop_path)
    elif auto_fetch:
        typer.echo(f"Fetching ACS population data ({acs_vintage})...")
        pop_path = ingest_tract_data(
            acs_vintage=acs_vintage,
            tract_vintage=str(tract_vintage),
        )
        typer.echo(f"Saved population data to: {pop_path}")
        pop_df = pd.read_parquet(pop_path)
    else:
        typer.echo(
            f"Warning: Population data not found for ACS {acs_vintage}. "
            f"Use --auto-fetch to download, or run:\n"
            f"  hhplab ingest acs5-tract --acs {acs_vintage} "
            f"--tracts {tract_vintage}",
            err=True,
        )
        return False

    # Standardize column names (population data uses 'tract_geoid')
    if "GEOID" in pop_df.columns and "tract_geoid" not in pop_df.columns:
        pop_df = pop_df.rename(columns={"GEOID": "tract_geoid"})

    # Apply population weights
    typer.echo("Computing population-weighted shares...")
    weighted_xwalk = add_population_weights(tract_xwalk, pop_df)

    # Copy pop_share back to original dataframe (in-place modification)
    tract_xwalk["pop_share"] = weighted_xwalk["pop_share"]

    # Validate population shares
    validation = validate_population_shares(tract_xwalk)
    invalid_cocs = validation[~validation["is_valid"]]

    if len(invalid_cocs) > 0:
        typer.echo(
            f"Warning: {len(invalid_cocs)} CoCs have pop_share sum outside [0.99, 1.01]:",
            err=True,
        )
        for _, row in invalid_cocs.head(5).iterrows():
            typer.echo(f"  {row['coc_id']}: {row['pop_share_sum']:.4f}", err=True)
        if len(invalid_cocs) > 5:
            typer.echo(f"  ... and {len(invalid_cocs) - 5} more", err=True)

    # Log coverage statistics
    total_tracts = tract_xwalk["tract_geoid"].nunique()
    tracts_with_pop = tract_xwalk[tract_xwalk["pop_share"].notna()]["tract_geoid"].nunique()
    coverage_pct = 100 * tracts_with_pop / total_tracts if total_tracts > 0 else 0
    typer.echo(
        f"Population coverage: {tracts_with_pop}/{total_tracts} tracts ({coverage_pct:.1f}%)"
    )

    return True
