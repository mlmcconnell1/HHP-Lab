"""CLI command for building CoC crosswalks."""

import json
from pathlib import Path
from typing import Annotated, Literal

import click
import geopandas as gpd
import pandas as pd
import typer

from hhplab.measures.measures_diagnostics import (
    compute_crosswalk_diagnostics,
    summarize_diagnostics,
)
from hhplab.naming import county_path
from hhplab.paths import curated_dir
from hhplab.registry.boundary_registry import latest_vintage, list_boundaries
from hhplab.xwalks.county import build_coc_county_crosswalk, save_county_crosswalk
from hhplab.xwalks.tract import (
    add_population_weights,
    build_coc_tract_crosswalk,
    save_crosswalk,
    validate_population_shares,
)
from hhplab.xwalks.tract_mediated import (
    WEIGHT_COLUMNS,
    build_tract_mediated_county_crosswalk,
    save_tract_mediated_county_crosswalk,
)

XwalkType = Literal["tracts", "counties", "tract-mediated", "all"]
WeightingMode = Literal["area", "population", "household", "renter_household"]
DenominatorSource = Literal["acs", "decennial"]
DEFAULT_TRACT_MEDIATED_WEIGHTING_MODES: tuple[WeightingMode, ...] = (
    "area",
    "population",
    "household",
    "renter_household",
)
WEIGHTING_MODE_TO_COLUMN: dict[WeightingMode, str] = {
    "area": "area_weight",
    "population": "population_weight",
    "household": "household_weight",
    "renter_household": "renter_household_weight",
}


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
            help=("Which crosswalks to build: 'tracts', 'counties', 'tract-mediated', or 'all'."),
        ),
    ] = "all",
    acs_vintage: Annotated[
        str | None,
        typer.Option(
            "--acs",
            help=(
                "ACS denominator vintage for tract-mediated county weights. "
                "Defaults to the 5-year range ending in --tracts."
            ),
        ),
    ] = None,
    denominator_source: Annotated[
        DenominatorSource,
        typer.Option(
            "--denominator-source",
            help=(
                "Tract denominator source for tract-mediated county weights: 'acs' or 'decennial'."
            ),
        ),
    ] = "acs",
    denominator_vintage: Annotated[
        str | None,
        typer.Option(
            "--denominator-vintage",
            help=(
                "Explicit tract denominator vintage. Required for decennial "
                "denominators, e.g. 2010 or 2020."
            ),
        ),
    ] = None,
    weighting_modes: Annotated[
        list[str] | None,
        typer.Option(
            "--weighting-mode",
            help=(
                "Weighting mode to validate for tract-mediated output. "
                "Repeat for multiple modes; defaults to all supported modes."
            ),
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Validate inputs and planned outputs without writing artifacts.",
        ),
    ] = False,
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
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output machine-readable JSON instead of human text.",
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

        # Preflight tract-mediated county weights
        hhplab generate xwalks --boundary 2025 --type tract-mediated \
            --tracts 2020 --counties 2020 --acs 2019-2023 --dry-run --json

        # Build tract-mediated county weights
        hhplab generate xwalks --boundary 2025 --type tract-mediated \
            --tracts 2020 --counties 2020 --acs 2019-2023 --json

        # Build only tract crosswalk
        hhplab generate xwalks --boundary 2025 --type tracts --tracts 2023

        # Write crosswalks to the curated xwalk store
        hhplab generate xwalks --boundary 2025 --tracts 2023
    """
    # Determine what to build
    build_tracts = xwalk_type in ("tracts", "all")
    build_counties = xwalk_type in ("counties", "all")
    build_tract_mediated = xwalk_type == "tract-mediated"

    output_dir = curated_dir("xwalks")

    # Resolve county vintage (defaults to tracts vintage if not specified)
    county_vintage = counties if counties is not None else tracts
    resolved_acs_vintage = acs_vintage or f"{tracts - 4}-{tracts}"
    try:
        resolved_denominator_vintage = _resolve_denominator_vintage(
            denominator_source=denominator_source,
            denominator_vintage=denominator_vintage,
            acs_vintage=resolved_acs_vintage,
            tract_vintage=tracts,
        )
    except ValueError as exc:
        _emit_error(str(exc), json_output=json_output)
        raise typer.Exit(1) from exc
    try:
        selected_weighting_modes = _normalize_weighting_modes(weighting_modes)
    except ValueError as exc:
        _emit_error(str(exc), json_output=json_output)
        raise typer.Exit(1) from exc

    # Resolve boundary vintage from registry
    if boundary is None:
        boundary = latest_vintage()
        if boundary is None:
            _emit_error(
                "No boundary vintages found in registry. "
                "Run 'hhplab ingest boundaries --source hud_exchange --vintage <year>' first.",
                json_output=json_output,
            )
            raise typer.Exit(1)
        if not json_output:
            typer.echo(f"Using latest boundary vintage: {boundary}")
    else:
        # Verify the boundary vintage exists
        vintages = list_boundaries()
        vintage_ids = [v.boundary_vintage for v in vintages]
        if boundary not in vintage_ids:
            _emit_error(
                f"Boundary vintage '{boundary}' not found in registry. "
                f"Available: {vintage_ids}. "
                f"Run 'hhplab ingest boundaries --source hud_exchange --vintage {boundary}' first.",
                json_output=json_output,
            )
            raise typer.Exit(1)

    if build_tract_mediated:
        _build_tract_mediated_xwalk_cli(
            boundary=boundary,
            county_vintage=county_vintage,
            tract_vintage=tracts,
            acs_vintage=resolved_acs_vintage,
            denominator_source=denominator_source,
            denominator_vintage=resolved_denominator_vintage,
            selected_weighting_modes=selected_weighting_modes,
            force=force,
            dry_run=dry_run,
            json_output=json_output,
        )
        return

    # Load CoC boundaries from registry
    if json_output:
        _emit_error(
            "--json is currently supported for --type tract-mediated only. "
            "Run 'hhplab generate xwalks --type tract-mediated --json', or omit --json.",
            json_output=True,
        )
        raise typer.Exit(1)

    if dry_run:
        _emit_error(
            "--dry-run is currently supported for --type tract-mediated only.",
            json_output=False,
        )
        raise typer.Exit(1)

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


def _emit_error(message: str, *, json_output: bool) -> None:
    if json_output:
        typer.echo(json.dumps({"status": "error", "error": message}))
    else:
        typer.echo(f"Error: {message}", err=True)


def _normalize_weighting_modes(values: list[str] | None) -> tuple[WeightingMode, ...]:
    if not values:
        return DEFAULT_TRACT_MEDIATED_WEIGHTING_MODES
    valid = set(WEIGHTING_MODE_TO_COLUMN)
    invalid = sorted(set(values) - valid)
    if invalid:
        raise ValueError(
            f"Invalid --weighting-mode value(s): {', '.join(invalid)}. "
            f"Supported values: {', '.join(sorted(valid))}."
        )
    return tuple(values)  # type: ignore[return-value]


def _resolve_denominator_vintage(
    *,
    denominator_source: DenominatorSource,
    denominator_vintage: str | None,
    acs_vintage: str,
    tract_vintage: int,
) -> str:
    if denominator_source == "acs":
        return denominator_vintage or acs_vintage
    if denominator_source == "decennial":
        if denominator_vintage is None:
            raise ValueError(
                "--denominator-vintage is required when --denominator-source decennial."
            )
        if denominator_vintage not in {"2010", "2020"}:
            raise ValueError(
                "Unsupported decennial denominator vintage "
                f"{denominator_vintage!r}. Supported vintages: 2010, 2020."
            )
        if str(tract_vintage) != denominator_vintage:
            raise ValueError(
                "Decennial tract-mediated denominators are native to their "
                f"tract era; got --denominator-vintage {denominator_vintage} "
                f"with --tracts {tract_vintage}."
            )
        return denominator_vintage
    raise ValueError(
        f"Invalid denominator source {denominator_source!r}; use 'acs' or 'decennial'."
    )


def _input_status(path: Path) -> dict[str, str | bool]:
    return {"path": str(path), "exists": path.exists()}


def _tract_mediated_paths(
    *,
    boundary: str,
    county_vintage: int,
    tract_vintage: int,
    acs_vintage: str,
    denominator_source: DenominatorSource,
    denominator_vintage: str | int,
) -> dict[str, Path]:
    from hhplab.acs.ingest.tract_population import get_output_path as acs_tract_path
    from hhplab.census.ingest.decennial_tract_population import (
        get_output_path as decennial_tract_path,
    )
    from hhplab.naming import (
        tract_mediated_county_xwalk_path,
        tract_xwalk_path,
    )

    denominator_path = (
        acs_tract_path(acs_vintage, str(tract_vintage))
        if denominator_source == "acs"
        else decennial_tract_path(str(denominator_vintage), str(tract_vintage))
    )
    return {
        "tract_crosswalk": tract_xwalk_path(boundary, tract_vintage),
        "denominator_tracts": denominator_path,
        "counties": county_path(county_vintage),
        "output": tract_mediated_county_xwalk_path(
            boundary,
            county_vintage,
            tract_vintage,
            acs_vintage,
            denominator_source=denominator_source,
            denominator_vintage=denominator_vintage,
        ),
    }


def _build_tract_mediated_xwalk_cli(
    *,
    boundary: str,
    county_vintage: int,
    tract_vintage: int,
    acs_vintage: str,
    denominator_source: DenominatorSource,
    denominator_vintage: str | int,
    selected_weighting_modes: tuple[WeightingMode, ...],
    force: bool,
    dry_run: bool,
    json_output: bool,
) -> None:
    paths = _tract_mediated_paths(
        boundary=boundary,
        county_vintage=county_vintage,
        tract_vintage=tract_vintage,
        acs_vintage=acs_vintage,
        denominator_source=denominator_source,
        denominator_vintage=denominator_vintage,
    )
    inputs = {
        "tract_crosswalk": _input_status(paths["tract_crosswalk"]),
        "denominator_tracts": _input_status(paths["denominator_tracts"]),
    }
    payload: dict[str, object] = {
        "status": "ok",
        "action": "dry_run" if dry_run else "generate",
        "boundary_vintage": boundary,
        "county_vintage": str(county_vintage),
        "tract_vintage": str(tract_vintage),
        "acs_vintage": acs_vintage,
        "denominator_source": denominator_source,
        "denominator_vintage": str(denominator_vintage),
        "weighting_family": "tract_mediated",
        "weighting_modes": list(selected_weighting_modes),
        "inputs": inputs,
        "artifact": str(paths["output"]),
        "will_write": not dry_run,
        "force": force,
    }

    missing_inputs = [name for name, status in inputs.items() if not bool(status["exists"])]
    if missing_inputs:
        commands = {
            "tract_crosswalk": (
                "hhplab generate xwalks "
                f"--boundary {boundary} --type tracts --tracts {tract_vintage}"
            ),
            "denominator_tracts": (
                f"hhplab ingest acs5-tract --acs {acs_vintage} --tracts {tract_vintage}"
                if denominator_source == "acs"
                else "hhplab ingest decennial-tracts "
                f"--decennial {denominator_vintage} --tracts {tract_vintage}"
            ),
        }
        payload.update(
            {
                "status": "error",
                "error": "missing_inputs",
                "missing_inputs": missing_inputs,
                "commands": {
                    name: command for name, command in commands.items() if name in missing_inputs
                },
            }
        )
        if json_output:
            typer.echo(json.dumps(payload))
        else:
            missing = ", ".join(missing_inputs)
            command_hints = "\n".join(
                f"  {command}" for name, command in commands.items() if name in missing_inputs
            )
            typer.echo(
                f"Error: Missing tract-mediated input(s): {missing}. Run:\n{command_hints}",
                err=True,
            )
        raise typer.Exit(1)

    output_exists = paths["output"].exists()
    payload["output_exists"] = output_exists
    if output_exists and not force and not dry_run:
        payload.update({"status": "error", "error": "artifact_exists"})
        if json_output:
            typer.echo(json.dumps(payload))
        else:
            typer.echo(
                f"Error: Tract-mediated county crosswalk already exists at {paths['output']}. "
                "Use --force to regenerate the crosswalk.",
                err=True,
            )
        raise typer.Exit(1)

    if dry_run:
        if json_output:
            typer.echo(json.dumps(payload))
        else:
            typer.echo("Tract-mediated county crosswalk preflight passed.")
            typer.echo(f"  Output: {paths['output']}")
        return

    try:
        tract_crosswalk = pd.read_parquet(paths["tract_crosswalk"])
        denominator_tracts = pd.read_parquet(paths["denominator_tracts"])
        expected_county_fips = None
        if paths["counties"].exists():
            counties = pd.read_parquet(paths["counties"])
            county_col = "GEOID" if "GEOID" in counties.columns else "county_fips"
            if county_col in counties.columns:
                expected_county_fips = counties[county_col]
        crosswalk = build_tract_mediated_county_crosswalk(
            tract_crosswalk,
            denominator_tracts,
            boundary_vintage=boundary,
            county_vintage=county_vintage,
            tract_vintage=tract_vintage,
            acs_vintage=acs_vintage,
            denominator_source=denominator_source,
            denominator_vintage=denominator_vintage,
            expected_county_fips=expected_county_fips,
        )
    except (ValueError, OSError) as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": str(exc)}))
        else:
            typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc

    written_path = save_tract_mediated_county_crosswalk(
        crosswalk,
        boundary_vintage=boundary,
        county_vintage=county_vintage,
        tract_vintage=tract_vintage,
        acs_vintage=acs_vintage,
        denominator_source=denominator_source,
        denominator_vintage=denominator_vintage,
    )
    payload.update(
        {
            "rows": int(len(crosswalk)),
            "geo_count": int(crosswalk["coc_id"].nunique()) if "coc_id" in crosswalk else 0,
            "county_count": (
                int(crosswalk["county_fips"].nunique()) if "county_fips" in crosswalk else 0
            ),
            "artifact": str(written_path),
            "validation": _summarize_tract_mediated_crosswalk(
                crosswalk,
                selected_weighting_modes,
            ),
        }
    )
    if json_output:
        typer.echo(json.dumps(payload))
        return

    typer.echo(f"Saved tract-mediated county crosswalk to: {written_path}")
    validation = payload["validation"]
    if isinstance(validation, dict):
        typer.echo(
            "Validation: "
            f"{validation.get('county_count', 0)} counties, "
            f"{validation.get('full_coverage_count', 0)} fully covered."
        )


def _summarize_tract_mediated_crosswalk(
    crosswalk: pd.DataFrame,
    selected_weighting_modes: tuple[WeightingMode, ...],
) -> dict[str, object]:
    selected_columns = [
        WEIGHTING_MODE_TO_COLUMN[mode]
        for mode in selected_weighting_modes
        if WEIGHTING_MODE_TO_COLUMN[mode] in crosswalk.columns
    ]
    available_columns = [col for col in WEIGHT_COLUMNS if col in crosswalk.columns]
    county_count = int(crosswalk["county_fips"].nunique()) if "county_fips" in crosswalk else 0
    summary: dict[str, object] = {
        "county_count": county_count,
        "available_weight_columns": available_columns,
        "selected_weight_columns": selected_columns,
    }
    if "county_area_coverage_ratio" in crosswalk.columns:
        coverage = (
            crosswalk[["county_fips", "county_area_coverage_ratio"]]
            .drop_duplicates("county_fips")
            .dropna()
        )
        summary["min_area_coverage_ratio"] = (
            float(coverage["county_area_coverage_ratio"].min()) if not coverage.empty else None
        )
        summary["full_coverage_count"] = int(
            (coverage["county_area_coverage_ratio"] >= 0.999999).sum()
        )
    for column in selected_columns:
        non_null = crosswalk[column].dropna()
        summary[f"{column}_non_null_count"] = int(non_null.shape[0])
        summary[f"{column}_max"] = float(non_null.max()) if not non_null.empty else None
    return summary


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
