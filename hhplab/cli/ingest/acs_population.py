"""CLI command for ingesting ACS tract-level data."""

from typing import Annotated

import typer

from hhplab.sources.acs.ingest.tract_population import get_output_path
from hhplab.sources.acs.variables import acs5_registry_measure_names, acs5_registry_tables


def _acs5_registry_metadata() -> dict[str, list[str]]:
    """Return registry-derived ACS5 command metadata."""
    return {
        "supported_acs_tables": acs5_registry_tables(),
        "supported_measures": acs5_registry_measure_names(),
    }


def _acs5_command_help() -> str:
    tables = ", ".join(acs5_registry_tables())
    measures = ", ".join(acs5_registry_measure_names())
    return f"""Ingest tract-level ACS 5-year estimates.

Downloads tract data from the Census Bureau API for the ACS5 tables declared
in hhplab.sources.acs.variables.ACS5_COVARIATE_REGISTRY:

    {tables}

Measures are derived from the same registry and include:

    {measures}

The output Parquet file includes provenance metadata and derived columns such
as adult_population and population_below_poverty.

Examples:

    hhplab ingest acs5-tract --acs 2019-2023 --tracts 2023

    hhplab ingest acs5-tract --acs 2015-2019 --tracts 2023

    hhplab ingest acs5-tract --acs 2019-2023 --tracts 2023 --json
"""


def ingest_acs_population(
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
    output_json: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output structured JSON instead of human-readable text.",
        ),
    ] = False,
) -> None:
    """Ingest tract-level ACS 5-year estimates."""
    import pandas as pd

    from hhplab.sources.acs.ingest.tract_population import ingest_tract_data
    from hhplab.sources.acs.translate import get_source_tract_vintage, needs_translation
    from hhplab.sources.acs.variables import ACS_TABLES

    registry_metadata = _acs5_registry_metadata()

    # Check if cached file exists
    output_path = get_output_path(acs, tracts)
    if output_path.exists() and not force:
        df = pd.read_parquet(output_path)
        if output_json:
            import json

            result = {
                "status": "ok",
                "cached": True,
                "acs_vintage": acs,
                "tract_vintage": tracts,
                "output_path": str(output_path),
                "total_tracts": len(df),
                **registry_metadata,
            }
            typer.echo(json.dumps(result, indent=2))
            return
        typer.echo(f"Cached file found: {output_path}")
        typer.echo(f"Rows: {len(df)}")
        typer.echo("")
        typer.echo("Use --force to re-ingest.")
        return

    # Check if translation is needed
    source_tract_vintage = get_source_tract_vintage(acs)
    translation_needed = needs_translation(acs, tracts)

    if not output_json:
        typer.echo("Ingesting ACS tract data...")
        typer.echo(f"  ACS vintage:     {acs}")
        typer.echo(f"  Tables:          {', '.join(ACS_TABLES)}")
        typer.echo(f"  Source tracts:   {source_tract_vintage} (Census API geography)")
        typer.echo(f"  Target tracts:   {tracts}")
        if translation_needed:
            typer.echo("  Translation:     needed")
        else:
            typer.echo("  Translation:     not needed")
        typer.echo("")

    try:
        path = ingest_tract_data(
            acs_vintage=acs,
            tract_vintage=tracts,
            force=force,
        )
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e

    # Load and summarize results
    df = pd.read_parquet(path)

    # Validate required columns exist
    required_columns = ["tract_geoid", "total_population", "adult_population"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        typer.echo(
            f"Error: Output file is missing required columns: {missing_columns}. "
            f"Available columns: {list(df.columns)}",
            err=True,
        )
        raise typer.Exit(1)

    if output_json:
        import json

        result = {
            "status": "ok",
            "cached": False,
            "acs_vintage": acs,
            "tract_vintage": tracts,
            "output_path": str(path),
            "total_tracts": len(df),
            "total_population": int(df["total_population"].sum()),
            **registry_metadata,
        }
        if "adult_population" in df.columns:
            result["adult_population"] = int(df["adult_population"].sum())
        if "median_household_income" in df.columns:
            result["median_household_income"] = float(df["median_household_income"].median())
        if "median_gross_rent" in df.columns:
            result["median_gross_rent"] = float(df["median_gross_rent"].median())
        typer.echo(json.dumps(result, indent=2))
        return

    typer.echo("")
    typer.echo("=" * 60)
    typer.echo("INGEST SUMMARY")
    typer.echo("=" * 60)
    typer.echo(f"Output file:       {path}")
    typer.echo(f"Total tracts:      {len(df):,}")
    typer.echo(f"Total population:  {df['total_population'].sum():,.0f}")
    if "adult_population" in df.columns:
        typer.echo(f"Adult population:  {df['adult_population'].sum():,.0f}")
    if "median_household_income" in df.columns:
        typer.echo(f"Median income:     ${df['median_household_income'].median():,.0f}")
    if "median_gross_rent" in df.columns:
        typer.echo(f"Median rent:       ${df['median_gross_rent'].median():,.0f}")

    typer.echo("")

    # Show state coverage
    df["state_fips"] = df["tract_geoid"].str[:2]
    state_counts = df.groupby("state_fips").size()
    typer.echo(f"States/territories: {len(state_counts)}")
    typer.echo("")

    # Show population stats
    pop = df["total_population"]
    typer.echo("Population per tract:")
    typer.echo(f"  Mean:   {pop.mean():,.0f}")
    typer.echo(f"  Median: {pop.median():,.0f}")
    typer.echo(f"  Min:    {pop.min():,.0f}")
    typer.echo(f"  Max:    {pop.max():,.0f}")
    typer.echo("")
    typer.echo("=" * 60)
    typer.echo("Ingest complete!")


ingest_acs_population.__doc__ = _acs5_command_help()
