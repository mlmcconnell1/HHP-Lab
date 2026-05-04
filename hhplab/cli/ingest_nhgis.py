"""CLI command for ingesting NHGIS shapefiles (tracts and counties)."""

from pathlib import Path
from typing import Annotated, Literal

import typer

from hhplab.paths import curated_dir

GeoType = Literal["tracts", "counties", "all"]


def ingest_nhgis(
    years: Annotated[
        list[int],
        typer.Option(
            "--year",
            "-y",
            help="Census year(s) to download (2010, 2020). Can specify multiple.",
        ),
    ],
    geo_type: Annotated[
        GeoType,
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
    """Download census shapefiles (tracts and/or counties) from NHGIS.

    Submits an extract request to NHGIS via the IPUMS API, waits for
    completion, and downloads the national shapefile. This is
    especially useful for 2010 data, which TIGER distributes as
    many separate files.

    Requires an IPUMS API key. Get one at:
    https://account.ipums.org/api_keys

    Examples:

        hhplab ingest-nhgis --year 2010 --year 2020

        hhplab ingest-nhgis --year 2010 --type counties

        hhplab ingest-nhgis --year 2010 --type all

        hhplab ingest-nhgis --year 2010 --poll-interval 5

        IPUMS_API_KEY=your_key hhplab ingest-nhgis --year 2020
    """
    from hhplab.naming import county_filename, tract_filename
    from hhplab.nhgis.nhgis_ingest import (
        SUPPORTED_YEARS,
        NhgisExtractError,
        ingest_nhgis_counties,
        ingest_nhgis_tracts,
    )

    # Validate API key
    if not api_key:
        typer.echo(
            "Error: IPUMS API key required.\n"
            "Set IPUMS_API_KEY environment variable or use --api-key.\n"
            "Get a key at: https://account.ipums.org/api_keys",
            err=True,
        )
        raise typer.Exit(1)

    # Validate years
    invalid_years = [y for y in years if y not in SUPPORTED_YEARS]
    if invalid_years:
        supported = ", ".join(str(y) for y in sorted(SUPPORTED_YEARS))
        typer.echo(
            f"Error: Unsupported year(s): {invalid_years}. Supported: {supported}",
            err=True,
        )
        raise typer.Exit(1)

    # Determine which geo types to process
    process_tracts = geo_type in ("tracts", "all")
    process_counties = geo_type in ("counties", "all")

    # Track results: list of (year, geo_type, path)
    downloaded: list[tuple[int, str, Path]] = []
    skipped: list[tuple[int, str]] = []
    failed: list[tuple[int, str, str]] = []

    def progress(msg: str) -> None:
        typer.echo(f"  {msg}")

    for year in years:
        # Process tracts if requested
        if process_tracts:
            output_path = curated_dir("tiger") / tract_filename(year)

            if output_path.exists() and not force:
                typer.echo(f"Tracts file exists for {year}: {output_path}")
                typer.echo("  Use --force to re-download.")
                skipped.append((year, "tracts"))
            else:
                if output_path.exists() and force:
                    typer.echo(f"Forcing re-download of tracts for {year}")

                typer.echo(f"\nIngesting NHGIS tracts for {year}...")
                typer.echo(f"  Poll interval: {poll_interval} minutes")
                typer.echo(f"  Max wait: {max_wait} minutes")
                typer.echo("")

                try:
                    result_path = ingest_nhgis_tracts(
                        year=year,
                        api_key=api_key,
                        poll_interval_minutes=poll_interval,
                        max_wait_minutes=max_wait,
                        progress_callback=progress,
                    )
                    downloaded.append((year, "tracts", result_path))
                    typer.echo(f"  Success: {result_path}")
                except NhgisExtractError as e:
                    typer.echo(f"  Error: {e}", err=True)
                    failed.append((year, "tracts", str(e)))
                except Exception as e:
                    typer.echo(f"  Unexpected error: {e}", err=True)
                    failed.append((year, "tracts", str(e)))

        # Process counties if requested
        if process_counties:
            output_path = curated_dir("tiger") / county_filename(year)

            if output_path.exists() and not force:
                typer.echo(f"Counties file exists for {year}: {output_path}")
                typer.echo("  Use --force to re-download.")
                skipped.append((year, "counties"))
            else:
                if output_path.exists() and force:
                    typer.echo(f"Forcing re-download of counties for {year}")

                typer.echo(f"\nIngesting NHGIS counties for {year}...")
                typer.echo(f"  Poll interval: {poll_interval} minutes")
                typer.echo(f"  Max wait: {max_wait} minutes")
                typer.echo("")

                try:
                    result_path = ingest_nhgis_counties(
                        year=year,
                        api_key=api_key,
                        poll_interval_minutes=poll_interval,
                        max_wait_minutes=max_wait,
                        progress_callback=progress,
                    )
                    downloaded.append((year, "counties", result_path))
                    typer.echo(f"  Success: {result_path}")
                except NhgisExtractError as e:
                    typer.echo(f"  Error: {e}", err=True)
                    failed.append((year, "counties", str(e)))
                except Exception as e:
                    typer.echo(f"  Unexpected error: {e}", err=True)
                    failed.append((year, "counties", str(e)))

    # Summary
    typer.echo("")
    typer.echo("=" * 60)
    typer.echo("NHGIS INGEST SUMMARY")
    typer.echo("=" * 60)

    if downloaded:
        typer.echo(f"\nDownloaded ({len(downloaded)}):")
        for year, gtype, path in downloaded:
            typer.echo(f"  {year} {gtype}: {path}")

    if skipped:
        typer.echo(f"\nSkipped - already exists ({len(skipped)}):")
        for year, gtype in skipped:
            typer.echo(f"  {year} {gtype}")

    if failed:
        typer.echo(f"\nFailed ({len(failed)}):")
        for year, gtype, error in failed:
            typer.echo(f"  {year} {gtype}: {error}")

    typer.echo("")

    if failed:
        raise typer.Exit(1)
