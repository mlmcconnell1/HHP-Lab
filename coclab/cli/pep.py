"""CLI commands for PEP (Population Estimates Program) ingestion and aggregation.

Provides Typer commands for:
- ingest pep: Download and normalize PEP county population data
- build pep: Aggregate PEP from county to CoC geography

These commands implement the PEP ingest spec (see background/coclab_pep_county_spec.md).
"""

from pathlib import Path
from typing import Annotated, Literal

import httpx
import typer

from coclab.builds import build_curated_dir, require_build_dir, resolve_build_dir

# Default directories
DEFAULT_OUTPUT_DIR = Path("data/curated/pep")
DEFAULT_RAW_DIR = Path("data/raw/pep")
DEFAULT_XWALK_DIR = Path("data/curated/xwalks")


def ingest_pep(
    series: Annotated[
        str,
        typer.Option(
            "--series",
            "-s",
            help="Series to ingest: 'auto', 'postcensal', 'intercensal-2010-2020', or 'all'.",
        ),
    ] = "auto",
    vintage: Annotated[
        str | None,
        typer.Option(
            "--vintage",
            "-v",
            help="Postcensal vintage year (required for postcensal or all). Defaults to latest.",
        ),
    ] = None,
    url: Annotated[
        str | None,
        typer.Option(
            "--url",
            help="Override download URL (single vintage only).",
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
    prefer_postcensal_2020: Annotated[
        bool,
        typer.Option(
            "--prefer-postcensal-2020",
            help="When combining series, use postcensal values for 2020.",
        ),
    ] = False,
    start: Annotated[
        int | None,
        typer.Option(
            "--start",
            help="First year to include (YYYY). Defaults to earliest in data.",
        ),
    ] = None,
    end: Annotated[
        int | None,
        typer.Option(
            "--end",
            help="Last year to include (YYYY). Defaults to latest in data.",
        ),
    ] = None,
) -> None:
    """Download and normalize PEP county population estimates from Census Bureau.

    Ingests Census Bureau Population Estimates Program (PEP) county-level
    annual population estimates. Supports multiple vintages:

    - auto: Best available (intercensal if available, else postcensal)
    - postcensal: Current estimates (use --vintage for specific release)
    - intercensal-2010-2020: Bridged intercensal series (not yet available)
    - all: Combine intercensal + postcensal (falls back to postcensal if unavailable)

    Population estimates are as of July 1 of each year.

    Exit codes:
    - 0: Success
    - 2: Validation/parse error
    - 3: Download error

    Examples:

        coclab ingest pep --series auto

        coclab ingest pep --series postcensal --vintage 2024

        coclab ingest pep --series intercensal-2010-2020

        coclab ingest pep --series postcensal --vintage 2024 --start 2015 --end 2020
    """
    from coclab.pep.ingest import (
        ALL_SERIES,
        INTERCENSAL_SERIES,
        AUTO_SERIES,
        POSTCENSAL_SERIES,
        PEP_URLS,
        _intercensal_available,
        get_output_path,
        ingest_pep_county,
    )
    from coclab.provenance import read_provenance

    if start is not None and end is not None and start > end:
        typer.echo("Error: --start must be <= --end.", err=True)
        raise typer.Exit(2)

    if series not in {AUTO_SERIES, POSTCENSAL_SERIES, INTERCENSAL_SERIES, ALL_SERIES}:
        typer.echo(
            f"Error: Invalid series '{series}'. "
            f"Expected one of: {AUTO_SERIES}, {POSTCENSAL_SERIES}, "
            f"{INTERCENSAL_SERIES}, {ALL_SERIES}.",
            err=True,
        )
        raise typer.Exit(2)

    parsed_vintage: int | None = None
    if series in {POSTCENSAL_SERIES, ALL_SERIES, AUTO_SERIES}:
        if vintage is None:
            parsed_vintage = max(PEP_URLS.keys())
        else:
            try:
                parsed_vintage = int(vintage)
            except ValueError:
                typer.echo(
                    f"Error: Invalid vintage '{vintage}'. Must be a year (e.g., 2024).",
                    err=True,
                )
                raise typer.Exit(2)

    if series == INTERCENSAL_SERIES and vintage is not None:
        typer.echo("Warning: --vintage is ignored for intercensal ingest.", err=True)

    if url and series != POSTCENSAL_SERIES:
        typer.echo("Warning: --url is only used for postcensal ingest.", err=True)
    if prefer_postcensal_2020 and series not in {ALL_SERIES, AUTO_SERIES}:
        typer.echo(
            "Warning: --prefer-postcensal-2020 is only used when --series all or auto.",
            err=True,
        )
    if series in {AUTO_SERIES, ALL_SERIES} and not _intercensal_available():
        typer.echo(
            "Note: Intercensal PEP estimates are not available; using postcensal data.",
            err=False,
        )

    if series == ALL_SERIES:
        output_path = get_output_path("combined", output_dir, start_year=start, end_year=end)
    elif series == POSTCENSAL_SERIES:
        output_path = get_output_path(
            parsed_vintage, output_dir, start_year=start, end_year=end
        )
    elif series == AUTO_SERIES:
        output_path = get_output_path(
            "combined" if _intercensal_available() else parsed_vintage,
            output_dir,
            start_year=start,
            end_year=end,
        )
    else:
        output_path = get_output_path(
            INTERCENSAL_SERIES, output_dir, start_year=start, end_year=end
        )

    # Check for existing output
    if output_path.exists() and not force:
        typer.echo(f"PEP county data already exists at: {output_path}")
        typer.echo("Use --force to re-download and reprocess.")
        raise typer.Exit(0)

    typer.echo(
        "Ingesting PEP county population estimates "
        f"(series: {series}, vintage: {parsed_vintage or 'n/a'})..."
    )

    try:
        result_path = ingest_pep_county(
            series=series,
            vintage=parsed_vintage,
            url=url,
            force=force,
            output_dir=output_dir,
            raw_dir=raw_dir,
            prefer_postcensal_2020=prefer_postcensal_2020,
            start_year=start,
            end_year=end,
        )

        # Report results
        import pandas as pd
        df = pd.read_parquet(result_path)
        county_count = df["county_fips"].nunique()
        year_range = f"{df['year'].min()}-{df['year'].max()}"

        provenance = read_provenance(result_path)
        series_note = None
        if provenance and provenance.extra:
            series_note = provenance.extra.get("series")
            series_used = provenance.extra.get("series_used")
            if series_note == "postcensal" and provenance.extra.get("vintage") is not None:
                series_note = f"postcensal (vintage {provenance.extra['vintage']})"
            if series_note == "intercensal_2010_2020":
                series_note = "intercensal 2010-2020"
            if series_used:
                intercensal_range = series_used.get("intercensal")
                postcensal_vintage = series_used.get("postcensal_vintage")
                prefer_2020 = series_used.get("prefer_postcensal_2020")
                parts = []
                if intercensal_range:
                    parts.append(f"intercensal {intercensal_range[0]}-{intercensal_range[1]}")
                if postcensal_vintage:
                    parts.append(f"postcensal vintage {postcensal_vintage}")
                if prefer_2020 is not None:
                    parts.append(f"prefer_postcensal_2020={prefer_2020}")
                if parts:
                    series_note = "combined (" + ", ".join(parts) + ")"

        typer.echo(f"Successfully ingested PEP data to: {result_path}")
        typer.echo(f"  Counties: {county_count}")
        typer.echo(f"  Years: {year_range}")
        typer.echo(f"  Records: {len(df):,}")
        if series_note:
            typer.echo(f"  Series used: {series_note}")

    except httpx.HTTPStatusError as e:
        typer.echo(f"Error: Download failed: {e}", err=True)
        raise typer.Exit(3) from e

    except ValueError as e:
        typer.echo(f"Error: Validation failed: {e}", err=True)
        raise typer.Exit(2) from e

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e


def build_pep(
    boundary: Annotated[
        str,
        typer.Option(
            "--boundary",
            "-b",
            help="CoC boundary vintage year (e.g., '2024').",
        ),
    ],
    counties: Annotated[
        str,
        typer.Option(
            "--counties",
            "-c",
            help="TIGER county vintage year for crosswalk (e.g., '2024').",
        ),
    ],
    weighting: Annotated[
        str,
        typer.Option(
            "--weighting",
            "-w",
            help="Weighting method: 'area_share' (default) or 'equal'.",
        ),
    ] = "area_share",
    build: Annotated[
        str | None,
        typer.Option(
            "--build",
            help="Named build directory for outputs and build-local artifacts.",
        ),
    ] = None,
    pep_path: Annotated[
        Path | None,
        typer.Option(
            "--pep-path",
            help="Explicit path to PEP county parquet. Auto-detects if not specified.",
        ),
    ] = None,
    xwalk_path: Annotated[
        Path | None,
        typer.Option(
            "--xwalk-path",
            help="Explicit path to crosswalk. Auto-detects if not specified.",
        ),
    ] = None,
    start_year: Annotated[
        int | None,
        typer.Option(
            "--start-year",
            help="First year to include. Defaults to earliest in data.",
        ),
    ] = None,
    end_year: Annotated[
        int | None,
        typer.Option(
            "--end-year",
            help="Last year to include. Defaults to latest in data.",
        ),
    ] = None,
    min_coverage: Annotated[
        float,
        typer.Option(
            "--min-coverage",
            help="Minimum coverage ratio for valid CoC-year. Default 0.95.",
        ),
    ] = 0.95,
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            "-o",
            help="Output directory for CoC-level data.",
        ),
    ] = DEFAULT_OUTPUT_DIR,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Recompute even if output exists.",
        ),
    ] = False,
) -> None:
    """Aggregate PEP county population estimates to CoC geography.

    Uses CoC-county crosswalks to aggregate county-level population estimates
    to Continuum of Care geography. Requires:

    1. PEP county data (run 'coclab ingest pep' first)
    2. CoC-county crosswalk (run 'coclab build xwalks --boundary X --counties Y')

    Output includes coverage diagnostics showing what fraction of each CoC's
    total area has population data available.

    Exit codes:
    - 0: Success
    - 2: Missing required inputs or validation error

    Examples:

        coclab build pep --boundary 2024 --counties 2024

        coclab build pep --boundary 2024 --counties 2024 --weighting equal

        coclab build pep --boundary 2024 --counties 2024 --start-year 2015 --end-year 2024

        coclab build pep --build demo --boundary 2024 --counties 2024
    """
    from coclab.pep.aggregate import aggregate_pep_to_coc, get_output_path

    # Determine output path for cache check
    import pandas as pd
    from coclab.pep.ingest import DEFAULT_OUTPUT_DIR as PEP_DIR

    # Check if PEP data exists
    combined_path = PEP_DIR / "pep_county__combined.parquet"
    v2024_path = PEP_DIR / "pep_county__v2024.parquet"

    if pep_path is None and not combined_path.exists() and not v2024_path.exists():
        typer.echo("Error: PEP county data not found.", err=True)
        typer.echo("Run: coclab ingest pep --vintage all", err=True)
        raise typer.Exit(2)

    # Check if crosswalk exists
    xwalk_dir = DEFAULT_XWALK_DIR
    if build is not None:
        try:
            build_dir = require_build_dir(build)
        except FileNotFoundError:
            build_path = resolve_build_dir(build)
            typer.echo(f"Error: Build '{build}' not found at {build_path}", err=True)
            typer.echo("Run: coclab build create --name <build>", err=True)
            raise typer.Exit(2)

        build_curated = build_curated_dir(build_dir)
        if output_dir == DEFAULT_OUTPUT_DIR:
            output_dir = build_curated / "pep"
        if xwalk_path is None:
            xwalk_dir = build_curated / "xwalks"

    expected_xwalk = xwalk_dir / f"xwalk__B{boundary}xC{counties}.parquet"
    if xwalk_path is None and build is not None:
        xwalk_path = expected_xwalk

    if xwalk_path is not None and not xwalk_path.exists():
        typer.echo(f"Error: Crosswalk not found: {xwalk_path}", err=True)
        typer.echo(f"Run: coclab build xwalks --boundary {boundary} --counties {counties}", err=True)
        raise typer.Exit(2)

    if xwalk_path is None and not expected_xwalk.exists():
        typer.echo(f"Error: Crosswalk not found: {expected_xwalk}", err=True)
        typer.echo(f"Run: coclab build xwalks --boundary {boundary} --counties {counties}", err=True)
        raise typer.Exit(2)

    typer.echo(f"Aggregating PEP county data to CoC (boundary {boundary}, counties {counties})...")

    try:
        result_path = aggregate_pep_to_coc(
            boundary_vintage=boundary,
            county_vintage=counties,
            weighting=weighting,
            pep_path=pep_path,
            xwalk_path=xwalk_path,
            start_year=start_year,
            end_year=end_year,
            min_coverage=min_coverage,
            output_dir=output_dir,
            force=force,
        )

        # Report results
        df = pd.read_parquet(result_path)
        coc_count = df["coc_id"].nunique()
        year_range = f"{df['year'].min()}-{df['year'].max()}"
        valid_count = df["population"].notna().sum()
        total_count = len(df)

        typer.echo(f"Successfully aggregated PEP data to: {result_path}")
        typer.echo(f"  CoCs: {coc_count}")
        typer.echo(f"  Years: {year_range}")
        typer.echo(f"  CoC-years with population: {valid_count:,}/{total_count:,}")

        # Coverage summary
        mean_coverage = df["coverage_ratio"].mean()
        min_cov = df["coverage_ratio"].min()
        typer.echo(f"  Mean coverage ratio: {mean_coverage:.1%}")
        typer.echo(f"  Min coverage ratio: {min_cov:.1%}")

    except FileNotFoundError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(2) from e

    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(2) from e

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e
