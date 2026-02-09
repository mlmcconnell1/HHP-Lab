"""CLI commands for ZORI (Zillow Observed Rent Index) ingestion and aggregation.

Provides Typer commands for:
- ingest zori: Download and normalize ZORI data from Zillow
- build zori: Aggregate ZORI from county to CoC geography
- diagnostics zori: Summarize CoC ZORI coverage and quality

These commands implement Agent A from the ZORI spec (section 6).
"""

from pathlib import Path
from typing import Annotated, Literal

import httpx
import typer

from coclab.builds import build_curated_dir, require_build_dir, resolve_build_dir

# Default directories matching the spec
DEFAULT_OUTPUT_DIR = Path("data/curated/zori")
DEFAULT_RAW_DIR = Path("data/raw/zori")
DEFAULT_XWALK_DIR = Path("data/curated/xwalks")

# Weighting method choices
WeightingChoice = Literal["renter_households", "housing_units", "population", "equal"]

# Yearly collapse method choices
YearlyMethodChoice = Literal["pit_january", "calendar_mean", "calendar_median"]


def ingest_zori(
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
    """Download and normalize ZORI data from Zillow Economic Research.

    Downloads ZORI (Zillow Observed Rent Index) data for the specified geography
    level, normalizes it to a canonical long-format schema, and saves as a
    GeoParquet file with embedded provenance metadata.

    Exit codes:
    - 0: Success
    - 2: Validation/parse error
    - 3: Download error

    Examples:

        coclab ingest zori --geography county

        coclab ingest zori --geography county --force

        coclab ingest zori --geography county --start 2020-01-01 --end 2024-12-31
    """
    # Validate geography
    valid_geographies = {"county", "zip"}
    if geography not in valid_geographies:
        typer.echo(
            f"Error: Invalid geography '{geography}'. "
            f"Must be one of: {', '.join(sorted(valid_geographies))}",
            err=True,
        )
        raise typer.Exit(2)

    from coclab.naming import discover_zori_ingest
    from coclab.rents.ingest import ingest_zori as do_ingest

    # Check for existing output via discovery
    existing = discover_zori_ingest(geography, output_dir)
    if existing is not None and not force:
        typer.echo(f"ZORI {geography} data already exists at: {existing}")
        typer.echo("Use --force to re-download and reprocess.")
        raise typer.Exit(0)

    typer.echo(f"Ingesting ZORI {geography} data from Zillow Economic Research...")

    if url:
        typer.echo(f"Using custom URL: {url}")

    try:
        result_path = do_ingest(
            geography=geography,
            url=url,
            force=force,
            output_dir=output_dir,
            raw_dir=raw_dir,
            start=start,
            end=end,
        )
        typer.echo(f"Successfully ingested ZORI data to: {result_path}")

    except httpx.HTTPStatusError as e:
        typer.echo(f"Error: Download failed: {e}", err=True)
        raise typer.Exit(3) from e

    except ValueError as e:
        typer.echo(f"Error: Validation failed: {e}", err=True)
        raise typer.Exit(2) from e

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e


def aggregate_zori(
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
    build: Annotated[
        str | None,
        typer.Option(
            "--build",
            help="Named build directory for outputs and build-local artifacts.",
        ),
    ] = None,
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
    """Aggregate ZORI from county geography to CoC geography.

    Aggregates county-level ZORI data to CoC (Continuum of Care) geography
    using area-weighted crosswalks and ACS-based demographic weights.

    Prerequisite commands:
    - coclab ingest boundaries --source hud_exchange --vintage <boundary>
    - coclab ingest tiger --year <counties> --type counties
    - coclab generate xwalks --boundary <boundary> --counties <counties>
    - coclab ingest zori --geography county

    Exit codes:
    - 0: Success
    - 2: Missing required inputs / mismatched vintages
    - 3: Failure to compute weights (ACS missing)

    Examples:

        coclab build zori --boundary 2025 --counties 2023 --acs 2019-2023

        coclab build zori -b 2025 -c 2023 --acs 2019-2023 -w renter_households --to-yearly

        coclab build zori -b 2025 -c 2023 --acs 2019-2023 --force

        coclab build zori --build demo --boundary 2025 --counties 2023 --acs 2019-2023
    """
    # Validate weighting method
    valid_weightings = {"renter_households", "housing_units", "population", "equal"}
    if weighting not in valid_weightings:
        typer.echo(
            f"Error: Invalid weighting method '{weighting}'. "
            f"Must be one of: {', '.join(sorted(valid_weightings))}",
            err=True,
        )
        raise typer.Exit(2)

    # Validate yearly method
    valid_yearly_methods = {"pit_january", "calendar_mean", "calendar_median"}
    if yearly_method not in valid_yearly_methods:
        typer.echo(
            f"Error: Invalid yearly method '{yearly_method}'. "
            f"Must be one of: {', '.join(sorted(valid_yearly_methods))}",
            err=True,
        )
        raise typer.Exit(2)

    if build is not None:
        try:
            build_dir = require_build_dir(build)
        except FileNotFoundError as exc:
            build_path = resolve_build_dir(build)
            typer.echo(f"Error: Build '{build}' not found at {build_path}", err=True)
            typer.echo("Run: coclab build create --name <build>", err=True)
            raise typer.Exit(2) from exc

        build_curated = build_curated_dir(build_dir)
        if output_dir == DEFAULT_OUTPUT_DIR:
            output_dir = build_curated / "zori"
        if xwalk_path is None:
            xwalk_path = (build_curated / "xwalks" / f"xwalk__B{boundary}xC{counties}.parquet")

    # Validate geography
    if geography != "county":
        typer.echo(
            f"Error: Geography '{geography}' not yet supported. Only 'county' is implemented.",
            err=True,
        )
        raise typer.Exit(2)

    if xwalk_path is not None and not Path(xwalk_path).exists():
        typer.echo(f"Error: Crosswalk not found: {xwalk_path}", err=True)
        typer.echo(
            f"Run: coclab generate xwalks --boundary {boundary} --counties {counties}",
            err=True,
        )
        raise typer.Exit(2)

    from coclab.rents.aggregate import (
        aggregate_zori_to_coc as do_aggregate,
    )
    from coclab.rents.aggregate import (
        get_coc_zori_path,
    )

    # Check for existing output
    output_path = get_coc_zori_path(geography, boundary, counties, acs, weighting, output_dir)
    if output_path.exists() and not force:
        typer.echo(f"CoC ZORI output already exists at: {output_path}")
        typer.echo("Use --force to recompute.")
        raise typer.Exit(0)

    typer.echo(
        f"Aggregating ZORI to CoC geography:\n"
        f"  Boundary vintage: {boundary}\n"
        f"  County vintage: {counties}\n"
        f"  ACS vintage: {acs}\n"
        f"  Weighting method: {weighting}"
    )

    try:
        result_path = do_aggregate(
            boundary=boundary,
            counties=counties,
            acs_vintage=acs,
            weighting=weighting,
            geography=geography,
            zori_path=zori_path,
            xwalk_path=xwalk_path,
            output_dir=output_dir,
            to_yearly=to_yearly,
            yearly_method=yearly_method,
            force=force,
        )
        typer.echo("")
        typer.echo(f"Successfully wrote CoC ZORI data to: {result_path}")

        if to_yearly:
            from coclab.rents.aggregate import get_coc_zori_yearly_path

            yearly_path = get_coc_zori_yearly_path(
                geography, boundary, counties, acs, weighting, yearly_method, output_dir
            )
            typer.echo(f"Yearly output: {yearly_path}")

    except FileNotFoundError as e:
        typer.echo(f"Error: Missing input file: {e}", err=True)
        typer.echo("")
        typer.echo("Ensure you have run the prerequisite commands:")
        typer.echo(f"  coclab ingest boundaries --source hud_exchange --vintage {boundary}")
        typer.echo(f"  coclab ingest tiger --year {counties} --type counties")
        typer.echo(f"  coclab generate xwalks --boundary {boundary} --counties {counties}")
        typer.echo(f"  coclab ingest zori --geography {geography}")
        raise typer.Exit(2) from e

    except ValueError as e:
        error_str = str(e).lower()
        if "acs" in error_str or "weight" in error_str:
            typer.echo(f"Error: Failed to compute weights: {e}", err=True)
            raise typer.Exit(3) from e
        else:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(2) from e

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e


def zori_diagnostics(
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
    """Summarize CoC ZORI coverage, missingness, and quality metrics.

    Analyzes a CoC-level ZORI file and reports per-CoC quality metrics
    including coverage ratios, missingness patterns, and concentration.

    Examples:

        coclab diagnostics zori --coc-zori coc_zori__county__b2025.parquet

        coclab diagnostics zori --coc-zori coc_zori.parquet --output diagnostics.csv
    """
    # Validate input file exists
    if not coc_zori.exists():
        typer.echo(f"Error: CoC ZORI file not found: {coc_zori}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Loading CoC ZORI data from: {coc_zori}")

    try:
        from coclab.rents.diagnostics import summarize_coc_zori

        summary_text, diag_df = summarize_coc_zori(
            coc_zori,
            min_coverage=coverage_threshold,
            dominance_threshold=dominance_threshold,
        )

        typer.echo("")
        typer.echo(summary_text)

        # Save output if requested
        if output:
            output = Path(output)
            if output.suffix == ".parquet":
                diag_df.to_parquet(output, index=False)
            else:
                diag_df.to_csv(output, index=False)
            typer.echo(f"Saved diagnostics to: {output}")

    except FileNotFoundError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e

    except ValueError as e:
        typer.echo(f"Error: Invalid data format: {e}", err=True)
        raise typer.Exit(2) from e

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e
