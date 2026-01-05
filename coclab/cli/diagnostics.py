"""CLI command for running crosswalk quality diagnostics."""

from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from coclab.measures.diagnostics import (
    compute_crosswalk_diagnostics,
    identify_problem_cocs,
    summarize_diagnostics,
)


def diagnostics(
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
    """Run crosswalk quality diagnostics.

    Analyzes a tract-to-CoC crosswalk file and reports per-CoC quality
    metrics including coverage ratios and tract concentration.

    Examples:

        coclab diagnostics --crosswalk data/curated/xwalks/coc_tract_xwalk__2025__2023.parquet

        coclab diagnostics -x data/curated/xwalks/coc_tract_xwalk__2025__2023.parquet --show-problems

        coclab diagnostics -x crosswalk.parquet --coverage-threshold 0.90 -o diagnostics.csv
    """
    # Validate crosswalk file exists
    if not crosswalk.exists():
        typer.echo(f"Error: Crosswalk file not found: {crosswalk}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Loading crosswalk from: {crosswalk}")

    try:
        xwalk_df = pd.read_parquet(crosswalk)
    except Exception as e:
        typer.echo(f"Error reading crosswalk file: {e}", err=True)
        raise typer.Exit(1) from e

    typer.echo(f"Loaded {len(xwalk_df)} crosswalk records")
    typer.echo("")

    # Compute diagnostics
    typer.echo("Computing diagnostics...")
    try:
        diag_df = compute_crosswalk_diagnostics(xwalk_df)
    except ValueError as e:
        typer.echo(f"Error computing diagnostics: {e}", err=True)
        raise typer.Exit(1) from e

    # Print summary
    summary = summarize_diagnostics(diag_df)
    typer.echo(summary)

    # Save to CSV if requested
    if output:
        typer.echo("")
        diag_df.to_csv(output, index=False)
        typer.echo(f"Saved diagnostics to: {output}")

    # Show problem CoCs if requested
    if show_problems:
        typer.echo("")
        typer.echo("=" * 60)
        typer.echo("PROBLEM CoCs")
        typer.echo("=" * 60)
        typer.echo(f"Thresholds: coverage < {coverage_threshold}, max_contribution > {max_contribution}")
        typer.echo("")

        problem_df = identify_problem_cocs(
            diag_df,
            coverage_threshold=coverage_threshold,
            max_contribution_threshold=max_contribution,
        )

        if problem_df.empty:
            typer.echo("No problem CoCs identified.")
        else:
            typer.echo(f"Found {len(problem_df)} problem CoC(s):")
            typer.echo("")

            # Format and print the problem CoCs table
            for _, row in problem_df.iterrows():
                typer.echo(f"  {row['coc_id']}:")
                typer.echo(f"    Issues: {row['issues']}")
                if "num_tracts" in row:
                    typer.echo(f"    Tracts: {row['num_tracts']}")
                if "coverage_ratio_area" in row:
                    typer.echo(f"    Coverage (area): {row['coverage_ratio_area']:.4f}")
                if "max_tract_contribution" in row:
                    typer.echo(f"    Max tract contribution: {row['max_tract_contribution']:.4f}")
                typer.echo("")
