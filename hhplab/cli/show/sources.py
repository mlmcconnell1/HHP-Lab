"""CLI command for tracked external source status."""

import json
from typing import Annotated

import typer


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
    """Show status of tracked external data sources."""
    from hhplab.source_registry import (
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
                            "sources": json.loads(df.to_json(orient="records", date_format="iso")),
                        },
                    )
                )
        return

    if check_changes:
        changes = detect_upstream_changes()
        if changes.empty:
            typer.echo("No upstream changes detected. All sources have consistent hashes.")
        else:
            typer.echo("UPSTREAM DATA CHANGES DETECTED:\n")
            for _, row in changes.iterrows():
                typer.echo(f"  {row['source_type']}: {row['source_url'][:60]}...")
                typer.echo(f"    Versions seen: {row['hash_count']}")
                typer.echo(f"    First: {row['first_seen']} (hash: {row['first_hash'][:12]}...)")
                typer.echo(f"    Last:  {row['last_seen']} (hash: {row['last_hash'][:12]}...)")
                typer.echo("")
        return

    summary = summarize_registry()
    typer.echo(summary)
