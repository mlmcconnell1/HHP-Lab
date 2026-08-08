"""CLI command for listing core panel-source temporal coverage."""

from __future__ import annotations

import json
from typing import Annotated

import typer

from hhplab.sources.coverage import list_core_source_coverage


def build_sources_inventory() -> dict[str, object]:
    """Return machine-readable temporal coverage for core panel sources."""
    sources = [spec.to_dict() for spec in list_core_source_coverage()]
    return {"status": "ok", "source_count": len(sources), "sources": sources}


def list_sources(
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output machine-readable JSON."),
    ] = False,
) -> None:
    """List temporal coverage for core panel sources."""
    payload = build_sources_inventory()
    if json_output:
        typer.echo(json.dumps(payload, indent=2))
        return

    typer.echo("Core Panel Source Coverage:\n")
    typer.echo(f"{'Source':<8} {'Provider':<8} {'Product':<8} {'Native geo':<12} Years")
    typer.echo("-" * 64)
    for source in payload["sources"]:
        last_year = source["last_year"] or "ongoing"
        typer.echo(
            f"{source['source_id']:<8} {source['provider']:<8} "
            f"{source['product']:<8} {source['native_geo']:<12} "
            f"{source['first_year']}-{last_year}"
        )
    typer.echo("")
    typer.echo("Use --json for notes and exact null last_year values.")
