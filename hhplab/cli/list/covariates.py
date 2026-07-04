"""CLI command for listing expanded covariate sources."""

import json
from typing import Annotated

import typer

from hhplab.covariates.catalog import COVARIATE_SOURCE_SPECS


def list_covariates(
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output machine-readable JSON."),
    ] = False,
) -> None:
    """List supported hidden-cause covariate source specs."""
    sources = [spec.to_dict() for spec in COVARIATE_SOURCE_SPECS.values()]
    payload = {"status": "ok", "source_count": len(sources), "sources": sources}
    if json_output:
        typer.echo(json.dumps(payload, indent=2))
        return
    for source in sources:
        typer.echo(
            f"{source['source_id']}: {source['provider']} {source['product']} "
            f"({source['native_geo']}, {source['first_year']}+)"
        )
