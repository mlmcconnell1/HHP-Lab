"""Show one covariate finding sidecar."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from hhplab.results.findings import read_finding


def show_covariate_finding(
    finding: Annotated[
        Path,
        typer.Option("--finding", exists=True, help="Path to a *.finding.json sidecar."),
    ],
    use_json: Annotated[
        bool,
        typer.Option("--json", help="Emit machine-readable JSON."),
    ] = False,
) -> None:
    """Show a standardized covariate-screen finding sidecar."""
    payload = read_finding(finding)
    if use_json:
        typer.echo(json.dumps({"finding": payload}, indent=2))
        return
    typer.echo(json.dumps(payload, indent=2))
