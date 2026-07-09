"""List covariate finding sidecars."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from hhplab.results.findings import FINDINGS_DIR, list_findings


def list_covariate_findings(
    directory: Annotated[
        Path,
        typer.Option("--directory", help="Directory containing *.finding.json sidecars."),
    ] = FINDINGS_DIR,
    source_id: Annotated[
        str | None,
        typer.Option("--source-id", help="Filter to a covariate source id."),
    ] = None,
    use_json: Annotated[
        bool,
        typer.Option("--json", help="Emit machine-readable JSON."),
    ] = False,
) -> None:
    """List standardized covariate-screen finding sidecars."""
    findings = list_findings(directory)
    if source_id is not None:
        findings = [finding for finding in findings if finding.get("source_id") == source_id]
    payload = {"finding_count": len(findings), "findings": findings}
    if use_json:
        typer.echo(json.dumps(payload, indent=2))
        return
    for finding in findings:
        typer.echo(
            f"{finding.get('workflow_id')} {finding.get('source_id') or '-'} "
            f"{finding.get('direction')}: {finding.get('headline_result')}"
        )
