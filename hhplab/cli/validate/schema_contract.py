"""CLI command for canonical artifact schema contract validation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from hhplab.schema.contracts import ARTIFACT_CONTRACTS, validate_artifact_contract

ARTIFACT_TYPE_HELP = ", ".join(sorted(ARTIFACT_CONTRACTS))


def _build_payload(
    *,
    artifact_path: Path,
    artifact_type: str,
    row_count: int,
    columns: list[str],
    warnings_as_errors: bool,
) -> dict[str, object]:
    contract = ARTIFACT_CONTRACTS[artifact_type]
    findings = validate_artifact_contract(pd.DataFrame(columns=columns), contract)
    error_count = sum(1 for finding in findings if finding.severity == "error")
    warning_count = sum(1 for finding in findings if finding.severity == "warning")
    failed = error_count > 0 or (warnings_as_errors and warning_count > 0)

    return {
        "status": "error" if failed else "ok",
        "artifact_path": str(artifact_path),
        "artifact_type": artifact_type,
        "contract": contract.name,
        "row_count": row_count,
        "column_count": len(columns),
        "error_count": error_count,
        "warning_count": warning_count,
        "warnings_as_errors": warnings_as_errors,
        "exit_behavior": {
            "errors": "nonzero_exit",
            "warnings": "nonzero_exit" if warnings_as_errors else "reported_only",
        },
        "findings": [finding.to_dict() for finding in findings],
    }


def validate_schema_contract_cmd(
    artifact_path: Annotated[
        Path,
        typer.Argument(
            exists=True,
            dir_okay=False,
            readable=True,
            help="Parquet artifact to validate against a canonical schema contract.",
        ),
    ],
    artifact_type: Annotated[
        str,
        typer.Option(
            "--artifact-type",
            "-t",
            help=f"Artifact contract to apply. Choices: {ARTIFACT_TYPE_HELP}.",
        ),
    ],
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output structured JSON instead of human-readable text.",
        ),
    ] = False,
    warnings_as_errors: Annotated[
        bool,
        typer.Option(
            "--warnings-as-errors",
            help="Treat schema warnings as command failures. Errors always fail.",
        ),
    ] = False,
) -> None:
    """Validate a parquet artifact against a canonical schema contract.

    Contract errors are emitted for missing required columns and cause a
    nonzero exit. Contract warnings are emitted for ambiguous drift-prone
    columns and missing lineage columns; they are reported but do not fail
    unless ``--warnings-as-errors`` is set.
    """
    if artifact_type not in ARTIFACT_CONTRACTS:
        valid = ", ".join(sorted(ARTIFACT_CONTRACTS))
        raise typer.BadParameter(
            f"Unknown artifact type {artifact_type!r}. Expected one of: {valid}."
        )

    try:
        df = pd.read_parquet(artifact_path)
    except Exception as exc:
        raise typer.BadParameter(
            f"Could not read parquet artifact {artifact_path}: {exc}"
        ) from exc

    payload = _build_payload(
        artifact_path=artifact_path,
        artifact_type=artifact_type,
        row_count=len(df),
        columns=list(df.columns),
        warnings_as_errors=warnings_as_errors,
    )

    if json_output:
        typer.echo(json.dumps(payload))
    else:
        typer.echo(
            f"Schema contract {payload['status']}: {artifact_type} "
            f"({payload['error_count']} error(s), {payload['warning_count']} warning(s))"
        )
        for finding in payload["findings"]:
            typer.echo(
                f"  [{finding['severity']}] {finding['code']}: {finding['message']}"
            )

    if payload["status"] != "ok":
        raise typer.Exit(code=1)
