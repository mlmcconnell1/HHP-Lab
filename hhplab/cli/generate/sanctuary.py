"""CLI command for generating DOJ sanctuary jurisdiction MSA matches."""

from __future__ import annotations

from typing import Annotated

import typer

from hhplab.cli.shared.output import JsonOutput, cli_error, emit_result
from hhplab.msa import DEFINITION_VERSION as MSA_DEFINITION_VERSION
from hhplab.sanctuary import DOJ_SANCTUARY_SOURCE_DATE


def generate_sanctuary_msa(
    msa_definition_version: Annotated[
        str,
        typer.Option(
            "--msa-definition-version",
            "-m",
            help="MSA definition version to match against.",
        ),
    ] = MSA_DEFINITION_VERSION,
    source_date: Annotated[
        str,
        typer.Option(
            "--source-date",
            help="DOJ source date in YYYY-MM-DD form.",
        ),
    ] = DOJ_SANCTUARY_SOURCE_DATE,
    skip_raw_download: Annotated[
        bool,
        typer.Option(
            "--skip-raw-download",
            help="Do not download the DOJ HTML snapshot before generating matches.",
        ),
    ] = False,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Overwrite the existing artifact if it already exists.",
        ),
    ] = False,
    json_output: JsonOutput = False,
) -> None:
    """Generate a regression-ready MSA file from DOJ sanctuary designations."""
    import hhplab.naming as naming
    from hhplab.sanctuary import write_sanctuary_msa_matches

    output_path = naming.sanctuary_msa_matches_path(source_date, msa_definition_version)
    if output_path.exists() and not force:
        cli_error(
            f"Sanctuary MSA artifact already exists: {output_path}. Use --force to overwrite.",
            json_output,
            json_payload={
                "status": "error",
                "error": "artifact_exists",
                "existing": str(output_path),
            },
        )

    if not json_output:
        typer.echo(
            "Generating DOJ sanctuary MSA matches "
            f"(source_date={source_date}, msa={msa_definition_version})..."
        )

    try:
        matches, written_path, raw_path = write_sanctuary_msa_matches(
            msa_definition_version=msa_definition_version,
            source_date=source_date,
            download_raw=not skip_raw_download,
        )
    except (FileNotFoundError, ValueError) as exc:
        cli_error(exc, json_output)

    if emit_result(
        {
            "status": "ok",
            "source_date": source_date,
            "msa_definition_version": msa_definition_version,
            "row_count": len(matches),
            "artifact": str(written_path),
            "raw_snapshot": str(raw_path) if raw_path is not None else None,
        },
        json_output,
    ):
        return

    typer.echo(f"  Written: {written_path}")
    if raw_path is not None:
        typer.echo(f"  Raw snapshot: {raw_path}")
    typer.echo(f"  Rows: {len(matches)}")
