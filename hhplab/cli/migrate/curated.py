"""CLI command for curated data migration."""

import json
from pathlib import Path
from typing import Annotated

import typer

from hhplab.curated_migrate import apply_migration, scan_curated_for_migration
from hhplab.paths import curated_root


def migrate_curated_cmd(
    base_dir: Annotated[
        Path | None,
        typer.Option(
            "--dir",
            "-d",
            help="Path to the curated data directory.",
        ),
    ] = None,
    apply: Annotated[
        bool,
        typer.Option(
            "--apply",
            help="Apply renames (default is dry-run only).",
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
    """Scan curated data for legacy filenames and propose canonical renames.

    By default runs in dry-run mode showing what would change.
    Pass --apply to execute renames.

    Examples:

        hhplab migrate curated-layout

        hhplab migrate curated-layout --apply

        hhplab migrate curated-layout --json
    """
    if base_dir is None:
        base_dir = curated_root()

    plan = scan_curated_for_migration(base_dir)

    total = len(plan.renames) + len(plan.duplicates) + len(plan.unknown)

    if json_output:
        applied = None
        if total > 0 and apply:
            apply_migration(plan, dry_run=False)
            applied = True
        elif total > 0:
            applied = False

        payload: dict = {
            "status": "ok",
            "renames": [
                {"from": str(a.source), "to": str(a.target)}
                for a in plan.renames
            ],
            "duplicates": [
                {"from": str(a.source), "to": str(a.target), "message": a.message}
                for a in plan.duplicates
            ],
            "unknown": [
                {"path": str(a.source), "message": a.message}
                for a in plan.unknown
            ],
            "summary": {
                "renames": len(plan.renames),
                "duplicates": len(plan.duplicates),
                "unknown": len(plan.unknown),
                "total": total,
            },
        }
        if applied is not None:
            payload["applied"] = applied
        typer.echo(json.dumps(payload))
        return

    if total == 0:
        typer.echo("No migration candidates found. Curated layout is clean.")
        return

    log = apply_migration(plan, dry_run=not apply)
    for line in log:
        typer.echo(f"  {line}")

    typer.echo("")
    typer.echo(
        f"Renames: {len(plan.renames)}  "
        f"Duplicates: {len(plan.duplicates)}  "
        f"Unknown: {len(plan.unknown)}"
    )

    if not apply and plan.renames:
        typer.echo("\nPass --apply to execute renames.")
