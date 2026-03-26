"""CLI command for listing curated dataset files by provider/product."""

import json
from datetime import datetime
from pathlib import Path
from typing import Annotated

import pyarrow.parquet as pq
import typer

from coclab.curated_policy import CURATED_SUBDIRS


def _format_size(size_bytes: int) -> str:
    """Format file size in human-readable format."""
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def _parquet_metadata(filepath: Path) -> dict:
    """Read lightweight metadata from a parquet file."""
    try:
        pf = pq.ParquetFile(filepath)
        schema = pf.schema_arrow
        return {
            "rows": pf.metadata.num_rows,
            "columns": [f.name for f in schema],
        }
    except Exception:
        return {"rows": -1, "columns": []}


def list_curated(
    subdir: Annotated[
        str | None,
        typer.Option(
            "--subdir",
            "-s",
            help=(
                "Filter to a specific subdirectory "
                f"({', '.join(sorted(CURATED_SUBDIRS))}). "
                "Shows all subdirs if omitted."
            ),
        ),
    ] = None,
    directory: Annotated[
        Path,
        typer.Option(
            "--dir",
            "-d",
            help="Path to curated data root.",
        ),
    ] = Path("data/curated"),
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output machine-readable JSON instead of human text.",
        ),
    ] = False,
) -> None:
    """List curated dataset files with metadata.

    Scans data/curated/ subdirectories and reports every parquet file
    with its row count, column list, and file size.  Use --subdir to
    narrow to a single provider/product (e.g., pit, zori, acs).

    Examples:

        coclab list curated

        coclab list curated --subdir pit

        coclab list curated --subdir acs --json
    """
    if subdir is not None and subdir not in CURATED_SUBDIRS:
        typer.echo(
            f"Error: Unknown subdirectory '{subdir}'. "
            f"Valid: {', '.join(sorted(CURATED_SUBDIRS))}",
            err=True,
        )
        raise typer.Exit(1)

    if not directory.exists():
        if json_output:
            typer.echo(json.dumps({"status": "ok", "count": 0, "artifacts": []}, indent=2))
        else:
            typer.echo(f"Directory not found: {directory}")
        return

    # Decide which subdirs to scan
    if subdir is not None:
        scan_dirs = [directory / subdir]
    else:
        scan_dirs = sorted(
            d for d in directory.iterdir()
            if d.is_dir() and d.name in CURATED_SUBDIRS
        )

    artifacts: list[dict] = []
    for scan_dir in scan_dirs:
        if not scan_dir.exists():
            continue
        for filepath in sorted(scan_dir.glob("*.parquet")):
            stat = filepath.stat()
            meta = _parquet_metadata(filepath)
            artifacts.append({
                "subdir": scan_dir.name,
                "filename": filepath.name,
                "path": str(filepath),
                "rows": meta["rows"],
                "columns": meta["columns"],
                "bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime),
            })

    if json_output:
        json_items = [
            {
                "subdir": a["subdir"],
                "filename": a["filename"],
                "path": a["path"],
                "rows": a["rows"],
                "columns": a["columns"],
                "bytes": a["bytes"],
                "modified_at": a["modified_at"].isoformat(),
            }
            for a in artifacts
        ]
        typer.echo(json.dumps(
            {"status": "ok", "count": len(json_items), "artifacts": json_items},
            indent=2,
        ))
        return

    if not artifacts:
        typer.echo("No curated parquet files found.")
        return

    typer.echo(f"\nCurated datasets in {directory}:\n")
    typer.echo(
        f"{'Subdir':<18} {'Filename':<50} {'Rows':>10} {'Size':>10} {'Modified'}"
    )
    typer.echo("-" * 110)

    for a in artifacts:
        row_str = f"{a['rows']:,}" if a["rows"] >= 0 else "?"
        size_str = _format_size(a["bytes"])
        mod_str = a["modified_at"].strftime("%Y-%m-%d %H:%M")
        typer.echo(
            f"{a['subdir']:<18} {a['filename']:<50} {row_str:>10} {size_str:>10} {mod_str}"
        )

    typer.echo(f"\nTotal: {len(artifacts)} file(s)")
