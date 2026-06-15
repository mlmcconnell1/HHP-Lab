"""CLI command for ingesting HUD Housing Inventory Count data."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from hhplab.hic import get_canonical_output_path, parse_hic_file, write_hic_parquet
from hhplab.hic.hud_user import (
    HICManualDownloadRequired,
    discover_local_hic_file,
    download_hic_data,
    get_hic_source_url,
)
from hhplab.paths import raw_root


def ingest_hic(
    year: Annotated[
        int,
        typer.Option("--year", "-y", help="HIC inventory year to ingest."),
    ],
    force: Annotated[
        bool,
        typer.Option("--force", help="Re-download and reprocess even if files exist."),
    ] = False,
    parse_only: Annotated[
        bool,
        typer.Option("--parse-only", help="Parse an existing file under data/raw/hic/<YEAR>/."),
    ] = False,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output structured JSON instead of human-readable text."),
    ] = False,
) -> None:
    """Ingest HUD Housing Inventory Count data for one year."""
    raw_dir = raw_root() / "hic" / str(year)
    source_url = get_hic_source_url(year)

    try:
        if parse_only:
            raw_file = discover_local_hic_file(year, raw_dir)
            if raw_file is None:
                raise FileNotFoundError(
                    f"No HIC file found in {raw_dir}. Place a HUD HIC CSV/XLS/XLSX/XLSB "
                    f"file there and rerun `hhplab ingest hic --year {year} --parse-only`."
                )
            raw_sha256 = None
        else:
            download = download_hic_data(year, raw_dir, force=force)
            raw_file = download.path
            source_url = download.source_url
            raw_sha256 = download.raw_sha256

        parse_result = parse_hic_file(raw_file, year=year, source_ref=source_url)
        if raw_sha256 is not None and raw_sha256 != parse_result.raw_sha256:
            raise RuntimeError("Downloaded HIC hash changed before parsing completed.")

        output_path = get_canonical_output_path(year)
        write_hic_parquet(
            parse_result.df,
            output_path,
            raw_path=raw_file,
            raw_sha256=parse_result.raw_sha256,
            source_ref=source_url,
            rows_read=parse_result.rows_read,
            rows_skipped=parse_result.rows_skipped,
        )
    except (FileNotFoundError, HICManualDownloadRequired, ValueError) as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": str(exc)}))
        else:
            typer.echo(f"Error ingesting HIC data: {exc}", err=True)
        raise typer.Exit(1) from exc

    payload = {
        "status": "ok",
        "year": year,
        "raw_path": str(raw_file),
        "output_path": str(output_path),
        "row_count": len(parse_result.df),
        "rows_read": parse_result.rows_read,
        "rows_skipped": parse_result.rows_skipped,
        "raw_sha256": parse_result.raw_sha256,
    }
    if json_output:
        typer.echo(json.dumps(payload))
        return

    typer.echo("HIC ingestion complete:")
    typer.echo(f"  Year: {year}")
    typer.echo(f"  CoCs: {len(parse_result.df)}")
    typer.echo(f"  Raw: {Path(raw_file)}")
    typer.echo(f"  Output: {output_path}")
