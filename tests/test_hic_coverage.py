"""Tests for HIC/PIT coverage diagnostics."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.hic.coverage import validate_hic_pit_coverage

runner = CliRunner()


def _write_pit(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def _write_hic(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def _pit_row(year: int, coc_id: str, total: int = 100) -> dict:
    return {
        "pit_year": year,
        "coc_id": coc_id,
        "pit_total": total,
    }


def _hic_row(year: int, coc_id: str, beds: int = 80, units: int = 40) -> dict:
    return {
        "hic_year": year,
        "coc_id": coc_id,
        "total_beds": beds,
        "total_units": units,
    }


def test_validate_hic_pit_coverage_reports_mismatched_cocs(tmp_path: Path) -> None:
    pit_dir = tmp_path / "pit"
    hic_dir = tmp_path / "hic"
    _write_pit(
        pit_dir / "pit__P2024.parquet",
        [_pit_row(2024, "CO-500"), _pit_row(2024, "MO-604")],
    )
    _write_hic(
        hic_dir / "hic__H2024.parquet",
        [_hic_row(2024, "CO-500"), _hic_row(2024, "CA-600")],
    )

    result = validate_hic_pit_coverage(pit_dir=pit_dir, hic_dir=hic_dir)

    assert result.report.passed
    coverage = result.coverage.iloc[0].to_dict()
    assert coverage["pit_coc_count"] == 2
    assert coverage["hic_coc_count"] == 2
    assert coverage["matched_coc_count"] == 1
    assert coverage["missing_hic_count"] == 1
    assert coverage["unexpected_hic_count"] == 1
    checks = {issue.check_name for issue in result.report.issues}
    assert checks == {"missing_hic_coc_year", "unexpected_hic_coc_year"}


def test_validate_hic_pit_coverage_flags_duplicates_and_yoy_swings(tmp_path: Path) -> None:
    pit_dir = tmp_path / "pit"
    hic_dir = tmp_path / "hic"
    _write_pit(pit_dir / "pit__P2023.parquet", [_pit_row(2023, "CO-500")])
    _write_pit(pit_dir / "pit__P2024.parquet", [_pit_row(2024, "CO-500")])
    _write_hic(hic_dir / "hic__H2023.parquet", [_hic_row(2023, "CO-500", beds=100)])
    _write_hic(
        hic_dir / "hic__H2024.parquet",
        [_hic_row(2024, "CO-500", beds=1000), _hic_row(2024, "CO-500", beds=50)],
    )

    result = validate_hic_pit_coverage(
        pit_dir=pit_dir,
        hic_dir=hic_dir,
        yoy_threshold=0.75,
    )

    assert not result.report.passed
    checks = [issue.check_name for issue in result.report.issues]
    assert "duplicate_hic_coc_year" in checks
    assert "large_hic_bed_yoy_swing" in checks


def test_validate_hic_pit_coverage_missing_hic_files_is_actionable(tmp_path: Path) -> None:
    pit_dir = tmp_path / "pit"
    _write_pit(pit_dir / "pit__P2024.parquet", [_pit_row(2024, "CO-500")])

    try:
        validate_hic_pit_coverage(pit_dir=pit_dir, hic_dir=tmp_path / "missing-hic")
    except FileNotFoundError as exc:
        assert "hhplab ingest hic --year <YEAR> --parse-only" in str(exc)
    else:
        raise AssertionError("Expected missing HIC files to raise FileNotFoundError")


def test_hic_coverage_cli_json_reports_coverage(tmp_path: Path) -> None:
    pit_dir = tmp_path / "pit"
    hic_dir = tmp_path / "hic"
    _write_pit(pit_dir / "pit__P2024.parquet", [_pit_row(2024, "CO-500")])
    _write_hic(hic_dir / "hic__H2024.parquet", [_hic_row(2024, "CO-500")])

    result = runner.invoke(
        app,
        [
            "diagnostics",
            "hic-coverage",
            "--pit-dir",
            str(pit_dir),
            "--hic-dir",
            str(hic_dir),
            "--years",
            "2024",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["passed"] is True
    assert payload["coverage"][0]["matched_coc_count"] == 1
