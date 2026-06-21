"""Tests for HIC/PIT coverage diagnostics."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.hic.coverage import validate_expanded_hic_artifacts, validate_hic_pit_coverage
from hhplab.schema.columns import HIC_PROJECT_TYPES

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


def _expanded_hic_row(year: int, coc_id: str) -> dict:
    bed_components = {
        f"hic_{project_type}_year_round_beds": (index + 1) * 10
        for index, project_type in enumerate(HIC_PROJECT_TYPES)
    }
    unit_components = {
        f"hic_{project_type}_family_units": index + 1
        for index, project_type in enumerate(HIC_PROJECT_TYPES)
    }
    shelter_beds = sum(
        bed_components[f"hic_{project_type}_year_round_beds"]
        for project_type in ("es", "th", "sh")
    )
    shelter_units = sum(
        unit_components[f"hic_{project_type}_family_units"]
        for project_type in ("es", "th", "sh")
    )
    total_beds = sum(bed_components.values())
    total_units = sum(unit_components.values())
    return {
        "hic_year": year,
        "coc_id": coc_id,
        "total_beds": total_beds,
        "total_units": total_units,
        **bed_components,
        **unit_components,
        "hic_shelter_year_round_beds": shelter_beds,
        "hic_shelter_family_units": shelter_units,
        "hic_total_beds": total_beds,
        "hic_total_units": total_units,
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


def test_validate_hic_pit_coverage_uses_pit_vintage_files(tmp_path: Path) -> None:
    pit_dir = tmp_path / "pit"
    hic_dir = tmp_path / "hic"
    _write_pit(
        pit_dir / "pit_vintage__P2020.parquet",
        [_pit_row(2019, "CO-500"), _pit_row(2020, "CO-500")],
    )
    _write_hic(hic_dir / "hic__H2020.parquet", [_hic_row(2020, "CO-500")])

    result = validate_hic_pit_coverage(pit_dir=pit_dir, hic_dir=hic_dir, years=[2020])

    assert result.report.passed
    assert result.coverage.iloc[0]["year"] == 2020
    assert result.coverage.iloc[0]["matched_coc_count"] == 1


def test_validate_hic_pit_coverage_prefers_latest_pit_vintage(tmp_path: Path) -> None:
    pit_dir = tmp_path / "pit"
    hic_dir = tmp_path / "hic"
    _write_pit(pit_dir / "pit_vintage__P2020.parquet", [_pit_row(2020, "CO-500")])
    _write_pit(
        pit_dir / "pit_vintage__P2024.parquet",
        [_pit_row(2020, "CO-500"), _pit_row(2024, "CO-500")],
    )
    _write_hic(hic_dir / "hic__H2020.parquet", [_hic_row(2020, "CO-500")])
    _write_hic(hic_dir / "hic__H2024.parquet", [_hic_row(2024, "CO-500")])

    result = validate_hic_pit_coverage(
        pit_dir=pit_dir,
        hic_dir=hic_dir,
        years=[2020, 2024],
    )

    assert result.report.passed
    assert [path.name for path in result.pit_files] == ["pit_vintage__P2024.parquet"]
    assert set(result.coverage["year"]) == {2020, 2024}
    assert {issue.check_name for issue in result.report.issues} == set()


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


def test_validate_hic_pit_coverage_skips_yoy_swings_across_gap_years(
    tmp_path: Path,
) -> None:
    pit_dir = tmp_path / "pit"
    hic_dir = tmp_path / "hic"
    _write_pit(pit_dir / "pit__P2020.parquet", [_pit_row(2020, "CO-500")])
    _write_pit(pit_dir / "pit__P2024.parquet", [_pit_row(2024, "CO-500")])
    _write_hic(hic_dir / "hic__H2020.parquet", [_hic_row(2020, "CO-500", beds=100)])
    _write_hic(hic_dir / "hic__H2024.parquet", [_hic_row(2024, "CO-500", beds=1000)])

    result = validate_hic_pit_coverage(
        pit_dir=pit_dir,
        hic_dir=hic_dir,
        yoy_threshold=0.75,
    )

    checks = {issue.check_name for issue in result.report.issues}
    assert "large_hic_bed_yoy_swing" not in checks


def test_validate_hic_pit_coverage_missing_hic_files_is_actionable(tmp_path: Path) -> None:
    pit_dir = tmp_path / "pit"
    _write_pit(pit_dir / "pit__P2024.parquet", [_pit_row(2024, "CO-500")])

    try:
        validate_hic_pit_coverage(pit_dir=pit_dir, hic_dir=tmp_path / "missing-hic")
    except FileNotFoundError as exc:
        assert "hhplab ingest hic --year <YEAR> --parse-only" in str(exc)
    else:
        raise AssertionError("Expected missing HIC files to raise FileNotFoundError")


def test_validate_expanded_hic_artifacts_accepts_consistent_modern_schema(
    tmp_path: Path,
) -> None:
    hic_dir = tmp_path / "hic"
    _write_hic(hic_dir / "hic__H2024.parquet", [_expanded_hic_row(2024, "CO-500")])

    report = validate_expanded_hic_artifacts(hic_dir=hic_dir, years=[2024])

    assert report.passed
    assert report.issues == []


def test_validate_expanded_hic_artifacts_reports_missing_columns(tmp_path: Path) -> None:
    hic_dir = tmp_path / "hic"
    row = _expanded_hic_row(2024, "CO-500")
    row.pop("hic_oph_year_round_beds")
    _write_hic(hic_dir / "hic__H2024.parquet", [row])

    report = validate_expanded_hic_artifacts(hic_dir=hic_dir, years=[2024])

    assert not report.passed
    issue = report.errors[0]
    assert issue.check_name == "missing_expanded_hic_columns"
    assert issue.details is not None
    assert issue.details["missing_columns"] == ["hic_oph_year_round_beds"]


def test_validate_expanded_hic_artifacts_reports_total_component_mismatch(
    tmp_path: Path,
) -> None:
    hic_dir = tmp_path / "hic"
    row = _expanded_hic_row(2024, "CO-500")
    row["hic_total_beds"] += 1
    row["total_beds"] = row["hic_total_beds"]
    _write_hic(hic_dir / "hic__H2024.parquet", [row])

    report = validate_expanded_hic_artifacts(hic_dir=hic_dir, years=[2024])

    checks = {issue.check_name for issue in report.errors}
    assert "hic_total_beds_component_mismatch" in checks


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
