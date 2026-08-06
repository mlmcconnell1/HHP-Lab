"""Tests for configurable MSA unemployment source resolution."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hhplab.geographies.msa.unemployment import (
    MSA_ACS5_UNEMPLOYMENT_COLUMNS,
    MSA_LAUS_UNEMPLOYMENT_COLUMNS,
    load_laus_msa_unemployment,
    resolve_msa_unemployment,
    validate_laus_years_available,
)
from hhplab.naming import laus_metro_path

ACS5_MSA_UNEMPLOYMENT_FIXTURE = pd.DataFrame(
    {
        "msa_id": ["35620", "31080"],
        "year": [2024, 2024],
        "civilian_labor_force": [1000.0, 2000.0],
        "unemployed_count": [41.0, 100.0],
    }
)

LAUS_MSA_UNEMPLOYMENT_FIXTURE = pd.DataFrame(
    {
        "metro_id": ["35620", "31080"],
        "year": [2024, 2024],
        "labor_force": [1000, 2000],
        "employed": [959, 1900],
        "unemployed": [41, 100],
        "unemployment_rate": [4.1, 5.0],
    }
)


def test_resolve_acs5_msa_unemployment_derives_rate_from_counts() -> None:
    result = resolve_msa_unemployment(
        "acs5",
        acs5_msa=ACS5_MSA_UNEMPLOYMENT_FIXTURE,
        years=[2024],
    )

    assert tuple(result.columns) == MSA_ACS5_UNEMPLOYMENT_COLUMNS
    assert result["unemployment_source"].tolist() == ["acs5", "acs5"]
    assert result.loc[result["msa_id"] == "35620", "msa_unemployment"].iloc[0] == pytest.approx(
        0.041
    )
    assert result.loc[result["msa_id"] == "31080", "msa_unemployment_acs5"].iloc[0] == (
        pytest.approx(0.05)
    )


def test_resolve_laus_msa_unemployment_normalizes_percent_to_fraction() -> None:
    result = resolve_msa_unemployment(
        "laus",
        laus_msa=LAUS_MSA_UNEMPLOYMENT_FIXTURE,
        years=[2024],
    )

    assert tuple(result.columns) == MSA_LAUS_UNEMPLOYMENT_COLUMNS
    assert result["unemployment_source"].tolist() == ["laus", "laus"]
    assert result.loc[result["msa_id"] == "35620", "msa_unemployment"].iloc[0] == pytest.approx(
        0.041
    )
    assert result.loc[result["msa_id"] == "35620", "msa_laus_unemployment_rate_percent"].iloc[
        0
    ] == pytest.approx(4.1)
    assert result["laus_vintage_used"].tolist() == ["2024", "2024"]


def test_validate_laus_years_available_reports_actionable_missing_years() -> None:
    with pytest.raises(ValueError, match="hhplab ingest laus-metro --year 2025"):
        validate_laus_years_available(
            LAUS_MSA_UNEMPLOYMENT_FIXTURE,
            [2024, 2025],
            definition_version="census_msa_2023",
        )


def test_load_laus_msa_unemployment_reads_required_artifacts(tmp_path: Path) -> None:
    out_path = laus_metro_path(2024, "census_msa_2023", base_dir=tmp_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    LAUS_MSA_UNEMPLOYMENT_FIXTURE.to_parquet(out_path)

    result = load_laus_msa_unemployment(
        [2024],
        "census_msa_2023",
        laus_dir=tmp_path / "curated" / "laus",
    )

    assert len(result) == len(LAUS_MSA_UNEMPLOYMENT_FIXTURE)
    assert set(result["metro_id"]) == {"31080", "35620"}


def test_load_laus_msa_unemployment_missing_artifact_names_ingest_command(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="hhplab ingest laus-metro --year 2024"):
        load_laus_msa_unemployment(
            [2024],
            "census_msa_2023",
            laus_dir=tmp_path / "curated" / "laus",
        )
