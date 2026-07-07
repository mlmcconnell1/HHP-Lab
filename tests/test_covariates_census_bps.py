"""Tests for Census BPS county annual raw-file ingest.

Fixture truth table (derived from CENSUS_BPS_FIXTURE_ROWS below):

| county_fips | year | permitted_units | permitted_buildings |
| ----------- | ---- | --------------- | ------------------- |
| 01001       | 2020 | 10+2+3+50 = 65  | 10+1+1+1 = 13       |
| 01003       | 2020 | 20              | 20                  |
| 01001       | 2021 | 12              | 12                  |

Reporting-places-only ("rep") columns are filled with a poison value so any
accidental inclusion breaks the sums.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hhplab.covariates.census_bps_contract import (
    CENSUS_BPS_MEASURE_COLUMNS,
    CENSUS_BPS_SOURCE_ID,
)
from hhplab.covariates.ingest import ingest_covariate_source
from hhplab.provenance import read_provenance

BPS_HEADER_LINES = (
    "Survey,FIPS,FIPS,Region,Division,County,,1-unit,,,2-units,,,3-4 units,,,5+ units,,,"
    "1-unit rep,,,2-units rep,,,3-4 units rep,,, 5+units rep",
    "Date,State,County,Code,Code,Name,Bldgs,Units,Value,Bldgs,Units,Value,Bldgs,Units,Value,"
    "Bldgs,Units,Value,Bldgs,Units,Value,Bldgs,Units,Value,Bldgs,Units,Value,Bldgs,Units,Value",
    " ",
)

# Poison value for the reporting-places-only columns: including any of them in a
# measure sum makes the expected truth table fail loudly.
BPS_REP_POISON = 9999

# (year, state_fips, county3, [(bldgs, units, value) x 4 structure classes])
CENSUS_BPS_FIXTURE_ROWS: list[tuple[int, str, str, list[tuple[int, int, int]]]] = [
    (2020, "01", "001", [(10, 10, 1000), (1, 2, 200), (1, 3, 300), (1, 50, 5000)]),
    (2020, "01", "003", [(20, 20, 2000), (0, 0, 0), (0, 0, 0), (0, 0, 0)]),
    (2021, "01", "001", [(12, 12, 1200), (0, 0, 0), (0, 0, 0), (0, 0, 0)]),
]

EXPECTED_BPS_MEASURES: dict[tuple[str, int], dict[str, int]] = {
    (state + county3.zfill(3), year): {
        "permitted_units": sum(units for _, units, _ in classes),
        "permitted_buildings": sum(bldgs for bldgs, _, _ in classes),
    }
    for year, state, county3, classes in CENSUS_BPS_FIXTURE_ROWS
}


def _write_bps_annual_file(directory: Path, year: int) -> Path:
    rows = [row for row in CENSUS_BPS_FIXTURE_ROWS if row[0] == year]
    lines = list(BPS_HEADER_LINES)
    for row_year, state, county3, classes in rows:
        estimate_cells = [str(cell) for triple in classes for cell in triple]
        rep_cells = [str(BPS_REP_POISON)] * 12
        lines.append(
            ",".join(
                [str(row_year), state, county3, "3", "6", "Test County        "]
                + estimate_cells
                + rep_cells
            )
        )
    path = directory / f"co{year}a.txt"
    path.write_text("\n".join(lines) + "\n")
    return path


@pytest.fixture
def bps_raw_dir(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setattr("hhplab.covariates.ingest.register_source", lambda **_: None)
    raw_dir = tmp_path / "census_bps"
    raw_dir.mkdir()
    for year in sorted({row[0] for row in CENSUS_BPS_FIXTURE_ROWS}):
        _write_bps_annual_file(raw_dir, year)
    return raw_dir


def test_census_bps_raw_directory_ingest_matches_truth_table(
    bps_raw_dir: Path, tmp_path: Path
) -> None:
    curated = ingest_covariate_source(
        CENSUS_BPS_SOURCE_ID,
        bps_raw_dir,
        output_dir=tmp_path,
        force=True,
    )
    assert curated.name == "covariate__census_bps__Y2020-2021.parquet"
    curated_df = pd.read_parquet(curated)
    assert set(curated_df["geo_type"]) == {"county"}
    observed = {
        (row.county_fips, row.year): {
            column: int(getattr(row, column)) for column in CENSUS_BPS_MEASURE_COLUMNS
        }
        for row in curated_df.itertuples()
    }
    assert observed == EXPECTED_BPS_MEASURES


def test_census_bps_raw_ingest_records_provenance_policies(
    bps_raw_dir: Path, tmp_path: Path
) -> None:
    curated = ingest_covariate_source(
        CENSUS_BPS_SOURCE_ID,
        bps_raw_dir,
        output_dir=tmp_path,
        force=True,
    )
    provenance = read_provenance(curated)
    assert provenance is not None
    assert provenance.extra["source_id"] == CENSUS_BPS_SOURCE_ID
    assert provenance.extra["years_present"] == [2020, 2021]
    assert provenance.extra["year_range_token_policy"] == "derived_from_staged_file_years"
    assert provenance.extra["raw_files"] == ["co2020a.txt", "co2021a.txt"]


def test_census_bps_single_raw_file_ingest(bps_raw_dir: Path, tmp_path: Path) -> None:
    curated = ingest_covariate_source(
        CENSUS_BPS_SOURCE_ID,
        bps_raw_dir / "co2021a.txt",
        output_dir=tmp_path,
        force=True,
    )
    assert curated.name == "covariate__census_bps__Y2021-2021.parquet"
    curated_df = pd.read_parquet(curated)
    assert curated_df["county_fips"].tolist() == ["01001"]
    assert curated_df["permitted_units"].tolist() == [
        EXPECTED_BPS_MEASURES[("01001", 2021)]["permitted_units"]
    ]


def test_census_bps_staged_canonical_csv_still_uses_generic_ingest(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr("hhplab.covariates.ingest.register_source", lambda **_: None)
    staged = tmp_path / "staged_bps.csv"
    pd.DataFrame(
        {
            "county_fips": ["01001"],
            "year": [2020],
            "permitted_units": [100],
            "permitted_buildings": [8],
        }
    ).to_csv(staged, index=False)
    curated = ingest_covariate_source(
        CENSUS_BPS_SOURCE_ID,
        staged,
        output_dir=tmp_path,
        force=True,
    )
    curated_df = pd.read_parquet(curated)
    assert curated_df["permitted_units"].tolist() == [100]
    assert curated_df["permitted_buildings"].tolist() == [8]
