"""Tests for Census BPS county annual raw-file ingest.

Fixture truth table (derived from CENSUS_BPS_FIXTURE_ROWS below):

| county_fips | year | permitted_units | permitted_buildings | permit_value_thousands |
| ----------- | ---- | --------------- | ------------------- | ---------------------- |
| 01001       | 2020 | 10+2+3+50 = 65  | 10+1+1+1 = 13       | 1000+200+300+5000=6500 |
| 01003       | 2020 | 20              | 20                  | 2000                   |
| 01001       | 2021 | 12              | 12                  | 1200                   |

Reporting-places-only ("rep") columns are filled with a poison value so any
accidental inclusion breaks the sums.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hhplab.covariates.aggregate import aggregate_covariate_source
from hhplab.covariates.census_bps_contract import (
    CENSUS_BPS_CLASS_UNIT_COLUMNS,
    CENSUS_BPS_CLASS_VALUE_COLUMNS,
    CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN,
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
        "permit_value_thousands": sum(value for _, _, value in classes),
    }
    for year, state, county3, classes in CENSUS_BPS_FIXTURE_ROWS
}

BPS_REQUIRED_CLASS_DROPS_TRUTH_TABLE = {
    2020: "non-null: base year weights 1-unit and 2-unit classes equally",
    2021: "null: 2-unit class has positive base weight but no current units",
    2022: "non-null: 1-unit and 2-unit classes remain available; new 3-4 unit class is ignored",
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
            column: int(getattr(row, column))
            for column in ("permitted_units", "permitted_buildings", "permit_value_thousands")
        }
        for row in curated_df.itertuples()
    }
    assert observed == EXPECTED_BPS_MEASURES
    autauga_2020 = curated_df.set_index(["county_fips", "year"]).loc[("01001", 2020)]
    assert autauga_2020[list(CENSUS_BPS_CLASS_UNIT_COLUMNS)].tolist() == [10, 2, 3, 50]
    assert autauga_2020[list(CENSUS_BPS_CLASS_VALUE_COLUMNS)].tolist() == [
        1000,
        200,
        300,
        5000,
    ]


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


def test_census_bps_aggregation_discovers_data_driven_year_token(
    bps_raw_dir: Path, tmp_path: Path
) -> None:
    """BPS aggregation should not fall back to the catalog Y1980-ongoing token."""
    ingest_covariate_source(
        CENSUS_BPS_SOURCE_ID,
        bps_raw_dir,
        output_dir=tmp_path,
        force=True,
    )

    panel = aggregate_covariate_source(
        CENSUS_BPS_SOURCE_ID,
        output_dir=tmp_path,
        target_geo="county",
        force=True,
    )

    assert panel.name == "covariate_panel__census_bps__Y2020-2021.parquet"
    panel_df = pd.read_parquet(panel)
    assert sorted(panel_df["year"].unique().tolist()) == [2020, 2021]
    assert CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN in panel_df.columns


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
            "permit_value_thousands": [12_500],
            **{column: [0] for column in CENSUS_BPS_CLASS_UNIT_COLUMNS},
            **{column: [0] for column in CENSUS_BPS_CLASS_VALUE_COLUMNS},
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
    assert curated_df["permit_value_thousands"].tolist() == [12_500]


def test_census_bps_msa_aggregation_derives_fixed_mix_value_per_unit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "hhplab.covariates.aggregate.read_msa_county_membership",
        lambda _definition_version, _data_root=None: pd.DataFrame(
            {
                "msa_id": ["11111", "11111"],
                "county_fips": ["01001", "01003"],
            }
        ),
    )
    monkeypatch.setattr(
        "hhplab.covariates.aggregate.load_pep_county",
        lambda _county_population_path=None: pd.DataFrame(
            {
                "county_fips": ["01001", "01003", "01001", "01003"],
                "year": [2020, 2020, 2021, 2021],
                "population": [1.0, 1.0, 1.0, 1.0],
            }
        ),
    )
    rows = []
    cases = [
        ("01001", 2020, [10, 0, 0, 0], [1000, 0, 0, 0]),
        ("01003", 2020, [0, 10, 0, 0], [0, 2000, 0, 0]),
        ("01001", 2021, [20, 0, 0, 0], [2400, 0, 0, 0]),
        ("01003", 2021, [0, 5, 0, 0], [0, 1500, 0, 0]),
    ]
    for county_fips, year, class_units, class_values in cases:
        rows.append(
            {
                "geo_type": "county",
                "geo_id": county_fips,
                "county_fips": county_fips,
                "year": year,
                "permitted_units": sum(class_units),
                "permitted_buildings": sum(class_units),
                "permit_value_thousands": sum(class_values),
                **dict(zip(CENSUS_BPS_CLASS_UNIT_COLUMNS, class_units, strict=True)),
                **dict(zip(CENSUS_BPS_CLASS_VALUE_COLUMNS, class_values, strict=True)),
            }
        )
    curated = tmp_path / "covariate__census_bps__Y2020-2021.parquet"
    pd.DataFrame(rows).to_parquet(curated, index=False)

    panel = aggregate_covariate_source(
        CENSUS_BPS_SOURCE_ID,
        curated_path=curated,
        output_path=tmp_path / "bps_msa.parquet",
        target_geo="msa",
        force=True,
    )

    result = pd.read_parquet(panel).set_index("year")
    assert result.loc[2020, CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN] == pytest.approx(150.0)
    assert result.loc[2021, CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN] == pytest.approx(210.0)
    naive_2021 = result.loc[2021, "permit_value_thousands"] / result.loc[2021, "permitted_units"]
    assert naive_2021 == pytest.approx(156.0)


def test_census_bps_mix_adjusted_value_masks_when_required_class_drops(
    tmp_path: Path,
) -> None:
    rows = []
    cases = [
        (2020, [10, 10, 0, 0], [1000, 2000, 0, 0]),
        (2021, [20, 0, 0, 0], [3000, 0, 0, 0]),
        (2022, [20, 5, 7, 0], [3200, 900, 7000, 0]),
    ]
    for year, class_units, class_values in cases:
        rows.append(
            {
                "geo_type": "county",
                "geo_id": "01001",
                "county_fips": "01001",
                "year": year,
                "permitted_units": sum(class_units),
                "permitted_buildings": sum(class_units),
                "permit_value_thousands": sum(class_values),
                **dict(zip(CENSUS_BPS_CLASS_UNIT_COLUMNS, class_units, strict=True)),
                **dict(zip(CENSUS_BPS_CLASS_VALUE_COLUMNS, class_values, strict=True)),
            }
        )
    curated = tmp_path / "covariate__census_bps__Y2020-2022.parquet"
    pd.DataFrame(rows).to_parquet(curated, index=False)

    panel = aggregate_covariate_source(
        CENSUS_BPS_SOURCE_ID,
        curated_path=curated,
        output_path=tmp_path / "bps_county_panel.parquet",
        target_geo="county",
        force=True,
    )

    result = pd.read_parquet(panel).set_index("year")
    assert set(BPS_REQUIRED_CLASS_DROPS_TRUTH_TABLE) == set(result.index)
    assert result.loc[2020, CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN] == pytest.approx(150.0)
    assert pd.isna(result.loc[2021, CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN])
    assert result.loc[2022, CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN] == pytest.approx(170.0)
