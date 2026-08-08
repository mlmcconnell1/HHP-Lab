"""Tests for Vera county incarceration trends ingestion.

Truth table for the fixture rows:

| row label       | expected outcome                                      |
|-----------------|-------------------------------------------------------|
| complete-2023   | kept as county `01001`, jail/prison counts numeric    |
| jail-only-2024  | kept as county `01003`, prison columns allowed null   |
| boolean-strings | converted to nullable boolean columns                 |
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hhplab.schema.columns import VERA_INCARCERATION_COUNTY_MEASURE_COLUMNS
from hhplab.sources.vera.ingest import (
    VERA_REQUIRED_COLUMNS,
    ingest_county_incarceration_trends,
    parse_county_incarceration_trends,
)
from hhplab.storage.provenance import read_provenance

FIXTURE_YEARS = (2023, 2024)
VERA_FIXTURE_ROWS: tuple[dict[str, object], ...] = (
    {
        "year": 2023,
        "county_fips": "1001",
        "county_name": "Autauga County",
        "state_abbr": "AL",
        "state_fips": "01",
        "urbanicity": "small/mid",
        "region": "South",
        "division": "East South Central",
        "metro_area": "33860",
        "commuting_zone": "111",
        "land_area": 1539.6,
        "is_regional_jail": "false",
        "is_unified_state": "false",
        "total_jail_pop": 120,
        "male_jail_pop": 90,
        "female_jail_pop": 30,
        "black_jail_pop": 40,
        "latinx_jail_pop": 5,
        "white_jail_pop": 75,
        "native_jail_pop": 0,
        "aapi_jail_pop": 0,
        "other_race_jail_pop": 0,
        "total_pretrial_custody": 80,
        "total_sentenced_custody": 40,
        "total_jail_admits": 1000,
        "male_jail_admits": 700,
        "female_jail_admits": 300,
        "total_jail_discharges": 950,
        "male_jail_discharges": 650,
        "female_jail_discharges": 300,
        "jail_rated_capacity": 150,
        "total_prison_pop": 44,
        "male_prison_pop": 40,
        "female_prison_pop": 4,
        "black_prison_pop": 20,
        "latinx_prison_pop": 2,
        "white_prison_pop": 22,
        "native_prison_pop": 0,
        "aapi_prison_pop": 0,
        "other_race_prison_pop": 0,
        "total_prison_admits": 12,
        "male_prison_admits": 10,
        "female_prison_admits": 2,
        "total_pop_15to64": 39000,
        "male_pop_15to64": 19000,
        "female_pop_15to64": 20000,
        "black_pop_15to64": 7000,
        "latinx_pop_15to64": 1000,
        "white_pop_15to64": 30000,
        "native_pop_15to64": 200,
        "aapi_pop_15to64": 800,
        "total_incarceration": 164,
    },
    {
        "year": 2024,
        "county_fips": "01003",
        "county_name": "Baldwin County",
        "state_abbr": "AL",
        "state_fips": "01",
        "urbanicity": "suburban",
        "region": "South",
        "division": "East South Central",
        "metro_area": "19300",
        "commuting_zone": "112",
        "land_area": 4117.5,
        "is_regional_jail": "true",
        "is_unified_state": "false",
        "total_jail_pop": 240,
        "male_jail_pop": 180,
        "female_jail_pop": 60,
        "total_pop_15to64": 150000,
        "male_pop_15to64": 73000,
        "female_pop_15to64": 77000,
        "total_incarceration": 240,
    },
)


def write_vera_fixture(path: Path, rows: tuple[dict[str, object], ...] = VERA_FIXTURE_ROWS) -> None:
    columns = list(
        dict.fromkeys(
            [
                *VERA_REQUIRED_COLUMNS,
                "urbanicity",
                "region",
                "division",
                "metro_area",
                "commuting_zone",
                "land_area",
                "is_regional_jail",
                "is_unified_state",
                *[
                    column
                    for column in VERA_INCARCERATION_COUNTY_MEASURE_COLUMNS
                    if column.endswith("_pop")
                    or column.endswith("_admits")
                    or column.endswith("_discharges")
                    or column
                    in {
                        "total_pretrial_custody",
                        "total_sentenced_custody",
                        "jail_rated_capacity",
                        "total_pop_15to64",
                        "male_pop_15to64",
                        "female_pop_15to64",
                        "black_pop_15to64",
                        "latinx_pop_15to64",
                        "white_pop_15to64",
                        "native_pop_15to64",
                        "aapi_pop_15to64",
                        "total_incarceration",
                    }
                ],
            ]
        )
    )
    pd.DataFrame(rows).reindex(columns=columns).to_csv(path, index=False)


def test_parse_county_incarceration_trends(tmp_path: Path) -> None:
    raw_path = tmp_path / "incarceration_trends_county.csv"
    write_vera_fixture(raw_path)

    result = parse_county_incarceration_trends(
        raw_path,
        county_vintage=2020,
        expected_years=FIXTURE_YEARS,
    )

    assert list(result.columns) == VERA_INCARCERATION_COUNTY_MEASURE_COLUMNS
    assert result["geo_type"].eq("county").all()
    assert list(result["county_fips"]) == ["01001", "01003"]
    assert list(result["geo_id"]) == ["01001", "01003"]
    assert set(result["year"]) == {2023, 2024}
    assert result.loc[result["county_fips"] == "01001", "total_jail_pop"].iloc[0] == 120
    assert pd.isna(result.loc[result["county_fips"] == "01003", "total_prison_pop"].iloc[0])
    assert result.loc[result["county_fips"] == "01003", "is_regional_jail"].iloc[0]
    assert result["raw_sha256"].str.len().eq(64).all()


def test_parse_missing_expected_year_raises(tmp_path: Path) -> None:
    raw_path = tmp_path / "incarceration_trends_county.csv"
    write_vera_fixture(raw_path)

    with pytest.raises(ValueError, match="missing expected years"):
        parse_county_incarceration_trends(raw_path, expected_years=(2022, 2023, 2024))


def test_ingest_county_incarceration_trends_writes_provenance(tmp_path: Path) -> None:
    raw_path = tmp_path / "incarceration_trends_county.csv"
    output_dir = tmp_path / "curated" / "vera"
    write_vera_fixture(raw_path)

    output = ingest_county_incarceration_trends(
        raw_path,
        output_dir=output_dir,
        expected_years=FIXTURE_YEARS,
        download=False,
        force=True,
    )

    result = pd.read_parquet(output)
    provenance = read_provenance(output)
    assert output == output_dir / "vera_incarceration_county__Y2023-2024@C2020.parquet"
    assert len(result) == len(VERA_FIXTURE_ROWS)
    assert provenance is not None
    assert provenance.extra["source_file_path"] == str(raw_path)
    assert provenance.extra["county_vintage"] == 2020
    assert provenance.extra["output_schema"] == list(result.columns)
