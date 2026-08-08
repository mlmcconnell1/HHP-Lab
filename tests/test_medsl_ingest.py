"""Tests for MEDSL county presidential returns ingestion.

Truth table for the fixture rows:

| row label          | expected outcome                                      |
|--------------------|-------------------------------------------------------|
| normal-democrat    | kept as county `01001`, party_simplified `DEMOCRAT`   |
| normal-republican  | kept as county `01001`, party_simplified `REPUBLICAN` |
| dc-equivalent      | kept as county-equivalent `11001`                     |
| missing-party      | kept with `party == UNKNOWN`                          |
| statewide-writein  | dropped because it is a known non-county row          |
| kansas-city-place  | dropped because it is a known non-county place row    |
| alaska-district    | dropped because Alaska districts are not counties     |
| absentee-mode      | dropped because mode is not `TOTAL`                   |
| no-total-mode      | counted because no `TOTAL` row exists for county/year |
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hhplab.sources.medsl.medsl.ingest import (
    MEDSL_COUNTY_PRESIDENTIAL_COLUMNS,
    ingest_county_presidential_returns,
    parse_county_presidential_returns,
)
from hhplab.storage.provenance import read_provenance

FIXTURE_YEARS = (2000, 2004)
MEDSL_FIXTURE_ROWS = [
    {
        "state": "ALABAMA",
        "county_name": "AUTAUGA",
        "year": 2000,
        "state_po": "AL",
        "county_fips": "1001",
        "office": "US PRESIDENT",
        "candidate": "AL GORE",
        "party": "DEMOCRAT",
        "candidatevotes": 4942,
        "totalvotes": 17208,
        "version": 20260225,
        "mode": "TOTAL",
    },
    {
        "state": "ALABAMA",
        "county_name": "AUTAUGA",
        "year": 2000,
        "state_po": "AL",
        "county_fips": "1001",
        "office": "US PRESIDENT",
        "candidate": "GEORGE W. BUSH",
        "party": "REPUBLICAN",
        "candidatevotes": 11993,
        "totalvotes": 17208,
        "version": 20260225,
        "mode": "TOTAL",
    },
    {
        "state": "DISTRICT OF COLUMBIA",
        "county_name": "DISTRICT OF COLUMBIA",
        "year": 2004,
        "state_po": "DC",
        "county_fips": "11001",
        "office": "US PRESIDENT",
        "candidate": "JOHN KERRY",
        "party": "DEMOCRATIC",
        "candidatevotes": 202970,
        "totalvotes": 227586,
        "version": 20260225,
        "mode": "TOTAL",
    },
    {
        "state": "ALABAMA",
        "county_name": "BALDWIN",
        "year": 2004,
        "state_po": "AL",
        "county_fips": "1003",
        "office": "US PRESIDENT",
        "candidate": "WRITE-IN",
        "party": "",
        "candidatevotes": 7,
        "totalvotes": 76205,
        "version": 20260225,
        "mode": "TOTAL",
    },
    {
        "state": "CONNECTICUT",
        "county_name": "STATEWIDE WRITEIN",
        "year": 2000,
        "state_po": "CT",
        "county_fips": "",
        "office": "US PRESIDENT",
        "candidate": "OTHER",
        "party": "OTHER",
        "candidatevotes": 0,
        "totalvotes": 0,
        "version": 20260225,
        "mode": "TOTAL",
    },
    {
        "state": "ALABAMA",
        "county_name": "AUTAUGA",
        "year": 2000,
        "state_po": "AL",
        "county_fips": "1001",
        "office": "US PRESIDENT",
        "candidate": "AL GORE",
        "party": "DEMOCRAT",
        "candidatevotes": 30,
        "totalvotes": 50,
        "version": 20260225,
        "mode": "ABSENTEE",
    },
    {
        "state": "MISSOURI",
        "county_name": "KANSAS CITY",
        "year": 2000,
        "state_po": "MO",
        "county_fips": "2938000",
        "office": "US PRESIDENT",
        "candidate": "AL GORE",
        "party": "DEMOCRAT",
        "candidatevotes": 100,
        "totalvotes": 250,
        "version": 20260225,
        "mode": "TOTAL",
    },
    {
        "state": "ALASKA",
        "county_name": "DISTRICT 1",
        "year": 2000,
        "state_po": "AK",
        "county_fips": "2001",
        "office": "US PRESIDENT",
        "candidate": "AL GORE",
        "party": "DEMOCRAT",
        "candidatevotes": 1000,
        "totalvotes": 2500,
        "version": 20260225,
        "mode": "TOTAL",
    },
    {
        "state": "ALABAMA",
        "county_name": "BARBOUR",
        "year": 2004,
        "state_po": "AL",
        "county_fips": "1005",
        "office": "US PRESIDENT",
        "candidate": "MODE ONLY DEMOCRAT",
        "party": "DEMOCRAT",
        "candidatevotes": 4000,
        "totalvotes": 9000,
        "version": 20260225,
        "mode": "ABSENTEE",
    },
    {
        "state": "ALABAMA",
        "county_name": "BARBOUR",
        "year": 2004,
        "state_po": "AL",
        "county_fips": "1005",
        "office": "US PRESIDENT",
        "candidate": "MODE ONLY DEMOCRAT",
        "party": "DEMOCRAT",
        "candidatevotes": 300,
        "totalvotes": 9000,
        "version": 20260225,
        "mode": "ELECTION DAY",
    },
]


def write_medsl_fixture(path: Path, rows: list[dict[str, object]] | None = None) -> None:
    frame = pd.DataFrame(rows or MEDSL_FIXTURE_ROWS, columns=MEDSL_COUNTY_PRESIDENTIAL_COLUMNS)
    frame.to_csv(path, sep="\t", index=False)


class TestParseCountyPresidentialReturns:
    def test_parses_total_presidential_rows(self, tmp_path: Path) -> None:
        raw_path = tmp_path / "countypres_2000-2024.tab"
        write_medsl_fixture(raw_path)

        result = parse_county_presidential_returns(raw_path, expected_years=FIXTURE_YEARS)

        assert len(result) == 5
        assert list(result.columns) == [
            "geo_type",
            "geo_id",
            "county_fips",
            "state",
            "state_po",
            "county_name",
            "year",
            "office",
            "candidate",
            "party",
            "party_simplified",
            "candidatevotes",
            "totalvotes",
            "source_version",
            "data_source",
            "source_url",
            "source_doi",
            "raw_sha256",
            "ingested_at",
        ]
        assert set(result["geo_type"]) == {"county"}
        assert {"01001", "01003", "01005", "11001"} == set(result["county_fips"])
        assert not (result["candidatevotes"] == 30).any()
        assert result.loc[
            (result["county_fips"] == "01005") & (result["candidate"] == "MODE ONLY DEMOCRAT"),
            "candidatevotes",
        ].iloc[0] == 4300
        assert "02001" not in set(result["county_fips"])
        assert result.attrs["missing_county_fips_dropped"] == 1
        assert result.attrs["invalid_county_fips_dropped"] == 1
        assert result.attrs["alaska_election_district_rows_dropped"] == 1
        assert result.attrs["presidential_county_years_without_total"] == 1

    @pytest.mark.parametrize(
        ("candidate", "expected_party", "expected_simplified"),
        [
            ("AL GORE", "DEMOCRAT", "DEMOCRAT"),
            ("GEORGE W. BUSH", "REPUBLICAN", "REPUBLICAN"),
            ("JOHN KERRY", "DEMOCRATIC", "DEMOCRAT"),
            ("WRITE-IN", "UNKNOWN", "UNKNOWN"),
        ],
        ids=["democrat", "republican", "democratic-alias", "missing-party"],
    )
    def test_normalizes_party_labels(
        self,
        tmp_path: Path,
        candidate: str,
        expected_party: str,
        expected_simplified: str,
    ) -> None:
        raw_path = tmp_path / "countypres_2000-2024.tab"
        write_medsl_fixture(raw_path)

        result = parse_county_presidential_returns(raw_path, expected_years=FIXTURE_YEARS)
        row = result[result["candidate"] == candidate].iloc[0]

        assert row["party"] == expected_party
        assert row["party_simplified"] == expected_simplified

    def test_rejects_nonzero_missing_county_fips(self, tmp_path: Path) -> None:
        raw_path = tmp_path / "bad.tab"
        unknown_missing_fips = {
            **MEDSL_FIXTURE_ROWS[4],
            "state_po": "XX",
            "county_name": "UNKNOWN FLOATING ROW",
            "totalvotes": 10,
        }
        rows = [MEDSL_FIXTURE_ROWS[0], unknown_missing_fips]
        write_medsl_fixture(raw_path, rows)

        with pytest.raises(ValueError, match="missing county_fips"):
            parse_county_presidential_returns(raw_path, expected_years=(2000,))

    def test_fills_missing_candidate_votes(self, tmp_path: Path) -> None:
        raw_path = tmp_path / "bad.tab"
        rows = [{**MEDSL_FIXTURE_ROWS[0], "candidatevotes": None}]
        write_medsl_fixture(raw_path, rows)

        result = parse_county_presidential_returns(raw_path, expected_years=(2000,))

        assert result.iloc[0]["candidatevotes"] == 0
        assert result.attrs["missing_candidatevotes_filled"] == 1

    def test_rejects_missing_total_votes(self, tmp_path: Path) -> None:
        raw_path = tmp_path / "bad.tab"
        rows = [{**MEDSL_FIXTURE_ROWS[0], "totalvotes": None}]
        write_medsl_fixture(raw_path, rows)

        with pytest.raises(ValueError, match="totalvotes"):
            parse_county_presidential_returns(raw_path, expected_years=(2000,))

    def test_rejects_coverage_mismatch(self, tmp_path: Path) -> None:
        raw_path = tmp_path / "bad.tab"
        write_medsl_fixture(raw_path, [MEDSL_FIXTURE_ROWS[0]])

        with pytest.raises(ValueError, match="coverage mismatch"):
            parse_county_presidential_returns(raw_path, expected_years=FIXTURE_YEARS)


class TestIngestCountyPresidentialReturns:
    def test_writes_parquet_with_provenance(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        raw_path = tmp_path / "countypres_2000-2024.tab"
        output_dir = tmp_path / "curated"
        write_medsl_fixture(raw_path)
        registered: list[dict[str, object]] = []

        def fake_register_source(**kwargs: object) -> None:
            registered.append(kwargs)

        monkeypatch.setattr(
            "hhplab.sources.medsl.medsl.ingest.register_source", fake_register_source
        )

        output = ingest_county_presidential_returns(
            raw_path,
            output_dir=output_dir,
            expected_years=FIXTURE_YEARS,
        )

        result = pd.read_parquet(output)
        provenance = read_provenance(output)
        assert output == output_dir / "medsl_county_presidential_returns__Y2000-2024.parquet"
        assert len(result) == 5
        assert provenance is not None
        assert provenance.extra["dataset"] == "medsl_county_presidential_returns"
        assert provenance.extra["years"] == [2000, 2004]
        assert provenance.extra["alaska_election_district_rows_dropped"] == 1
        assert provenance.extra["presidential_county_years_without_total"] == 1
        assert provenance.extra["mode_filter"] == (
            "TOTAL when present, otherwise aggregate county-year mode rows"
        )
        assert registered[0]["source_type"] == "medsl"
        assert registered[0]["metadata"]["mode_filter"] == (
            "TOTAL when present, otherwise aggregate county-year mode rows"
        )
