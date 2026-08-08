"""Tests for MEDSL county-year political leaning materialization."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hhplab.sources.medsl.medsl.materialize import (
    build_county_political_leaning_measures,
    materialize_county_political_leaning,
)
from hhplab.storage.provenance import read_provenance
from tests.test_medsl_ingest import FIXTURE_YEARS, write_medsl_fixture


def candidate_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "geo_type": "county",
                "geo_id": "01001",
                "county_fips": "01001",
                "year": 2000,
                "party_simplified": "DEMOCRAT",
                "candidatevotes": 4942,
                "totalvotes": 17208,
                "data_source": "medsl",
                "source_url": "https://example.test/medsl",
                "source_version": "20260225",
                "raw_sha256": "abc123",
            },
            {
                "geo_type": "county",
                "geo_id": "01001",
                "county_fips": "01001",
                "year": 2000,
                "party_simplified": "REPUBLICAN",
                "candidatevotes": 11993,
                "totalvotes": 17208,
                "data_source": "medsl",
                "source_url": "https://example.test/medsl",
                "source_version": "20260225",
                "raw_sha256": "abc123",
            },
            {
                "geo_type": "county",
                "geo_id": "01001",
                "county_fips": "01001",
                "year": 2000,
                "party_simplified": "OTHER",
                "candidatevotes": 273,
                "totalvotes": 17208,
                "data_source": "medsl",
                "source_url": "https://example.test/medsl",
                "source_version": "20260225",
                "raw_sha256": "abc123",
            },
        ]
    )


def test_build_county_political_leaning_measures() -> None:
    result = build_county_political_leaning_measures(candidate_rows(), county_vintage=2020)

    row = result.iloc[0]
    assert len(result) == 1
    assert row["geo_type"] == "county"
    assert row["geo_id"] == "01001"
    assert row["democratic_votes"] == 4942
    assert row["republican_votes"] == 11993
    assert row["two_party_votes"] == 16935
    assert row["totalvotes"] == 17208
    assert row["democratic_vote_share"] == pytest.approx(4942 / 17208)
    assert row["republican_vote_share"] == pytest.approx(11993 / 17208)
    assert row["democratic_republican_vote_ratio"] == pytest.approx(4942 / 11993)
    assert row["republican_democratic_vote_ratio"] == pytest.approx(11993 / 4942)
    assert row["democratic_margin"] == pytest.approx((4942 - 11993) / 17208)
    assert row["major_party_vote_share"] == pytest.approx(16935 / 17208)


def test_build_county_political_leaning_measures_flags_cross_era_fips() -> None:
    retired_fips_rows = candidate_rows().assign(county_fips="46113", geo_id="46113", year=2000)
    rows = pd.concat([candidate_rows(), retired_fips_rows], ignore_index=True)

    result = build_county_political_leaning_measures(
        rows,
        county_vintage=2020,
        declared_county_fips={"01001", "46102"},
    )

    assert set(result["county_fips"]) == {"01001", "46113"}
    assert result.attrs["county_vintage_universe_checked"] is True
    assert result.attrs["county_fips_not_in_declared_vintage_count"] == 1
    assert result.attrs["county_fips_not_in_declared_vintage"] == ["46113"]


def test_ratio_denominators_of_zero_are_null() -> None:
    rows = candidate_rows()
    rows["candidatevotes"] = 0
    rows["totalvotes"] = 0

    result = build_county_political_leaning_measures(rows)
    row = result.iloc[0]

    assert pd.isna(row["democratic_vote_share"])
    assert pd.isna(row["republican_vote_share"])
    assert pd.isna(row["democratic_republican_vote_ratio"])
    assert pd.isna(row["republican_democratic_vote_ratio"])
    assert pd.isna(row["major_party_vote_share"])


def test_inconsistent_totalvotes_raise() -> None:
    rows = candidate_rows()
    rows.loc[1, "totalvotes"] = 999

    with pytest.raises(ValueError, match="totalvotes vary"):
        build_county_political_leaning_measures(rows)


def test_materialize_county_political_leaning_writes_provenance(tmp_path: Path) -> None:
    raw_path = tmp_path / "countypres_2000-2024.tab"
    output_dir = tmp_path / "curated" / "medsl"
    write_medsl_fixture(raw_path)

    output = materialize_county_political_leaning(
        raw_path,
        output_dir=output_dir,
        expected_years=FIXTURE_YEARS,
    )

    result = pd.read_parquet(output)
    provenance = read_provenance(output)
    assert output == output_dir / "medsl_president_county__Y2000-2004@C2020.parquet"
    assert set(result["year"]) == {2000, 2004}
    assert {"democratic_votes", "republican_votes", "democratic_margin"}.issubset(result.columns)
    assert provenance is not None
    assert provenance.extra["raw_sha256"]
    assert provenance.extra["source_file_path"] == str(raw_path)
    assert provenance.extra["county_vintage_universe_checked"] is True
    assert provenance.extra["county_fips_not_in_declared_vintage_count"] == 0
    assert provenance.extra["output_schema"] == list(result.columns)
