"""Tests for ACS 1-year metro-native ingest module."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd
import pytest

from coclab.acs.ingest.metro_acs1 import (
    CBSA_GEO_PARAM,
    fetch_acs1_cbsa_data,
    ingest_metro_acs1,
)
from coclab.acs.variables_acs1 import (
    ACS1_METRO_OUTPUT_COLUMNS,
    ACS1_UNAVAILABLE_VINTAGES,
    ACS1_UNEMPLOYMENT_VARIABLES,
)
from coclab.provenance import read_provenance

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


def make_cbsa_response(
    cbsas: list[dict[str, Any]],
) -> list[list[str]]:
    """Create a mock Census API response for ACS 1-year CBSA data.

    Parameters
    ----------
    cbsas : list[dict]
        List of CBSA data dicts. Each should have a "cbsa_code" key and
        optionally Census variable codes as keys. Missing variables default
        to "0".

    Returns
    -------
    list[list[str]]
        Census API-style JSON response: header row + data rows.
    """
    headers = ["NAME"] + ACS1_UNEMPLOYMENT_VARIABLES + [CBSA_GEO_PARAM]

    rows = [headers]
    for cbsa in cbsas:
        row = [cbsa.get("NAME", "Test Metro Area")]
        for var in ACS1_UNEMPLOYMENT_VARIABLES:
            row.append(str(cbsa.get(var, "0")))
        row.append(cbsa.get("cbsa_code", "99999"))
        rows.append(row)

    return rows


# Sample CBSA data: includes GF metros and non-GF metros
SAMPLE_CBSAS = [
    {
        "NAME": "New York-Newark-Jersey City, NY-NJ-PA Metro Area",
        "cbsa_code": "35620",  # GF01
        "B23025_001E": "16000000",
        "B23025_003E": "10000000",
        "B23025_005E": "500000",
    },
    {
        "NAME": "Los Angeles-Long Beach-Anaheim, CA Metro Area",
        "cbsa_code": "31080",  # GF02
        "B23025_001E": "10500000",
        "B23025_003E": "6800000",
        "B23025_005E": "340000",
    },
    {
        "NAME": "Denver-Aurora-Lakewood, CO Metro Area",
        "cbsa_code": "19740",  # GF21
        "B23025_001E": "2400000",
        "B23025_003E": "1600000",
        "B23025_005E": "48000",
    },
    {
        # A CBSA NOT in Glynn/Fox mapping -- should be dropped
        "NAME": "Abilene, TX Metro Area",
        "cbsa_code": "10180",
        "B23025_001E": "130000",
        "B23025_003E": "85000",
        "B23025_005E": "3000",
    },
    {
        # Another non-GF CBSA
        "NAME": "Albany, GA Metro Area",
        "cbsa_code": "10500",
        "B23025_001E": "120000",
        "B23025_003E": "75000",
        "B23025_005E": "5000",
    },
]


CENSUS_API_URL_PATTERN = re.compile(
    r"https://api\.census\.gov/data/\d{4}/acs/acs1.*"
)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFetchParsesCensusResponse:
    """Test that fetch_acs1_cbsa_data correctly parses Census API responses."""

    def test_fetch_parses_census_response(self, httpx_mock):
        """Mock API returns valid data, verify DataFrame shape and columns."""
        response_data = make_cbsa_response(SAMPLE_CBSAS)

        httpx_mock.add_response(
            url=CENSUS_API_URL_PATTERN,
            json=response_data,
        )

        df = fetch_acs1_cbsa_data(vintage=2023)

        # Should have all 5 CBSAs
        assert len(df) == 5

        # Should have variable columns and cbsa_code
        assert "cbsa_code" in df.columns
        for var in ACS1_UNEMPLOYMENT_VARIABLES:
            assert var in df.columns

        # Verify numeric conversion happened
        assert df["B23025_001E"].dtype in ("int64", "float64", "Int64", "Float64")

    def test_fetch_handles_missing_values(self, httpx_mock):
        """Negative values (Census missing indicator) are converted to NA."""
        cbsas = [
            {
                "cbsa_code": "35620",
                "B23025_001E": "-666666666",
                "B23025_003E": "-666666666",
                "B23025_005E": "-666666666",
            }
        ]
        response_data = make_cbsa_response(cbsas)

        httpx_mock.add_response(
            url=CENSUS_API_URL_PATTERN,
            json=response_data,
        )

        df = fetch_acs1_cbsa_data(vintage=2023)

        assert len(df) == 1
        assert pd.isna(df.iloc[0]["B23025_001E"])
        assert pd.isna(df.iloc[0]["B23025_003E"])
        assert pd.isna(df.iloc[0]["B23025_005E"])

    def test_fetch_empty_response_raises(self, httpx_mock):
        """Empty API response raises ValueError with actionable message."""
        httpx_mock.add_response(
            url=CENSUS_API_URL_PATTERN,
            json=[],
        )

        with pytest.raises(ValueError, match="empty or invalid response"):
            fetch_acs1_cbsa_data(vintage=2023)


class TestCbsaToMetroMapping:
    """Test that CBSA-to-metro mapping works correctly."""

    def test_cbsa_to_metro_mapping(self, httpx_mock, tmp_path):
        """Verify only GF metros are retained, others dropped."""
        response_data = make_cbsa_response(SAMPLE_CBSAS)

        httpx_mock.add_response(
            url=CENSUS_API_URL_PATTERN,
            json=response_data,
        )

        path = ingest_metro_acs1(
            vintage=2023,
            project_root=tmp_path,
        )

        df = pd.read_parquet(path)

        # Should have exactly 3 GF metros (35620=GF01, 31080=GF02, 19740=GF21)
        assert len(df) == 3
        assert set(df["metro_id"]) == {"GF01", "GF02", "GF21"}

    def test_unknown_cbsa_dropped(self, httpx_mock):
        """CBSAs not in mapping are excluded; if none map, raise ValueError."""
        cbsas = [
            {
                "cbsa_code": "99999",  # Not a real CBSA
                "B23025_001E": "100000",
                "B23025_003E": "50000",
                "B23025_005E": "5000",
            },
            {
                "cbsa_code": "88888",  # Also not real
                "B23025_001E": "200000",
                "B23025_003E": "100000",
                "B23025_005E": "8000",
            },
        ]
        response_data = make_cbsa_response(cbsas)

        httpx_mock.add_response(
            url=CENSUS_API_URL_PATTERN,
            json=response_data,
        )

        with pytest.raises(ValueError, match="No CBSAs.*could be mapped"):
            ingest_metro_acs1(vintage=2023)


class TestUnemploymentRateCalculation:
    """Test unemployment rate derivation."""

    def test_unemployment_rate_calculation(self, httpx_mock, tmp_path):
        """Verify rate = unemployed/civilian_labor_force."""
        cbsas = [
            {
                "cbsa_code": "35620",  # GF01
                "B23025_001E": "16000000",
                "B23025_003E": "10000000",
                "B23025_005E": "500000",
            },
        ]
        response_data = make_cbsa_response(cbsas)

        httpx_mock.add_response(
            url=CENSUS_API_URL_PATTERN,
            json=response_data,
        )

        path = ingest_metro_acs1(vintage=2023, project_root=tmp_path)
        df = pd.read_parquet(path)

        assert len(df) == 1
        row = df.iloc[0]
        expected_rate = 500000 / 10000000  # 0.05
        assert abs(row["unemployment_rate_acs1"] - expected_rate) < 1e-10

    def test_division_by_zero_handling(self, httpx_mock, tmp_path):
        """civilian_labor_force=0 produces NaN unemployment rate."""
        cbsas = [
            {
                "cbsa_code": "35620",  # GF01
                "B23025_001E": "100000",
                "B23025_003E": "0",  # Zero labor force
                "B23025_005E": "0",
            },
        ]
        response_data = make_cbsa_response(cbsas)

        httpx_mock.add_response(
            url=CENSUS_API_URL_PATTERN,
            json=response_data,
        )

        path = ingest_metro_acs1(vintage=2023, project_root=tmp_path)
        df = pd.read_parquet(path)

        assert len(df) == 1
        assert pd.isna(df.iloc[0]["unemployment_rate_acs1"])


class TestOutputSchema:
    """Test that output matches canonical schema."""

    def test_output_schema(self, httpx_mock, tmp_path):
        """Output matches ACS1_METRO_OUTPUT_COLUMNS."""
        cbsas = [
            {
                "cbsa_code": "35620",  # GF01
                "B23025_001E": "16000000",
                "B23025_003E": "10000000",
                "B23025_005E": "500000",
            },
            {
                "cbsa_code": "19740",  # GF21
                "B23025_001E": "2400000",
                "B23025_003E": "1600000",
                "B23025_005E": "48000",
            },
        ]
        response_data = make_cbsa_response(cbsas)

        httpx_mock.add_response(
            url=CENSUS_API_URL_PATTERN,
            json=response_data,
        )

        path = ingest_metro_acs1(vintage=2023, project_root=tmp_path)
        df = pd.read_parquet(path)

        # All canonical columns should be present
        for col in ACS1_METRO_OUTPUT_COLUMNS:
            assert col in df.columns, f"Missing column: {col}"

        # Column order should match canonical order
        output_cols = list(df.columns)
        canonical = [c for c in ACS1_METRO_OUTPUT_COLUMNS if c in output_cols]
        assert output_cols == canonical


class TestProvenanceColumns:
    """Test provenance columns in output."""

    def test_provenance_columns(self, httpx_mock, tmp_path):
        """data_source, source_ref, ingested_at are present and correct."""
        cbsas = [
            {
                "cbsa_code": "35620",  # GF01
                "B23025_001E": "16000000",
                "B23025_003E": "10000000",
                "B23025_005E": "500000",
            },
        ]
        response_data = make_cbsa_response(cbsas)

        httpx_mock.add_response(
            url=CENSUS_API_URL_PATTERN,
            json=response_data,
        )

        path = ingest_metro_acs1(vintage=2023, project_root=tmp_path)
        df = pd.read_parquet(path)

        row = df.iloc[0]
        assert row["data_source"] == "census_acs1"
        assert "acs1" in row["source_ref"]
        assert "B23025" in row["source_ref"]
        assert pd.notna(row["ingested_at"])
        assert row["acs1_vintage"] == "2023"
        assert row["cbsa_code"] == "35620"

        # Check embedded Parquet provenance metadata
        prov = read_provenance(path)
        assert prov is not None
        assert prov.acs_vintage == "2023"
        assert prov.geo_type == "metro"
        assert prov.definition_version == "glynn_fox_v1"
        assert prov.extra["acs_product"] == "acs1"
        assert prov.extra["dataset_type"] == "metro_acs1_unemployment"


class TestIngestWritesParquet:
    """Full integration test: mock API -> written file with correct schema."""

    def test_ingest_writes_parquet(self, httpx_mock, tmp_path):
        """Full pipeline: fetch from mock API, write Parquet, verify contents."""
        response_data = make_cbsa_response(SAMPLE_CBSAS)

        httpx_mock.add_response(
            url=CENSUS_API_URL_PATTERN,
            json=response_data,
        )

        path = ingest_metro_acs1(
            vintage=2023,
            definition_version="glynn_fox_v1",
            project_root=tmp_path,
        )

        assert path.exists()
        assert path.suffix == ".parquet"

        df = pd.read_parquet(path)

        # Should contain only GF metros (3 of 5 CBSAs)
        assert len(df) == 3
        assert set(df["metro_id"]) == {"GF01", "GF02", "GF21"}

        # All should have unemployment rates
        assert df["unemployment_rate_acs1"].notna().all()

        # Verify GF01 rate: 500000 / 10000000 = 0.05
        gf01 = df[df["metro_id"] == "GF01"].iloc[0]
        assert abs(gf01["unemployment_rate_acs1"] - 0.05) < 1e-10

        # Verify GF02 rate: 340000 / 6800000 = 0.05
        gf02 = df[df["metro_id"] == "GF02"].iloc[0]
        assert abs(gf02["unemployment_rate_acs1"] - 0.05) < 1e-10

        # Verify GF21 rate: 48000 / 1600000 = 0.03
        gf21 = df[df["metro_id"] == "GF21"].iloc[0]
        assert abs(gf21["unemployment_rate_acs1"] - 0.03) < 1e-10

        # Verify column types
        assert df["metro_id"].dtype == "object"  # str
        assert df["pop_16_plus"].dtype == "Int64"
        assert df["civilian_labor_force"].dtype == "Int64"
        assert df["unemployed_count"].dtype == "Int64"
        assert df["unemployment_rate_acs1"].dtype == "Float64"

        # Verify sorted by metro_id
        assert list(df["metro_id"]) == sorted(df["metro_id"])

        # Verify metro names are present
        assert df["metro_name"].notna().all()
        assert "New York" in df[df["metro_id"] == "GF01"].iloc[0]["metro_name"]


class TestAcs12020Unavailability:
    def test_2020_vintage_raises_before_api_call(self):
        """fetch_acs1_cbsa_data must raise ValueError for 2020 without hitting the API."""
        with pytest.raises(ValueError, match="not available from Census"):
            fetch_acs1_cbsa_data(vintage=2020)

    def test_2020_ingest_raises_actionable_error(self, tmp_path):
        """ingest_metro_acs1 should raise with a message suggesting LAUS."""
        with pytest.raises(ValueError) as exc_info:
            ingest_metro_acs1(vintage=2020, project_root=tmp_path)
        msg = str(exc_info.value)
        assert "2020" in msg
        assert "laus" in msg.lower() or "LAUS" in msg

    def test_unavailable_vintages_constant(self):
        assert 2020 in ACS1_UNAVAILABLE_VINTAGES

    def test_other_vintages_not_blocked(self):
        """Vintages other than 2020 should not be blocked by the unavailability check."""
        assert 2019 not in ACS1_UNAVAILABLE_VINTAGES
        assert 2021 not in ACS1_UNAVAILABLE_VINTAGES
        assert 2023 not in ACS1_UNAVAILABLE_VINTAGES
