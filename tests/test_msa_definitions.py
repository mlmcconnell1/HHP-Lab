"""Tests for Census MSA definition parsing, builders, and validation."""

from __future__ import annotations

from io import BytesIO

import pandas as pd
import pytest

from hhplab.msa.definitions import (
    DEFINITION_VERSION,
    MSA_AREA_TYPE,
    build_county_membership_df,
    build_definitions_df,
    parse_delineation_workbook,
)
from hhplab.msa.validate import validate_msa_artifacts

WORKBOOK_ROWS = [
    {
        "CBSA Code": 35620,
        "Metropolitan Division Code": None,
        "CSA Code": 408,
        "CBSA Title": "New York-Newark-Jersey City, NY-NJ-PA",
        "Metropolitan/Micropolitan Statistical Area": "Metropolitan Statistical Area",
        "Metropolitan Division Title": None,
        "CSA Title": "New York-Newark, NY-NJ-CT-PA",
        "County/County Equivalent": "New York County",
        "State Name": "New York",
        "FIPS State Code": 36,
        "FIPS County Code": 61,
        "Central/Outlying County": "Central",
    },
    {
        "CBSA Code": 35620,
        "Metropolitan Division Code": None,
        "CSA Code": 408,
        "CBSA Title": "New York-Newark-Jersey City, NY-NJ-PA",
        "Metropolitan/Micropolitan Statistical Area": "Metropolitan Statistical Area",
        "Metropolitan Division Title": None,
        "CSA Title": "New York-Newark, NY-NJ-CT-PA",
        "County/County Equivalent": "Kings County",
        "State Name": "New York",
        "FIPS State Code": 36,
        "FIPS County Code": 47,
        "Central/Outlying County": "Central",
    },
    {
        "CBSA Code": 31080,
        "Metropolitan Division Code": None,
        "CSA Code": 348,
        "CBSA Title": "Los Angeles-Long Beach-Anaheim, CA",
        "Metropolitan/Micropolitan Statistical Area": "Metropolitan Statistical Area",
        "Metropolitan Division Title": None,
        "CSA Title": "Los Angeles-Long Beach, CA",
        "County/County Equivalent": "Los Angeles County",
        "State Name": "California",
        "FIPS State Code": 6,
        "FIPS County Code": 37,
        "Central/Outlying County": "Central",
    },
    {
        "CBSA Code": 10100,
        "Metropolitan Division Code": None,
        "CSA Code": None,
        "CBSA Title": "Aberdeen, SD",
        "Metropolitan/Micropolitan Statistical Area": "Micropolitan Statistical Area",
        "Metropolitan Division Title": None,
        "CSA Title": None,
        "County/County Equivalent": "Brown County",
        "State Name": "South Dakota",
        "FIPS State Code": 46,
        "FIPS County Code": 13,
        "Central/Outlying County": "Central",
    },
    {
        "CBSA Code": (
            "Note: The Office of Management and Budget's (OMB's) 2020 Standards "
            "for Delineating Core Based Statistical Areas"
        ),
        "Metropolitan Division Code": None,
        "CSA Code": None,
        "CBSA Title": None,
        "Metropolitan/Micropolitan Statistical Area": None,
        "Metropolitan Division Title": None,
        "CSA Title": None,
        "County/County Equivalent": None,
        "State Name": None,
        "FIPS State Code": None,
        "FIPS County Code": None,
        "Central/Outlying County": None,
    },
]

EXPECTED_MSA_IDS = ["31080", "35620"]
EXPECTED_COUNTY_FIPS = ["06037", "36047", "36061"]


def _workbook_bytes() -> bytes:
    data = pd.DataFrame(WORKBOOK_ROWS)
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        data.to_excel(writer, index=False, startrow=2)
    return buffer.getvalue()


class TestParseDelineationWorkbook:
    def test_standardizes_expected_columns(self):
        parsed = parse_delineation_workbook(_workbook_bytes())
        assert list(parsed.columns) == [
            "cbsa_code",
            "cbsa_title",
            "area_type",
            "county_name",
            "state_name",
            "county_fips",
            "central_outlying",
        ]
        assert parsed.loc[0, "cbsa_code"] == "35620"
        assert parsed.loc[2, "county_fips"] == "06037"
        assert len(parsed) == 4


class TestBuildDefinitions:
    def test_filters_to_metropolitan_rows(self):
        parsed = parse_delineation_workbook(_workbook_bytes())
        definitions = build_definitions_df(parsed)
        assert list(definitions["msa_id"]) == EXPECTED_MSA_IDS
        assert list(definitions["cbsa_code"]) == EXPECTED_MSA_IDS
        assert (definitions["area_type"] == MSA_AREA_TYPE).all()
        assert (definitions["definition_version"] == DEFINITION_VERSION).all()

    def test_membership_keeps_all_metro_counties(self):
        parsed = parse_delineation_workbook(_workbook_bytes())
        membership = build_county_membership_df(parsed)
        assert list(membership["county_fips"]) == EXPECTED_COUNTY_FIPS
        assert list(membership["msa_id"].unique()) == EXPECTED_MSA_IDS
        assert list(membership["cbsa_code"].unique()) == EXPECTED_MSA_IDS


class TestValidateMSAArtifacts:
    def test_valid_sample_passes(self):
        parsed = parse_delineation_workbook(_workbook_bytes())
        result = validate_msa_artifacts(
            build_definitions_df(parsed),
            build_county_membership_df(parsed),
        )
        assert result.passed
        assert result.errors == []

    def test_bad_id_format_fails(self):
        parsed = parse_delineation_workbook(_workbook_bytes())
        definitions = build_definitions_df(parsed)
        definitions.loc[0, "msa_id"] = "BAD"
        result = validate_msa_artifacts(definitions, build_county_membership_df(parsed))
        assert not result.passed
        assert any("invalid msa_id format" in error for error in result.errors)

    def test_orphan_membership_fails(self):
        parsed = parse_delineation_workbook(_workbook_bytes())
        membership = build_county_membership_df(parsed)
        membership.loc[0, "msa_id"] = "99999"
        result = validate_msa_artifacts(build_definitions_df(parsed), membership)
        assert not result.passed
        assert any("msa_ids not in definitions" in error for error in result.errors)

    def test_missing_workbook_column_raises(self):
        data = pd.DataFrame(WORKBOOK_ROWS).drop(columns=["CBSA Title"])
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            data.to_excel(writer, index=False, startrow=2)
        with pytest.raises(ValueError, match="missing expected columns"):
            parse_delineation_workbook(buffer.getvalue())
