"""Tests for Census MSA definition parsing, builders, and validation."""

from __future__ import annotations

from io import BytesIO

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import GeometryCollection, box

from hhplab.msa.definitions import (
    DEFINITION_VERSION,
    MSA_AREA_TYPE,
    build_county_membership_df,
    build_definitions_df,
    parse_delineation_workbook,
)
from hhplab.msa.io import read_msa_definitions
from hhplab.msa.validate import validate_msa_artifacts, validate_msa_boundaries

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

# Truth table for validate_msa_artifacts branch coverage:
# - bad area_type -> definitions unexpected area_type error
# - mismatched definitions cbsa_code -> definitions identifier-contract error
# - mismatched membership cbsa_code -> county_membership identifier-contract error
# - wrong definition_version -> definition_version mismatch error
# - duplicate msa_id -> duplicate definitions error
# - duplicate county membership pair -> duplicate membership-pair error
# - missing membership -> warning only, validation still passes
# Truth table for validate_msa_boundaries branch coverage:
# - text ingested_at -> datetime-like error
# - duplicate msa_id -> duplicate boundary error
# - mismatched cbsa_code -> identifier-contract error
# - None geometry -> null geometry error
# - empty geometry -> empty geometry error
# - dropped polygon -> missing MSA polygon error
# - extra polygon -> unmatched definition error
# - wrong definition_version -> definition_version mismatch error


def _workbook_bytes() -> bytes:
    data = pd.DataFrame(WORKBOOK_ROWS)
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        data.to_excel(writer, index=False, startrow=2)
    return buffer.getvalue()


def _valid_msa_artifacts() -> tuple[pd.DataFrame, pd.DataFrame]:
    parsed = parse_delineation_workbook(_workbook_bytes())
    return build_definitions_df(parsed), build_county_membership_df(parsed)


def _set_unexpected_area_type(
    definitions: pd.DataFrame,
    membership: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    definitions.loc[0, "area_type"] = "Micropolitan Statistical Area"
    return definitions, membership


def _mismatch_definition_cbsa_code(
    definitions: pd.DataFrame,
    membership: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    definitions.loc[0, "cbsa_code"] = "99999"
    return definitions, membership


def _mismatch_membership_cbsa_code(
    definitions: pd.DataFrame,
    membership: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    membership.loc[0, "cbsa_code"] = "99999"
    return definitions, membership


def _set_wrong_definition_version(
    definitions: pd.DataFrame,
    membership: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    definitions.loc[0, "definition_version"] = "old_version"
    membership.loc[0, "definition_version"] = "old_version"
    return definitions, membership


def _duplicate_definition_msa_id(
    definitions: pd.DataFrame,
    membership: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    duplicated = pd.concat([definitions, definitions.iloc[[0]]], ignore_index=True)
    return duplicated, membership


def _duplicate_membership_pair(
    definitions: pd.DataFrame,
    membership: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    duplicated = pd.concat([membership, membership.iloc[[0]]], ignore_index=True)
    return definitions, duplicated


def _valid_msa_boundaries() -> gpd.GeoDataFrame:
    definitions, _membership = _valid_msa_artifacts()
    return gpd.GeoDataFrame(
        {
            "msa_id": definitions["msa_id"].to_list(),
            "cbsa_code": definitions["cbsa_code"].to_list(),
            "msa_name": definitions["msa_name"].to_list(),
            "area_type": definitions["area_type"].to_list(),
            "definition_version": definitions["definition_version"].to_list(),
            "geometry_vintage": ["2023", "2023"],
            "source": ["census_tiger_cbsa", "census_tiger_cbsa"],
            "source_ref": ["https://example.test/cbsa.zip", "https://example.test/cbsa.zip"],
            "ingested_at": [
                pd.Timestamp("2026-04-30T00:00:00Z"),
                pd.Timestamp("2026-04-30T00:00:00Z"),
            ],
        },
        geometry=[box(0, 0, 1, 1), box(1, 1, 2, 2)],
        crs="EPSG:4326",
    )


def _set_text_ingested_at(boundaries: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    boundaries["ingested_at"] = "2026-04-30"
    return boundaries


def _duplicate_boundary_msa_id(boundaries: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    duplicated = pd.concat([boundaries, boundaries.iloc[[0]]], ignore_index=True)
    return gpd.GeoDataFrame(duplicated, geometry="geometry", crs=boundaries.crs)


def _mismatch_boundary_cbsa_code(boundaries: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    boundaries.loc[0, "cbsa_code"] = "99999"
    return boundaries


def _set_null_geometry(boundaries: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    boundaries.loc[0, "geometry"] = None
    return boundaries


def _set_empty_geometry(boundaries: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    boundaries.loc[0, "geometry"] = GeometryCollection()
    return boundaries


def _drop_boundary_polygon(boundaries: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return boundaries[boundaries["msa_id"] != "31080"].reset_index(drop=True)


def _add_extra_boundary_polygon(boundaries: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    extra = boundaries.iloc[[0]].copy()
    extra.loc[extra.index[0], "msa_id"] = "99999"
    extra.loc[extra.index[0], "cbsa_code"] = "99999"
    expanded = pd.concat([boundaries, extra], ignore_index=True)
    return gpd.GeoDataFrame(expanded, geometry="geometry", crs=boundaries.crs)


def _set_wrong_boundary_definition_version(
    boundaries: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    boundaries.loc[0, "definition_version"] = "old_version"
    return boundaries


VALIDATION_ERROR_CASES = [
    pytest.param(
        _set_unexpected_area_type,
        "definitions: unexpected area_type values",
        id="unexpected-area-type",
    ),
    pytest.param(
        _mismatch_definition_cbsa_code,
        "definitions: msa_id must match cbsa_code",
        id="definition-cbsa-mismatch",
    ),
    pytest.param(
        _mismatch_membership_cbsa_code,
        "county_membership: msa_id must match cbsa_code",
        id="membership-cbsa-mismatch",
    ),
    pytest.param(
        _set_wrong_definition_version,
        "definition_version mismatch",
        id="definition-version-mismatch",
    ),
    pytest.param(
        _duplicate_definition_msa_id,
        "definitions: 1 duplicate msa_id(s)",
        id="duplicate-definition-msa-id",
    ),
    pytest.param(
        _duplicate_membership_pair,
        "county_membership: 1 duplicate (msa_id, county_fips) pair(s)",
        id="duplicate-membership-pair",
    ),
]

BOUNDARY_VALIDATION_ERROR_CASES = [
    pytest.param(
        _set_text_ingested_at,
        "boundaries: ingested_at must be datetime-like",
        id="text-ingested-at",
    ),
    pytest.param(
        _duplicate_boundary_msa_id,
        "boundaries: 1 duplicate msa_id(s)",
        id="duplicate-msa-id",
    ),
    pytest.param(
        _mismatch_boundary_cbsa_code,
        "boundaries: msa_id must match cbsa_code",
        id="cbsa-code-mismatch",
    ),
    pytest.param(
        _set_null_geometry,
        "boundaries: null geometry values are not allowed",
        id="null-geometry",
    ),
    pytest.param(
        _set_empty_geometry,
        "boundaries: empty geometry values are not allowed",
        id="empty-geometry",
    ),
    pytest.param(
        _drop_boundary_polygon,
        "boundaries: missing MSA polygons for definition ids ['31080']",
        id="missing-boundary",
    ),
    pytest.param(
        _add_extra_boundary_polygon,
        "boundaries: found polygons without matching definitions ['99999']",
        id="extra-boundary",
    ),
    pytest.param(
        _set_wrong_boundary_definition_version,
        "boundaries: definition_version mismatch",
        id="definition-version-mismatch",
    ),
]


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


class TestReadMSADefinitions:
    def test_missing_artifact_error_is_actionable(self, tmp_path):
        with pytest.raises(
            FileNotFoundError,
            match=(
                "MSA definitions artifact not found .* "
                "Run: hhplab generate msa --definition-version census_msa_2023"
            ),
        ):
            read_msa_definitions(DEFINITION_VERSION, tmp_path / "data")


class TestValidateMSAArtifacts:
    def test_valid_sample_passes(self):
        definitions, membership = _valid_msa_artifacts()
        result = validate_msa_artifacts(definitions, membership)
        assert result.passed
        assert result.errors == []

    def test_bad_id_format_fails(self):
        definitions, membership = _valid_msa_artifacts()
        definitions.loc[0, "msa_id"] = "BAD"
        result = validate_msa_artifacts(definitions, membership)
        assert not result.passed
        assert any("invalid msa_id format" in error for error in result.errors)

    def test_orphan_membership_fails(self):
        definitions, membership = _valid_msa_artifacts()
        membership.loc[0, "msa_id"] = "99999"
        result = validate_msa_artifacts(definitions, membership)
        assert not result.passed
        assert any("msa_ids not in definitions" in error for error in result.errors)

    @pytest.mark.parametrize(
        ("mutate_artifacts", "expected_error"),
        VALIDATION_ERROR_CASES,
    )
    def test_validation_error_branches_fail(self, mutate_artifacts, expected_error):
        definitions, membership = _valid_msa_artifacts()
        definitions, membership = mutate_artifacts(definitions, membership)

        result = validate_msa_artifacts(definitions, membership)

        assert not result.passed
        assert any(expected_error in error for error in result.errors)

    def test_msa_with_no_county_membership_warns(self):
        definitions, membership = _valid_msa_artifacts()
        membership = membership[membership["msa_id"] != "31080"].reset_index(drop=True)

        result = validate_msa_artifacts(definitions, membership)

        assert result.passed
        assert result.errors == []
        assert result.warnings == ["definitions: MSAs with no county membership: ['31080']"]

    def test_missing_workbook_column_raises(self):
        data = pd.DataFrame(WORKBOOK_ROWS).drop(columns=["CBSA Title"])
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            data.to_excel(writer, index=False, startrow=2)
        with pytest.raises(ValueError, match="missing expected columns"):
            parse_delineation_workbook(buffer.getvalue())


class TestValidateMSABoundaries:
    def test_valid_boundaries_pass(self):
        definitions, _membership = _valid_msa_artifacts()
        result = validate_msa_boundaries(_valid_msa_boundaries(), definitions)

        assert result.passed
        assert result.errors == []

    @pytest.mark.parametrize(
        ("mutate_boundaries", "expected_error"),
        BOUNDARY_VALIDATION_ERROR_CASES,
    )
    def test_boundary_validation_error_branches_fail(
        self,
        mutate_boundaries,
        expected_error,
    ):
        definitions, _membership = _valid_msa_artifacts()
        boundaries = mutate_boundaries(_valid_msa_boundaries())

        result = validate_msa_boundaries(boundaries, definitions)

        assert not result.passed
        assert any(expected_error in error for error in result.errors)
