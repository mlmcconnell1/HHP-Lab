"""Tests for coclab.metro definitions and validation.

Covers the metro definition data integrity (coclab-djrh.2), including
truth-table consistency, identifier formats, and validation logic.

Truth table for metro definition structure
------------------------------------------
- 25 metros total (GF01-GF25)
- Every metro has at least one CoC
- Every metro has at least one county
- All metro_ids in membership tables exist in definitions
- definition_version is consistent across all three tables
"""

import pytest

from coclab.metro.definitions import (
    DEFINITION_VERSION,
    METRO_COC_MEMBERSHIP,
    METRO_COUNTY_MEMBERSHIP,
    METRO_DEFINITIONS,
    build_coc_membership_df,
    build_county_membership_df,
    build_definitions_df,
)
from coclab.metro.validate import validate_metro_artifacts

# ---------------------------------------------------------------------------
# Constants from truth table
# ---------------------------------------------------------------------------

#: Expected metro count.
EXPECTED_METRO_COUNT = 25

#: Expected total CoC membership rows (from Table 1).
EXPECTED_COC_ROWS = 33

#: Expected total county membership rows (from Table 1).
#: 25 single-county metros + 4 extra for NYC (5 boroughs) + 1 for Houston
#: + 1 for St. Louis + 1 for Baltimore + 6 for Denver = 38
EXPECTED_COUNTY_ROWS = 38

#: Metros with multiple CoCs (from Table 1).
MULTI_COC_METROS = {"GF02", "GF03", "GF09", "GF12", "GF18", "GF20"}

#: Metros with multiple counties (from Table 1).
MULTI_COUNTY_METROS = {"GF01", "GF06", "GF18", "GF20", "GF21"}


# ---------------------------------------------------------------------------
# Raw constant tests
# ---------------------------------------------------------------------------


class TestDefinitionConstants:
    def test_metro_count(self):
        assert len(METRO_DEFINITIONS) == EXPECTED_METRO_COUNT

    def test_metro_ids_unique(self):
        ids = [m[0] for m in METRO_DEFINITIONS]
        assert len(ids) == len(set(ids))

    def test_metro_ids_sequential(self):
        ids = [m[0] for m in METRO_DEFINITIONS]
        expected = [f"GF{i:02d}" for i in range(1, EXPECTED_METRO_COUNT + 1)]
        assert ids == expected

    def test_coc_membership_count(self):
        assert len(METRO_COC_MEMBERSHIP) == EXPECTED_COC_ROWS

    def test_county_membership_count(self):
        assert len(METRO_COUNTY_MEMBERSHIP) == EXPECTED_COUNTY_ROWS

    def test_all_metros_have_coc_membership(self):
        metro_ids_with_coc = {m[0] for m in METRO_COC_MEMBERSHIP}
        all_metro_ids = {m[0] for m in METRO_DEFINITIONS}
        assert metro_ids_with_coc == all_metro_ids

    def test_all_metros_have_county_membership(self):
        metro_ids_with_county = {m[0] for m in METRO_COUNTY_MEMBERSHIP}
        all_metro_ids = {m[0] for m in METRO_DEFINITIONS}
        assert metro_ids_with_county == all_metro_ids

    def test_definition_version(self):
        assert DEFINITION_VERSION == "glynn_fox_v1"


# ---------------------------------------------------------------------------
# DataFrame builder tests
# ---------------------------------------------------------------------------


class TestDataFrameBuilders:
    def test_definitions_shape(self):
        df = build_definitions_df()
        assert len(df) == EXPECTED_METRO_COUNT
        assert "metro_id" in df.columns
        assert "metro_name" in df.columns
        assert "membership_type" in df.columns
        assert "definition_version" in df.columns

    def test_coc_membership_shape(self):
        df = build_coc_membership_df()
        assert len(df) == EXPECTED_COC_ROWS
        assert "metro_id" in df.columns
        assert "coc_id" in df.columns

    def test_county_membership_shape(self):
        df = build_county_membership_df()
        assert len(df) == EXPECTED_COUNTY_ROWS
        assert "metro_id" in df.columns
        assert "county_fips" in df.columns

    @pytest.mark.parametrize(
        "metro_id,expected_coc_count",
        [
            ("GF01", 1),   # New York: NY-600
            ("GF02", 4),   # LA: CA-600, CA-606, CA-607, CA-612
            ("GF03", 2),   # Chicago: IL-510, IL-511
            ("GF04", 1),   # Dallas: TX-600
            ("GF21", 1),   # Denver: CO-503
        ],
    )
    def test_coc_membership_per_metro(self, metro_id, expected_coc_count):
        df = build_coc_membership_df()
        actual = len(df[df["metro_id"] == metro_id])
        assert actual == expected_coc_count

    @pytest.mark.parametrize(
        "metro_id,expected_county_count",
        [
            ("GF01", 5),   # New York: 5 boroughs
            ("GF02", 1),   # LA: Los Angeles County
            ("GF06", 2),   # Houston: Harris, Fort Bend
            ("GF18", 2),   # St. Louis: county + city
            ("GF21", 7),   # Denver: 7 counties
        ],
    )
    def test_county_membership_per_metro(self, metro_id, expected_county_count):
        df = build_county_membership_df()
        actual = len(df[df["metro_id"] == metro_id])
        assert actual == expected_county_count


# ---------------------------------------------------------------------------
# Multi-entity metro tests
# ---------------------------------------------------------------------------


class TestMultiEntityMetros:
    def test_multi_coc_metros(self):
        df = build_coc_membership_df()
        coc_counts = df.groupby("metro_id").size()
        actual = set(coc_counts[coc_counts > 1].index)
        assert actual == MULTI_COC_METROS

    def test_multi_county_metros(self):
        df = build_county_membership_df()
        county_counts = df.groupby("metro_id").size()
        actual = set(county_counts[county_counts > 1].index)
        assert actual == MULTI_COUNTY_METROS


# ---------------------------------------------------------------------------
# Identifier format tests
# ---------------------------------------------------------------------------


class TestIdentifierFormats:
    def test_metro_id_format(self):
        df = build_definitions_df()
        assert df["metro_id"].str.match(r"^GF\d{2}$").all()

    def test_coc_id_format(self):
        df = build_coc_membership_df()
        assert df["coc_id"].str.match(r"^[A-Z]{2}-\d{3}$").all()

    def test_county_fips_format(self):
        df = build_county_membership_df()
        assert df["county_fips"].str.match(r"^\d{5}$").all()


# ---------------------------------------------------------------------------
# Validation function tests
# ---------------------------------------------------------------------------


class TestValidation:
    def test_valid_artifacts_pass(self):
        result = validate_metro_artifacts(
            build_definitions_df(),
            build_coc_membership_df(),
            build_county_membership_df(),
        )
        assert result.passed
        assert len(result.errors) == 0

    def test_missing_column_fails(self):
        defs = build_definitions_df().drop(columns=["metro_name"])
        result = validate_metro_artifacts(
            defs,
            build_coc_membership_df(),
            build_county_membership_df(),
        )
        assert not result.passed
        assert any("missing columns" in e for e in result.errors)

    def test_bad_metro_id_format_fails(self):
        defs = build_definitions_df()
        defs.loc[0, "metro_id"] = "BADID"
        result = validate_metro_artifacts(
            defs,
            build_coc_membership_df(),
            build_county_membership_df(),
        )
        assert not result.passed
        assert any("invalid metro_id format" in e for e in result.errors)

    def test_orphan_metro_in_membership_fails(self):
        coc = build_coc_membership_df()
        coc.loc[0, "metro_id"] = "GF99"
        result = validate_metro_artifacts(
            build_definitions_df(),
            coc,
            build_county_membership_df(),
        )
        assert not result.passed
        assert any("not in definitions" in e for e in result.errors)

    def test_version_mismatch_fails(self):
        coc = build_coc_membership_df()
        coc["definition_version"] = "wrong_version"
        result = validate_metro_artifacts(
            build_definitions_df(),
            coc,
            build_county_membership_df(),
        )
        assert not result.passed
        assert any("definition_version mismatch" in e for e in result.errors)
