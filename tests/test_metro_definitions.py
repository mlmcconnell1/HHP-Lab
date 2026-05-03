"""Tests for hhplab.metro definitions and validation.

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

from hhplab.metro.definitions import (
    CANONICAL_UNIVERSE_DEFINITION_VERSION,
    DEFINITION_VERSION,
    METRO_COC_MEMBERSHIP,
    METRO_COUNTY_MEMBERSHIP,
    METRO_DEFINITIONS,
    PROFILE_NAME,
    build_glynn_fox_subset_profile_df,
    build_coc_membership_df,
    build_county_membership_df,
    build_definitions_df,
    build_metro_universe_df,
)
from hhplab.metro.validate import (
    validate_metro_artifacts,
    validate_metro_universe_artifacts,
)

MSA_UNIVERSE_ROWS = [
    ("35620", "35620", "New York-Newark-Jersey City, NY-NJ-PA", "Metropolitan Statistical Area"),
    ("31080", "31080", "Los Angeles-Long Beach-Anaheim, CA", "Metropolitan Statistical Area"),
    ("16980", "16980", "Chicago-Naperville-Elgin, IL-IN-WI", "Metropolitan Statistical Area"),
    ("19100", "19100", "Dallas-Fort Worth-Arlington, TX", "Metropolitan Statistical Area"),
    ("37980", "37980", "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD", "Metropolitan Statistical Area"),
    ("26420", "26420", "Houston-The Woodlands-Sugar Land, TX", "Metropolitan Statistical Area"),
    ("47900", "47900", "Washington-Arlington-Alexandria, DC-VA-MD-WV", "Metropolitan Statistical Area"),
    ("33100", "33100", "Miami-Fort Lauderdale-Pompano Beach, FL", "Metropolitan Statistical Area"),
    ("12060", "12060", "Atlanta-Sandy Springs-Roswell, GA", "Metropolitan Statistical Area"),
    ("14460", "14460", "Boston-Cambridge-Newton, MA-NH", "Metropolitan Statistical Area"),
    ("41860", "41860", "San Francisco-Oakland-Berkeley, CA", "Metropolitan Statistical Area"),
    ("19820", "19820", "Detroit-Warren-Dearborn, MI", "Metropolitan Statistical Area"),
    ("40140", "40140", "Riverside-San Bernardino-Ontario, CA", "Metropolitan Statistical Area"),
    ("38060", "38060", "Phoenix-Mesa-Chandler, AZ", "Metropolitan Statistical Area"),
    ("42660", "42660", "Seattle-Tacoma-Bellevue, WA", "Metropolitan Statistical Area"),
    ("33460", "33460", "Minneapolis-St. Paul-Bloomington, MN-WI", "Metropolitan Statistical Area"),
    ("41740", "41740", "San Diego-Chula Vista-Carlsbad, CA", "Metropolitan Statistical Area"),
    ("41180", "41180", "St. Louis, MO-IL", "Metropolitan Statistical Area"),
    ("45300", "45300", "Tampa-St. Petersburg-Clearwater, FL", "Metropolitan Statistical Area"),
    ("12580", "12580", "Baltimore-Columbia-Towson, MD", "Metropolitan Statistical Area"),
    ("19740", "19740", "Denver-Aurora-Centennial, CO", "Metropolitan Statistical Area"),
    ("38300", "38300", "Pittsburgh, PA", "Metropolitan Statistical Area"),
    ("38900", "38900", "Portland-Vancouver-Hillsboro, OR-WA", "Metropolitan Statistical Area"),
    ("16740", "16740", "Charlotte-Concord-Gastonia, NC-SC", "Metropolitan Statistical Area"),
    ("40900", "40900", "Sacramento-Roseville-Folsom, CA", "Metropolitan Statistical Area"),
]


def build_msa_universe_fixture():
    import pandas as pd

    df = pd.DataFrame(
        MSA_UNIVERSE_ROWS,
        columns=["msa_id", "cbsa_code", "msa_name", "area_type"],
    )
    df["definition_version"] = CANONICAL_UNIVERSE_DEFINITION_VERSION
    df["source"] = "census_msa_delineation_2023"
    df["source_ref"] = "https://example.test/census_msa"
    return df

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

    def test_metro_universe_shape(self):
        df = build_metro_universe_df(build_msa_universe_fixture())
        assert len(df) == EXPECTED_METRO_COUNT
        assert list(df.columns) == [
            "metro_id",
            "cbsa_code",
            "metro_name",
            "area_type",
            "definition_version",
            "source_definition_version",
            "source",
            "source_ref",
        ]

    def test_glynn_fox_subset_profile_shape(self):
        df = build_glynn_fox_subset_profile_df(build_msa_universe_fixture())
        assert len(df) == EXPECTED_METRO_COUNT
        assert list(df.columns) == [
            "profile",
            "profile_definition_version",
            "metro_definition_version",
            "metro_id",
            "cbsa_code",
            "metro_name",
            "profile_metro_id",
            "profile_metro_name",
            "profile_rank",
            "source",
            "source_ref",
        ]

    def test_universe_uses_cbsa_code_as_metro_id(self):
        df = build_metro_universe_df(build_msa_universe_fixture())
        assert (df["metro_id"] == df["cbsa_code"]).all()

    def test_subset_profile_keeps_gf_labels_separate(self):
        df = build_glynn_fox_subset_profile_df(build_msa_universe_fixture())
        assert df.loc[0, "profile"] == PROFILE_NAME
        assert df.loc[0, "profile_metro_id"] == "GF01"
        assert df.loc[0, "metro_id"] == "35620"

    @pytest.mark.parametrize(
        "profile_metro_id,expected_cbsa_code,expected_rank",
        [
            ("GF01", "35620", 1),
            ("GF02", "31080", 2),
            ("GF21", "19740", 21),
        ],
    )
    def test_subset_profile_truth_table(
        self,
        profile_metro_id,
        expected_cbsa_code,
        expected_rank,
    ):
        df = build_glynn_fox_subset_profile_df(build_msa_universe_fixture())
        row = df[df["profile_metro_id"] == profile_metro_id].iloc[0]
        assert row["metro_id"] == expected_cbsa_code
        assert row["cbsa_code"] == expected_cbsa_code
        assert row["profile_rank"] == expected_rank


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

    def test_universe_metro_id_format(self):
        df = build_metro_universe_df(build_msa_universe_fixture())
        assert df["metro_id"].str.match(r"^\d{5}$").all()

    def test_subset_profile_metro_id_formats(self):
        df = build_glynn_fox_subset_profile_df(build_msa_universe_fixture())
        assert df["metro_id"].str.match(r"^\d{5}$").all()
        assert df["profile_metro_id"].str.match(r"^GF\d{2}$").all()


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

    def test_valid_universe_artifacts_pass(self):
        result = validate_metro_universe_artifacts(
            build_metro_universe_df(build_msa_universe_fixture()),
            build_glynn_fox_subset_profile_df(build_msa_universe_fixture()),
        )
        assert result.passed
        assert result.errors == []

    def test_universe_subset_missing_cbsa_fails(self):
        subset = build_glynn_fox_subset_profile_df(build_msa_universe_fixture())
        subset.loc[0, "metro_id"] = "99999"
        result = validate_metro_universe_artifacts(
            build_metro_universe_df(build_msa_universe_fixture()),
            subset,
        )
        assert not result.passed
        assert any("not in canonical metro universe" in e for e in result.errors)
