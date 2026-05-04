"""Tests for metro CBSA mapping and ACS 1-year variable definitions.

Covers CBSA mapping data integrity, round-trip lookups, DataFrame builder,
and ACS1 unemployment variable completeness.
"""

import pytest

from hhplab.acs.variables_acs1 import (
    ACS1_FIRST_RELIABLE_YEAR,
    ACS1_METRO_OUTPUT_COLUMNS,
    ACS1_UNEMPLOYMENT_TABLE,
    ACS1_UNEMPLOYMENT_VARIABLES,
    ACS1_VARIABLE_NAMES,
    DERIVED_ACS1_MEASURES,
)
from hhplab.metro.metro_definitions import (
    _CBSA_METRO_NAMES,
    _CBSA_TO_METRO,
    METRO_CBSA_MAPPING,
    build_cbsa_mapping_df,
    cbsa_to_metro_id,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EXPECTED_CBSA_COUNT = 25


# ---------------------------------------------------------------------------
# CBSA mapping data integrity
# ---------------------------------------------------------------------------


class TestCBSAMappingConstants:
    def test_all_25_metros_have_cbsa_codes(self):
        assert len(METRO_CBSA_MAPPING) == EXPECTED_CBSA_COUNT

    def test_cbsa_codes_are_5_digit_strings(self):
        for metro_id, cbsa_code in METRO_CBSA_MAPPING.items():
            assert isinstance(cbsa_code, str), (
                f"{metro_id}: CBSA code should be str, got {type(cbsa_code)}"
            )
            assert len(cbsa_code) == 5 and cbsa_code.isdigit(), (
                f"{metro_id}: CBSA code {cbsa_code!r} is not a 5-digit string"
            )

    def test_cbsa_codes_are_unique(self):
        codes = list(METRO_CBSA_MAPPING.values())
        assert len(codes) == len(set(codes))

    def test_metro_ids_are_valid_format(self):
        for metro_id in METRO_CBSA_MAPPING:
            assert metro_id[:2] == "GF" and len(metro_id) == 4 and metro_id[2:].isdigit()

    def test_metro_names_match_mapping(self):
        assert set(METRO_CBSA_MAPPING.keys()) == set(_CBSA_METRO_NAMES.keys())

    def test_reverse_mapping_size(self):
        assert len(_CBSA_TO_METRO) == EXPECTED_CBSA_COUNT


# ---------------------------------------------------------------------------
# Reverse lookup function
# ---------------------------------------------------------------------------


class TestCBSAToMetroId:
    @pytest.mark.parametrize(
        "metro_id,cbsa_code",
        list(METRO_CBSA_MAPPING.items()),
    )
    def test_round_trip(self, metro_id, cbsa_code):
        assert cbsa_to_metro_id(cbsa_code) == metro_id

    def test_unknown_code_returns_none(self):
        assert cbsa_to_metro_id("99999") is None

    def test_empty_string_returns_none(self):
        assert cbsa_to_metro_id("") is None

    def test_specific_lookups(self):
        assert cbsa_to_metro_id("35620") == "GF01"  # New York
        assert cbsa_to_metro_id("31080") == "GF02"  # Los Angeles
        assert cbsa_to_metro_id("16740") == "GF24"  # Charlotte


# ---------------------------------------------------------------------------
# DataFrame builder
# ---------------------------------------------------------------------------


class TestBuildCBSAMappingDF:
    def test_shape(self):
        df = build_cbsa_mapping_df()
        assert len(df) == EXPECTED_CBSA_COUNT

    def test_columns(self):
        df = build_cbsa_mapping_df()
        assert list(df.columns) == ["metro_id", "metro_name", "cbsa_code"]

    def test_metro_id_dtype(self):
        df = build_cbsa_mapping_df()
        assert df["metro_id"].dtype == object  # pandas string

    def test_cbsa_code_dtype(self):
        df = build_cbsa_mapping_df()
        assert df["cbsa_code"].dtype == object  # pandas string

    def test_all_cbsa_codes_are_5_digit(self):
        df = build_cbsa_mapping_df()
        assert df["cbsa_code"].str.match(r"^\d{5}$").all()

    def test_metro_names_populated(self):
        df = build_cbsa_mapping_df()
        assert df["metro_name"].notna().all()
        assert (df["metro_name"].str.len() > 0).all()


# ---------------------------------------------------------------------------
# ACS 1-year variable definitions
# ---------------------------------------------------------------------------


class TestACS1VariableDefinitions:
    def test_unemployment_table_name(self):
        assert ACS1_UNEMPLOYMENT_TABLE == "B23025"

    def test_unemployment_variables_count(self):
        assert len(ACS1_UNEMPLOYMENT_VARIABLES) == 3

    def test_variable_codes_match_table(self):
        for var_code in ACS1_UNEMPLOYMENT_VARIABLES:
            assert var_code.startswith("B23025_"), (
                f"Variable {var_code} does not belong to table B23025"
            )

    def test_variable_codes_end_with_E(self):
        for var_code in ACS1_UNEMPLOYMENT_VARIABLES:
            assert var_code.endswith("E"), f"Variable {var_code} should end with 'E' (estimate)"

    def test_friendly_names_are_unique(self):
        names = list(ACS1_VARIABLE_NAMES.values())
        assert len(names) == len(set(names))

    def test_key_variables_present(self):
        friendly = set(ACS1_VARIABLE_NAMES.values())
        assert "pop_16_plus" in friendly
        assert "civilian_labor_force" in friendly
        assert "unemployed_count" in friendly

    def test_derived_measures_defined(self):
        assert "unemployment_rate_acs1" in DERIVED_ACS1_MEASURES

    def test_output_columns_complete(self):
        required = {"metro_id", "cbsa_code", "unemployment_rate_acs1"}
        assert required.issubset(set(ACS1_METRO_OUTPUT_COLUMNS))

    def test_first_reliable_year(self):
        assert ACS1_FIRST_RELIABLE_YEAR == 2012
