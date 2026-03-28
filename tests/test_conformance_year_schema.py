"""Year coverage and schema conformance checks for CoC Lab panels.

Tests for coclab-2jtk/coclab-d0qm: check_year_coverage, check_schema_measures, check_schema_zori.

Year coverage truth table
-------------------------
Scenario             | Panel years    | Requested  | Expected
---------------------|----------------|------------|-------------------
full_coverage        | 2020-2024      | 2020-2024  | no results
partial_coverage     | 2022-2024      | 2020-2024  | 1 warning (missing 2020,2021)
zero_overlap         | 2010-2012      | 2020-2024  | 1 error
single_year          | [2022]         | 2022-2022  | no results
single_year_missing  | [2023]         | 2022-2022  | 1 error

Measure schema truth table (coclab-d0qm)
----------------------------------------
Scenario               | measure_columns      | Columns in df           | Expected
-----------------------|----------------------|-------------------------|-------------
all_present            | None (ACS default)   | all 5 ACS columns       | no results
some_present           | None (ACS default)   | only total_population   | 1 warning (missing cols)
none_present           | None (ACS default)   | no ACS columns          | 1 error
pep_population_present | ["population"]       | population              | no results
pep_population_missing | ["population"]       | (none)                  | 1 error
alias_backward_compat  | None (ACS default)   | total_population        | no results (via alias)

ZORI schema truth table
-----------------------
Scenario               | include_zori | Data cols | Provenance cols | Expected
-----------------------|--------------|-----------|-----------------|-------------------
zori_not_requested     | False        | none      | none            | no results
zori_all_present       | True         | all       | all             | no results
zori_data_missing      | True         | none      | none            | 1 error
zori_provenance_missing| True         | all       | none            | 1 warning
zori_partial_provenance| True         | all       | 1 of 3          | 1 warning
"""

from __future__ import annotations

import pandas as pd
import pytest

from coclab.panel.assemble import ZORI_COLUMNS, ZORI_PROVENANCE_COLUMNS
from coclab.panel.conformance import (
    _CHECKS,
    ACS_MEASURE_COLUMNS,
    PanelRequest,
    check_schema_acs,
    check_schema_measures,
    check_schema_zori,
    check_year_coverage,
)

# ============================================================================
# Fixture: isolate the check registry to avoid cross-test pollution
# ============================================================================


@pytest.fixture(autouse=True)
def _isolate_checks():
    """Snapshot and restore the global check registry around each test."""
    saved = list(_CHECKS)
    yield
    _CHECKS.clear()
    _CHECKS.extend(saved)


# ============================================================================
# Helpers
# ============================================================================


def _make_panel(years: list[int], columns: list[str] | None = None) -> pd.DataFrame:
    """Build a minimal panel DataFrame with the given years and columns.

    Each year gets one row per CoC (just ``coc_id`` + ``year``).
    Extra *columns* are added as NA-filled.
    """
    cocs = ["CO-500", "CA-600"]
    rows = [{"coc_id": coc, "year": y} for y in years for coc in cocs]
    df = pd.DataFrame(rows)
    if columns:
        for col in columns:
            if col not in df.columns:
                df[col] = pd.NA
    return df


def _default_request(**overrides) -> PanelRequest:
    """Create a PanelRequest with sensible defaults, accepting overrides."""
    kwargs = {
        "start_year": 2020,
        "end_year": 2024,
        "include_zori": False,
    }
    kwargs.update(overrides)
    return PanelRequest(**kwargs)


# ============================================================================
# Year coverage tests
# ============================================================================

YEAR_COVERAGE_CASES = {
    "full_coverage": {
        "panel_years": list(range(2020, 2025)),
        "start_year": 2020,
        "end_year": 2024,
        "expected_count": 0,
        "expected_severity": None,
    },
    "partial_coverage": {
        "panel_years": [2022, 2023, 2024],
        "start_year": 2020,
        "end_year": 2024,
        "expected_count": 1,
        "expected_severity": "warning",
        "expected_missing": [2020, 2021],
        "expected_fraction": "3/5",
    },
    "zero_overlap": {
        "panel_years": [2010, 2011, 2012],
        "start_year": 2020,
        "end_year": 2024,
        "expected_count": 1,
        "expected_severity": "error",
    },
    "single_year": {
        "panel_years": [2022],
        "start_year": 2022,
        "end_year": 2022,
        "expected_count": 0,
        "expected_severity": None,
    },
    "single_year_missing": {
        "panel_years": [2023],
        "start_year": 2022,
        "end_year": 2022,
        "expected_count": 1,
        "expected_severity": "error",
    },
}


@pytest.mark.parametrize(
    "case_name",
    list(YEAR_COVERAGE_CASES),
    ids=list(YEAR_COVERAGE_CASES),
)
def test_check_year_coverage(case_name: str) -> None:
    case = YEAR_COVERAGE_CASES[case_name]
    df = _make_panel(case["panel_years"])
    request = _default_request(
        start_year=case["start_year"],
        end_year=case["end_year"],
    )

    results = check_year_coverage(df, request)

    assert len(results) == case["expected_count"]
    if case["expected_count"] == 0:
        return

    result = results[0]
    assert result.severity == case["expected_severity"]
    assert result.check_name == "check_year_coverage"

    # Validate details keys
    assert "requested_years" in result.details
    assert "present_years" in result.details
    assert "missing_years" in result.details
    assert "coverage_fraction" in result.details

    # Extra assertions for partial coverage
    if "expected_missing" in case:
        assert result.details["missing_years"] == case["expected_missing"]
    if "expected_fraction" in case:
        assert result.details["coverage_fraction"] == case["expected_fraction"]


# ============================================================================
# Measure schema tests  (coclab-d0qm)
# ============================================================================

MEASURE_SCHEMA_CASES = {
    "all_present": {
        "columns": list(ACS_MEASURE_COLUMNS),
        "measure_columns": None,
        "expected_count": 0,
        "expected_severity": None,
    },
    "some_present": {
        "columns": ["total_population"],
        "measure_columns": None,
        "expected_count": 1,
        "expected_severity": "warning",
    },
    "none_present": {
        "columns": [],
        "measure_columns": None,
        "expected_count": 1,
        "expected_severity": "error",
    },
    "pep_population_present": {
        "columns": ["population"],
        "measure_columns": ["population"],
        "expected_count": 0,
        "expected_severity": None,
    },
    "pep_population_missing": {
        "columns": [],
        "measure_columns": ["population"],
        "expected_count": 1,
        "expected_severity": "error",
    },
}


@pytest.mark.parametrize(
    "case_name",
    list(MEASURE_SCHEMA_CASES),
    ids=list(MEASURE_SCHEMA_CASES),
)
def test_check_schema_measures(case_name: str) -> None:
    case = MEASURE_SCHEMA_CASES[case_name]
    df = _make_panel([2022], columns=case["columns"])
    request = _default_request(measure_columns=case["measure_columns"])

    results = check_schema_measures(df, request)

    assert len(results) == case["expected_count"]
    if case["expected_count"] == 0:
        return

    result = results[0]
    assert result.severity == case["expected_severity"]
    assert result.check_name == "check_schema_measures"
    if case["expected_severity"] == "error":
        # Error = no columns present at all
        assert result.details["present_columns"] == []


def test_check_schema_acs_alias() -> None:
    """check_schema_acs is a backward-compatible alias for check_schema_measures."""
    assert check_schema_acs is check_schema_measures
    df = _make_panel([2022], columns=list(ACS_MEASURE_COLUMNS))
    request = _default_request()
    results = check_schema_acs(df, request)
    assert len(results) == 0


# ============================================================================
# ZORI schema tests
# ============================================================================

ZORI_SCHEMA_CASES = {
    "zori_not_requested": {
        "include_zori": False,
        "data_columns": [],
        "provenance_columns": [],
        "expected_count": 0,
        "expected_severity": None,
    },
    "zori_all_present": {
        "include_zori": True,
        "data_columns": list(ZORI_COLUMNS),
        "provenance_columns": list(ZORI_PROVENANCE_COLUMNS),
        "expected_count": 0,
        "expected_severity": None,
    },
    "zori_data_missing": {
        "include_zori": True,
        "data_columns": [],
        "provenance_columns": [],
        "expected_count": 1,
        "expected_severity": "error",
    },
    "zori_provenance_missing": {
        "include_zori": True,
        "data_columns": list(ZORI_COLUMNS),
        "provenance_columns": [],
        "expected_count": 1,
        "expected_severity": "warning",
        "expected_missing_provenance": list(ZORI_PROVENANCE_COLUMNS),
    },
    "zori_partial_provenance": {
        "include_zori": True,
        "data_columns": list(ZORI_COLUMNS),
        # Only first provenance column present — remaining two missing
        "provenance_columns": [ZORI_PROVENANCE_COLUMNS[0]],
        "expected_count": 1,
        "expected_severity": "warning",
        "expected_missing_provenance": list(ZORI_PROVENANCE_COLUMNS[1:]),
    },
}


@pytest.mark.parametrize(
    "case_name",
    list(ZORI_SCHEMA_CASES),
    ids=list(ZORI_SCHEMA_CASES),
)
def test_check_schema_zori(case_name: str) -> None:
    case = ZORI_SCHEMA_CASES[case_name]
    all_cols = case["data_columns"] + case["provenance_columns"]
    df = _make_panel([2022], columns=all_cols)
    request = _default_request(include_zori=case["include_zori"])

    results = check_schema_zori(df, request)

    assert len(results) == case["expected_count"]
    if case["expected_count"] == 0:
        return

    result = results[0]
    assert result.severity == case["expected_severity"]
    assert result.check_name == "check_schema_zori"

    # Validate details keys
    assert "expected_columns" in result.details
    assert "present_columns" in result.details

    # Extra assertions for provenance warnings
    if "expected_missing_provenance" in case:
        assert "missing_provenance_columns" in result.details
        assert (
            result.details["missing_provenance_columns"]
            == case["expected_missing_provenance"]
        )
