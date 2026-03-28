"""ACS1 conformance constants, panel schema, and provenance fields.

Tests for coclab-6nvh: ACS 1-year measure column validation, product-aware
PanelRequest, and METRO_PANEL_COLUMNS schema additions.

ACS product-aware conformance truth table
------------------------------------------
Scenario                     | acs_products    | Columns in df              | Expected
-----------------------------|-----------------|----------------------------|----------
acs5_only_default            | ["acs5"]        | all ACS5 columns           | no results
acs5_and_acs1_both_present   | ["acs5","acs1"] | ACS5 + ACS1 columns       | no results
acs1_columns_missing         | ["acs5","acs1"] | only ACS5 columns          | 1 error
acs5_only_no_acs1_needed     | ["acs5"]        | only ACS5 columns          | no results
measure_columns_override     | ["acs5","acs1"] | ["population"]             | no results (override)
"""

from __future__ import annotations

import pandas as pd
import pytest

from coclab.panel.assemble import METRO_PANEL_COLUMNS
from coclab.panel.conformance import (
    ACS1_MEASURE_COLUMNS,
    ACS_MEASURE_COLUMNS,
    PanelRequest,
    _CHECKS,
    _effective_measure_columns,
    check_schema_measures,
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
# PanelRequest.acs_products default (backward compatibility)
# ============================================================================


def test_panel_request_default_acs_products() -> None:
    """PanelRequest defaults to acs_products=["acs5"] for backward compat."""
    request = _default_request()
    assert request.acs_products == ["acs5"]


def test_panel_request_acs_products_dual() -> None:
    """PanelRequest accepts dual ACS products."""
    request = _default_request(acs_products=["acs5", "acs1"])
    assert request.acs_products == ["acs5", "acs1"]


# ============================================================================
# _effective_measure_columns product-awareness
# ============================================================================


def test_effective_measures_acs5_only() -> None:
    """With acs_products=["acs5"], only ACS5 measure columns are returned."""
    request = _default_request(acs_products=["acs5"])
    cols = _effective_measure_columns(request)
    assert cols == list(ACS_MEASURE_COLUMNS)
    assert "unemployment_rate_acs1" not in cols


def test_effective_measures_acs5_and_acs1() -> None:
    """With acs_products=["acs5", "acs1"], both ACS5 and ACS1 columns are returned."""
    request = _default_request(acs_products=["acs5", "acs1"])
    cols = _effective_measure_columns(request)
    assert all(c in cols for c in ACS_MEASURE_COLUMNS)
    assert all(c in cols for c in ACS1_MEASURE_COLUMNS)


def test_effective_measures_override_trumps_products() -> None:
    """Explicit measure_columns override takes precedence over acs_products."""
    request = _default_request(
        acs_products=["acs5", "acs1"],
        measure_columns=["population"],
    )
    cols = _effective_measure_columns(request)
    assert cols == ["population"]


# ============================================================================
# check_schema_measures with acs_products
# ============================================================================


def test_schema_acs5_only_passes() -> None:
    """Panel with ACS5 columns passes when acs_products=["acs5"]."""
    df = _make_panel([2022], columns=list(ACS_MEASURE_COLUMNS))
    request = _default_request(acs_products=["acs5"])
    results = check_schema_measures(df, request)
    assert len(results) == 0


def test_schema_acs5_and_acs1_both_present_passes() -> None:
    """Panel with ACS5+ACS1 columns passes when both products requested."""
    all_cols = list(ACS_MEASURE_COLUMNS) + list(ACS1_MEASURE_COLUMNS)
    df = _make_panel([2022], columns=all_cols)
    request = _default_request(acs_products=["acs5", "acs1"])
    results = check_schema_measures(df, request)
    assert len(results) == 0


def test_schema_acs1_missing_fails() -> None:
    """Panel missing ACS1 columns warns when acs_products includes "acs1"."""
    # Only ACS5 columns present, but both products requested
    df = _make_panel([2022], columns=list(ACS_MEASURE_COLUMNS))
    request = _default_request(acs_products=["acs5", "acs1"])
    # check_schema_measures now reports partially missing columns as a warning
    results = check_schema_measures(df, request)
    assert len(results) == 1
    assert results[0].severity == "warning"
    assert "missing_columns" in results[0].details


def test_schema_no_measures_at_all_fails_dual_product() -> None:
    """Panel with NO measure columns fails when both products requested."""
    df = _make_panel([2022], columns=[])
    request = _default_request(acs_products=["acs5", "acs1"])
    results = check_schema_measures(df, request)
    assert len(results) == 1
    assert results[0].severity == "error"
    assert results[0].check_name == "check_schema_measures"
    # The expected columns should include both ACS5 and ACS1 columns
    expected_in_details = results[0].details["expected_columns"]
    assert "unemployment_rate_acs1" in expected_in_details
    assert "total_population" in expected_in_details


def test_schema_only_acs1_present_passes_dual_product() -> None:
    """Panel with only ACS1 columns warns about missing ACS5 columns."""
    df = _make_panel([2022], columns=list(ACS1_MEASURE_COLUMNS))
    request = _default_request(acs_products=["acs5", "acs1"])
    results = check_schema_measures(df, request)
    # Now properly reports missing ACS5 columns as a warning
    assert len(results) == 1
    assert results[0].severity == "warning"
    assert "missing_columns" in results[0].details


def test_schema_acs5_only_ignores_acs1_columns() -> None:
    """Panel with only one ACS5 column warns about missing ACS5 columns."""
    df = _make_panel([2022], columns=["total_population"])
    request = _default_request(acs_products=["acs5"])
    results = check_schema_measures(df, request)
    # Only total_population present; other ACS5 columns missing → warning
    assert len(results) == 1
    assert results[0].severity == "warning"


def test_schema_measure_columns_override_still_works() -> None:
    """Explicit measure_columns override bypasses acs_products logic."""
    df = _make_panel([2022], columns=["population"])
    request = _default_request(
        acs_products=["acs5", "acs1"],
        measure_columns=["population"],
    )
    results = check_schema_measures(df, request)
    assert len(results) == 0


# ============================================================================
# METRO_PANEL_COLUMNS schema additions
# ============================================================================


def test_metro_panel_columns_includes_acs1_measure() -> None:
    """METRO_PANEL_COLUMNS includes the ACS1 unemployment rate column."""
    assert "unemployment_rate_acs1" in METRO_PANEL_COLUMNS


def test_metro_panel_columns_includes_acs1_provenance() -> None:
    """METRO_PANEL_COLUMNS includes the ACS1 provenance column."""
    assert "acs1_vintage_used" in METRO_PANEL_COLUMNS


def test_metro_panel_columns_includes_acs_products_used() -> None:
    """METRO_PANEL_COLUMNS includes the acs_products_used provenance column."""
    assert "acs_products_used" in METRO_PANEL_COLUMNS


def test_metro_panel_columns_source_is_last() -> None:
    """source column remains at the end of METRO_PANEL_COLUMNS."""
    assert METRO_PANEL_COLUMNS[-1] == "source"


def test_metro_panel_columns_provenance_before_source() -> None:
    """ACS1 provenance columns appear before source."""
    source_idx = METRO_PANEL_COLUMNS.index("source")
    acs1_vintage_idx = METRO_PANEL_COLUMNS.index("acs1_vintage_used")
    products_idx = METRO_PANEL_COLUMNS.index("acs_products_used")
    assert acs1_vintage_idx < source_idx
    assert products_idx < source_idx


# ============================================================================
# ACS1_MEASURE_COLUMNS constant
# ============================================================================


def test_acs1_measure_columns_content() -> None:
    """ACS1_MEASURE_COLUMNS contains the expected columns."""
    assert ACS1_MEASURE_COLUMNS == ["unemployment_rate_acs1"]
