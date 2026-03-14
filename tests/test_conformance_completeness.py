"""Tests for data completeness conformance checks (coclab-1gmj).

Covers the three registered checks:
- ``check_column_null_rates``: Per-column null rate warnings.
- ``check_per_year_completeness``: Per-year null rate warnings.
- ``check_zori_eligibility_rate``: ZORI eligibility rate warning.

Each check is called directly (not via ``run_conformance``) to test in
isolation.

Truth tables
============

check_column_null_rates
-----------------------

    Scenario                      | null % | Expected
    ------------------------------|--------|----------------------------
    all_present                   |   0%   | no results
    partial_nulls_below_threshold |  30%   | no results
    partial_nulls_above_threshold |  60%   | 1 warning (column_high_null_rate)
    fully_null                    | 100%   | 1 warning (column_fully_null)
    mixed                         | 60%+100%| 2 warnings
    column_not_in_df              |  n/a   | no results

check_per_year_completeness
---------------------------

    Scenario              | Year null % | Expected
    ----------------------|-------------|----------------------------
    all_years_complete    |     0%      | no results
    one_year_high_nulls   |   >50%      | 1 warning
    all_years_high_nulls  |   >50%      | warning per year

check_zori_eligibility_rate
---------------------------

    Scenario             | include_zori | eligible % | Expected
    ---------------------|-------------|------------|----------------------------
    zori_not_requested   |   False     |    n/a     | no results
    zori_high_eligibility|   True      |   >20%     | no results
    zori_low_eligibility |   True      |   <20%     | 1 warning
    zori_column_missing  |   True      |    n/a     | no results

Beads: coclab-1gmj
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from coclab.panel.conformance import (
    ACS_MEASURE_COLUMNS,
    ZORI_MIN_ELIGIBILITY_RATE,
    ConformanceResult,
    PanelRequest,
    check_column_null_rates,
    check_per_year_completeness,
    check_zori_eligibility_rate,
)

# ============================================================================
# Helpers
# ============================================================================

# Default request with the standard 50% null-rate threshold.
DEFAULT_REQUEST = PanelRequest(start_year=2020, end_year=2024)


def _make_panel(
    rows: int = 10,
    year: int = 2022,
    acs_columns: list[str] | None = None,
    null_columns: dict[str, int] | None = None,
) -> pd.DataFrame:
    """Build a minimal panel DataFrame for testing.

    Parameters
    ----------
    rows : int
        Number of rows.
    year : int
        Year value for all rows.
    acs_columns : list[str] | None
        ACS columns to include.  Defaults to all ``ACS_MEASURE_COLUMNS``.
    null_columns : dict[str, int] | None
        Mapping of column name -> number of null values to inject (from
        the top of the column).
    """
    if acs_columns is None:
        acs_columns = list(ACS_MEASURE_COLUMNS)
    null_columns = null_columns or {}

    data: dict[str, list] = {
        "coc_id": [f"XX-{i:03d}" for i in range(rows)],
        "year": [year] * rows,
    }
    for col in acs_columns:
        values = list(range(1, rows + 1))
        n_nulls = null_columns.get(col, 0)
        for i in range(n_nulls):
            values[i] = np.nan
        data[col] = values

    return pd.DataFrame(data)


# ============================================================================
# check_column_null_rates
# ============================================================================


class TestCheckColumnNullRates:
    """Tests for check_column_null_rates."""

    @pytest.mark.parametrize(
        "scenario, null_columns, expected_count, expected_checks",
        [
            ("all_present", {}, 0, []),
            ("partial_nulls_below_threshold", {"total_population": 3}, 0, []),
            (
                "partial_nulls_above_threshold",
                {"total_population": 6},
                1,
                ["column_high_null_rate"],
            ),
            ("fully_null", {"total_population": 10}, 1, ["column_fully_null"]),
            (
                "mixed",
                {"total_population": 6, "median_gross_rent": 10},
                2,
                ["column_high_null_rate", "column_fully_null"],
            ),
        ],
        ids=[
            "all_present",
            "partial_nulls_below_threshold",
            "partial_nulls_above_threshold",
            "fully_null",
            "mixed",
        ],
    )
    def test_null_rate_scenarios(
        self,
        scenario: str,
        null_columns: dict[str, int],
        expected_count: int,
        expected_checks: list[str],
    ) -> None:
        df = _make_panel(rows=10, null_columns=null_columns)
        results = check_column_null_rates(df, DEFAULT_REQUEST)

        assert len(results) == expected_count
        assert [r.check_name for r in results] == expected_checks
        for r in results:
            assert r.severity == "warning"

    def test_column_not_in_df(self) -> None:
        """ACS column not present in DataFrame produces no result."""
        df = _make_panel(rows=10, acs_columns=["total_population"])
        results = check_column_null_rates(df, DEFAULT_REQUEST)
        assert len(results) == 0

    def test_fully_null_details(self) -> None:
        """Verify details dict for a fully-null column."""
        df = _make_panel(rows=5, null_columns={"total_population": 5})
        results = check_column_null_rates(df, DEFAULT_REQUEST)
        assert len(results) == 1
        r = results[0]
        assert r.check_name == "column_fully_null"
        assert r.details["column"] == "total_population"
        assert r.details["null_rate"] == 1.0
        assert r.details["row_count"] == 5

    def test_high_null_rate_details(self) -> None:
        """Verify details dict for a high-null-rate column."""
        df = _make_panel(rows=10, null_columns={"median_household_income": 6})
        results = check_column_null_rates(df, DEFAULT_REQUEST)
        assert len(results) == 1
        r = results[0]
        assert r.check_name == "column_high_null_rate"
        assert r.details["column"] == "median_household_income"
        assert r.details["null_rate"] == pytest.approx(0.6)
        assert r.details["null_count"] == 6
        assert r.details["total_count"] == 10
        assert r.details["threshold"] == 0.50

    def test_custom_threshold(self) -> None:
        """A stricter threshold catches lower null rates."""
        req = PanelRequest(
            start_year=2020, end_year=2024, null_rate_threshold=0.25
        )
        df = _make_panel(rows=10, null_columns={"total_population": 3})
        results = check_column_null_rates(df, req)
        assert len(results) == 1
        assert results[0].check_name == "column_high_null_rate"

    def test_empty_dataframe(self) -> None:
        """Empty DataFrame produces no results."""
        df = _make_panel(rows=0)
        results = check_column_null_rates(df, DEFAULT_REQUEST)
        assert len(results) == 0


# ============================================================================
# check_per_year_completeness
# ============================================================================


class TestCheckPerYearCompleteness:
    """Tests for check_per_year_completeness."""

    def test_all_years_complete(self) -> None:
        """No nulls per year -> no results."""
        df = pd.concat(
            [_make_panel(rows=5, year=y) for y in [2020, 2021, 2022]],
            ignore_index=True,
        )
        results = check_per_year_completeness(df, DEFAULT_REQUEST)
        assert len(results) == 0

    def test_one_year_high_nulls(self) -> None:
        """One year with >50% null rate across ACS columns -> 1 warning."""
        good = _make_panel(rows=5, year=2020)
        # For 2021: make all ACS columns fully null -> 100% null rate
        bad = _make_panel(
            rows=5,
            year=2021,
            null_columns={col: 5 for col in ACS_MEASURE_COLUMNS},
        )
        df = pd.concat([good, bad], ignore_index=True)
        results = check_per_year_completeness(df, DEFAULT_REQUEST)

        assert len(results) == 1
        r = results[0]
        assert r.check_name == "year_high_null_rate"
        assert r.severity == "warning"
        assert r.details["year"] == 2021
        assert r.details["null_rate"] == pytest.approx(1.0)

    def test_all_years_high_nulls(self) -> None:
        """All years with >50% null rate -> warning for each year."""
        years = [2020, 2021, 2022]
        frames = []
        for y in years:
            frames.append(
                _make_panel(
                    rows=5,
                    year=y,
                    null_columns={col: 5 for col in ACS_MEASURE_COLUMNS},
                )
            )
        df = pd.concat(frames, ignore_index=True)
        results = check_per_year_completeness(df, DEFAULT_REQUEST)

        assert len(results) == len(years)
        result_years = [r.details["year"] for r in results]
        assert result_years == sorted(years)
        for r in results:
            assert r.check_name == "year_high_null_rate"
            assert r.severity == "warning"

    def test_no_year_column(self) -> None:
        """Missing 'year' column produces no results."""
        df = pd.DataFrame({"coc_id": ["XX-001"], "total_population": [100]})
        results = check_per_year_completeness(df, DEFAULT_REQUEST)
        assert len(results) == 0

    def test_no_acs_columns(self) -> None:
        """No ACS columns present produces no results."""
        df = pd.DataFrame({"coc_id": ["XX-001"], "year": [2022]})
        results = check_per_year_completeness(df, DEFAULT_REQUEST)
        assert len(results) == 0

    def test_year_detail_fields(self) -> None:
        """Verify all expected detail fields are present."""
        bad = _make_panel(
            rows=4,
            year=2023,
            null_columns={col: 4 for col in ACS_MEASURE_COLUMNS},
        )
        results = check_per_year_completeness(bad, DEFAULT_REQUEST)
        assert len(results) == 1
        d = results[0].details
        assert set(d.keys()) == {
            "year",
            "null_rate",
            "null_columns",
            "total_measure_columns",
            "threshold",
        }
        assert d["total_measure_columns"] == len(ACS_MEASURE_COLUMNS)
        assert d["threshold"] == 0.50
        assert isinstance(d["null_columns"], list)


# ============================================================================
# check_zori_eligibility_rate
# ============================================================================


class TestCheckZoriEligibilityRate:
    """Tests for check_zori_eligibility_rate."""

    def test_zori_not_requested(self) -> None:
        """include_zori=False -> no results regardless of data."""
        df = pd.DataFrame(
            {
                "coc_id": ["XX-001"],
                "year": [2022],
                "zori_is_eligible": [False],
            }
        )
        req = PanelRequest(
            start_year=2020, end_year=2024, include_zori=False
        )
        results = check_zori_eligibility_rate(df, req)
        assert len(results) == 0

    def test_zori_high_eligibility(self) -> None:
        """>20% eligible -> no results."""
        n = 10
        df = pd.DataFrame(
            {
                "coc_id": [f"XX-{i:03d}" for i in range(n)],
                "year": [2022] * n,
                # 30% eligible -> above threshold
                "zori_is_eligible": [True] * 3 + [False] * 7,
            }
        )
        req = PanelRequest(
            start_year=2020, end_year=2024, include_zori=True
        )
        results = check_zori_eligibility_rate(df, req)
        assert len(results) == 0

    def test_zori_low_eligibility(self) -> None:
        """<20% eligible -> 1 warning."""
        n = 10
        df = pd.DataFrame(
            {
                "coc_id": [f"XX-{i:03d}" for i in range(n)],
                "year": [2022] * n,
                # 10% eligible -> below threshold
                "zori_is_eligible": [True] * 1 + [False] * 9,
            }
        )
        req = PanelRequest(
            start_year=2020, end_year=2024, include_zori=True
        )
        results = check_zori_eligibility_rate(df, req)
        assert len(results) == 1
        r = results[0]
        assert r.check_name == "zori_low_eligibility"
        assert r.severity == "warning"
        assert r.details["eligibility_rate"] == pytest.approx(0.1)
        assert r.details["eligible_count"] == 1
        assert r.details["total_count"] == 10
        assert r.details["threshold"] == ZORI_MIN_ELIGIBILITY_RATE

    def test_zori_column_missing(self) -> None:
        """include_zori=True but column not in df -> no results."""
        df = pd.DataFrame(
            {
                "coc_id": ["XX-001"],
                "year": [2022],
            }
        )
        req = PanelRequest(
            start_year=2020, end_year=2024, include_zori=True
        )
        results = check_zori_eligibility_rate(df, req)
        assert len(results) == 0

    def test_zori_exactly_at_threshold(self) -> None:
        """Exactly 20% eligible -> no warning (threshold is strict <)."""
        n = 10
        df = pd.DataFrame(
            {
                "coc_id": [f"XX-{i:03d}" for i in range(n)],
                "year": [2022] * n,
                # Exactly 20% eligible -> at threshold, no warning
                "zori_is_eligible": [True] * 2 + [False] * 8,
            }
        )
        req = PanelRequest(
            start_year=2020, end_year=2024, include_zori=True
        )
        results = check_zori_eligibility_rate(df, req)
        assert len(results) == 0

    def test_zori_zero_eligible(self) -> None:
        """0% eligible -> warning."""
        n = 5
        df = pd.DataFrame(
            {
                "coc_id": [f"XX-{i:03d}" for i in range(n)],
                "year": [2022] * n,
                "zori_is_eligible": [False] * n,
            }
        )
        req = PanelRequest(
            start_year=2020, end_year=2024, include_zori=True
        )
        results = check_zori_eligibility_rate(df, req)
        assert len(results) == 1
        assert results[0].details["eligibility_rate"] == 0.0


# ============================================================================
# PEP measure_columns tests  (coclab-d0qm)
# ============================================================================


class TestPepMeasureColumns:
    """Verify that custom measure_columns drive completeness checks."""

    def test_null_rate_checks_pep_column(self) -> None:
        """check_column_null_rates respects measure_columns=["population"]."""
        n = 10
        df = pd.DataFrame({
            "coc_id": [f"XX-{i:03d}" for i in range(n)],
            "year": [2022] * n,
            "population": [np.nan] * n,  # 100% null
        })
        req = PanelRequest(
            start_year=2020, end_year=2024,
            measure_columns=["population"],
        )
        results = check_column_null_rates(df, req)
        assert len(results) == 1
        assert results[0].check_name == "column_fully_null"
        assert results[0].details["column"] == "population"

    def test_null_rate_skips_acs_columns_for_pep(self) -> None:
        """With measure_columns=["population"], ACS columns are not checked."""
        n = 10
        df = pd.DataFrame({
            "coc_id": [f"XX-{i:03d}" for i in range(n)],
            "year": [2022] * n,
            "population": list(range(1, n + 1)),
            # ACS column present but fully null — should NOT be flagged
            "total_population": [np.nan] * n,
        })
        req = PanelRequest(
            start_year=2020, end_year=2024,
            measure_columns=["population"],
        )
        results = check_column_null_rates(df, req)
        assert len(results) == 0

    def test_per_year_completeness_pep(self) -> None:
        """check_per_year_completeness respects measure_columns."""
        n = 5
        df = pd.DataFrame({
            "metro_id": [f"GF{i:02d}" for i in range(1, n + 1)],
            "year": [2020] * n,
            "population": [np.nan] * n,
        })
        req = PanelRequest(
            start_year=2020, end_year=2020,
            measure_columns=["population"],
        )
        results = check_per_year_completeness(df, req)
        assert len(results) == 1
        assert results[0].details["total_measure_columns"] == 1
