"""Tests for PIT-exceeds-population conformance check (coclab-2472).

Covers ``check_pit_exceeds_population``: flags CoC-year rows where
``pit_total`` > ``total_population``, which is logically impossible.

Truth table
===========

    Scenario                    | pit vs pop        | Expected
    ----------------------------|-------------------|----------------------------
    all_plausible               | pit < pop         | no results
    one_bad_row                 | 1 row pit > pop   | 1 error, 1 example
    multiple_bad_rows           | 3 rows pit > pop  | 1 error, 3 in details
    equal_values                | pit == pop        | no results
    missing_pit_column          | no pit_total col  | no results
    missing_pop_column          | no total_pop col  | no results
    missing_geo_column          | no coc_id/geo_id  | no results
    all_null                    | both cols null    | no results
    partial_null                | some nulls        | only checks non-null pairs

Beads: coclab-2472
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from coclab.panel.conformance import (
    PanelRequest,
    check_pit_exceeds_population,
)

DEFAULT_REQUEST = PanelRequest(start_year=2020, end_year=2024)


def _make_panel(
    coc_ids: list[str],
    years: list[int],
    pit_totals: list[float | None],
    populations: list[float | None],
) -> pd.DataFrame:
    """Build a minimal panel with pit_total and total_population."""
    return pd.DataFrame({
        "coc_id": coc_ids,
        "year": years,
        "pit_total": pit_totals,
        "total_population": populations,
    })


class TestCheckPitExceedsPopulation:
    """Tests for check_pit_exceeds_population."""

    def test_all_plausible(self) -> None:
        """pit_total < total_population everywhere -> no results."""
        df = _make_panel(
            coc_ids=["XX-001", "XX-002", "XX-003"],
            years=[2022, 2022, 2022],
            pit_totals=[100, 200, 50],
            populations=[50000, 80000, 30000],
        )
        results = check_pit_exceeds_population(df, DEFAULT_REQUEST)
        assert len(results) == 0

    def test_one_bad_row(self) -> None:
        """Single row where pit > pop -> 1 error."""
        df = _make_panel(
            coc_ids=["XX-001", "XX-002"],
            years=[2022, 2022],
            pit_totals=[100, 999999],
            populations=[50000, 500],
        )
        results = check_pit_exceeds_population(df, DEFAULT_REQUEST)
        assert len(results) == 1
        r = results[0]
        assert r.check_name == "pit_exceeds_population"
        assert r.severity == "error"
        assert r.details["bad_row_count"] == 1
        assert len(r.details["examples"]) == 1
        assert r.details["examples"][0]["coc_id"] == "XX-002"

    def test_multiple_bad_rows(self) -> None:
        """Multiple bad rows -> 1 error with count and examples."""
        df = _make_panel(
            coc_ids=["XX-001", "XX-002", "XX-003", "XX-004"],
            years=[2022, 2022, 2023, 2023],
            pit_totals=[100, 5000, 9000, 50],
            populations=[50000, 200, 300, 80000],
        )
        results = check_pit_exceeds_population(df, DEFAULT_REQUEST)
        assert len(results) == 1
        r = results[0]
        assert r.details["bad_row_count"] == 2
        assert r.details["total_comparable_rows"] == 4
        example_cocs = {e["coc_id"] for e in r.details["examples"]}
        assert example_cocs == {"XX-002", "XX-003"}

    def test_equal_values(self) -> None:
        """pit_total == total_population -> no results (not strictly greater)."""
        df = _make_panel(
            coc_ids=["XX-001"],
            years=[2022],
            pit_totals=[500],
            populations=[500],
        )
        results = check_pit_exceeds_population(df, DEFAULT_REQUEST)
        assert len(results) == 0

    def test_missing_pit_column(self) -> None:
        """No pit_total column -> no results."""
        df = pd.DataFrame({
            "coc_id": ["XX-001"],
            "year": [2022],
            "total_population": [50000],
        })
        results = check_pit_exceeds_population(df, DEFAULT_REQUEST)
        assert len(results) == 0

    def test_missing_pop_column(self) -> None:
        """No total_population column -> no results."""
        df = pd.DataFrame({
            "coc_id": ["XX-001"],
            "year": [2022],
            "pit_total": [100],
        })
        results = check_pit_exceeds_population(df, DEFAULT_REQUEST)
        assert len(results) == 0

    def test_missing_geo_column(self) -> None:
        """No coc_id or geo_id column -> no results."""
        df = pd.DataFrame({
            "year": [2022],
            "pit_total": [99999],
            "total_population": [100],
        })
        results = check_pit_exceeds_population(df, DEFAULT_REQUEST)
        assert len(results) == 0

    def test_all_null(self) -> None:
        """Both columns fully null -> no comparable rows, no results."""
        df = _make_panel(
            coc_ids=["XX-001", "XX-002"],
            years=[2022, 2022],
            pit_totals=[np.nan, np.nan],
            populations=[np.nan, np.nan],
        )
        results = check_pit_exceeds_population(df, DEFAULT_REQUEST)
        assert len(results) == 0

    def test_partial_null(self) -> None:
        """Null rows are excluded; only non-null pairs are checked."""
        df = _make_panel(
            coc_ids=["XX-001", "XX-002", "XX-003"],
            years=[2022, 2022, 2022],
            pit_totals=[np.nan, 99999, 50],
            populations=[50000, 100, np.nan],
        )
        results = check_pit_exceeds_population(df, DEFAULT_REQUEST)
        assert len(results) == 1
        # Only XX-002 is comparable and bad
        assert results[0].details["bad_row_count"] == 1
        assert results[0].details["total_comparable_rows"] == 1

    def test_geo_id_fallback(self) -> None:
        """Uses geo_id when coc_id is absent."""
        df = pd.DataFrame({
            "geo_id": ["XX-001"],
            "year": [2022],
            "pit_total": [99999],
            "total_population": [100],
        })
        results = check_pit_exceeds_population(df, DEFAULT_REQUEST)
        assert len(results) == 1
        assert results[0].details["examples"][0]["geo_id"] == "XX-001"

    def test_examples_capped_at_five(self) -> None:
        """Examples list is capped at 5 even with more bad rows."""
        n = 8
        df = _make_panel(
            coc_ids=[f"XX-{i:03d}" for i in range(n)],
            years=[2022] * n,
            pit_totals=[99999] * n,
            populations=[100] * n,
        )
        results = check_pit_exceeds_population(df, DEFAULT_REQUEST)
        assert len(results) == 1
        assert results[0].details["bad_row_count"] == n
        assert len(results[0].details["examples"]) == 5

    def test_error_in_to_dict(self) -> None:
        """Result serializes cleanly via to_dict."""
        df = _make_panel(
            coc_ids=["XX-001"],
            years=[2022],
            pit_totals=[99999],
            populations=[100],
        )
        results = check_pit_exceeds_population(df, DEFAULT_REQUEST)
        d = results[0].to_dict()
        assert d["check_name"] == "pit_exceeds_population"
        assert d["severity"] == "error"
        assert isinstance(d["details"], dict)
