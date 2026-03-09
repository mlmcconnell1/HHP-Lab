"""Tests for the conformance report infrastructure.

Covers the core data classes (ConformanceResult, ConformanceReport,
PanelRequest), the check registry (register_check), and the runner
(run_conformance).

No real conformance checks are tested here — this validates the framework
itself. Individual check tests live in their own modules.

Truth table for ConformanceReport.passed / errors / warnings:

    Scenario        | errors | warnings | passed
    ----------------|--------|----------|-------
    empty           |   0    |    0     | True
    warnings_only   |   0    |    1     | True
    errors_only     |   1    |    0     | False
    mixed           |   1    |    1     | False

Beads: coclab-396b
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from coclab.panel.conformance import (
    ConformanceReport,
    ConformanceResult,
    PanelRequest,
    _CHECKS,
    register_check,
    run_conformance,
)

# ============================================================================
# Fixtures
# ============================================================================

# Reusable result instances keyed by severity for truth-table tests.
ERROR_RESULT = ConformanceResult(
    check_name="test_error",
    severity="error",
    message="Something broke",
    details={"column": "year"},
)

WARNING_RESULT = ConformanceResult(
    check_name="test_warning",
    severity="warning",
    message="Looks suspicious",
    details={"threshold": 0.5},
)

# Truth table: (label, result_list, expected_passed, expected_error_count, expected_warning_count)
REPORT_TRUTH_TABLE: list[tuple[str, list[ConformanceResult], bool, int, int]] = [
    ("empty", [], True, 0, 0),
    ("warnings_only", [WARNING_RESULT], True, 0, 1),
    ("errors_only", [ERROR_RESULT], False, 1, 0),
    ("mixed", [ERROR_RESULT, WARNING_RESULT], False, 1, 1),
]


@pytest.fixture()
def _isolate_checks():
    """Save and restore the global check registry around each test.

    Prevents dummy checks registered during tests from leaking into other
    test functions.
    """
    saved = _CHECKS.copy()
    _CHECKS.clear()
    yield
    _CHECKS.clear()
    _CHECKS.extend(saved)


@pytest.fixture()
def minimal_panel_df() -> pd.DataFrame:
    """A tiny DataFrame with the bare-minimum columns for testing."""
    return pd.DataFrame(
        {
            "coc_id": ["CO-500", "CA-600"],
            "year": [2022, 2022],
        }
    )


@pytest.fixture()
def default_request() -> PanelRequest:
    """A PanelRequest with default settings for 2020-2024."""
    return PanelRequest(start_year=2020, end_year=2024)


# ============================================================================
# ConformanceResult tests
# ============================================================================


class TestConformanceResult:
    """Tests for the ConformanceResult dataclass."""

    def test_creation(self) -> None:
        result = ConformanceResult(
            check_name="year_coverage",
            severity="error",
            message="Missing year 2023",
            details={"missing_years": [2023]},
        )
        assert result.check_name == "year_coverage"
        assert result.severity == "error"
        assert result.message == "Missing year 2023"
        assert result.details == {"missing_years": [2023]}

    def test_creation_default_details(self) -> None:
        result = ConformanceResult(
            check_name="simple",
            severity="warning",
            message="Heads up",
        )
        assert result.details == {}

    def test_to_dict(self) -> None:
        result = ConformanceResult(
            check_name="col_check",
            severity="warning",
            message="Column X is 40% null",
            details={"column": "X", "null_rate": 0.4},
        )
        d = result.to_dict()
        assert d == {
            "check_name": "col_check",
            "severity": "warning",
            "message": "Column X is 40% null",
            "details": {"column": "X", "null_rate": 0.4},
        }

    def test_to_dict_roundtrip_keys(self) -> None:
        """to_dict output contains exactly the expected keys."""
        result = ConformanceResult(
            check_name="k", severity="error", message="m"
        )
        assert set(result.to_dict().keys()) == {
            "check_name",
            "severity",
            "message",
            "details",
        }


# ============================================================================
# ConformanceReport tests
# ============================================================================


class TestConformanceReport:
    """Tests for the ConformanceReport class."""

    # -- Truth-table parametrised tests for passed / errors / warnings -------

    @pytest.mark.parametrize(
        "label, results, expected_passed, expected_errors, expected_warnings",
        REPORT_TRUTH_TABLE,
        ids=[row[0] for row in REPORT_TRUTH_TABLE],
    )
    def test_passed_and_counts(
        self,
        label: str,
        results: list[ConformanceResult],
        expected_passed: bool,
        expected_errors: int,
        expected_warnings: int,
    ) -> None:
        report = ConformanceReport(results=list(results))
        assert report.passed is expected_passed
        assert len(report.errors) == expected_errors
        assert len(report.warnings) == expected_warnings

    # -- summary() -----------------------------------------------------------

    def test_summary_empty(self) -> None:
        report = ConformanceReport()
        s = report.summary()
        assert "0 error(s)" in s
        assert "0 warning(s)" in s

    def test_summary_contains_check_names(self) -> None:
        report = ConformanceReport(results=[ERROR_RESULT, WARNING_RESULT])
        s = report.summary()
        assert "[test_error]" in s
        assert "[test_warning]" in s

    def test_summary_icons(self) -> None:
        report = ConformanceReport(results=[ERROR_RESULT, WARNING_RESULT])
        s = report.summary()
        # Error icon (✗) and warning icon (⚠)
        assert "\u2717" in s
        assert "\u26a0" in s

    # -- to_dict() -----------------------------------------------------------

    def test_to_dict_structure(self) -> None:
        report = ConformanceReport(results=[ERROR_RESULT])
        d = report.to_dict()
        assert set(d.keys()) == {"passed", "error_count", "warning_count", "results"}
        assert d["passed"] is False
        assert d["error_count"] == 1
        assert d["warning_count"] == 0
        assert len(d["results"]) == 1
        assert d["results"][0]["check_name"] == "test_error"

    def test_to_dict_empty(self) -> None:
        report = ConformanceReport()
        d = report.to_dict()
        assert d["passed"] is True
        assert d["error_count"] == 0
        assert d["warning_count"] == 0
        assert d["results"] == []

    # -- __len__ and __bool__ ------------------------------------------------

    def test_len_empty(self) -> None:
        assert len(ConformanceReport()) == 0

    def test_len_with_results(self) -> None:
        assert len(ConformanceReport(results=[ERROR_RESULT, WARNING_RESULT])) == 2

    def test_bool_empty(self) -> None:
        assert not ConformanceReport()

    def test_bool_with_results(self) -> None:
        assert ConformanceReport(results=[WARNING_RESULT])


# ============================================================================
# PanelRequest tests
# ============================================================================


class TestPanelRequest:
    """Tests for the PanelRequest dataclass."""

    def test_defaults(self) -> None:
        req = PanelRequest(start_year=2020, end_year=2024)
        assert req.start_year == 2020
        assert req.end_year == 2024
        assert req.include_zori is False
        assert req.weighting_method == "population"
        assert req.zori_min_coverage == 0.90
        assert req.expected_coc_count is None
        assert req.null_rate_threshold == 0.50

    def test_all_fields(self) -> None:
        req = PanelRequest(
            start_year=2018,
            end_year=2023,
            include_zori=True,
            weighting_method="area",
            zori_min_coverage=0.80,
            expected_coc_count=400,
            null_rate_threshold=0.25,
        )
        assert req.start_year == 2018
        assert req.end_year == 2023
        assert req.include_zori is True
        assert req.weighting_method == "area"
        assert req.zori_min_coverage == 0.80
        assert req.expected_coc_count == 400
        assert req.null_rate_threshold == 0.25


# ============================================================================
# Registry and runner tests
# ============================================================================


class TestRegistryAndRunner:
    """Tests for register_check decorator and run_conformance."""

    def test_register_check_adds_to_registry(
        self, _isolate_checks: None
    ) -> None:
        assert len(_CHECKS) == 0

        @register_check
        def dummy(df: pd.DataFrame, req: PanelRequest) -> list[ConformanceResult]:
            return []

        assert len(_CHECKS) == 1
        assert _CHECKS[0] is dummy

    def test_register_check_returns_original_function(
        self, _isolate_checks: None
    ) -> None:
        def my_check(df: pd.DataFrame, req: PanelRequest) -> list[ConformanceResult]:
            return []

        decorated = register_check(my_check)
        assert decorated is my_check

    def test_run_conformance_calls_check_with_correct_args(
        self,
        _isolate_checks: None,
        minimal_panel_df: pd.DataFrame,
        default_request: PanelRequest,
    ) -> None:
        received_args: list[tuple[pd.DataFrame, PanelRequest]] = []

        @register_check
        def spy(df: pd.DataFrame, req: PanelRequest) -> list[ConformanceResult]:
            received_args.append((df, req))
            return []

        run_conformance(minimal_panel_df, default_request)

        assert len(received_args) == 1
        pd.testing.assert_frame_equal(received_args[0][0], minimal_panel_df)
        assert received_args[0][1] is default_request

    def test_run_conformance_collects_results(
        self,
        _isolate_checks: None,
        minimal_panel_df: pd.DataFrame,
        default_request: PanelRequest,
    ) -> None:
        @register_check
        def check_a(df: pd.DataFrame, req: PanelRequest) -> list[ConformanceResult]:
            return [
                ConformanceResult(
                    check_name="a", severity="error", message="fail a"
                )
            ]

        @register_check
        def check_b(df: pd.DataFrame, req: PanelRequest) -> list[ConformanceResult]:
            return [
                ConformanceResult(
                    check_name="b", severity="warning", message="warn b"
                )
            ]

        report = run_conformance(minimal_panel_df, default_request)

        assert len(report) == 2
        assert report.results[0].check_name == "a"
        assert report.results[1].check_name == "b"
        assert not report.passed  # has an error

    def test_run_conformance_no_checks_returns_empty_report(
        self,
        _isolate_checks: None,
        minimal_panel_df: pd.DataFrame,
        default_request: PanelRequest,
    ) -> None:
        """With no registered checks, run_conformance returns a passing empty report."""
        report = run_conformance(minimal_panel_df, default_request)
        assert len(report) == 0
        assert report.passed is True
        assert not report  # __bool__ is False for empty

    def test_multiple_results_from_single_check(
        self,
        _isolate_checks: None,
        minimal_panel_df: pd.DataFrame,
        default_request: PanelRequest,
    ) -> None:
        """A single check can return multiple results."""

        @register_check
        def multi(df: pd.DataFrame, req: PanelRequest) -> list[ConformanceResult]:
            return [
                ConformanceResult(
                    check_name="multi_e", severity="error", message="e"
                ),
                ConformanceResult(
                    check_name="multi_w", severity="warning", message="w"
                ),
            ]

        report = run_conformance(minimal_panel_df, default_request)

        assert len(report) == 2
        assert len(report.errors) == 1
        assert len(report.warnings) == 1
