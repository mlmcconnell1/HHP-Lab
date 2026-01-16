"""Tests for PIT data quality assurance module (WP-3D).

Tests cover:
- QAIssue and QAReport dataclasses
- Duplicate CoC detection
- Missing CoCs relative to boundary vintages
- Invalid count validation (non-integer, negative)
- Year-over-year change detection
- Full validation pipeline
"""

from datetime import UTC, datetime

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

from coclab.pit.qa import (
    QAIssue,
    QAReport,
    Severity,
    check_duplicates,
    check_invalid_counts,
    check_missing_cocs,
    check_yoy_changes,
    validate_pit_data,
)


def make_pit_df(
    coc_ids: list[str] | None = None,
    totals: list[int] | None = None,
    year: int = 2024,
    include_sheltered: bool = True,
) -> pd.DataFrame:
    """Create a sample PIT DataFrame for testing."""
    if coc_ids is None:
        coc_ids = ["CO-500", "CA-600", "NY-501"]
    if totals is None:
        totals = [1000, 50000, 80000]

    n = len(coc_ids)
    data = {
        "pit_year": [year] * n,
        "coc_id": coc_ids,
        "pit_total": totals[:n] if len(totals) >= n else totals + [1000] * (n - len(totals)),
        "data_source": ["hud_exchange"] * n,
        "source_ref": ["https://example.com"] * n,
        "ingested_at": [datetime.now(UTC)] * n,
    }
    if include_sheltered:
        data["pit_sheltered"] = [int(t * 0.7) for t in data["pit_total"]]
        data["pit_unsheltered"] = [int(t * 0.3) for t in data["pit_total"]]
    return pd.DataFrame(data)


def make_boundary_gdf(coc_ids: list[str] | None = None) -> gpd.GeoDataFrame:
    """Create a sample boundary GeoDataFrame for testing."""
    if coc_ids is None:
        coc_ids = ["CO-500", "CA-600", "NY-501"]

    n = len(coc_ids)
    geometries = [
        Polygon([(-105 - i, 39), (-105 - i, 40), (-104 - i, 40), (-104 - i, 39)]) for i in range(n)
    ]
    return gpd.GeoDataFrame(
        {
            "coc_id": coc_ids,
            "coc_name": [f"CoC {cid}" for cid in coc_ids],
            "boundary_vintage": ["2024"] * n,
            "geometry": geometries,
        },
        crs="EPSG:4326",
    )


class TestQAIssue:
    """Tests for QAIssue dataclass."""

    def test_str_with_coc_and_year(self):
        issue = QAIssue(
            severity=Severity.ERROR,
            check_name="test_check",
            coc_id="CO-500",
            year=2024,
            message="Test error message",
        )
        s = str(issue)
        assert "[ERROR]" in s
        assert "test_check" in s
        assert "CO-500" in s
        assert "2024" in s
        assert "Test error message" in s

    def test_str_without_coc_or_year(self):
        issue = QAIssue(
            severity=Severity.WARNING,
            check_name="test_check",
            coc_id=None,
            year=None,
            message="Test warning",
        )
        s = str(issue)
        assert "[WARNING]" in s
        assert "test_check" in s
        assert "Test warning" in s
        assert "CoC:" not in s
        assert "Year:" not in s

    def test_to_dict(self):
        issue = QAIssue(
            severity=Severity.ERROR,
            check_name="duplicates",
            coc_id="CA-600",
            year=2024,
            message="Duplicate found",
            details={"count": 2},
        )
        d = issue.to_dict()
        assert d["severity"] == "error"
        assert d["check_name"] == "duplicates"
        assert d["coc_id"] == "CA-600"
        assert d["year"] == 2024
        assert d["details"]["count"] == 2


class TestQAReport:
    """Tests for QAReport dataclass."""

    def test_empty_report_passes(self):
        report = QAReport()
        assert report.passed
        assert len(report.errors) == 0
        assert len(report.warnings) == 0
        assert "no issues" in str(report)

    def test_add_error_fails_report(self):
        report = QAReport()
        report.add_error("test", "test error", coc_id="CO-500")
        assert not report.passed
        assert len(report.errors) == 1

    def test_add_warning_keeps_passing(self):
        report = QAReport()
        report.add_warning("test", "test warning")
        assert report.passed
        assert len(report.warnings) == 1

    def test_summary(self):
        report = QAReport()
        report.add_error("check1", "error 1")
        report.add_error("check1", "error 2")
        report.add_warning("check2", "warning 1")

        summary = report.summary
        assert summary["by_severity"]["error"] == 2
        assert summary["by_severity"]["warning"] == 1
        assert summary["by_severity"]["total"] == 3
        assert summary["by_check"]["check1"] == 2
        assert summary["by_check"]["check2"] == 1

    def test_extend(self):
        report = QAReport()
        issues = [
            QAIssue(Severity.ERROR, "check1", None, None, "error 1"),
            QAIssue(Severity.WARNING, "check2", None, None, "warning 1"),
        ]
        report.extend(issues)
        assert len(report.issues) == 2

    def test_to_list(self):
        report = QAReport()
        report.add_error("check1", "error message")
        report.add_warning("check2", "warning message")
        lines = report.to_list()
        assert len(lines) == 2
        assert any("[ERROR]" in line for line in lines)
        assert any("[WARNING]" in line for line in lines)

    def test_to_dataframe(self):
        report = QAReport()
        report.add_error("check1", "error", coc_id="CO-500", year=2024)
        df = report.to_dataframe()
        assert len(df) == 1
        assert df.iloc[0]["coc_id"] == "CO-500"
        assert df.iloc[0]["severity"] == "error"

    def test_to_dataframe_empty(self):
        report = QAReport()
        df = report.to_dataframe()
        assert len(df) == 0
        assert "severity" in df.columns


class TestCheckDuplicates:
    """Tests for check_duplicates function."""

    def test_no_duplicates(self):
        df = make_pit_df(coc_ids=["CO-500", "CA-600", "NY-501"])
        issues = check_duplicates(df)
        assert len(issues) == 0

    def test_duplicate_within_year(self):
        df = make_pit_df(coc_ids=["CO-500", "CO-500", "CA-600"])
        issues = check_duplicates(df)
        assert len(issues) == 1
        assert issues[0].coc_id == "CO-500"
        assert issues[0].severity == Severity.ERROR
        assert issues[0].check_name == "duplicates"

    def test_multiple_duplicates(self):
        df = make_pit_df(coc_ids=["CO-500", "CO-500", "CA-600", "CA-600"])
        issues = check_duplicates(df)
        assert len(issues) == 2
        coc_ids = {i.coc_id for i in issues}
        assert coc_ids == {"CO-500", "CA-600"}

    def test_same_coc_different_years_ok(self):
        df1 = make_pit_df(coc_ids=["CO-500"], year=2023)
        df2 = make_pit_df(coc_ids=["CO-500"], year=2024)
        df = pd.concat([df1, df2], ignore_index=True)
        issues = check_duplicates(df)
        assert len(issues) == 0

    def test_empty_df(self):
        df = pd.DataFrame()
        issues = check_duplicates(df)
        assert len(issues) == 0

    def test_missing_columns(self):
        df = pd.DataFrame({"coc_id": ["CO-500"]})  # Missing pit_year
        issues = check_duplicates(df)
        assert len(issues) == 1
        assert issues[0].severity == Severity.ERROR
        assert "Missing required columns" in issues[0].message


class TestCheckMissingCocs:
    """Tests for check_missing_cocs function."""

    def test_all_cocs_present(self):
        pit_df = make_pit_df(coc_ids=["CO-500", "CA-600", "NY-501"])
        boundary_gdf = make_boundary_gdf(coc_ids=["CO-500", "CA-600", "NY-501"])
        issues = check_missing_cocs(pit_df, "2024", boundary_gdf=boundary_gdf)
        assert len(issues) == 0

    def test_coc_missing_from_pit(self):
        pit_df = make_pit_df(coc_ids=["CO-500", "CA-600"])  # Missing NY-501
        boundary_gdf = make_boundary_gdf(coc_ids=["CO-500", "CA-600", "NY-501"])
        issues = check_missing_cocs(pit_df, "2024", boundary_gdf=boundary_gdf)
        assert len(issues) == 1
        assert issues[0].coc_id == "NY-501"
        assert "missing from PIT data" in issues[0].message
        assert issues[0].severity == Severity.WARNING

    def test_extra_coc_in_pit(self):
        pit_df = make_pit_df(coc_ids=["CO-500", "CA-600", "TX-500"])  # TX-500 not in boundaries
        boundary_gdf = make_boundary_gdf(coc_ids=["CO-500", "CA-600"])
        issues = check_missing_cocs(pit_df, "2024", boundary_gdf=boundary_gdf)
        assert len(issues) == 1
        assert issues[0].coc_id == "TX-500"
        assert "not found in boundary" in issues[0].message

    def test_multiple_discrepancies(self):
        pit_df = make_pit_df(coc_ids=["CO-500", "TX-500"])  # Missing CA-600, extra TX-500
        boundary_gdf = make_boundary_gdf(coc_ids=["CO-500", "CA-600"])
        issues = check_missing_cocs(pit_df, "2024", boundary_gdf=boundary_gdf)
        assert len(issues) == 2

    def test_boundary_gdf_missing_coc_column(self):
        pit_df = make_pit_df()
        boundary_gdf = gpd.GeoDataFrame({"name": ["Test"]})
        issues = check_missing_cocs(pit_df, "2024", boundary_gdf=boundary_gdf)
        assert len(issues) == 1
        assert issues[0].severity == Severity.ERROR
        assert "missing 'coc_id' column" in issues[0].message

    def test_pit_df_missing_coc_column(self):
        pit_df = pd.DataFrame({"pit_year": [2024], "pit_total": [1000]})
        boundary_gdf = make_boundary_gdf()
        issues = check_missing_cocs(pit_df, "2024", boundary_gdf=boundary_gdf)
        assert len(issues) == 1
        assert "Missing required column" in issues[0].message

    def test_empty_pit_df(self):
        pit_df = pd.DataFrame()
        boundary_gdf = make_boundary_gdf()
        issues = check_missing_cocs(pit_df, "2024", boundary_gdf=boundary_gdf)
        assert len(issues) == 0


class TestCheckInvalidCounts:
    """Tests for check_invalid_counts function."""

    def test_valid_counts(self):
        df = make_pit_df()
        issues = check_invalid_counts(df)
        assert len(issues) == 0

    def test_negative_total(self):
        df = make_pit_df(coc_ids=["CO-500"], totals=[1000], include_sheltered=False)
        df.loc[0, "pit_total"] = -100
        issues = check_invalid_counts(df)
        assert len(issues) == 1
        assert issues[0].severity == Severity.ERROR
        assert "Negative value" in issues[0].message
        assert issues[0].coc_id == "CO-500"

    def test_negative_sheltered(self):
        df = make_pit_df(coc_ids=["CO-500"], totals=[1000])
        df.loc[0, "pit_sheltered"] = -500
        issues = check_invalid_counts(df)
        assert len(issues) == 1
        assert "pit_sheltered" in issues[0].details["column"]

    def test_non_integer_count(self):
        df = make_pit_df(coc_ids=["CO-500"], totals=[1000])
        df.loc[0, "pit_total"] = 1000.5
        issues = check_invalid_counts(df)
        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING
        assert "Non-integer" in issues[0].message

    def test_null_total_is_error(self):
        df = make_pit_df(coc_ids=["CO-500"], totals=[1000])
        df.loc[0, "pit_total"] = None
        issues = check_invalid_counts(df)
        assert len(issues) == 1
        assert issues[0].severity == Severity.ERROR
        assert "null" in issues[0].message.lower()

    def test_null_sheltered_ok(self):
        df = make_pit_df(coc_ids=["CO-500"], totals=[1000])
        df.loc[0, "pit_sheltered"] = None
        issues = check_invalid_counts(df)
        # Null in optional columns is not an issue
        assert len(issues) == 0

    def test_non_numeric_value(self):
        df = make_pit_df(coc_ids=["CO-500"], totals=[1000])
        df["pit_total"] = df["pit_total"].astype(object)
        df.loc[0, "pit_total"] = "not a number"
        issues = check_invalid_counts(df)
        assert len(issues) == 1
        assert issues[0].severity == Severity.ERROR
        assert "Non-numeric" in issues[0].message

    def test_no_count_columns(self):
        df = pd.DataFrame(
            {
                "coc_id": ["CO-500"],
                "pit_year": [2024],
            }
        )
        issues = check_invalid_counts(df)
        assert len(issues) == 1
        assert "No count columns" in issues[0].message

    def test_empty_df(self):
        df = pd.DataFrame()
        issues = check_invalid_counts(df)
        assert len(issues) == 0


class TestCheckYoyChanges:
    """Tests for check_yoy_changes function."""

    def test_no_significant_change(self):
        df_prev = make_pit_df(coc_ids=["CO-500", "CA-600"], totals=[1000, 50000], year=2023)
        df_curr = make_pit_df(coc_ids=["CO-500", "CA-600"], totals=[1100, 52000], year=2024)
        issues = check_yoy_changes(df_curr, df_prev, threshold=0.5)
        assert len(issues) == 0

    def test_large_increase(self):
        df_prev = make_pit_df(coc_ids=["CO-500"], totals=[1000], year=2023)
        df_curr = make_pit_df(coc_ids=["CO-500"], totals=[2000], year=2024)  # 100% increase
        issues = check_yoy_changes(df_curr, df_prev, threshold=0.5)
        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING
        assert "100.0%" in issues[0].message
        assert "increase" in issues[0].message.lower()
        assert issues[0].coc_id == "CO-500"

    def test_large_decrease(self):
        df_prev = make_pit_df(coc_ids=["CO-500"], totals=[2000], year=2023)
        df_curr = make_pit_df(coc_ids=["CO-500"], totals=[800], year=2024)  # 60% decrease
        issues = check_yoy_changes(df_curr, df_prev, threshold=0.5)
        assert len(issues) == 1
        assert "60.0%" in issues[0].message
        assert "decrease" in issues[0].message.lower()

    def test_exactly_at_threshold(self):
        df_prev = make_pit_df(coc_ids=["CO-500"], totals=[1000], year=2023)
        df_curr = make_pit_df(coc_ids=["CO-500"], totals=[1500], year=2024)  # Exactly 50%
        issues = check_yoy_changes(df_curr, df_prev, threshold=0.5)
        assert len(issues) == 1

    def test_custom_threshold(self):
        df_prev = make_pit_df(coc_ids=["CO-500"], totals=[1000], year=2023)
        df_curr = make_pit_df(coc_ids=["CO-500"], totals=[1300], year=2024)  # 30% increase

        # Should flag at 25% threshold
        issues = check_yoy_changes(df_curr, df_prev, threshold=0.25)
        assert len(issues) == 1

        # Should not flag at 35% threshold
        issues = check_yoy_changes(df_curr, df_prev, threshold=0.35)
        assert len(issues) == 0

    def test_new_coc_not_flagged(self):
        df_prev = make_pit_df(coc_ids=["CO-500"], totals=[1000], year=2023)
        df_curr = make_pit_df(coc_ids=["CO-500", "CA-600"], totals=[1100, 50000], year=2024)
        issues = check_yoy_changes(df_curr, df_prev, threshold=0.5)
        # CA-600 is new, should not be flagged
        assert len(issues) == 0

    def test_zero_previous_to_nonzero(self):
        df_prev = make_pit_df(coc_ids=["CO-500"], totals=[0], year=2023)
        df_curr = make_pit_df(coc_ids=["CO-500"], totals=[1000], year=2024)
        issues = check_yoy_changes(df_curr, df_prev, threshold=0.5)
        assert len(issues) == 1
        assert "0 to 1000" in issues[0].message
        assert "infinite" in issues[0].message.lower()

    def test_empty_previous_df(self):
        df_curr = make_pit_df(coc_ids=["CO-500"], totals=[1000], year=2024)
        df_prev = pd.DataFrame()
        issues = check_yoy_changes(df_curr, df_prev, threshold=0.5)
        assert len(issues) == 1
        assert "No previous year data" in issues[0].message

    def test_missing_columns(self):
        df_prev = pd.DataFrame({"coc_id": ["CO-500"]})  # Missing pit_total
        df_curr = make_pit_df(coc_ids=["CO-500"], totals=[1000])
        issues = check_yoy_changes(df_curr, df_prev, threshold=0.5)
        assert len(issues) == 1
        assert issues[0].severity == Severity.ERROR
        assert "Missing required columns" in issues[0].message

    def test_details_include_years(self):
        df_prev = make_pit_df(coc_ids=["CO-500"], totals=[1000], year=2023)
        df_curr = make_pit_df(coc_ids=["CO-500"], totals=[2000], year=2024)
        issues = check_yoy_changes(df_curr, df_prev, threshold=0.5)
        assert issues[0].details["previous_year"] == 2023
        assert issues[0].details["current_year"] == 2024
        assert issues[0].details["previous_total"] == 1000
        assert issues[0].details["current_total"] == 2000


class TestValidatePitData:
    """Tests for the main validate_pit_data function."""

    def test_valid_data_passes(self):
        df = make_pit_df()
        report = validate_pit_data(df)
        assert report.passed

    def test_empty_df_fails(self):
        df = pd.DataFrame()
        report = validate_pit_data(df)
        assert not report.passed
        assert any(i.check_name == "data_quality" for i in report.errors)

    def test_none_df_fails(self):
        report = validate_pit_data(None)
        assert not report.passed

    def test_duplicate_check_integrated(self):
        df = make_pit_df(coc_ids=["CO-500", "CO-500", "CA-600"])
        report = validate_pit_data(df)
        assert any(i.check_name == "duplicates" for i in report.issues)

    def test_invalid_counts_check_integrated(self):
        df = make_pit_df(coc_ids=["CO-500"], totals=[-100])
        report = validate_pit_data(df)
        assert any(i.check_name == "invalid_counts" for i in report.issues)

    def test_missing_cocs_check_integrated(self):
        pit_df = make_pit_df(coc_ids=["CO-500", "CA-600"])
        boundary_gdf = make_boundary_gdf(coc_ids=["CO-500", "CA-600", "NY-501"])
        report = validate_pit_data(
            pit_df,
            boundary_vintage="2024",
            boundary_gdf=boundary_gdf,
        )
        assert any(i.check_name == "missing_cocs" for i in report.issues)

    def test_yoy_check_integrated(self):
        df_prev = make_pit_df(coc_ids=["CO-500"], totals=[1000], year=2023)
        df_curr = make_pit_df(coc_ids=["CO-500"], totals=[2000], year=2024)
        report = validate_pit_data(df_curr, df_previous=df_prev)
        assert any(i.check_name == "yoy_changes" for i in report.issues)

    def test_custom_yoy_threshold(self):
        df_prev = make_pit_df(coc_ids=["CO-500"], totals=[1000], year=2023)
        df_curr = make_pit_df(coc_ids=["CO-500"], totals=[1300], year=2024)

        # Should flag at 25% threshold
        report = validate_pit_data(df_curr, df_previous=df_prev, yoy_threshold=0.25)
        assert any(i.check_name == "yoy_changes" for i in report.issues)

        # Should not flag at 35% threshold
        report = validate_pit_data(df_curr, df_previous=df_prev, yoy_threshold=0.35)
        assert not any(i.check_name == "yoy_changes" for i in report.issues)

    def test_all_checks_run(self):
        """Test that all checks can run together without conflict."""
        df_prev = make_pit_df(coc_ids=["CO-500", "CA-600"], totals=[1000, 50000], year=2023)
        df_curr = make_pit_df(
            coc_ids=["CO-500", "CA-600", "NY-501"], totals=[1100, 52000, 30000], year=2024
        )
        boundary_gdf = make_boundary_gdf(coc_ids=["CO-500", "CA-600"])

        report = validate_pit_data(
            df_curr,
            df_previous=df_prev,
            boundary_vintage="2024",
            boundary_gdf=boundary_gdf,
            yoy_threshold=0.5,
        )

        # Should have at least one issue (NY-501 not in boundaries)
        assert len(report.issues) >= 1
