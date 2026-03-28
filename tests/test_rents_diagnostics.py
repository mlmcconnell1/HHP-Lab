"""Tests for ZORI diagnostics and reporting module.

Tests cover:
- compute_coc_diagnostics: period counts, coverage quantiles, flag logic, monthly vs yearly
- generate_text_summary: expected sections, edge cases (0 CoCs, all-flagged)
- summarize_coc_zori: DataFrame input, backwards-compat with missing max_geo_contribution
- identify_problem_cocs: correct filtering, empty result when no flags
- run_zori_diagnostics: saves CSV and parquet correctly
"""

import pandas as pd
import pytest

from coclab.rents.diagnostics import (
    compute_coc_diagnostics,
    generate_text_summary,
    identify_problem_cocs,
    run_zori_diagnostics,
    summarize_coc_zori,
)

# ---------------------------------------------------------------------------
# Module-level test DataFrames
# ---------------------------------------------------------------------------

# Monthly data: 2 CoCs, 3 months, full coverage columns
MONTHLY_DF = pd.DataFrame(
    {
        "coc_id": ["AA-500"] * 3 + ["BB-501"] * 3,
        "date": pd.to_datetime(
            ["2024-01-01", "2024-02-01", "2024-03-01"] * 2
        ),
        "zori_coc": [1200.0, 1210.0, 1220.0, 900.0, 910.0, 920.0],
        "coverage_ratio": [0.95, 0.96, 0.97, 0.60, 0.55, 0.50],
        "max_geo_contribution": [0.40, 0.42, 0.41, 0.85, 0.87, 0.90],
    }
)

# Yearly data: 2 CoCs, 2 years
YEARLY_DF = pd.DataFrame(
    {
        "coc_id": ["AA-500", "AA-500", "BB-501", "BB-501"],
        "year": [2023, 2024, 2023, 2024],
        "zori_coc": [1100.0, 1200.0, 800.0, 850.0],
        "coverage_ratio": [0.98, 0.99, 0.70, 0.65],
        "max_geo_contribution": [0.30, 0.32, 0.82, 0.88],
    }
)

# DataFrame with no max_geo_contribution column (backwards-compat)
NO_DOMINANCE_DF = pd.DataFrame(
    {
        "coc_id": ["XX-100", "XX-100"],
        "date": pd.to_datetime(["2024-01-01", "2024-02-01"]),
        "zori_coc": [1000.0, 1050.0],
        "coverage_ratio": [0.92, 0.94],
    }
)

# All-good CoCs: high coverage, low dominance
ALL_GOOD_DF = pd.DataFrame(
    {
        "coc_id": ["G1"] * 4 + ["G2"] * 4,
        "date": pd.to_datetime(
            ["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01"] * 2
        ),
        "zori_coc": [1000.0, 1010.0, 1020.0, 1030.0] * 2,
        "coverage_ratio": [0.99, 0.98, 0.97, 0.96, 1.0, 1.0, 1.0, 1.0],
        "max_geo_contribution": [0.30, 0.32, 0.31, 0.33, 0.20, 0.21, 0.22, 0.23],
    }
)


# ---------------------------------------------------------------------------
# compute_coc_diagnostics
# ---------------------------------------------------------------------------


class TestComputeCocDiagnostics:
    """Tests for compute_coc_diagnostics."""

    def test_periods_total_monthly(self):
        """Each CoC should have periods_total equal to its row count."""
        result = compute_coc_diagnostics(MONTHLY_DF)
        aa = result[result["coc_id"] == "AA-500"].iloc[0]
        bb = result[result["coc_id"] == "BB-501"].iloc[0]
        assert aa["periods_total"] == 3
        assert bb["periods_total"] == 3

    def test_periods_covered_from_zori_coc(self):
        """periods_covered counts non-null zori_coc rows."""
        # All zori_coc values are non-null in MONTHLY_DF
        result = compute_coc_diagnostics(MONTHLY_DF)
        aa = result[result["coc_id"] == "AA-500"].iloc[0]
        assert aa["periods_covered"] == 3

    def test_periods_covered_with_nulls(self):
        """periods_covered should exclude null zori_coc values."""
        df = MONTHLY_DF.copy()
        # Null out one zori_coc for BB-501
        df.loc[(df["coc_id"] == "BB-501") & (df["date"] == "2024-03-01"), "zori_coc"] = None
        result = compute_coc_diagnostics(df)
        bb = result[result["coc_id"] == "BB-501"].iloc[0]
        assert bb["periods_covered"] == 2

    def test_coverage_quantiles(self):
        """Coverage quantile values should be computed correctly."""
        result = compute_coc_diagnostics(MONTHLY_DF)
        aa = result[result["coc_id"] == "AA-500"].iloc[0]
        # AA-500 ratios: 0.95, 0.96, 0.97
        assert aa["coverage_ratio_mean"] == pytest.approx(0.96, abs=1e-6)
        assert aa["coverage_ratio_p50"] == pytest.approx(0.96, abs=1e-6)
        # p10 should be near the low end, p90 near the high end
        assert aa["coverage_ratio_p10"] < aa["coverage_ratio_p50"]
        assert aa["coverage_ratio_p90"] > aa["coverage_ratio_p50"]

    def test_flag_low_coverage_true(self):
        """CoC with mean coverage below threshold should be flagged."""
        result = compute_coc_diagnostics(MONTHLY_DF, min_coverage=0.90)
        bb = result[result["coc_id"] == "BB-501"].iloc[0]
        # BB-501 mean coverage: (0.60 + 0.55 + 0.50) / 3 = 0.55
        assert bool(bb["flag_low_coverage"]) is True

    def test_flag_low_coverage_false(self):
        """CoC with mean coverage above threshold should not be flagged."""
        result = compute_coc_diagnostics(MONTHLY_DF, min_coverage=0.90)
        aa = result[result["coc_id"] == "AA-500"].iloc[0]
        assert bool(aa["flag_low_coverage"]) is False

    def test_flag_high_dominance_true(self):
        """CoC with high max_geo_contribution_p90 should be flagged."""
        result = compute_coc_diagnostics(MONTHLY_DF, dominance_threshold=0.80)
        bb = result[result["coc_id"] == "BB-501"].iloc[0]
        # BB-501 contributions: 0.85, 0.87, 0.90 -> p90 above 0.80
        assert bool(bb["flag_high_dominance"]) is True

    def test_flag_high_dominance_false(self):
        """CoC with low max_geo_contribution_p90 should not be flagged."""
        result = compute_coc_diagnostics(MONTHLY_DF, dominance_threshold=0.80)
        aa = result[result["coc_id"] == "AA-500"].iloc[0]
        assert bool(aa["flag_high_dominance"]) is False

    def test_max_geo_contribution_p90_value(self):
        """max_geo_contribution_p90 should be the 90th percentile of the column."""
        result = compute_coc_diagnostics(MONTHLY_DF)
        bb = result[result["coc_id"] == "BB-501"].iloc[0]
        expected = pd.Series([0.85, 0.87, 0.90]).quantile(0.90)
        assert bb["max_geo_contribution_p90"] == pytest.approx(expected, abs=1e-6)

    def test_yearly_input(self):
        """Should work with year column instead of date."""
        result = compute_coc_diagnostics(YEARLY_DF)
        assert len(result) == 2
        aa = result[result["coc_id"] == "AA-500"].iloc[0]
        assert aa["periods_total"] == 2

    def test_missing_time_column_raises(self):
        """Should raise ValueError when neither date nor year is present."""
        df = pd.DataFrame(
            {
                "coc_id": ["X"],
                "coverage_ratio": [0.9],
            }
        )
        with pytest.raises(ValueError, match="Missing required time column"):
            compute_coc_diagnostics(df)

    def test_missing_required_columns_raises(self):
        """Should raise ValueError when required columns are missing."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-01"]),
                "something": [1],
            }
        )
        with pytest.raises(ValueError, match="Missing required columns"):
            compute_coc_diagnostics(df)

    def test_no_max_geo_contribution_column(self):
        """max_geo_contribution_p90 should be None when column is absent."""
        result = compute_coc_diagnostics(NO_DOMINANCE_DF)
        row = result.iloc[0]
        assert row["max_geo_contribution_p90"] is None
        assert bool(row["flag_high_dominance"]) is False

    def test_output_sorted_by_coc_id(self):
        """Result should be sorted by coc_id."""
        result = compute_coc_diagnostics(MONTHLY_DF)
        assert list(result["coc_id"]) == sorted(result["coc_id"].tolist())

    def test_periods_covered_fallback_without_zori_coc(self):
        """When zori_coc column is absent, periods_covered falls back to coverage threshold."""
        df = pd.DataFrame(
            {
                "coc_id": ["X", "X", "X"],
                "date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
                "coverage_ratio": [0.95, 0.80, 0.92],
            }
        )
        result = compute_coc_diagnostics(df, min_coverage=0.90)
        row = result.iloc[0]
        # Only 2 of 3 ratios >= 0.90
        assert row["periods_covered"] == 2


# ---------------------------------------------------------------------------
# generate_text_summary
# ---------------------------------------------------------------------------


class TestGenerateTextSummary:
    """Tests for generate_text_summary."""

    def test_contains_expected_sections(self):
        """Output should contain all major section headers."""
        diag = compute_coc_diagnostics(MONTHLY_DF)
        text = generate_text_summary(MONTHLY_DF, diag)
        assert "COC ZORI DIAGNOSTICS SUMMARY" in text
        assert "OVERVIEW" in text
        assert "COVERAGE STATISTICS" in text
        assert "MONTHS COVERED" in text
        assert "FLAGGED CoCs" in text
        assert "TOP" in text and "WORST COVERAGE" in text

    def test_monthly_shows_months_label(self):
        """Monthly data should use 'Months' label."""
        diag = compute_coc_diagnostics(MONTHLY_DF)
        text = generate_text_summary(MONTHLY_DF, diag)
        assert "Months per CoC" in text
        assert "Date range:" in text

    def test_yearly_shows_years_label(self):
        """Yearly data should use 'Years' label."""
        diag = compute_coc_diagnostics(YEARLY_DF)
        text = generate_text_summary(YEARLY_DF, diag)
        assert "Years per CoC" in text
        assert "Year range:" in text

    def test_dominance_section_present_when_data_exists(self):
        """Dominance section should appear when max_geo_contribution_p90 has values."""
        diag = compute_coc_diagnostics(MONTHLY_DF)
        text = generate_text_summary(MONTHLY_DF, diag)
        assert "DOMINANCE STATISTICS" in text

    def test_flagged_cocs_count(self):
        """Flagged CoCs section should reflect actual flag counts."""
        diag = compute_coc_diagnostics(MONTHLY_DF, min_coverage=0.90, dominance_threshold=0.80)
        text = generate_text_summary(
            MONTHLY_DF, diag, min_coverage=0.90, dominance_threshold=0.80
        )
        # BB-501 is flagged for both low coverage and high dominance
        assert "LOW_COV" in text
        assert "HIGH_DOM" in text

    def test_zero_cocs_edge_case(self):
        """Should handle empty diagnostics without crashing."""
        empty_input = pd.DataFrame(
            columns=["coc_id", "date", "zori_coc", "coverage_ratio", "max_geo_contribution"]
        )
        # Build with explicit dtypes so pandas operations (nsmallest, etc.) work
        empty_diag = pd.DataFrame(
            {
                "coc_id": pd.Series([], dtype="str"),
                "periods_total": pd.Series([], dtype="int64"),
                "periods_covered": pd.Series([], dtype="int64"),
                "coverage_ratio_mean": pd.Series([], dtype="float64"),
                "coverage_ratio_p10": pd.Series([], dtype="float64"),
                "coverage_ratio_p50": pd.Series([], dtype="float64"),
                "coverage_ratio_p90": pd.Series([], dtype="float64"),
                "max_geo_contribution_p90": pd.Series([], dtype="float64"),
                "flag_low_coverage": pd.Series([], dtype="bool"),
                "flag_high_dominance": pd.Series([], dtype="bool"),
            }
        )
        text = generate_text_summary(empty_input, empty_diag)
        assert "Total CoCs:" in text
        assert "0" in text

    def test_all_flagged_cocs(self):
        """When all CoCs are flagged, counts should match total CoCs."""
        # Build data where every CoC has low coverage and high dominance
        df = pd.DataFrame(
            {
                "coc_id": ["F1", "F1", "F2", "F2"],
                "date": pd.to_datetime(["2024-01-01", "2024-02-01"] * 2),
                "zori_coc": [500.0, 510.0, 600.0, 610.0],
                "coverage_ratio": [0.40, 0.45, 0.30, 0.35],
                "max_geo_contribution": [0.95, 0.96, 0.92, 0.93],
            }
        )
        diag = compute_coc_diagnostics(df, min_coverage=0.90, dominance_threshold=0.80)
        text = generate_text_summary(df, diag, min_coverage=0.90, dominance_threshold=0.80)
        # Both CoCs should be flagged
        assert diag["flag_low_coverage"].all()
        assert diag["flag_high_dominance"].all()
        assert "Either flag:" in text


# ---------------------------------------------------------------------------
# summarize_coc_zori
# ---------------------------------------------------------------------------


class TestSummarizeCocZori:
    """Tests for summarize_coc_zori."""

    def test_accepts_dataframe(self):
        """Should accept a DataFrame directly."""
        text, diag = summarize_coc_zori(MONTHLY_DF)
        assert isinstance(text, str)
        assert isinstance(diag, pd.DataFrame)
        assert len(diag) == 2

    def test_accepts_parquet_path(self, tmp_path):
        """Should accept a path to a parquet file."""
        pq_path = tmp_path / "coc_zori.parquet"
        MONTHLY_DF.to_parquet(pq_path, index=False)
        text, diag = summarize_coc_zori(str(pq_path))
        assert len(diag) == 2
        assert "COC ZORI DIAGNOSTICS SUMMARY" in text

    def test_file_not_found_raises(self):
        """Should raise FileNotFoundError for missing path."""
        with pytest.raises(FileNotFoundError):
            summarize_coc_zori("/nonexistent/path.parquet")

    def test_missing_time_column_raises(self):
        """Should raise ValueError when neither date nor year is present."""
        df = pd.DataFrame({"coc_id": ["X"], "coverage_ratio": [0.9], "something": [1]})
        with pytest.raises(ValueError, match="Missing required time column"):
            summarize_coc_zori(df)

    def test_missing_required_columns_raises(self):
        """Should raise ValueError when required columns are missing."""
        df = pd.DataFrame({"date": pd.to_datetime(["2024-01-01"]), "other": [1]})
        with pytest.raises(ValueError, match="Missing required columns"):
            summarize_coc_zori(df)

    def test_backwards_compat_no_max_geo_contribution(self):
        """Should work when max_geo_contribution column is missing."""
        text, diag = summarize_coc_zori(NO_DOMINANCE_DF)
        assert len(diag) == 1
        # Should not flag high dominance when column was absent
        assert bool(diag["flag_high_dominance"].iloc[0]) is False
        assert isinstance(text, str)

    def test_custom_thresholds_propagate(self):
        """Custom thresholds should affect flag results."""
        # With very low min_coverage, BB-501 should not be flagged
        _, diag = summarize_coc_zori(MONTHLY_DF, min_coverage=0.10)
        bb = diag[diag["coc_id"] == "BB-501"].iloc[0]
        assert bool(bb["flag_low_coverage"]) is False


# ---------------------------------------------------------------------------
# identify_problem_cocs
# ---------------------------------------------------------------------------


class TestIdentifyProblemCocs:
    """Tests for identify_problem_cocs."""

    def test_returns_only_flagged(self):
        """Should return only CoCs with at least one flag."""
        diag = compute_coc_diagnostics(MONTHLY_DF, min_coverage=0.90, dominance_threshold=0.80)
        problems = identify_problem_cocs(diag)
        # Only BB-501 should be flagged
        assert len(problems) == 1
        assert problems["coc_id"].iloc[0] == "BB-501"

    def test_issues_column_present(self):
        """Returned DataFrame should have an 'issues' column."""
        diag = compute_coc_diagnostics(MONTHLY_DF, min_coverage=0.90, dominance_threshold=0.80)
        problems = identify_problem_cocs(diag)
        assert "issues" in problems.columns
        issues_str = problems["issues"].iloc[0]
        assert "low_coverage" in issues_str
        assert "high_dominance" in issues_str

    def test_empty_result_when_no_flags(self):
        """Should return empty DataFrame when no CoCs are flagged."""
        diag = compute_coc_diagnostics(ALL_GOOD_DF, min_coverage=0.90, dominance_threshold=0.80)
        problems = identify_problem_cocs(diag)
        assert len(problems) == 0
        assert "coc_id" in problems.columns
        assert "issues" in problems.columns

    def test_multiple_flagged_cocs(self):
        """Should return all flagged CoCs, sorted by coverage_ratio_mean."""
        df = pd.DataFrame(
            {
                "coc_id": ["LOW1"] * 2 + ["LOW2"] * 2 + ["OK"] * 2,
                "date": pd.to_datetime(["2024-01-01", "2024-02-01"] * 3),
                "zori_coc": [500.0, 510.0, 400.0, 410.0, 1200.0, 1210.0],
                "coverage_ratio": [0.50, 0.55, 0.30, 0.35, 0.99, 0.98],
                "max_geo_contribution": [0.40, 0.42, 0.40, 0.42, 0.20, 0.21],
            }
        )
        diag = compute_coc_diagnostics(df, min_coverage=0.90)
        problems = identify_problem_cocs(diag, min_coverage=0.90)
        assert len(problems) == 2
        # Sorted by coverage_ratio_mean ascending
        assert problems["coc_id"].iloc[0] == "LOW2"
        assert problems["coc_id"].iloc[1] == "LOW1"

    def test_only_dominance_flag(self):
        """CoC with only high dominance flag should be included."""
        df = pd.DataFrame(
            {
                "coc_id": ["DOM"] * 3,
                "date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
                "zori_coc": [1000.0, 1010.0, 1020.0],
                "coverage_ratio": [0.99, 0.98, 0.97],
                "max_geo_contribution": [0.90, 0.92, 0.95],
            }
        )
        diag = compute_coc_diagnostics(df, min_coverage=0.90, dominance_threshold=0.80)
        problems = identify_problem_cocs(diag)
        assert len(problems) == 1
        assert "high_dominance" in problems["issues"].iloc[0]
        assert "low_coverage" not in problems["issues"].iloc[0]


# ---------------------------------------------------------------------------
# run_zori_diagnostics
# ---------------------------------------------------------------------------


class TestRunZoriDiagnostics:
    """Tests for run_zori_diagnostics (CLI integration)."""

    @pytest.fixture
    def input_parquet(self, tmp_path):
        """Write MONTHLY_DF to a parquet file and return its path."""
        p = tmp_path / "coc_zori_input.parquet"
        MONTHLY_DF.to_parquet(p, index=False)
        return p

    def test_file_not_found(self, tmp_path):
        """Should raise FileNotFoundError for missing input."""
        with pytest.raises(FileNotFoundError):
            run_zori_diagnostics(tmp_path / "nonexistent.parquet")

    def test_returns_text_and_dataframe(self, input_parquet):
        """Should return a (str, DataFrame) tuple."""
        text, diag = run_zori_diagnostics(input_parquet)
        assert isinstance(text, str)
        assert isinstance(diag, pd.DataFrame)
        assert len(diag) == 2

    def test_saves_csv(self, input_parquet, tmp_path):
        """Should save diagnostics as CSV when output_path ends with .csv."""
        csv_out = tmp_path / "diag.csv"
        run_zori_diagnostics(input_parquet, output_path=csv_out)
        assert csv_out.exists()
        saved = pd.read_csv(csv_out)
        assert "coc_id" in saved.columns
        assert len(saved) == 2

    def test_saves_parquet(self, input_parquet, tmp_path):
        """Should save diagnostics as Parquet when output_path ends with .parquet."""
        pq_out = tmp_path / "diag.parquet"
        run_zori_diagnostics(input_parquet, output_path=pq_out)
        assert pq_out.exists()
        saved = pd.read_parquet(pq_out)
        assert "coc_id" in saved.columns
        assert len(saved) == 2

    def test_creates_output_directory(self, input_parquet, tmp_path):
        """Should create parent directories for output_path if needed."""
        nested_out = tmp_path / "sub" / "dir" / "diag.csv"
        run_zori_diagnostics(input_parquet, output_path=nested_out)
        assert nested_out.exists()

    def test_no_output_path_skips_save(self, input_parquet, tmp_path):
        """When output_path is None, no file should be written."""
        text, diag = run_zori_diagnostics(input_parquet, output_path=None)
        # Only the input file should exist in tmp_path
        files = list(tmp_path.glob("*"))
        assert len(files) == 1  # just the input parquet

    def test_custom_thresholds(self, input_parquet, tmp_path):
        """Custom thresholds should propagate through to diagnostics."""
        csv_out = tmp_path / "diag.csv"
        _, diag = run_zori_diagnostics(
            input_parquet,
            output_path=csv_out,
            min_coverage=0.10,
            dominance_threshold=0.99,
        )
        # With very low min_coverage, no CoCs should be flagged for low coverage
        assert not diag["flag_low_coverage"].any()
        # With very high dominance threshold, no CoCs should be flagged for dominance
        assert not diag["flag_high_dominance"].any()
