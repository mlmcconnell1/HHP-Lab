"""Tests for ZORI panel integration (Agent D).

Tests cover:
- rent_to_income math calculations
- Null handling for ZORI and income
- Eligibility logic based on coverage thresholds
- Integration tests for full panel assembly with ZORI
- Regression tests ensuring baseline behavior without ZORI

These tests validate the implementation from Agents A, B, and C:
- Agent A: Panel integration (core logic)
- Agent B: CLI surface area
- Agent C: Eligibility rules and provenance
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from coclab.panel.assemble import (
    PANEL_COLUMNS,
    ZORI_COLUMNS,
    ZORI_PROVENANCE_COLUMNS,
    build_panel,
    save_panel,
)
from coclab.panel.zori_eligibility import (
    DEFAULT_ZORI_MIN_COVERAGE,
    EXCLUDED_LOW_COVERAGE,
    EXCLUDED_MISSING,
    EXCLUDED_ZERO_COVERAGE,
    ZoriProvenance,
    add_provenance_columns,
    apply_zori_eligibility,
    compute_rent_to_income,
    determine_exclusion_reason,
    get_zori_panel_columns,
    summarize_zori_eligibility,
)
from coclab.provenance import read_provenance

# =============================================================================
# Unit Tests: rent_to_income Math
# =============================================================================


class TestRentToIncomeMath:
    """Tests for rent_to_income calculation correctness."""

    def test_basic_calculation_income_60000_zori_1500(self):
        """Test: Income=60,000, ZORI=1,500 -> ratio=0.30."""
        # Formula: rent_to_income = zori / (income / 12)
        # 1500 / (60000 / 12) = 1500 / 5000 = 0.30
        df = pd.DataFrame(
            {
                "zori_coc": [1500.0],
                "median_household_income": [60000.0],
                "zori_is_eligible": [True],
            }
        )

        result = compute_rent_to_income(df)

        assert "rent_to_income" in result.columns
        assert result["rent_to_income"].iloc[0] == pytest.approx(0.30, rel=1e-6)

    def test_basic_calculation_income_48000_zori_2000(self):
        """Test: Income=48,000, ZORI=2,000 -> ratio=0.50."""
        # 2000 / (48000 / 12) = 2000 / 4000 = 0.50
        df = pd.DataFrame(
            {
                "zori_coc": [2000.0],
                "median_household_income": [48000.0],
                "zori_is_eligible": [True],
            }
        )

        result = compute_rent_to_income(df)

        assert result["rent_to_income"].iloc[0] == pytest.approx(0.50, rel=1e-6)

    def test_calculation_with_multiple_rows(self):
        """Test calculation across multiple rows with varying values."""
        df = pd.DataFrame(
            {
                "zori_coc": [1500.0, 2000.0, 1200.0, 2500.0],
                "median_household_income": [60000.0, 48000.0, 72000.0, 36000.0],
                "zori_is_eligible": [True, True, True, True],
            }
        )

        result = compute_rent_to_income(df)

        # Expected: 1500/(60000/12)=0.30, 2000/(48000/12)=0.50,
        #           1200/(72000/12)=0.20, 2500/(36000/12)=0.833...
        assert result["rent_to_income"].iloc[0] == pytest.approx(0.30, rel=1e-6)
        assert result["rent_to_income"].iloc[1] == pytest.approx(0.50, rel=1e-6)
        assert result["rent_to_income"].iloc[2] == pytest.approx(0.20, rel=1e-6)
        assert result["rent_to_income"].iloc[3] == pytest.approx(0.8333333, rel=1e-5)

    def test_high_cost_burden_ratio(self):
        """Test scenario with severe cost burden (ratio > 0.50)."""
        # HUD considers >0.50 as "severely cost-burdened"
        df = pd.DataFrame(
            {
                "zori_coc": [2000.0],
                "median_household_income": [24000.0],  # Monthly: 2000
                "zori_is_eligible": [True],
            }
        )

        result = compute_rent_to_income(df)

        # 2000 / (24000/12) = 2000 / 2000 = 1.0
        assert result["rent_to_income"].iloc[0] == pytest.approx(1.0, rel=1e-6)

    def test_moderate_cost_burden_ratio(self):
        """Test scenario with moderate cost burden (ratio ~0.35)."""
        df = pd.DataFrame(
            {
                "zori_coc": [1750.0],
                "median_household_income": [60000.0],
                "zori_is_eligible": [True],
            }
        )

        result = compute_rent_to_income(df)

        # 1750 / (60000/12) = 1750 / 5000 = 0.35
        assert result["rent_to_income"].iloc[0] == pytest.approx(0.35, rel=1e-6)


# =============================================================================
# Unit Tests: Null Handling
# =============================================================================


class TestRentToIncomeNullHandling:
    """Tests for null handling in rent_to_income calculation."""

    def test_null_zori_returns_null_ratio(self):
        """Test: Null ZORI -> null rent_to_income."""
        df = pd.DataFrame(
            {
                "zori_coc": [None],
                "median_household_income": [60000.0],
                "zori_is_eligible": [True],
            }
        )

        result = compute_rent_to_income(df)

        assert pd.isna(result["rent_to_income"].iloc[0])

    def test_null_income_returns_null_ratio(self):
        """Test: Null income -> null rent_to_income."""
        df = pd.DataFrame(
            {
                "zori_coc": [1500.0],
                "median_household_income": [None],
                "zori_is_eligible": [True],
            }
        )

        result = compute_rent_to_income(df)

        assert pd.isna(result["rent_to_income"].iloc[0])

    def test_zero_income_returns_null_ratio(self):
        """Test: Zero income -> null rent_to_income (avoid division by zero)."""
        df = pd.DataFrame(
            {
                "zori_coc": [1500.0],
                "median_household_income": [0.0],
                "zori_is_eligible": [True],
            }
        )

        result = compute_rent_to_income(df)

        assert pd.isna(result["rent_to_income"].iloc[0])

    def test_both_null_returns_null_ratio(self):
        """Test: Both ZORI and income null -> null rent_to_income."""
        df = pd.DataFrame(
            {
                "zori_coc": [None],
                "median_household_income": [None],
                "zori_is_eligible": [True],
            }
        )

        result = compute_rent_to_income(df)

        assert pd.isna(result["rent_to_income"].iloc[0])

    def test_ineligible_row_returns_null_ratio(self):
        """Test: Ineligible row -> null rent_to_income even with valid data."""
        df = pd.DataFrame(
            {
                "zori_coc": [1500.0],
                "median_household_income": [60000.0],
                "zori_is_eligible": [False],
            }
        )

        result = compute_rent_to_income(df)

        assert pd.isna(result["rent_to_income"].iloc[0])

    def test_mixed_valid_and_null_rows(self):
        """Test calculation with mix of valid and null values."""
        df = pd.DataFrame(
            {
                "zori_coc": [1500.0, None, 2000.0, 1200.0, 1800.0],
                "median_household_income": [60000.0, 50000.0, None, 0.0, 48000.0],
                "zori_is_eligible": [True, True, True, True, True],
            }
        )

        result = compute_rent_to_income(df)

        # Row 0: valid
        assert result["rent_to_income"].iloc[0] == pytest.approx(0.30, rel=1e-6)
        # Row 1: null ZORI
        assert pd.isna(result["rent_to_income"].iloc[1])
        # Row 2: null income
        assert pd.isna(result["rent_to_income"].iloc[2])
        # Row 3: zero income
        assert pd.isna(result["rent_to_income"].iloc[3])
        # Row 4: valid
        assert result["rent_to_income"].iloc[4] == pytest.approx(0.45, rel=1e-6)

    def test_missing_zori_column_returns_all_null(self):
        """Test: Missing ZORI column in DataFrame -> all null rent_to_income."""
        df = pd.DataFrame(
            {
                "median_household_income": [60000.0, 48000.0],
            }
        )

        result = compute_rent_to_income(df)

        assert "rent_to_income" in result.columns
        assert result["rent_to_income"].isna().all()

    def test_missing_income_column_returns_all_null(self):
        """Test: Missing income column in DataFrame -> all null rent_to_income."""
        df = pd.DataFrame(
            {
                "zori_coc": [1500.0, 2000.0],
            }
        )

        result = compute_rent_to_income(df)

        assert "rent_to_income" in result.columns
        assert result["rent_to_income"].isna().all()


# =============================================================================
# Unit Tests: Eligibility Logic
# =============================================================================


class TestDetermineExclusionReason:
    """Tests for determine_exclusion_reason function."""

    def test_eligible_when_coverage_above_threshold(self):
        """Test: coverage >= 0.90 -> no exclusion reason (eligible)."""
        reason = determine_exclusion_reason(
            coverage_ratio=0.95,
            zori_value=1500.0,
            min_coverage=0.90,
        )
        assert reason is None  # None means eligible

    def test_eligible_at_exact_threshold(self):
        """Test: coverage == 0.90 -> eligible (threshold is inclusive)."""
        reason = determine_exclusion_reason(
            coverage_ratio=0.90,
            zori_value=1500.0,
            min_coverage=0.90,
        )
        assert reason is None

    def test_ineligible_low_coverage(self):
        """Test: coverage < 0.90 -> ineligible, reason='low_coverage'."""
        reason = determine_exclusion_reason(
            coverage_ratio=0.85,
            zori_value=1500.0,
            min_coverage=0.90,
        )
        assert reason == EXCLUDED_LOW_COVERAGE

    def test_ineligible_zero_coverage(self):
        """Test: coverage == 0 -> ineligible, reason='zero_coverage'."""
        reason = determine_exclusion_reason(
            coverage_ratio=0.0,
            zori_value=1500.0,
            min_coverage=0.90,
        )
        assert reason == EXCLUDED_ZERO_COVERAGE

    def test_ineligible_missing_zori(self):
        """Test: null ZORI value -> ineligible, reason='missing'."""
        reason = determine_exclusion_reason(
            coverage_ratio=0.95,
            zori_value=None,
            min_coverage=0.90,
        )
        assert reason == EXCLUDED_MISSING

    def test_ineligible_missing_coverage(self):
        """Test: null coverage ratio -> ineligible, reason='missing'."""
        reason = determine_exclusion_reason(
            coverage_ratio=None,
            zori_value=1500.0,
            min_coverage=0.90,
        )
        assert reason == EXCLUDED_MISSING

    def test_custom_threshold(self):
        """Test eligibility with custom coverage threshold."""
        # At 0.85 threshold, 0.87 should be eligible
        reason = determine_exclusion_reason(
            coverage_ratio=0.87,
            zori_value=1500.0,
            min_coverage=0.85,
        )
        assert reason is None  # Eligible

        # At 0.95 threshold, 0.87 should be ineligible
        reason = determine_exclusion_reason(
            coverage_ratio=0.87,
            zori_value=1500.0,
            min_coverage=0.95,
        )
        assert reason == EXCLUDED_LOW_COVERAGE


class TestApplyZoriEligibility:
    """Tests for apply_zori_eligibility function."""

    def test_eligible_rows_keep_zori_value(self):
        """Test that eligible rows retain their ZORI values."""
        df = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CA-600"],
                "zori_coc": [1500.0, 2000.0],
                "zori_coverage_ratio": [0.95, 0.92],
            }
        )

        result = apply_zori_eligibility(df, min_coverage=0.90)

        assert result["zori_is_eligible"].all()
        assert result["zori_coc"].iloc[0] == 1500.0
        assert result["zori_coc"].iloc[1] == 2000.0
        assert result["zori_excluded_reason"].isna().all()

    def test_ineligible_rows_get_null_zori(self):
        """Test that ineligible rows have ZORI values set to null."""
        df = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CA-600"],
                "zori_coc": [1500.0, 2000.0],
                "zori_coverage_ratio": [0.95, 0.80],  # CA-600 is below threshold
            }
        )

        result = apply_zori_eligibility(df, min_coverage=0.90)

        # CO-500 should be eligible
        assert result["zori_is_eligible"].iloc[0]
        assert result["zori_coc"].iloc[0] == 1500.0
        assert pd.isna(result["zori_excluded_reason"].iloc[0])

        # CA-600 should be ineligible with null ZORI
        assert not result["zori_is_eligible"].iloc[1]
        assert pd.isna(result["zori_coc"].iloc[1])
        assert result["zori_excluded_reason"].iloc[1] == EXCLUDED_LOW_COVERAGE

    def test_zero_coverage_gets_correct_reason(self):
        """Test that zero coverage is flagged with 'zero_coverage' reason."""
        df = pd.DataFrame(
            {
                "coc_id": ["CO-500"],
                "zori_coc": [1500.0],
                "zori_coverage_ratio": [0.0],
            }
        )

        result = apply_zori_eligibility(df, min_coverage=0.90)

        assert not result["zori_is_eligible"].iloc[0]
        assert pd.isna(result["zori_coc"].iloc[0])
        assert result["zori_excluded_reason"].iloc[0] == EXCLUDED_ZERO_COVERAGE

    def test_missing_columns_handled_gracefully(self):
        """Test that missing ZORI columns result in all ineligible."""
        df = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CA-600"],
                # Missing zori_coc and zori_coverage_ratio columns
            }
        )

        result = apply_zori_eligibility(df, min_coverage=0.90)

        assert "zori_is_eligible" in result.columns
        assert not result["zori_is_eligible"].all()
        assert result["zori_excluded_reason"].eq(EXCLUDED_MISSING).all()

    def test_adds_required_columns(self):
        """Test that eligibility check adds required columns."""
        df = pd.DataFrame(
            {
                "coc_id": ["CO-500"],
                "zori_coc": [1500.0],
                "zori_coverage_ratio": [0.95],
            }
        )

        result = apply_zori_eligibility(df, min_coverage=0.90)

        assert "zori_is_eligible" in result.columns
        assert "zori_excluded_reason" in result.columns

    def test_dominance_warning_does_not_exclude(self):
        """Test that high dominance generates warning but doesn't exclude."""
        df = pd.DataFrame(
            {
                "coc_id": ["CO-500"],
                "zori_coc": [1500.0],
                "zori_coverage_ratio": [0.95],
                "zori_max_geo_contribution": [0.85],  # High dominance
            }
        )

        result = apply_zori_eligibility(
            df,
            min_coverage=0.90,
            dominance_col="zori_max_geo_contribution",
            dominance_threshold=0.80,
        )

        # Should still be eligible despite high dominance
        assert result["zori_is_eligible"].iloc[0]
        assert result["zori_coc"].iloc[0] == 1500.0


# =============================================================================
# Unit Tests: Provenance
# =============================================================================


class TestZoriProvenance:
    """Tests for ZoriProvenance dataclass and provenance handling."""

    def test_provenance_to_dict(self):
        """Test conversion of ZoriProvenance to dictionary."""
        prov = ZoriProvenance(
            rent_alignment="pit_january",
            zori_min_coverage=0.90,
        )

        d = prov.to_dict()

        assert d["rent_metric"] == "ZORI"
        assert d["rent_alignment"] == "pit_january"
        assert d["zori_min_coverage"] == 0.90
        assert d["zori_source"] == "Zillow Economic Research"

    def test_provenance_from_dict(self):
        """Test reconstruction of ZoriProvenance from dictionary."""
        d = {
            "rent_metric": "ZORI",
            "rent_alignment": "calendar_mean",
            "zori_min_coverage": 0.85,
        }

        prov = ZoriProvenance.from_dict(d)

        assert prov.rent_metric == "ZORI"
        assert prov.rent_alignment == "calendar_mean"
        assert prov.zori_min_coverage == 0.85

    def test_add_provenance_columns(self):
        """Test adding provenance columns to DataFrame."""
        df = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CA-600"],
                "zori_coc": [1500.0, 2000.0],
            }
        )
        prov = ZoriProvenance(
            rent_alignment="pit_january",
            zori_min_coverage=0.90,
        )

        result = add_provenance_columns(df, prov)

        assert "rent_metric" in result.columns
        assert "rent_alignment" in result.columns
        assert "zori_min_coverage" in result.columns
        assert result["rent_metric"].iloc[0] == "ZORI"
        assert result["rent_alignment"].iloc[0] == "pit_january"
        assert result["zori_min_coverage"].iloc[0] == 0.90


class TestSummarizeZoriEligibility:
    """Tests for summarize_zori_eligibility function."""

    def test_summary_with_eligible_rows(self):
        """Test summary generation with eligible rows."""
        df = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CA-600", "NY-501"],
                "zori_is_eligible": [True, True, False],
                "zori_excluded_reason": [None, None, EXCLUDED_LOW_COVERAGE],
                "rent_to_income": [0.30, 0.40, None],
            }
        )

        summary = summarize_zori_eligibility(df)

        assert summary["total_rows"] == 3
        assert summary["zori_integrated"] is True
        assert summary["zori_eligible_count"] == 2
        assert summary["zori_ineligible_count"] == 1
        assert summary["exclusion_reasons"] == {EXCLUDED_LOW_COVERAGE: 1}
        assert summary["rent_to_income_count"] == 2
        assert summary["rent_to_income_mean"] == pytest.approx(0.35, rel=1e-6)

    def test_summary_without_zori_columns(self):
        """Test summary when ZORI columns are not present."""
        df = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CA-600"],
                "pit_total": [1000, 2000],
            }
        )

        summary = summarize_zori_eligibility(df)

        assert summary["total_rows"] == 2
        assert summary["zori_integrated"] is False


# =============================================================================
# Integration Tests: Panel Assembly with ZORI
# =============================================================================


class TestPanelAssemblyWithZori:
    """Integration tests for panel assembly with ZORI enabled."""

    @pytest.fixture
    def panel_data_dirs(self, tmp_path):
        """Create temporary directories with sample panel data."""
        pit_dir = tmp_path / "pit"
        pit_dir.mkdir()
        measures_dir = tmp_path / "measures"
        measures_dir.mkdir()
        rents_dir = tmp_path / "rents"
        rents_dir.mkdir()
        panel_dir = tmp_path / "panel"

        # Create PIT data for 2023 and 2024
        for year in [2023, 2024]:
            df_pit = pd.DataFrame(
                {
                    "coc_id": ["CO-500", "CA-600", "NY-501"],
                    "pit_total": [1200 + (year - 2023) * 100, 45000, 75000],
                    "pit_sheltered": [800, 30000, 55000],
                    "pit_unsheltered": [400 + (year - 2023) * 100, 15000, 20000],
                    "pit_year": [year, year, year],
                }
            )
            df_pit.to_parquet(pit_dir / f"pit_counts__{year}.parquet", index=False)

        # Create ACS measures for boundary/acs combinations
        for acs_year in [2022, 2023]:
            boundary_year = acs_year + 1
            df_acs = pd.DataFrame(
                {
                    "coc_id": ["CO-500", "CA-600", "NY-501"],
                    "total_population": [500000, 10000000, 8000000],
                    "adult_population": [400000, 8000000, 6400000],
                    "population_below_poverty": [50000, 1500000, 1200000],
                    "median_household_income": [60000, 72000, 84000],  # For rent_to_income calc
                    "median_gross_rent": [1200, 1800, 2200],
                    "coverage_ratio": [0.95, 0.98, 0.99],
                    "weighting_method": ["population", "population", "population"],
                }
            )
            df_acs.to_parquet(
                measures_dir / f"coc_measures__{boundary_year}__{acs_year}.parquet",
                index=False,
            )

        # Create yearly ZORI data
        df_zori = pd.DataFrame(
            {
                "coc_id": ["CO-500", "CO-500", "CA-600", "CA-600", "NY-501", "NY-501"],
                "year": [2023, 2024, 2023, 2024, 2023, 2024],
                "zori_coc": [1500.0, 1550.0, 2400.0, 2500.0, 2800.0, 2900.0],
                "coverage_ratio": [0.95, 0.94, 0.98, 0.97, 0.85, 0.80],  # NY-501 low coverage
                "max_geo_contribution": [0.40, 0.42, 0.35, 0.36, 0.50, 0.52],
                "method": ["pit_january"] * 6,
                "geo_count": [5, 5, 10, 10, 3, 3],
            }
        )
        df_zori.to_parquet(
            rents_dir / "coc_zori_yearly__test.parquet",
            index=False,
        )

        return {
            "pit_dir": pit_dir,
            "measures_dir": measures_dir,
            "rents_dir": rents_dir,
            "panel_dir": panel_dir,
        }

    def test_build_panel_with_zori_includes_expected_columns(self, panel_data_dirs):
        """Test that panel with ZORI includes all expected ZORI columns."""
        zori_path = panel_data_dirs["rents_dir"] / "coc_zori_yearly__test.parquet"

        panel_df = build_panel(
            2023,
            2024,
            pit_dir=panel_data_dirs["pit_dir"],
            measures_dir=panel_data_dirs["measures_dir"],
            include_zori=True,
            zori_yearly_path=zori_path,
            rents_dir=panel_data_dirs["rents_dir"],
            zori_min_coverage=0.90,
        )

        # Check ZORI columns are present
        for col in ZORI_COLUMNS:
            assert col in panel_df.columns, f"Missing ZORI column: {col}"

        # Check provenance columns are present
        for col in ZORI_PROVENANCE_COLUMNS:
            assert col in panel_df.columns, f"Missing provenance column: {col}"

    def test_build_panel_with_zori_computes_rent_to_income(self, panel_data_dirs):
        """Test that rent_to_income is computed correctly."""
        zori_path = panel_data_dirs["rents_dir"] / "coc_zori_yearly__test.parquet"

        panel_df = build_panel(
            2023,
            2024,
            pit_dir=panel_data_dirs["pit_dir"],
            measures_dir=panel_data_dirs["measures_dir"],
            include_zori=True,
            zori_yearly_path=zori_path,
            rents_dir=panel_data_dirs["rents_dir"],
            zori_min_coverage=0.90,
        )

        # Check rent_to_income for CO-500 in 2023
        # ZORI=1500, Income=60000 -> 1500/(60000/12) = 0.30
        co500_2023 = panel_df[(panel_df["coc_id"] == "CO-500") & (panel_df["year"] == 2023)].iloc[0]

        assert co500_2023["rent_to_income"] == pytest.approx(0.30, rel=1e-6)

    def test_build_panel_with_zori_eligibility_counts(self, panel_data_dirs):
        """Test that eligible/ineligible counts match expectations."""
        zori_path = panel_data_dirs["rents_dir"] / "coc_zori_yearly__test.parquet"

        panel_df = build_panel(
            2023,
            2024,
            pit_dir=panel_data_dirs["pit_dir"],
            measures_dir=panel_data_dirs["measures_dir"],
            include_zori=True,
            zori_yearly_path=zori_path,
            rents_dir=panel_data_dirs["rents_dir"],
            zori_min_coverage=0.90,
        )

        # NY-501 has coverage 0.85 and 0.80 (both < 0.90 threshold)
        # So NY-501 should be ineligible for both years
        # CO-500 and CA-600 should be eligible (coverage >= 0.90)
        eligible_count = panel_df["zori_is_eligible"].sum()
        ineligible_count = (~panel_df["zori_is_eligible"]).sum()

        assert eligible_count == 4  # CO-500 (2), CA-600 (2)
        assert ineligible_count == 2  # NY-501 (2)

    def test_build_panel_with_zori_ineligible_rows_have_null_ratio(self, panel_data_dirs):
        """Test that ineligible rows have null rent_to_income."""
        zori_path = panel_data_dirs["rents_dir"] / "coc_zori_yearly__test.parquet"

        panel_df = build_panel(
            2023,
            2024,
            pit_dir=panel_data_dirs["pit_dir"],
            measures_dir=panel_data_dirs["measures_dir"],
            include_zori=True,
            zori_yearly_path=zori_path,
            rents_dir=panel_data_dirs["rents_dir"],
            zori_min_coverage=0.90,
        )

        # NY-501 should have null rent_to_income
        ny501_rows = panel_df[panel_df["coc_id"] == "NY-501"]

        assert ny501_rows["rent_to_income"].isna().all()
        assert ny501_rows["zori_coc"].isna().all()
        assert (ny501_rows["zori_excluded_reason"] == EXCLUDED_LOW_COVERAGE).all()

    def test_build_panel_with_zori_provenance_columns(self, panel_data_dirs):
        """Test that provenance columns have correct values."""
        zori_path = panel_data_dirs["rents_dir"] / "coc_zori_yearly__test.parquet"

        panel_df = build_panel(
            2023,
            2024,
            pit_dir=panel_data_dirs["pit_dir"],
            measures_dir=panel_data_dirs["measures_dir"],
            include_zori=True,
            zori_yearly_path=zori_path,
            rents_dir=panel_data_dirs["rents_dir"],
            zori_min_coverage=0.90,
        )

        # Check provenance values
        assert (panel_df["rent_metric"] == "ZORI").all()
        assert (panel_df["rent_alignment"] == "pit_january").all()
        assert (panel_df["zori_min_coverage"] == 0.90).all()

    def test_build_panel_no_zori_available_raises_error(self, panel_data_dirs):
        """Test that building with ZORI enabled but no data raises error."""
        with pytest.raises(ValueError, match="ZORI integration requested"):
            build_panel(
                2023,
                2024,
                pit_dir=panel_data_dirs["pit_dir"],
                measures_dir=panel_data_dirs["measures_dir"],
                include_zori=True,
                zori_yearly_path=Path("/nonexistent/path.parquet"),
                rents_dir=panel_data_dirs["rents_dir"],
            )

    def test_build_panel_with_custom_coverage_threshold(self, panel_data_dirs):
        """Test that custom coverage threshold affects eligibility."""
        zori_path = panel_data_dirs["rents_dir"] / "coc_zori_yearly__test.parquet"

        # Use lower threshold (0.80) - NY-501 2024 (0.80) should still be excluded
        # NY-501 2023 (0.85) should now be eligible
        panel_df = build_panel(
            2023,
            2024,
            pit_dir=panel_data_dirs["pit_dir"],
            measures_dir=panel_data_dirs["measures_dir"],
            include_zori=True,
            zori_yearly_path=zori_path,
            rents_dir=panel_data_dirs["rents_dir"],
            zori_min_coverage=0.85,
        )

        # NY-501 2023 (coverage=0.85) should now be eligible
        ny501_2023 = panel_df[(panel_df["coc_id"] == "NY-501") & (panel_df["year"] == 2023)].iloc[0]

        assert ny501_2023["zori_is_eligible"]
        assert ny501_2023["rent_to_income"] is not None

        # NY-501 2024 (coverage=0.80) should still be ineligible
        ny501_2024 = panel_df[(panel_df["coc_id"] == "NY-501") & (panel_df["year"] == 2024)].iloc[0]

        assert not ny501_2024["zori_is_eligible"]

    def test_save_panel_with_zori_includes_provenance(self, panel_data_dirs):
        """Test that saved panel includes ZORI provenance metadata."""
        zori_path = panel_data_dirs["rents_dir"] / "coc_zori_yearly__test.parquet"

        panel_df = build_panel(
            2023,
            2024,
            pit_dir=panel_data_dirs["pit_dir"],
            measures_dir=panel_data_dirs["measures_dir"],
            include_zori=True,
            zori_yearly_path=zori_path,
            rents_dir=panel_data_dirs["rents_dir"],
            zori_min_coverage=0.90,
        )

        zori_prov = ZoriProvenance(rent_alignment="pit_january", zori_min_coverage=0.90)
        output_path = save_panel(
            panel_df,
            2023,
            2024,
            output_dir=panel_data_dirs["panel_dir"],
            zori_provenance=zori_prov,
        )

        # Read provenance from saved file
        provenance = read_provenance(output_path)

        assert provenance is not None
        assert "zori" in provenance.extra
        assert provenance.extra["zori"]["rent_metric"] == "ZORI"
        assert provenance.extra["zori"]["zori_min_coverage"] == 0.90


# =============================================================================
# Regression Tests: Panel Assembly without ZORI
# =============================================================================


class TestPanelAssemblyWithoutZori:
    """Regression tests ensuring baseline behavior without ZORI."""

    @pytest.fixture
    def baseline_data_dirs(self, tmp_path):
        """Create temporary directories with baseline panel data."""
        pit_dir = tmp_path / "pit"
        pit_dir.mkdir()
        measures_dir = tmp_path / "measures"
        measures_dir.mkdir()
        panel_dir = tmp_path / "panel"

        # Create PIT data
        for year in [2023, 2024]:
            df_pit = pd.DataFrame(
                {
                    "coc_id": ["CO-500", "CA-600", "NY-501", "TX-500"],
                    "pit_total": [1200 + (year - 2023) * 100, 45000, 75000, 3000],
                    "pit_sheltered": [800, 30000, 55000, 2000],
                    "pit_unsheltered": [400, 15000, 20000, 1000],
                    "pit_year": [year] * 4,
                }
            )
            df_pit.to_parquet(pit_dir / f"pit_counts__{year}.parquet", index=False)

        # Create ACS measures
        for acs_year in [2022, 2023]:
            boundary_year = acs_year + 1
            df_acs = pd.DataFrame(
                {
                    "coc_id": ["CO-500", "CA-600", "NY-501", "TX-500"],
                    "total_population": [500000, 10000000, 8000000, 2000000],
                    "adult_population": [400000, 8000000, 6400000, 1600000],
                    "population_below_poverty": [50000, 1500000, 1200000, 300000],
                    "median_household_income": [60000, 72000, 84000, 55000],
                    "median_gross_rent": [1200, 1800, 2200, 1000],
                    "coverage_ratio": [0.95, 0.98, 0.99, 0.90],
                    "weighting_method": ["population"] * 4,
                }
            )
            df_acs.to_parquet(
                measures_dir / f"coc_measures__{boundary_year}__{acs_year}.parquet",
                index=False,
            )

        return {
            "pit_dir": pit_dir,
            "measures_dir": measures_dir,
            "panel_dir": panel_dir,
        }

    def test_baseline_panel_has_only_canonical_columns(self, baseline_data_dirs):
        """Test that panel without ZORI has only canonical columns."""
        panel_df = build_panel(
            2023,
            2024,
            pit_dir=baseline_data_dirs["pit_dir"],
            measures_dir=baseline_data_dirs["measures_dir"],
            include_zori=False,
        )

        # Should have exactly the canonical columns
        assert set(panel_df.columns) == set(PANEL_COLUMNS)

        # Should NOT have ZORI columns
        for col in ZORI_COLUMNS:
            assert col not in panel_df.columns

        # Should NOT have ZORI provenance columns
        for col in ZORI_PROVENANCE_COLUMNS:
            assert col not in panel_df.columns

    def test_baseline_panel_row_count_unchanged(self, baseline_data_dirs):
        """Test that panel row count is as expected without ZORI."""
        panel_df = build_panel(
            2023,
            2024,
            pit_dir=baseline_data_dirs["pit_dir"],
            measures_dir=baseline_data_dirs["measures_dir"],
            include_zori=False,
        )

        # 4 CoCs x 2 years = 8 rows
        assert len(panel_df) == 8
        assert panel_df["coc_id"].nunique() == 4
        assert panel_df["year"].nunique() == 2

    def test_baseline_panel_schema_unchanged(self, baseline_data_dirs):
        """Test that panel schema matches baseline (no ZORI columns)."""
        panel_df = build_panel(
            2023,
            2024,
            pit_dir=baseline_data_dirs["pit_dir"],
            measures_dir=baseline_data_dirs["measures_dir"],
            include_zori=False,
        )

        # Check all canonical columns are present
        for col in PANEL_COLUMNS:
            assert col in panel_df.columns, f"Missing canonical column: {col}"

        # Check column count matches expected
        assert len(panel_df.columns) == len(PANEL_COLUMNS)

    def test_baseline_panel_includes_required_metadata(self, baseline_data_dirs):
        """Test that baseline panel has correct metadata columns."""
        panel_df = build_panel(
            2023,
            2024,
            pit_dir=baseline_data_dirs["pit_dir"],
            measures_dir=baseline_data_dirs["measures_dir"],
            include_zori=False,
        )

        # Check source column
        assert (panel_df["source"] == "coclab_panel").all()

        # Check vintage columns are populated
        assert panel_df["boundary_vintage_used"].notna().all()
        assert panel_df["acs_vintage_used"].notna().all()
        assert panel_df["weighting_method"].notna().all()

    def test_baseline_panel_boundary_changed_detection(self, baseline_data_dirs):
        """Test that boundary_changed column is computed correctly."""
        panel_df = build_panel(
            2023,
            2024,
            pit_dir=baseline_data_dirs["pit_dir"],
            measures_dir=baseline_data_dirs["measures_dir"],
            include_zori=False,
        )

        # boundary_changed should be a boolean column
        assert panel_df["boundary_changed"].dtype == bool

        # First year for each CoC should be False
        first_year_rows = panel_df[panel_df["year"] == 2023]
        assert not first_year_rows["boundary_changed"].all()

    def test_baseline_panel_save_and_reload(self, baseline_data_dirs):
        """Test that baseline panel can be saved and reloaded correctly."""
        panel_df = build_panel(
            2023,
            2024,
            pit_dir=baseline_data_dirs["pit_dir"],
            measures_dir=baseline_data_dirs["measures_dir"],
            include_zori=False,
        )

        output_path = save_panel(
            panel_df,
            2023,
            2024,
            output_dir=baseline_data_dirs["panel_dir"],
        )

        # Reload and verify
        reloaded = pd.read_parquet(output_path)

        assert len(reloaded) == len(panel_df)
        assert set(reloaded.columns) == set(panel_df.columns)

        # Verify provenance
        provenance = read_provenance(output_path)
        assert provenance is not None
        assert provenance.extra.get("dataset_type") == "coc_panel"
        assert provenance.extra.get("start_year") == 2023
        assert provenance.extra.get("end_year") == 2024


# =============================================================================
# Helper Function Tests
# =============================================================================


class TestHelperFunctions:
    """Tests for helper functions in zori_eligibility module."""

    def test_get_zori_panel_columns(self):
        """Test that get_zori_panel_columns returns expected columns."""
        columns = get_zori_panel_columns()

        expected = [
            "zori_coc",
            "zori_coverage_ratio",
            "zori_is_eligible",
            "zori_excluded_reason",
            "rent_to_income",
            "rent_metric",
            "rent_alignment",
            "zori_min_coverage",
        ]

        assert columns == expected

    def test_default_zori_min_coverage_value(self):
        """Test that default coverage threshold is 0.90."""
        assert DEFAULT_ZORI_MIN_COVERAGE == 0.90
