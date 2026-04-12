"""Tests for ZORI eligibility with metro-type DataFrames.

The existing test_zori_panel_integration.py covers the CoC path (coc_id-bearing
DataFrames).  This module adds dedicated coverage for the metro path where the
geography column is metro_id instead of coc_id.

Covers:
1. apply_zori_eligibility happy path with metro_id-bearing DataFrame
2. apply_zori_eligibility high-dominance warning path with metro_id
3. compute_rent_to_income with metro-type DataFrames
4. Schema parity: CoC and metro paths produce identical output columns
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest

from coclab.panel.zori_eligibility import (
    EXCLUDED_LOW_COVERAGE,
    EXCLUDED_MISSING,
    EXCLUDED_ZERO_COVERAGE,
    apply_zori_eligibility,
    compute_rent_to_income,
    get_zori_panel_columns,
)

# ---------------------------------------------------------------------------
# Helpers: declarative DataFrame builders
# ---------------------------------------------------------------------------


def _make_metro_df(
    metro_ids: list[str],
    zori_values: list[float | None],
    coverage_ratios: list[float | None],
    incomes: list[float | None] | None = None,
    dominance_values: list[float | None] | None = None,
) -> pd.DataFrame:
    """Build a metro-type panel DataFrame for testing."""
    data: dict = {
        "metro_id": metro_ids,
        "zori_coc": zori_values,
        "zori_coverage_ratio": coverage_ratios,
    }
    if incomes is not None:
        data["median_household_income"] = incomes
    if dominance_values is not None:
        data["zori_max_geo_contribution"] = dominance_values
    return pd.DataFrame(data)


def _make_coc_df(
    coc_ids: list[str],
    zori_values: list[float | None],
    coverage_ratios: list[float | None],
    incomes: list[float | None] | None = None,
    dominance_values: list[float | None] | None = None,
) -> pd.DataFrame:
    """Build a CoC-type panel DataFrame for testing."""
    data: dict = {
        "coc_id": coc_ids,
        "zori_coc": zori_values,
        "zori_coverage_ratio": coverage_ratios,
    }
    if incomes is not None:
        data["median_household_income"] = incomes
    if dominance_values is not None:
        data["zori_max_geo_contribution"] = dominance_values
    return pd.DataFrame(data)


# =============================================================================
# 1. apply_zori_eligibility — metro happy path
# =============================================================================


class TestApplyZoriEligibilityMetroHappyPath:
    """apply_zori_eligibility with a metro_id-bearing DataFrame (no coc_id)."""

    def test_all_eligible_metro_rows_keep_values(self):
        """All rows above coverage threshold retain their ZORI values."""
        df = _make_metro_df(
            metro_ids=["M001", "M002", "M003"],
            zori_values=[1500.0, 1800.0, 2200.0],
            coverage_ratios=[0.95, 0.92, 1.0],
        )

        result = apply_zori_eligibility(df, min_coverage=0.90)

        assert "zori_is_eligible" in result.columns
        assert "zori_excluded_reason" in result.columns
        assert result["zori_is_eligible"].all()
        assert result["zori_excluded_reason"].isna().all()
        # Original values preserved
        assert list(result["zori_coc"]) == [1500.0, 1800.0, 2200.0]
        # metro_id column still present and unchanged
        assert list(result["metro_id"]) == ["M001", "M002", "M003"]

    def test_mixed_eligible_and_ineligible_metro(self):
        """Rows below coverage threshold are marked ineligible and nulled."""
        df = _make_metro_df(
            metro_ids=["M001", "M002", "M003", "M004"],
            zori_values=[1500.0, 1800.0, 2200.0, 900.0],
            coverage_ratios=[0.95, 0.80, 0.0, 0.50],
        )

        result = apply_zori_eligibility(df, min_coverage=0.90)

        # M001 eligible
        assert result["zori_is_eligible"].iloc[0]
        assert result["zori_coc"].iloc[0] == 1500.0
        assert pd.isna(result["zori_excluded_reason"].iloc[0])

        # M002 low coverage
        assert not result["zori_is_eligible"].iloc[1]
        assert pd.isna(result["zori_coc"].iloc[1])
        assert result["zori_excluded_reason"].iloc[1] == EXCLUDED_LOW_COVERAGE

        # M003 zero coverage
        assert not result["zori_is_eligible"].iloc[2]
        assert pd.isna(result["zori_coc"].iloc[2])
        assert result["zori_excluded_reason"].iloc[2] == EXCLUDED_ZERO_COVERAGE

        # M004 low coverage
        assert not result["zori_is_eligible"].iloc[3]
        assert pd.isna(result["zori_coc"].iloc[3])
        assert result["zori_excluded_reason"].iloc[3] == EXCLUDED_LOW_COVERAGE

    def test_missing_zori_value_excluded_as_missing(self):
        """A metro row with null ZORI gets reason='missing'."""
        df = _make_metro_df(
            metro_ids=["M001"],
            zori_values=[None],
            coverage_ratios=[0.95],
        )

        result = apply_zori_eligibility(df, min_coverage=0.90)

        assert not result["zori_is_eligible"].iloc[0]
        assert result["zori_excluded_reason"].iloc[0] == EXCLUDED_MISSING

    def test_missing_coverage_ratio_excluded_as_missing(self):
        """A metro row with null coverage gets reason='missing'."""
        df = _make_metro_df(
            metro_ids=["M001"],
            zori_values=[1500.0],
            coverage_ratios=[None],
        )

        result = apply_zori_eligibility(df, min_coverage=0.90)

        assert not result["zori_is_eligible"].iloc[0]
        assert result["zori_excluded_reason"].iloc[0] == EXCLUDED_MISSING

    def test_exact_threshold_is_eligible(self):
        """Coverage exactly equal to threshold is considered eligible."""
        df = _make_metro_df(
            metro_ids=["M001"],
            zori_values=[1500.0],
            coverage_ratios=[0.90],
        )

        result = apply_zori_eligibility(df, min_coverage=0.90)

        assert result["zori_is_eligible"].iloc[0]

    def test_does_not_mutate_input(self):
        """apply_zori_eligibility returns a copy, not a mutation of input."""
        df = _make_metro_df(
            metro_ids=["M001"],
            zori_values=[1500.0],
            coverage_ratios=[0.50],
        )
        original_zori = df["zori_coc"].iloc[0]

        _ = apply_zori_eligibility(df, min_coverage=0.90)

        # Original DataFrame unchanged
        assert df["zori_coc"].iloc[0] == original_zori
        assert "zori_is_eligible" not in df.columns


# =============================================================================
# 2. apply_zori_eligibility — metro high-dominance warning path
# =============================================================================


class TestApplyZoriEligibilityMetroDominance:
    """High-dominance warnings with metro_id DataFrames."""

    def test_high_dominance_warns_but_stays_eligible(self, caplog):
        """Metro rows with high dominance remain eligible and emit a warning."""
        df = _make_metro_df(
            metro_ids=["M001", "M002"],
            zori_values=[1500.0, 1800.0],
            coverage_ratios=[0.95, 0.95],
            dominance_values=[0.85, 0.90],
        )

        with caplog.at_level(logging.WARNING):
            result = apply_zori_eligibility(
                df,
                min_coverage=0.90,
                dominance_col="zori_max_geo_contribution",
                dominance_threshold=0.80,
            )

        # Both remain eligible
        assert result["zori_is_eligible"].all()
        assert result["zori_coc"].iloc[0] == 1500.0
        assert result["zori_coc"].iloc[1] == 1800.0

        # Warning was emitted
        warning_messages = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert any("high" in msg.lower() and "dominance" in msg.lower() for msg in warning_messages)

    def test_high_dominance_warning_reports_metro_geographies(self, caplog):
        """The dominance warning message references affected geographies when
        metro_id is the geo column."""
        df = _make_metro_df(
            metro_ids=["M001", "M002", "M003"],
            zori_values=[1500.0, 1800.0, 2200.0],
            coverage_ratios=[0.95, 0.95, 0.95],
            dominance_values=[0.85, 0.60, 0.95],
        )

        with caplog.at_level(logging.WARNING):
            apply_zori_eligibility(
                df,
                min_coverage=0.90,
                dominance_col="zori_max_geo_contribution",
                dominance_threshold=0.80,
            )

        warning_messages = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        # Should mention affected geographies count (2 metros exceed threshold)
        assert any("Affected geographies: 2" in msg for msg in warning_messages)

    def test_no_dominance_warning_when_below_threshold(self, caplog):
        """No warning emitted when all dominance values are below threshold."""
        df = _make_metro_df(
            metro_ids=["M001", "M002"],
            zori_values=[1500.0, 1800.0],
            coverage_ratios=[0.95, 0.95],
            dominance_values=[0.50, 0.70],
        )

        with caplog.at_level(logging.WARNING):
            result = apply_zori_eligibility(
                df,
                min_coverage=0.90,
                dominance_col="zori_max_geo_contribution",
                dominance_threshold=0.80,
            )

        assert result["zori_is_eligible"].all()
        warning_messages = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert not any("dominance" in msg.lower() for msg in warning_messages)

    def test_dominance_warning_skipped_for_ineligible_rows(self, caplog):
        """Ineligible rows with high dominance do not trigger the warning."""
        df = _make_metro_df(
            metro_ids=["M001"],
            zori_values=[1500.0],
            coverage_ratios=[0.50],  # Below threshold -> ineligible
            dominance_values=[0.95],  # High dominance
        )

        with caplog.at_level(logging.WARNING):
            result = apply_zori_eligibility(
                df,
                min_coverage=0.90,
                dominance_col="zori_max_geo_contribution",
                dominance_threshold=0.80,
            )

        assert not result["zori_is_eligible"].iloc[0]
        warning_messages = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert not any("dominance" in msg.lower() for msg in warning_messages)

    def test_dominance_col_none_skips_dominance_check(self):
        """When dominance_col=None, no dominance check is performed."""
        df = _make_metro_df(
            metro_ids=["M001"],
            zori_values=[1500.0],
            coverage_ratios=[0.95],
            dominance_values=[0.99],
        )

        # Should not raise even though dominance is very high
        result = apply_zori_eligibility(
            df,
            min_coverage=0.90,
            dominance_col=None,
        )

        assert result["zori_is_eligible"].iloc[0]


# =============================================================================
# 3. compute_rent_to_income — metro-type DataFrames
# =============================================================================


class TestComputeRentToIncomeMetro:
    """compute_rent_to_income with metro_id DataFrames (no coc_id)."""

    def test_basic_metro_rent_to_income(self):
        """Standard rent_to_income on metro data."""
        df = _make_metro_df(
            metro_ids=["M001", "M002"],
            zori_values=[1500.0, 2000.0],
            coverage_ratios=[0.95, 0.95],
            incomes=[60000.0, 48000.0],
        )
        df["zori_is_eligible"] = True

        result = compute_rent_to_income(df)

        assert "rent_to_income" in result.columns
        # 1500 / (60000/12) = 0.30
        assert result["rent_to_income"].iloc[0] == pytest.approx(0.30, rel=1e-6)
        # 2000 / (48000/12) = 0.50
        assert result["rent_to_income"].iloc[1] == pytest.approx(0.50, rel=1e-6)

    def test_metro_ineligible_gets_null_ratio(self):
        """Ineligible metro rows produce null rent_to_income."""
        df = _make_metro_df(
            metro_ids=["M001", "M002"],
            zori_values=[1500.0, 2000.0],
            coverage_ratios=[0.95, 0.50],
            incomes=[60000.0, 48000.0],
        )
        # Run eligibility first
        df = apply_zori_eligibility(df, min_coverage=0.90)

        result = compute_rent_to_income(df)

        # M001 eligible -> ratio computed
        assert result["rent_to_income"].iloc[0] == pytest.approx(0.30, rel=1e-6)
        # M002 ineligible -> null
        assert pd.isna(result["rent_to_income"].iloc[1])

    def test_metro_null_income_gives_null_ratio(self):
        """Metro row with null income produces null rent_to_income."""
        df = _make_metro_df(
            metro_ids=["M001"],
            zori_values=[1500.0],
            coverage_ratios=[0.95],
            incomes=[None],
        )
        df["zori_is_eligible"] = True

        result = compute_rent_to_income(df)

        assert pd.isna(result["rent_to_income"].iloc[0])

    def test_metro_zero_income_gives_null_ratio(self):
        """Metro row with zero income produces null (no division-by-zero)."""
        df = _make_metro_df(
            metro_ids=["M001"],
            zori_values=[1500.0],
            coverage_ratios=[0.95],
            incomes=[0.0],
        )
        df["zori_is_eligible"] = True

        result = compute_rent_to_income(df)

        assert pd.isna(result["rent_to_income"].iloc[0])

    def test_metro_mixed_eligibility_and_income(self):
        """Full pipeline: eligibility then rent_to_income across several metros."""
        df = _make_metro_df(
            metro_ids=["M001", "M002", "M003", "M004"],
            zori_values=[1500.0, 2000.0, 1200.0, None],
            coverage_ratios=[0.95, 0.80, 0.92, 0.95],
            incomes=[60000.0, 48000.0, None, 36000.0],
        )

        df = apply_zori_eligibility(df, min_coverage=0.90)
        result = compute_rent_to_income(df)

        # M001: eligible, valid income -> 0.30
        assert result["rent_to_income"].iloc[0] == pytest.approx(0.30, rel=1e-6)
        # M002: ineligible (low coverage) -> null
        assert pd.isna(result["rent_to_income"].iloc[1])
        # M003: eligible but null income -> null
        assert pd.isna(result["rent_to_income"].iloc[2])
        # M004: ineligible (missing ZORI) -> null
        assert pd.isna(result["rent_to_income"].iloc[3])

    def test_compute_does_not_mutate_input(self):
        """compute_rent_to_income returns a copy, not a mutation of input."""
        df = _make_metro_df(
            metro_ids=["M001"],
            zori_values=[1500.0],
            coverage_ratios=[0.95],
            incomes=[60000.0],
        )
        df["zori_is_eligible"] = True

        _ = compute_rent_to_income(df)

        assert "rent_to_income" not in df.columns


# =============================================================================
# 4. Schema parity: CoC and metro paths produce the same output columns
# =============================================================================


class TestSchemaParity:
    """Both CoC and metro paths produce identical output schema."""

    def test_apply_zori_eligibility_same_output_columns(self):
        """apply_zori_eligibility adds the same columns regardless of geo type."""
        coc_df = _make_coc_df(
            coc_ids=["CO-500", "CA-600"],
            zori_values=[1500.0, 2000.0],
            coverage_ratios=[0.95, 0.80],
            dominance_values=[0.50, 0.85],
        )
        metro_df = _make_metro_df(
            metro_ids=["M001", "M002"],
            zori_values=[1500.0, 2000.0],
            coverage_ratios=[0.95, 0.80],
            dominance_values=[0.50, 0.85],
        )

        coc_result = apply_zori_eligibility(
            coc_df,
            min_coverage=0.90,
            dominance_col="zori_max_geo_contribution",
        )
        metro_result = apply_zori_eligibility(
            metro_df,
            min_coverage=0.90,
            dominance_col="zori_max_geo_contribution",
        )

        # Columns added by apply_zori_eligibility
        coc_added = set(coc_result.columns) - set(coc_df.columns)
        metro_added = set(metro_result.columns) - set(metro_df.columns)
        assert coc_added == metro_added
        assert {"zori_is_eligible", "zori_excluded_reason"} == coc_added

    def test_compute_rent_to_income_same_output_columns(self):
        """compute_rent_to_income adds the same column regardless of geo type."""
        coc_df = _make_coc_df(
            coc_ids=["CO-500"],
            zori_values=[1500.0],
            coverage_ratios=[0.95],
            incomes=[60000.0],
        )
        coc_df["zori_is_eligible"] = True

        metro_df = _make_metro_df(
            metro_ids=["M001"],
            zori_values=[1500.0],
            coverage_ratios=[0.95],
            incomes=[60000.0],
        )
        metro_df["zori_is_eligible"] = True

        coc_result = compute_rent_to_income(coc_df)
        metro_result = compute_rent_to_income(metro_df)

        coc_added = set(coc_result.columns) - set(coc_df.columns)
        metro_added = set(metro_result.columns) - set(metro_df.columns)
        assert coc_added == metro_added
        assert {"rent_to_income"} == coc_added

    def test_full_pipeline_same_schema(self):
        """Full eligibility + rent_to_income pipeline yields same schema."""
        coc_df = _make_coc_df(
            coc_ids=["CO-500", "CA-600"],
            zori_values=[1500.0, 2000.0],
            coverage_ratios=[0.95, 0.80],
            incomes=[60000.0, 48000.0],
        )
        metro_df = _make_metro_df(
            metro_ids=["M001", "M002"],
            zori_values=[1500.0, 2000.0],
            coverage_ratios=[0.95, 0.80],
            incomes=[60000.0, 48000.0],
        )

        coc_result = compute_rent_to_income(
            apply_zori_eligibility(coc_df, min_coverage=0.90)
        )
        metro_result = compute_rent_to_income(
            apply_zori_eligibility(metro_df, min_coverage=0.90)
        )

        # Same added columns (beyond original geo column difference)
        coc_added = set(coc_result.columns) - {"coc_id"}
        metro_added = set(metro_result.columns) - {"metro_id"}
        assert coc_added == metro_added

    def test_eligibility_values_match_across_geo_types(self):
        """Same data should produce identical eligibility and ratio values."""
        coc_df = _make_coc_df(
            coc_ids=["G1", "G2", "G3"],
            zori_values=[1500.0, 2000.0, 1200.0],
            coverage_ratios=[0.95, 0.80, 0.92],
            incomes=[60000.0, 48000.0, 72000.0],
        )
        metro_df = _make_metro_df(
            metro_ids=["G1", "G2", "G3"],
            zori_values=[1500.0, 2000.0, 1200.0],
            coverage_ratios=[0.95, 0.80, 0.92],
            incomes=[60000.0, 48000.0, 72000.0],
        )

        coc_result = compute_rent_to_income(
            apply_zori_eligibility(coc_df, min_coverage=0.90)
        )
        metro_result = compute_rent_to_income(
            apply_zori_eligibility(metro_df, min_coverage=0.90)
        )

        # Eligibility flags identical
        assert list(coc_result["zori_is_eligible"]) == list(metro_result["zori_is_eligible"])

        # Excluded reasons identical
        coc_reasons = coc_result["zori_excluded_reason"].tolist()
        metro_reasons = metro_result["zori_excluded_reason"].tolist()
        for c, m in zip(coc_reasons, metro_reasons, strict=False):
            if pd.isna(c):
                assert pd.isna(m)
            else:
                assert c == m

        # rent_to_income values identical
        for c, m in zip(
            coc_result["rent_to_income"].tolist(),
            metro_result["rent_to_income"].tolist(), strict=False,
        ):
            if pd.isna(c):
                assert pd.isna(m)
            else:
                assert c == pytest.approx(m, rel=1e-9)

    def test_output_columns_match_canonical_list(self):
        """The canonical ZORI panel columns from get_zori_panel_columns are
        present after a full pipeline run on both CoC and metro data."""
        canonical = set(get_zori_panel_columns())

        # Metro pipeline
        metro_df = _make_metro_df(
            metro_ids=["M001"],
            zori_values=[1500.0],
            coverage_ratios=[0.95],
            incomes=[60000.0],
        )
        metro_result = compute_rent_to_income(
            apply_zori_eligibility(metro_df, min_coverage=0.90)
        )

        # The pipeline adds zori_is_eligible, zori_excluded_reason,
        # and rent_to_income. The provenance columns (rent_metric,
        # rent_alignment, zori_min_coverage) are added by
        # add_provenance_columns which is a separate step.
        pipeline_added = {"zori_coc", "zori_coverage_ratio",
                          "zori_is_eligible", "zori_excluded_reason",
                          "rent_to_income"}
        assert pipeline_added.issubset(set(metro_result.columns))
        assert pipeline_added.issubset(canonical)
