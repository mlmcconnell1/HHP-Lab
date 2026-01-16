"""Tests for ZORI aggregation from county to CoC geography.

Tests cover:
- Basic weighted aggregation
- Zero-coverage CoC handling (CoC-PIT-8hu requirement)
- Coverage ratio computation
- Yearly collapse methods
"""

import pandas as pd
import pytest

from coclab.rents.aggregate import aggregate_monthly, collapse_to_yearly


class TestAggregateMonthly:
    """Tests for aggregate_monthly function."""

    @pytest.fixture
    def sample_xwalk(self):
        """Crosswalk with 3 CoCs: A (2 counties), B (2 counties), C (2 counties)."""
        return pd.DataFrame(
            {
                "coc_id": ["A", "A", "B", "B", "C", "C"],
                "county_fips": ["01001", "01002", "02001", "02002", "03001", "03002"],
                "area_share": [0.6, 0.4, 0.5, 0.5, 0.7, 0.3],
            }
        )

    @pytest.fixture
    def sample_weights(self):
        """Equal weights for all counties."""
        return pd.DataFrame(
            {
                "county_fips": ["01001", "01002", "02001", "02002", "03001", "03002"],
                "weight_value": [100, 100, 100, 100, 100, 100],
            }
        )

    @pytest.fixture
    def sample_zori_all_counties(self):
        """ZORI data for all counties."""
        return pd.DataFrame(
            {
                "geo_id": ["01001", "01002", "02001", "02002", "03001", "03002"],
                "date": pd.to_datetime(["2024-01-01"] * 6),
                "zori": [1000.0, 1200.0, 1500.0, 1600.0, 800.0, 900.0],
            }
        )

    @pytest.fixture
    def sample_zori_partial(self):
        """ZORI data only for CoCs A and B, NOT C (zero coverage for C)."""
        return pd.DataFrame(
            {
                "geo_id": ["01001", "01002", "02001", "02002"],
                "date": pd.to_datetime(["2024-01-01"] * 4),
                "zori": [1000.0, 1200.0, 1500.0, 1600.0],
            }
        )

    def test_basic_aggregation(self, sample_xwalk, sample_weights, sample_zori_all_counties):
        """Test basic weighted aggregation produces correct ZORI values."""
        result = aggregate_monthly(
            sample_zori_all_counties, sample_xwalk, sample_weights, min_coverage=0.90
        )

        assert len(result) == 3  # 3 CoCs, 1 date
        assert set(result["coc_id"]) == {"A", "B", "C"}

        # All should have full coverage
        assert (result["coverage_ratio"] == 1.0).all()
        assert result["zori_coc"].notna().all()

    def test_zero_coverage_coc_has_coverage_ratio_zero(
        self, sample_xwalk, sample_weights, sample_zori_partial
    ):
        """CoC-PIT-8hu: CoCs with no ZORI data should have coverage_ratio = 0.0, not NULL."""
        result = aggregate_monthly(
            sample_zori_partial, sample_xwalk, sample_weights, min_coverage=0.90
        )

        coc_c = result[result["coc_id"] == "C"]

        # CRITICAL: coverage_ratio must be 0.0, not NULL
        assert len(coc_c) == 1
        assert coc_c["coverage_ratio"].iloc[0] == 0.0
        assert not pd.isna(coc_c["coverage_ratio"].iloc[0])

    def test_zero_coverage_coc_still_appears_in_output(
        self, sample_xwalk, sample_weights, sample_zori_partial
    ):
        """CoC-PIT-8hu: CoCs with no ZORI data should still appear in output."""
        result = aggregate_monthly(
            sample_zori_partial, sample_xwalk, sample_weights, min_coverage=0.90
        )

        # All 3 CoCs must appear in output
        assert len(result) == 3
        assert "C" in result["coc_id"].values

    def test_zero_coverage_coc_has_null_zori(
        self, sample_xwalk, sample_weights, sample_zori_partial
    ):
        """CoC-PIT-8hu: CoCs with no ZORI data should have zori_coc = NULL."""
        result = aggregate_monthly(
            sample_zori_partial, sample_xwalk, sample_weights, min_coverage=0.90
        )

        coc_c = result[result["coc_id"] == "C"]
        assert pd.isna(coc_c["zori_coc"].iloc[0])

    def test_zero_coverage_coc_has_zero_geo_count(
        self, sample_xwalk, sample_weights, sample_zori_partial
    ):
        """CoC-PIT-8hu: CoCs with no ZORI data should have geo_count = 0."""
        result = aggregate_monthly(
            sample_zori_partial, sample_xwalk, sample_weights, min_coverage=0.90
        )

        coc_c = result[result["coc_id"] == "C"]
        assert coc_c["geo_count"].iloc[0] == 0

    def test_partial_coverage_below_threshold_nulls_zori(self, sample_xwalk, sample_weights):
        """ZORI should be null when coverage is below threshold."""
        # Only one county for CoC A
        zori_df = pd.DataFrame(
            {
                "geo_id": ["01001", "02001", "02002"],  # 01002 missing
                "date": pd.to_datetime(["2024-01-01"] * 3),
                "zori": [1000.0, 1500.0, 1600.0],
            }
        )

        result = aggregate_monthly(zori_df, sample_xwalk, sample_weights, min_coverage=0.90)

        coc_a = result[result["coc_id"] == "A"]
        # CoC A should have ~60% coverage (only county 01001 with 0.6 share)
        assert coc_a["coverage_ratio"].iloc[0] < 0.90
        assert pd.isna(coc_a["zori_coc"].iloc[0])  # Below threshold -> null

    def test_coverage_ratio_computed_correctly(self, sample_xwalk, sample_weights):
        """Coverage ratio should sum weights of counties with ZORI data."""
        # Create ZORI only for county 01001 (60% of CoC A)
        zori_df = pd.DataFrame(
            {
                "geo_id": ["01001"],
                "date": pd.to_datetime(["2024-01-01"]),
                "zori": [1000.0],
            }
        )

        result = aggregate_monthly(zori_df, sample_xwalk, sample_weights, min_coverage=0.0)

        coc_a = result[result["coc_id"] == "A"]
        # Should be approximately 0.6 (area_share of county 01001)
        assert abs(coc_a["coverage_ratio"].iloc[0] - 0.6) < 0.01

    def test_multiple_months(self, sample_xwalk, sample_weights):
        """Test aggregation across multiple months."""
        zori_df = pd.DataFrame(
            {
                "geo_id": ["01001", "01002", "01001", "01002"],
                "date": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-02-01", "2024-02-01"]),
                "zori": [1000.0, 1200.0, 1050.0, 1250.0],
            }
        )

        result = aggregate_monthly(zori_df, sample_xwalk, sample_weights, min_coverage=0.0)

        # Should have 3 CoCs x 2 months = 6 rows
        assert len(result) == 6

        # CoC A should have data for both months
        coc_a = result[result["coc_id"] == "A"].sort_values("date")
        assert len(coc_a) == 2
        assert coc_a["coverage_ratio"].iloc[0] == 1.0
        assert coc_a["coverage_ratio"].iloc[1] == 1.0

    def test_max_geo_contribution_computed(
        self, sample_xwalk, sample_weights, sample_zori_all_counties
    ):
        """Max geo contribution should reflect largest contributor."""
        result = aggregate_monthly(
            sample_zori_all_counties, sample_xwalk, sample_weights, min_coverage=0.90
        )

        coc_a = result[result["coc_id"] == "A"]
        # County 01001 has area_share 0.6, so normalized weight should be 0.6
        assert coc_a["max_geo_contribution"].iloc[0] == pytest.approx(0.6, rel=0.01)


class TestCollapseToYearly:
    """Tests for collapse_to_yearly function."""

    @pytest.fixture
    def sample_monthly_data(self):
        """Monthly data with full year of data."""
        dates = pd.date_range("2024-01-01", periods=12, freq="MS")
        data = []
        for coc in ["A", "B"]:
            for date in dates:
                data.append(
                    {
                        "coc_id": coc,
                        "date": date,
                        "zori_coc": 1000.0 + (date.month * 10),
                        "coverage_ratio": 0.95,
                        "max_geo_contribution": 0.5,
                        "geo_count": 2,
                    }
                )
        return pd.DataFrame(data)

    def test_pit_january_selects_january(self, sample_monthly_data):
        """pit_january method should select January values."""
        result = collapse_to_yearly(sample_monthly_data, method="pit_january")

        assert len(result) == 2  # 2 CoCs x 1 year
        assert "method" in result.columns
        assert (result["method"] == "pit_january").all()

        # Should have January value (month=1, so zori = 1000 + 10 = 1010)
        coc_a = result[result["coc_id"] == "A"]
        assert coc_a["zori_coc"].iloc[0] == pytest.approx(1010.0)

    def test_calendar_mean_computes_average(self, sample_monthly_data):
        """calendar_mean should compute average across all months."""
        result = collapse_to_yearly(sample_monthly_data, method="calendar_mean")

        assert len(result) == 2
        assert (result["method"] == "calendar_mean").all()

        # Mean of 1010, 1020, ..., 1120 = 1065
        coc_a = result[result["coc_id"] == "A"]
        assert coc_a["zori_coc"].iloc[0] == pytest.approx(1065.0)

    def test_calendar_median_computes_median(self, sample_monthly_data):
        """calendar_median should compute median across all months."""
        result = collapse_to_yearly(sample_monthly_data, method="calendar_median")

        assert len(result) == 2
        assert (result["method"] == "calendar_median").all()

        # Median of 1010, 1020, ..., 1120 = (1060 + 1070) / 2 = 1065
        coc_a = result[result["coc_id"] == "A"]
        assert coc_a["zori_coc"].iloc[0] == pytest.approx(1065.0)

    def test_invalid_method_raises(self, sample_monthly_data):
        """Invalid method should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown yearly method"):
            collapse_to_yearly(sample_monthly_data, method="invalid")

    def test_zero_coverage_preserved_in_yearly(self):
        """Zero coverage CoCs should have coverage_ratio=0 in yearly output too."""
        dates = pd.date_range("2024-01-01", periods=3, freq="MS")
        data = []
        # CoC with zero coverage
        for date in dates:
            data.append(
                {
                    "coc_id": "ZERO",
                    "date": date,
                    "zori_coc": None,
                    "coverage_ratio": 0.0,
                    "max_geo_contribution": None,
                    "geo_count": 0,
                }
            )
        monthly_df = pd.DataFrame(data)

        result = collapse_to_yearly(monthly_df, method="pit_january")

        assert len(result) == 1
        assert result["coverage_ratio"].iloc[0] == 0.0
        assert pd.isna(result["zori_coc"].iloc[0])
