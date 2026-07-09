"""Tests for the tracked BPS valuation benchmark workflow."""

from __future__ import annotations

import importlib

import pandas as pd
import pytest

from hhplab.covariates.census_bps_contract import (
    CENSUS_BPS_CLASS_UNIT_COLUMNS,
    CENSUS_BPS_CLASS_VALUE_COLUMNS,
    CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN,
)


def _load_builder():
    return importlib.import_module("hhplab.results.workflows.build_bps_valuation_benchmark")


def _class_row(year: int, units: list[float], values: list[float]) -> dict[str, object]:
    return {
        "year": year,
        **dict(zip(CENSUS_BPS_CLASS_UNIT_COLUMNS, units, strict=True)),
        **dict(zip(CENSUS_BPS_CLASS_VALUE_COLUMNS, values, strict=True)),
    }


def test_build_national_fixed_mix_bps_index_uses_base_year_weights() -> None:
    builder = _load_builder()
    county = pd.DataFrame(
        [
            _class_row(2000, [10, 30, 0, 0], [1000, 6000, 0, 0]),
            _class_row(2001, [20, 10, 5, 0], [3000, 2500, 5000, 0]),
        ]
    )

    result = builder.build_national_fixed_mix_bps_index(county).set_index("year")

    assert result.loc[2000, "national_fixed_mix_value_per_unit"] == pytest.approx(175.0)
    assert result.loc[2001, "national_fixed_mix_value_per_unit"] == pytest.approx(225.0)
    assert result.loc[2001, "bps_index_2000"] == pytest.approx(128.5714285714)


def test_ppi_benchmark_reproduces_reported_ranges_from_current_artifacts() -> None:
    builder = _load_builder()
    county = pd.read_parquet(builder.BPS_COUNTY_PATH)
    ppi = builder.load_ppi_annual_averages()

    benchmark = builder.build_ppi_benchmark(county, ppi)
    annual_changes = benchmark[["d_log_bps_index", "d_log_ppi_index"]].dropna()
    latest = benchmark[benchmark["year"].eq(2024)].iloc[0]

    assert len(annual_changes) == 24
    assert latest["bps_index_2000"] == pytest.approx(235.721, abs=0.001)
    assert latest["ppi_index_2000"] == pytest.approx(227.982, abs=0.001)
    assert annual_changes["d_log_bps_index"].corr(annual_changes["d_log_ppi_index"]) == (
        pytest.approx(0.521, abs=0.001)
    )


def test_distinctness_frame_reproduces_supply_constraint_correlation_range() -> None:
    builder = _load_builder()
    msa_bps = pd.read_parquet(builder.BPS_MSA_PANEL_PATH)

    distinctness = builder.build_distinctness_frame(msa_bps)
    correlation = distinctness["supply_constraint_bps"].corr(
        distinctness["log_mean_mix_adjusted_value_per_unit_1014"]
    )

    assert len(distinctness) == 316
    assert correlation == pytest.approx(0.155, abs=0.005)


def test_distinctness_frame_derives_expected_cross_section() -> None:
    builder = _load_builder()
    msa_bps = pd.DataFrame(
        {
            "msa_id": ["10000", "10000", "20000", "20000"],
            "year": [2010, 2011, 2010, 2011],
            "permitted_units": [100.0, 120.0, 20.0, 30.0],
            "population_weight_denominator": [1000.0, 1000.0, 500.0, 500.0],
            CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN: [100.0, 110.0, 300.0, 330.0],
        }
    )

    result = builder.build_distinctness_frame(msa_bps).set_index("msa_id")

    assert result.loc["10000", "bps_permits_per_1000_1014"] == pytest.approx(110.0)
    assert result.loc["20000", "bps_permits_per_1000_1014"] == pytest.approx(50.0)
    assert result.loc["10000", "mean_mix_adjusted_value_per_unit_1014"] == pytest.approx(105.0)
