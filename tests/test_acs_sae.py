"""Tests for ACS small-area estimation allocation helpers."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from hhplab.acs.sae import SAE_ALLOCATION_METHOD, allocate_acs1_county_to_tracts

COUNTY_SOURCE = pd.DataFrame(
    [
        {
            "county_fips": "08031",
            "acs1_vintage": "2023",
            "household_income_total": 1000,
            "household_income_200000_plus": 100,
            "gross_rent_pct_income_total": 300,
            "gross_rent_pct_income_50_plus": 90,
            "civilian_labor_force": 800,
            "unemployed_count": 40,
        },
        {
            "county_fips": "08059",
            "acs1_vintage": "2023",
            "household_income_total": 500,
            "household_income_200000_plus": 50,
            "gross_rent_pct_income_total": 200,
            "gross_rent_pct_income_50_plus": 80,
            "civilian_labor_force": 400,
            "unemployed_count": 20,
        },
    ]
)

TRACT_SUPPORT = pd.DataFrame(
    [
        {
            "tract_geoid": "08031001000",
            "county_fips": "08031",
            "acs_vintage": "2019-2023",
            "tract_vintage": "2023",
            "household_income_total": 60,
            "household_income_200000_plus": 25,
            "gross_rent_pct_income_total": 100,
            "gross_rent_pct_income_50_plus": 20,
            "civilian_labor_force": 600,
            "unemployed_count": 30,
        },
        {
            "tract_geoid": "08031001100",
            "county_fips": "08031",
            "acs_vintage": "2019-2023",
            "tract_vintage": "2023",
            "household_income_total": 40,
            "household_income_200000_plus": 75,
            "gross_rent_pct_income_total": 200,
            "gross_rent_pct_income_50_plus": 70,
            "civilian_labor_force": 200,
            "unemployed_count": 10,
        },
        {
            "tract_geoid": "08059000100",
            "county_fips": "08059",
            "acs_vintage": "2019-2023",
            "tract_vintage": "2023",
            "household_income_total": 500,
            "household_income_200000_plus": 50,
            "gross_rent_pct_income_total": 200,
            "gross_rent_pct_income_50_plus": 80,
            "civilian_labor_force": 400,
            "unemployed_count": 20,
        },
    ]
)


def test_allocates_county_components_by_matching_tract_support_shares() -> None:
    result = allocate_acs1_county_to_tracts(
        COUNTY_SOURCE,
        TRACT_SUPPORT,
        component_columns=[
            "household_income_total",
            "household_income_200000_plus",
            "gross_rent_pct_income_50_plus",
            "civilian_labor_force",
        ],
    )

    assert result["allocation_method"].unique().tolist() == [SAE_ALLOCATION_METHOD]
    assert result["source_county_fips"].tolist() == ["08031", "08031", "08059"]
    assert result["acs1_vintage"].tolist() == ["2023", "2023", "2023"]

    first = result[result["tract_geoid"] == "08031001000"].iloc[0]
    second = result[result["tract_geoid"] == "08031001100"].iloc[0]
    assert first["sae_household_income_total"] == pytest.approx(600.0)
    assert second["sae_household_income_total"] == pytest.approx(400.0)
    assert first["sae_household_income_200000_plus"] == pytest.approx(25.0)
    assert second["sae_household_income_200000_plus"] == pytest.approx(75.0)
    assert first["sae_civilian_labor_force"] == pytest.approx(600.0)
    assert second["sae_civilian_labor_force"] == pytest.approx(200.0)


def test_allocated_components_conserve_county_source_totals() -> None:
    result = allocate_acs1_county_to_tracts(
        COUNTY_SOURCE,
        TRACT_SUPPORT,
        component_columns=[
            "household_income_total",
            "household_income_200000_plus",
            "gross_rent_pct_income_50_plus",
        ],
    )

    allocated = result.groupby("source_county_fips")[
        [
            "sae_household_income_total",
            "sae_household_income_200000_plus",
            "sae_gross_rent_pct_income_50_plus",
        ]
    ].sum()

    assert allocated.loc["08031", "sae_household_income_total"] == pytest.approx(1000.0)
    assert allocated.loc["08031", "sae_household_income_200000_plus"] == pytest.approx(100.0)
    assert allocated.loc["08031", "sae_gross_rent_pct_income_50_plus"] == pytest.approx(90.0)
    assert allocated.loc["08059", "sae_household_income_total"] == pytest.approx(500.0)

    residuals = json.loads(
        result.loc[
            result["county_fips"] == "08031",
            "sae_allocation_residuals",
        ].iloc[0]
    )
    assert residuals["household_income_total"] == pytest.approx(0.0)
    assert residuals["gross_rent_pct_income_50_plus"] == pytest.approx(0.0)


def test_zero_denominator_produces_null_allocation_and_diagnostic() -> None:
    support = TRACT_SUPPORT.copy()
    support.loc[support["county_fips"] == "08031", "gross_rent_pct_income_total"] = 0

    result = allocate_acs1_county_to_tracts(
        COUNTY_SOURCE,
        support,
        component_columns=["gross_rent_pct_income_total"],
    )

    county_rows = result[result["county_fips"] == "08031"]
    assert county_rows["sae_gross_rent_pct_income_total"].isna().all()
    for diagnostic in county_rows["sae_zero_denominator_columns"]:
        assert json.loads(diagnostic) == ["gross_rent_pct_income_total"]
    assert county_rows["sae_zero_denominator_count"].tolist() == [1, 1]


def test_missing_distribution_support_marks_partial_coverage() -> None:
    support = TRACT_SUPPORT.copy()
    support.loc[support["tract_geoid"] == "08031001000", "household_income_total"] = pd.NA

    result = allocate_acs1_county_to_tracts(
        COUNTY_SOURCE,
        support,
        component_columns=["household_income_total"],
    )

    missing_row = result[result["tract_geoid"] == "08031001000"].iloc[0]
    covered_row = result[result["tract_geoid"] == "08031001100"].iloc[0]
    assert pd.isna(missing_row["sae_household_income_total"])
    assert covered_row["sae_household_income_total"] == pytest.approx(1000.0)
    assert json.loads(missing_row["sae_missing_support_columns"]) == ["household_income_total"]
    assert json.loads(missing_row["sae_partial_coverage_columns"]) == ["household_income_total"]
    assert json.loads(covered_row["sae_partial_coverage_columns"]) == ["household_income_total"]


def test_records_missing_county_coverage_in_attrs() -> None:
    source = pd.concat(
        [
            COUNTY_SOURCE,
            pd.DataFrame(
                [
                    {
                        "county_fips": "08123",
                        "acs1_vintage": "2023",
                        "household_income_total": 10,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    result = allocate_acs1_county_to_tracts(
        source,
        TRACT_SUPPORT,
        component_columns=["household_income_total"],
    )

    assert result.attrs["missing_support_counties"] == ["08123"]
    assert result.attrs["missing_source_counties"] == []


def test_rejects_duplicate_county_source_rows() -> None:
    source = pd.concat([COUNTY_SOURCE.iloc[[0]], COUNTY_SOURCE.iloc[[0]]], ignore_index=True)

    with pytest.raises(ValueError, match="one row per county_fips"):
        allocate_acs1_county_to_tracts(
            source,
            TRACT_SUPPORT,
            component_columns=["household_income_total"],
        )
