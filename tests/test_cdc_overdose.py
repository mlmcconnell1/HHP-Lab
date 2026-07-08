"""Tests for CDC county overdose normalization and MSA rollup."""

from __future__ import annotations

import pandas as pd
import pytest

from hhplab.cdc.overdose import (
    CDC_OVERDOSE_COUNTY_COLUMNS,
    CDC_OVERDOSE_MSA_COLUMNS,
    aggregate_county_overdose_to_msa,
    normalize_county_overdose,
)

RAW_OVERDOSE_FIXTURE = pd.DataFrame(
    {
        "Year": [2024, 2024, 2024, 2024, 2025],
        "Month": [1, 1, 2, 1, 1],
        "FIPS": ["1001", "01003", "01001", "01005", "01001"],
        "Provisional Drug Overdose Deaths": ["1,308", None, 99, 6, 15],
        "MonthEndingDate": [
            "01/31/2024",
            "01/31/2024",
            "02/29/2024",
            "01/31/2024",
            "01/31/2025",
        ],
        "ST_ABBREV": ["AL", "AL", "AL", "AL", "AL"],
        "STATE_NAME": ["Alabama", "Alabama", "Alabama", "Alabama", "Alabama"],
        "COUNTYNAME": ["Autauga", "Baldwin", "Autauga", "Barbour", "Autauga"],
        "Data as of": ["04/05/2026"] * 5,
    }
)

MSA_MEMBERSHIP_FIXTURE = pd.DataFrame(
    {
        "msa_id": ["12345", "12345", "67890"],
        "cbsa_code": ["12345", "12345", "67890"],
        "county_fips": ["01001", "01003", "01005"],
    }
)

COUNTY_POPULATION_FIXTURE = pd.DataFrame(
    {
        "county_fips": ["01001", "01003", "01005", "01001"],
        "year": [2024, 2024, 2024, 2025],
        "population": [900.0, 100.0, 50.0, 910.0],
    }
)


def test_normalize_county_overdose_uses_january_rows_for_requested_year() -> None:
    result = normalize_county_overdose(RAW_OVERDOSE_FIXTURE, years=[2024])

    assert tuple(result.columns) == CDC_OVERDOSE_COUNTY_COLUMNS
    assert result["county_fips"].tolist() == ["01001", "01003", "01005"]
    assert result["year"].tolist() == [2024, 2024, 2024]
    autauga = result.loc[result["county_fips"] == "01001"].iloc[0]
    baldwin = result.loc[result["county_fips"] == "01003"].iloc[0]
    assert autauga["overdose_deaths_12mo"] == pytest.approx(1308)
    assert baldwin["overdose_deaths_suppressed"]


def test_normalize_county_overdose_supports_multi_year_selection() -> None:
    result = normalize_county_overdose(RAW_OVERDOSE_FIXTURE, years=[2024, 2025])

    assert result["year"].tolist() == [2024, 2024, 2024, 2025]
    assert result.loc[result["year"] == 2025, "county_fips"].tolist() == ["01001"]
    assert result.loc[result["year"] == 2025, "overdose_deaths_12mo"].iloc[0] == (
        pytest.approx(15)
    )


def test_aggregate_county_overdose_to_msa_sums_available_county_counts() -> None:
    county = normalize_county_overdose(RAW_OVERDOSE_FIXTURE, years=[2024])

    result = aggregate_county_overdose_to_msa(
        county,
        MSA_MEMBERSHIP_FIXTURE,
        COUNTY_POPULATION_FIXTURE,
        definition_version="census_msa_2023",
    )

    assert tuple(result.columns) == CDC_OVERDOSE_MSA_COLUMNS
    row = result.loc[result["msa_id"] == "12345"].iloc[0]
    assert row["overdose_deaths_12mo"] == pytest.approx(1308)
    assert row["coverage_ratio"] == pytest.approx(0.9)
    assert row["county_coverage_ratio"] == pytest.approx(0.5)
    assert row["covered_population"] == pytest.approx(900.0)
    assert row["total_population"] == pytest.approx(1000.0)
    assert row["population_weight_denominator"] == pytest.approx(1000.0)
    assert row["county_count"] == 1
    assert row["county_expected"] == 2
    assert row["suppressed_counties"] == "01003"
    assert row["missing_counties"] == ""

    single_county_row = result.loc[result["msa_id"] == "67890"].iloc[0]
    assert single_county_row["overdose_deaths_12mo"] == pytest.approx(6)
    assert single_county_row["coverage_ratio"] == pytest.approx(1.0)
    assert single_county_row["county_coverage_ratio"] == pytest.approx(1.0)
    assert single_county_row["county_count"] == 1
    assert single_county_row["county_expected"] == 1
    assert single_county_row["missing_counties"] == ""
    assert single_county_row["suppressed_counties"] == ""


def test_aggregate_county_overdose_to_msa_applies_min_coverage() -> None:
    county = normalize_county_overdose(RAW_OVERDOSE_FIXTURE, years=[2024])

    result = aggregate_county_overdose_to_msa(
        county,
        MSA_MEMBERSHIP_FIXTURE,
        COUNTY_POPULATION_FIXTURE,
        definition_version="census_msa_2023",
        min_coverage=0.95,
    )

    value = result.loc[result["msa_id"] == "12345", "overdose_deaths_12mo"].iloc[0]
    assert pd.isna(value)


def test_aggregate_county_overdose_to_msa_requires_population_weights() -> None:
    county = normalize_county_overdose(RAW_OVERDOSE_FIXTURE, years=[2024])

    with pytest.raises(ValueError, match="requires county population weights"):
        aggregate_county_overdose_to_msa(
            county,
            MSA_MEMBERSHIP_FIXTURE,
            definition_version="census_msa_2023",
        )
