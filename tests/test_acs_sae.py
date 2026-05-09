"""Tests for ACS small-area estimation allocation helpers."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from hhplab.acs.sae import (
    GROSS_RENT_BINS,
    HOUSEHOLD_INCOME_BINS,
    SAE_ALLOCATION_METHOD,
    allocate_acs1_county_to_tracts,
    build_sae_provenance,
    compare_sae_to_direct_counties,
    derive_sae_burden_measures,
    derive_sae_distribution_measures,
    rollup_sae_tracts_to_geos,
    write_sae_parquet_with_provenance,
)
from hhplab.provenance import read_provenance

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

TRACT_TO_COC = pd.DataFrame(
    [
        {"coc_id": "COC-SINGLE", "tract_geoid": "08059000100", "area_share": 1.0},
        {"coc_id": "COC-PARTIAL", "tract_geoid": "08031001000", "area_share": 0.5},
        {"coc_id": "COC-MULTI", "tract_geoid": "08031001100", "area_share": 1.0},
        {"coc_id": "COC-MULTI", "tract_geoid": "08059000100", "area_share": 1.0},
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


def test_nan_source_component_produces_null_allocation_without_support_diagnostic() -> None:
    source = COUNTY_SOURCE.copy()
    source.loc[source["county_fips"] == "08031", "household_income_total"] = pd.NA

    result = allocate_acs1_county_to_tracts(
        source,
        TRACT_SUPPORT,
        component_columns=["household_income_total"],
    )

    county_rows = result[result["county_fips"] == "08031"]
    assert county_rows["sae_household_income_total"].isna().all()
    assert county_rows["sae_missing_support_count"].tolist() == [0, 0]
    residuals = json.loads(county_rows["sae_allocation_residuals"].iloc[0])
    assert residuals["household_income_total"] is None


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


def test_partial_coverage_boosts_nonmissing_tracts_to_conserve_county_total() -> None:
    support = TRACT_SUPPORT.copy()
    support.loc[support["tract_geoid"] == "08031001000", "household_income_total"] = pd.NA

    result = allocate_acs1_county_to_tracts(
        COUNTY_SOURCE,
        support,
        component_columns=["household_income_total"],
    )

    boosted_row = result[result["tract_geoid"] == "08031001100"].iloc[0]
    assert boosted_row["sae_household_income_total"] == pytest.approx(1000.0)
    assert json.loads(boosted_row["sae_partial_coverage_columns"]) == [
        "household_income_total"
    ]
    residuals = json.loads(boosted_row["sae_allocation_residuals"])
    assert residuals["household_income_total"] == pytest.approx(0.0)


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


def test_rolls_allocated_components_to_single_partial_and_multi_county_cocs() -> None:
    allocated = allocate_acs1_county_to_tracts(
        COUNTY_SOURCE,
        TRACT_SUPPORT,
        component_columns=["household_income_total"],
    )

    result = rollup_sae_tracts_to_geos(
        allocated,
        TRACT_TO_COC,
        component_columns=["household_income_total"],
    )

    by_coc = result.set_index("coc_id")
    assert by_coc.loc["COC-SINGLE", "sae_household_income_total"] == pytest.approx(500.0)
    assert by_coc.loc["COC-PARTIAL", "sae_household_income_total"] == pytest.approx(300.0)
    assert by_coc.loc["COC-MULTI", "sae_household_income_total"] == pytest.approx(900.0)
    assert by_coc.loc["COC-SINGLE", "sae_source_county_count"] == 1
    assert by_coc.loc["COC-MULTI", "sae_source_county_count"] == 2
    assert json.loads(by_coc.loc["COC-MULTI", "sae_source_counties"]) == ["08031", "08059"]
    assert by_coc.loc["COC-MULTI", "sae_crosswalk_coverage_ratio"] == pytest.approx(1.0)
    assert by_coc.loc["COC-MULTI", "sae_crosswalk_share_sum"] == pytest.approx(2.0)


def test_rollup_emits_missing_allocation_and_support_diagnostics() -> None:
    support = TRACT_SUPPORT.copy()
    support.loc[support["tract_geoid"] == "08031001000", "household_income_total"] = pd.NA
    allocated = allocate_acs1_county_to_tracts(
        COUNTY_SOURCE,
        support,
        component_columns=["household_income_total"],
    )
    crosswalk = pd.concat(
        [
            TRACT_TO_COC,
            pd.DataFrame(
                [
                    {
                        "coc_id": "COC-MISSING",
                        "tract_geoid": "08999000100",
                        "area_share": 1.0,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    result = rollup_sae_tracts_to_geos(
        allocated,
        crosswalk,
        component_columns=["household_income_total"],
    ).set_index("coc_id")

    assert result.loc["COC-PARTIAL", "sae_missing_support_count"] == 1
    assert result.loc["COC-PARTIAL", "sae_partial_coverage_count"] == 1
    assert result.loc["COC-MISSING", "sae_missing_allocation_tract_count"] == 1
    assert result.loc["COC-MISSING", "sae_crosswalk_coverage_ratio"] == pytest.approx(0.0)
    assert pd.isna(result.loc["COC-MISSING", "sae_household_income_total"])


def test_rollup_diagnoses_nan_crosswalk_share_rows() -> None:
    allocated = allocate_acs1_county_to_tracts(
        COUNTY_SOURCE,
        TRACT_SUPPORT,
        component_columns=["household_income_total"],
    )
    crosswalk = pd.DataFrame(
        [
            {"coc_id": "COC-A", "tract_geoid": "08031001000", "area_share": 0.5},
            {"coc_id": "COC-A", "tract_geoid": "08031001100", "area_share": pd.NA},
        ]
    )

    result = rollup_sae_tracts_to_geos(
        allocated,
        crosswalk,
        component_columns=["household_income_total"],
    ).set_index("coc_id")

    assert result.loc["COC-A", "sae_household_income_total"] == pytest.approx(300.0)
    assert result.loc["COC-A", "sae_nan_share_tract_count"] == 1
    assert result.loc["COC-A", "sae_crosswalk_share_sum"] == pytest.approx(0.5)


def test_rollup_preserves_partial_crosswalk_share_leakage() -> None:
    allocated = allocate_acs1_county_to_tracts(
        COUNTY_SOURCE,
        TRACT_SUPPORT,
        component_columns=["household_income_total"],
    )
    crosswalk = pd.DataFrame(
        [
            {"coc_id": "COC-A", "tract_geoid": "08031001000", "area_share": 0.4},
            {"coc_id": "COC-B", "tract_geoid": "08031001000", "area_share": 0.4},
        ]
    )

    result = rollup_sae_tracts_to_geos(
        allocated,
        crosswalk,
        component_columns=["household_income_total"],
    ).set_index("coc_id")

    assert result.loc["COC-A", "sae_household_income_total"] == pytest.approx(240.0)
    assert result.loc["COC-B", "sae_household_income_total"] == pytest.approx(240.0)
    assert result["sae_household_income_total"].sum() == pytest.approx(480.0)
    assert result.loc["COC-A", "sae_crosswalk_coverage_ratio"] == pytest.approx(1.0)
    assert result.loc["COC-A", "sae_crosswalk_share_sum"] == pytest.approx(0.4)


BURDEN_COMPONENTS = pd.DataFrame(
    [
        {
            "coc_id": "COC-A",
            "sae_gross_rent_pct_income_total": 100,
            "sae_gross_rent_pct_income_30_to_34_9": 20,
            "sae_gross_rent_pct_income_35_to_39_9": 10,
            "sae_gross_rent_pct_income_40_to_49_9": 5,
            "sae_gross_rent_pct_income_50_plus": 15,
            "sae_gross_rent_pct_income_not_computed": 10,
            "sae_owner_costs_pct_income_with_mortgage_total": 80,
            "sae_owner_costs_pct_income_with_mortgage_30_to_34_9": 10,
            "sae_owner_costs_pct_income_with_mortgage_35_to_39_9": 5,
            "sae_owner_costs_pct_income_with_mortgage_40_to_49_9": 5,
            "sae_owner_costs_pct_income_with_mortgage_50_plus": 10,
            "sae_owner_costs_pct_income_with_mortgage_not_computed": 5,
            "sae_owner_costs_pct_income_without_mortgage_total": 20,
            "sae_owner_costs_pct_income_without_mortgage_30_to_34_9": 2,
            "sae_owner_costs_pct_income_without_mortgage_35_to_39_9": 1,
            "sae_owner_costs_pct_income_without_mortgage_40_to_49_9": 1,
            "sae_owner_costs_pct_income_without_mortgage_50_plus": 1,
            "sae_owner_costs_pct_income_without_mortgage_not_computed": 0,
        }
    ]
)


def test_derives_rent_and_owner_burden_rates_from_allocated_bins() -> None:
    result = derive_sae_burden_measures(BURDEN_COMPONENTS)
    row = result.iloc[0]

    assert row["sae_rent_burden_not_computed_count"] == pytest.approx(10.0)
    assert row["sae_rent_burden_denominator"] == pytest.approx(90.0)
    assert row["sae_rent_burden_30_plus_count"] == pytest.approx(50.0)
    assert row["sae_rent_burden_50_plus_count"] == pytest.approx(15.0)
    assert row["sae_rent_burden_30_plus"] == pytest.approx(50.0 / 90.0)
    assert row["sae_rent_burden_50_plus"] == pytest.approx(15.0 / 90.0)

    assert row["sae_owner_cost_burden_not_computed_count"] == pytest.approx(5.0)
    assert row["sae_owner_cost_burden_denominator"] == pytest.approx(95.0)
    assert row["sae_owner_cost_burden_30_plus_count"] == pytest.approx(35.0)
    assert row["sae_owner_cost_burden_50_plus_count"] == pytest.approx(11.0)
    assert row["sae_owner_cost_burden_30_plus"] == pytest.approx(35.0 / 95.0)
    assert row["sae_owner_cost_burden_50_plus"] == pytest.approx(11.0 / 95.0)


def test_burden_rate_derivation_handles_zero_denominators_explicitly() -> None:
    components = BURDEN_COMPONENTS.copy()
    components["sae_gross_rent_pct_income_total"] = 10
    components["sae_gross_rent_pct_income_not_computed"] = 10
    components["sae_owner_costs_pct_income_with_mortgage_total"] = 5
    components["sae_owner_costs_pct_income_with_mortgage_not_computed"] = 5
    components["sae_owner_costs_pct_income_without_mortgage_total"] = 0

    result = derive_sae_burden_measures(components)
    row = result.iloc[0]

    assert row["sae_rent_burden_denominator"] == pytest.approx(0.0)
    assert row["sae_owner_cost_burden_denominator"] == pytest.approx(0.0)
    assert pd.isna(row["sae_rent_burden_30_plus"])
    assert pd.isna(row["sae_owner_cost_burden_30_plus"])
    diagnostics = json.loads(row["sae_burden_rate_diagnostics"])
    assert diagnostics["rent_denominator_zero"] is True
    assert diagnostics["owner_denominator_zero"] is True
    assert diagnostics["not_computed_excluded"] is True


def test_burden_rate_derivation_requires_allocated_bins() -> None:
    with pytest.raises(ValueError, match="missing required columns"):
        derive_sae_burden_measures(pd.DataFrame({"coc_id": ["COC-A"]}))


def _zero_distribution(columns: tuple[tuple[str, float, float | None], ...]) -> dict[str, int]:
    return {column: 0 for column, _, _ in columns}


def test_derives_income_median_and_quintiles_from_distribution_bins() -> None:
    row = {
        "coc_id": "COC-A",
        "median_household_income": 90000,
        "sae_household_income_total": 100,
        **_zero_distribution(HOUSEHOLD_INCOME_BINS),
    }
    row.update(
        {
            "sae_household_income_lt_10000": 20,
            "sae_household_income_10000_to_14999": 20,
            "sae_household_income_15000_to_19999": 20,
            "sae_household_income_20000_to_24999": 20,
            "sae_household_income_25000_to_29999": 20,
        }
    )

    result = derive_sae_distribution_measures(
        pd.DataFrame([row]),
        families=["household_income"],
    )
    output = result.iloc[0]

    assert output["sae_household_income_quintile_cutoff_20"] == pytest.approx(10000.0)
    assert output["sae_household_income_quintile_cutoff_40"] == pytest.approx(15000.0)
    assert output["sae_household_income_median"] == pytest.approx(17500.0)
    assert output["sae_household_income_quintile_cutoff_60"] == pytest.approx(20000.0)
    assert output["sae_household_income_quintile_cutoff_80"] == pytest.approx(25000.0)
    assert output["median_household_income"] == 90000
    diagnostics = json.loads(output["sae_household_income_distribution_diagnostics"])
    assert diagnostics["sae_household_income_median"]["interpolation"] == "linear_within_bin"


def test_distribution_derivation_returns_null_for_open_ended_quantile() -> None:
    row = {
        "coc_id": "COC-A",
        "sae_household_income_total": 100,
        **_zero_distribution(HOUSEHOLD_INCOME_BINS),
    }
    row["sae_household_income_200000_plus"] = 100

    result = derive_sae_distribution_measures(
        pd.DataFrame([row]),
        families=["household_income"],
    )
    output = result.iloc[0]

    assert pd.isna(output["sae_household_income_median"])
    diagnostics = json.loads(output["sae_household_income_distribution_diagnostics"])
    assert diagnostics["sae_household_income_median"]["status"] == "unsupported"
    assert diagnostics["sae_household_income_median"]["reason"] == "quantile_in_open_ended_bin"


def test_distribution_derivation_distinguishes_null_bins_from_short_totals() -> None:
    row = {
        "coc_id": "COC-A",
        "sae_household_income_total": 100,
        **_zero_distribution(HOUSEHOLD_INCOME_BINS),
    }
    row["sae_household_income_lt_10000"] = 40
    row["sae_household_income_10000_to_14999"] = pd.NA

    result = derive_sae_distribution_measures(
        pd.DataFrame([row]),
        families=["household_income"],
    )
    output = result.iloc[0]

    assert pd.isna(output["sae_household_income_median"])
    diagnostics = json.loads(output["sae_household_income_distribution_diagnostics"])
    assert diagnostics["sae_household_income_median"]["status"] == "unsupported"
    assert (
        diagnostics["sae_household_income_median"]["reason"]
        == "null_bins_prevent_reaching_quantile"
    )


def test_derives_gross_rent_median_from_cash_rent_distribution() -> None:
    row = {
        "coc_id": "COC-A",
        "median_gross_rent": 2500,
        "sae_gross_rent_distribution_with_cash_rent": 100,
        **_zero_distribution(GROSS_RENT_BINS),
    }
    row.update(
        {
            "sae_gross_rent_distribution_cash_rent_500_to_549": 40,
            "sae_gross_rent_distribution_cash_rent_550_to_599": 40,
            "sae_gross_rent_distribution_cash_rent_600_to_649": 20,
        }
    )

    result = derive_sae_distribution_measures(pd.DataFrame([row]), families=["gross_rent"])
    output = result.iloc[0]

    assert output["sae_gross_rent_median"] == pytest.approx(562.5)
    assert output["median_gross_rent"] == 2500
    diagnostics = json.loads(output["sae_gross_rent_distribution_diagnostics"])
    assert diagnostics["interpolation"] == "linear_within_bin"


def test_distribution_derivation_rejects_unsupported_family() -> None:
    with pytest.raises(ValueError, match="Unsupported SAE distribution measure families"):
        derive_sae_distribution_measures(pd.DataFrame({"coc_id": ["COC-A"]}), families=["median"])


def test_compares_sae_to_direct_counties_for_whole_county_cocs() -> None:
    county_source = pd.DataFrame(
        {
            "county_fips": ["08031", "08059"],
            "sae_household_income_total": [1000.0, 500.0],
        }
    )
    sae_geo = pd.DataFrame(
        {
            "coc_id": ["COC-SINGLE", "COC-MULTI"],
            "sae_household_income_total": [1005.0, 1490.0],
        }
    )
    coverage = pd.DataFrame(
        {
            "coc_id": ["COC-SINGLE", "COC-MULTI", "COC-MULTI"],
            "county_fips": ["08031", "08031", "08059"],
            "county_area_coverage_ratio": [1.0, 1.0, 1.0],
        }
    )

    result = compare_sae_to_direct_counties(
        sae_geo,
        county_source,
        coverage,
        measure_columns=["sae_household_income_total"],
    ).set_index("coc_id")

    assert bool(result.loc["COC-SINGLE", "comparable"]) is True
    assert result.loc["COC-SINGLE", "direct_county_value"] == pytest.approx(1000.0)
    assert result.loc["COC-SINGLE", "sae_value"] == pytest.approx(1005.0)
    assert result.loc["COC-SINGLE", "absolute_difference"] == pytest.approx(5.0)
    assert result.loc["COC-SINGLE", "relative_difference"] == pytest.approx(0.005)
    assert bool(result.loc["COC-MULTI", "comparable"]) is True
    assert result.loc["COC-MULTI", "direct_county_value"] == pytest.approx(1500.0)
    assert result.loc["COC-MULTI", "absolute_difference"] == pytest.approx(-10.0)
    assert json.loads(result.loc["COC-MULTI", "source_counties"]) == ["08031", "08059"]


def test_direct_county_comparison_labels_partial_and_mixed_containment() -> None:
    county_source = pd.DataFrame(
        {
            "county_fips": ["08031", "08059"],
            "sae_household_income_total": [1000.0, 500.0],
        }
    )
    sae_geo = pd.DataFrame(
        {
            "coc_id": ["COC-PARTIAL", "COC-MIXED"],
            "sae_household_income_total": [300.0, 1200.0],
        }
    )
    coverage = pd.DataFrame(
        {
            "coc_id": ["COC-PARTIAL", "COC-MIXED", "COC-MIXED"],
            "county_fips": ["08031", "08031", "08059"],
            "county_area_coverage_ratio": [0.5, 1.0, 0.75],
        }
    )

    result = compare_sae_to_direct_counties(
        sae_geo,
        county_source,
        coverage,
        measure_columns=["sae_household_income_total"],
    ).set_index("coc_id")

    assert bool(result.loc["COC-PARTIAL", "comparable"]) is False
    assert result.loc["COC-PARTIAL", "comparability_reason"] == "partial_county"
    assert pd.isna(result.loc["COC-PARTIAL", "direct_county_value"])
    assert result.loc["COC-PARTIAL", "sae_value"] == pytest.approx(300.0)
    assert bool(result.loc["COC-MIXED", "comparable"]) is False
    assert result.loc["COC-MIXED", "comparability_reason"] == "mixed_containment"


def test_direct_county_comparison_emits_uncovered_sae_geographies() -> None:
    county_source = pd.DataFrame(
        {
            "county_fips": ["08031"],
            "sae_household_income_total": [1000.0],
        }
    )
    sae_geo = pd.DataFrame(
        {
            "coc_id": ["COC-COVERED", "COC-NOT-IN-COVERAGE"],
            "sae_household_income_total": [1005.0, 250.0],
        }
    )
    coverage = pd.DataFrame(
        {
            "coc_id": ["COC-COVERED"],
            "county_fips": ["08031"],
            "county_area_coverage_ratio": [1.0],
        }
    )

    result = compare_sae_to_direct_counties(
        sae_geo,
        county_source,
        coverage,
        measure_columns=["sae_household_income_total"],
    ).set_index("coc_id")

    assert bool(result.loc["COC-NOT-IN-COVERAGE", "comparable"]) is False
    assert result.loc["COC-NOT-IN-COVERAGE", "comparability_reason"] == "not_in_coverage"
    assert pd.isna(result.loc["COC-NOT-IN-COVERAGE", "direct_county_value"])
    assert result.loc["COC-NOT-IN-COVERAGE", "sae_value"] == pytest.approx(250.0)
    assert result.loc["COC-NOT-IN-COVERAGE", "source_county_count"] == 0
    assert json.loads(result.loc["COC-NOT-IN-COVERAGE", "source_counties"]) == []


def test_direct_county_comparison_requires_measures() -> None:
    with pytest.raises(ValueError, match="measure_columns"):
        compare_sae_to_direct_counties(
            pd.DataFrame({"coc_id": ["COC-A"]}),
            pd.DataFrame({"county_fips": ["08031"]}),
            pd.DataFrame(
                {
                    "coc_id": ["COC-A"],
                    "county_fips": ["08031"],
                    "county_area_coverage_ratio": [1.0],
                }
            ),
            measure_columns=[],
        )


def test_builds_sae_provenance_with_lineage_fields() -> None:
    provenance = build_sae_provenance(
        acs1_vintage=2023,
        acs5_vintage=2022,
        tract_vintage=2020,
        target_geo_type="coc",
        target_vintage=2025,
        denominator_source="acs5_tract_support",
        crosswalk_id="tract2020_to_coc2025",
        source_dataset_path="data/curated/acs/acs1_county_sae__A2023.parquet",
        support_dataset_path="data/curated/acs/acs5_tract_sae_support__A2022xT2020.parquet",
        requested_measures=["rent_burden"],
        derived_output_columns=["sae_rent_burden_30_plus"],
        diagnostics_summary={"max_residual": 0.0},
        source_tables=["B25070"],
        support_tables=["B25070"],
        source_row_count=1,
        support_row_count=2,
        output_row_count=1,
    )

    assert provenance.acs_vintage == "2022"
    assert provenance.tract_vintage == "2020"
    assert provenance.boundary_vintage == "2025"
    assert provenance.notation == "A2022@B2025\u00d7T2020"
    assert provenance.extra["acs1_vintage"] == "2023"
    assert provenance.extra["acs5_terminal_vintage"] == "2022"
    assert provenance.extra["allocation_method"] == SAE_ALLOCATION_METHOD
    assert provenance.extra["denominator_source"] == "acs5_tract_support"
    assert provenance.extra["crosswalk_id"] == "tract2020_to_coc2025"
    assert provenance.extra["requested_measures"] == ["rent_burden"]
    assert provenance.extra["derived_output_columns"] == ["sae_rent_burden_30_plus"]
    assert provenance.extra["diagnostics_summary"] == {"max_residual": 0.0}


def test_writes_sae_parquet_with_embedded_provenance(tmp_path) -> None:
    output_path = tmp_path / "sae.parquet"
    df = pd.DataFrame(
        {
            "geo_type": ["coc"],
            "geo_id": ["COC-A"],
            "year": [2023],
            "sae_rent_burden_30_plus": [0.4],
        }
    )

    returned = write_sae_parquet_with_provenance(
        df,
        output_path,
        acs1_vintage=2023,
        acs5_vintage=2022,
        tract_vintage=2020,
        target_geo_type="coc",
        target_vintage=2025,
        denominator_source="acs5_tract_support",
        crosswalk_id="tract2020_to_coc2025",
        requested_measures=["rent_burden"],
        derived_output_columns=["sae_rent_burden_30_plus"],
    )

    provenance = read_provenance(returned)
    assert returned == output_path
    assert provenance is not None
    assert provenance.extra["dataset"] == "acs_sae"
    assert provenance.extra["acs1_vintage"] == "2023"
    assert provenance.extra["output_row_count"] == 1
