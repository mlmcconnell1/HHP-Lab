"""Tests for tract-mediated county-to-CoC crosswalk weights.

Truth table for county 01001:

| row | CoC | tract       | area_share | tract_area | pop | households | renters |
|-----|-----|-------------|------------|------------|-----|------------|---------|
| 1   | A   | 01001000100 | 0.5        | 100        | 100 | 40         | 10      |
| 2   | B   | 01001000100 | 0.5        | 100        | 100 | 40         | 10      |
| 3   | B   | 01001000200 | 1.0        | 100        | 300 | 60         | 30      |

County totals are area=200, population=400, households=100, renters=40.
Expected weights:
- A: area=0.25, population=0.125, households=0.20, renters=0.125
- B: area=0.75, population=0.875, households=0.80, renters=0.875
"""

from __future__ import annotations

import pandas as pd
import pytest

from hhplab.census.ingest.decennial_tract_population import STATE_FIPS_CODES
from hhplab.naming import tract_mediated_county_xwalk_filename
from hhplab.provenance import read_provenance
from hhplab.xwalks.tract_mediated import (
    COUNTY_VINTAGE_SEMANTICS,
    build_tract_mediated_county_crosswalk,
    save_tract_mediated_county_crosswalk,
    summarize_tract_mediated_crosswalk,
)

TRACT_CROSSWALK = pd.DataFrame(
    {
        "coc_id": ["A", "B", "B", "C"],
        "boundary_vintage": ["2025", "2025", "2025", "2025"],
        "tract_geoid": [
            "01001000100",
            "01001000100",
            "01001000200",
            "02001000100",
        ],
        "tract_vintage": ["2020", "2020", "2020", "2020"],
        "area_share": [0.5, 0.5, 1.0, 1.0],
        "intersection_area": [50.0, 50.0, 100.0, 80.0],
        "tract_area": [100.0, 100.0, 100.0, 80.0],
    }
)

ACS_TRACTS = pd.DataFrame(
    {
        "tract_geoid": ["01001000100", "01001000200", "02001000100"],
        "total_population": [100.0, 300.0, 0.0],
        "total_households": [40.0, 60.0, 0.0],
        "renter_households": [10.0, 30.0, 0.0],
    }
)

EXPECTED_01001 = {
    "A": {
        "area_weight": 0.25,
        "population_weight": 0.125,
        "household_weight": 0.20,
        "renter_household_weight": 0.125,
    },
    "B": {
        "area_weight": 0.75,
        "population_weight": 0.875,
        "household_weight": 0.80,
        "renter_household_weight": 0.875,
    },
}

TERRITORY_FIPS_CODES = {"60", "66", "69", "72", "78"}


def test_decennial_denominator_ingest_includes_territory_fips() -> None:
    """Decennial denominators must cover the same territory tracts as TIGER."""
    assert TERRITORY_FIPS_CODES <= set(STATE_FIPS_CODES)


def build_fixture() -> pd.DataFrame:
    return build_tract_mediated_county_crosswalk(
        TRACT_CROSSWALK,
        ACS_TRACTS,
        boundary_vintage="2025",
        county_vintage="2020",
        tract_vintage="2020",
        acs_vintage="2019-2023",
    )


def build_decennial_fixture(analysis_year: int) -> pd.DataFrame:
    return build_tract_mediated_county_crosswalk(
        TRACT_CROSSWALK.assign(analysis_year=analysis_year),
        ACS_TRACTS[["tract_geoid", "total_population"]],
        boundary_vintage="2025",
        county_vintage="2020",
        tract_vintage="2020",
        denominator_source="decennial",
        denominator_vintage="2020",
    )


class TestTractMediatedCountyCrosswalk:
    @pytest.mark.parametrize(
        ("coc_id", "weight_col", "expected"),
        [
            (coc_id, weight_col, expected)
            for coc_id, expected_by_col in EXPECTED_01001.items()
            for weight_col, expected in expected_by_col.items()
        ],
        ids=lambda value: str(value),
    )
    def test_weight_truth_table(self, coc_id, weight_col, expected):
        result = build_fixture()

        row = result[(result["coc_id"] == coc_id) & (result["county_fips"] == "01001")].iloc[0]

        assert row[weight_col] == pytest.approx(expected)

    def test_outputs_diagnostics_and_metadata(self):
        result = build_fixture()

        row = result[(result["coc_id"] == "B") & (result["county_fips"] == "01001")].iloc[0]
        assert row["boundary_vintage"] == "2025"
        assert row["county_vintage"] == "2020"
        assert row["tract_vintage"] == "2020"
        assert row["acs_vintage"] == "2019-2023"
        assert row["denominator_source"] == "acs"
        assert row["denominator_vintage"] == "2019-2023"
        assert row["county_vintage_semantics"] == COUNTY_VINTAGE_SEMANTICS
        assert row["weighting_method"] == "tract_mediated"
        assert row["population_denominator"] == pytest.approx(350.0)
        assert row["county_population_total"] == pytest.approx(400.0)
        assert row["geo_population_total"] == pytest.approx(350.0)
        assert row["county_population_coverage_ratio"] == pytest.approx(1.0)
        assert row["tract_count"] == 2
        assert row["denominator_tract_count"] == 2
        assert row["missing_denominator_tract_count"] == 0
        assert row["denominator_tract_coverage_ratio"] == pytest.approx(1.0)
        assert row["county_tract_count"] == 2
        assert row["county_denominator_tract_count"] == 2
        assert row["county_missing_denominator_tract_count"] == 0
        assert row["county_denominator_tract_coverage_ratio"] == pytest.approx(1.0)

    def test_zero_county_denominators_produce_nullable_weights(self):
        result = build_fixture()

        row = result[(result["coc_id"] == "C") & (result["county_fips"] == "02001")].iloc[0]
        assert row["area_weight"] == pytest.approx(1.0)
        assert pd.isna(row["population_weight"])
        assert pd.isna(row["household_weight"])
        assert pd.isna(row["renter_household_weight"])

    def test_duplicate_denominator_tract_rows_do_not_multiply_weights(self):
        duplicated_denominators = pd.concat(
            [
                ACS_TRACTS,
                pd.DataFrame(
                    {
                        "tract_geoid": ["01001000100"],
                        "total_population": [9999.0],
                        "total_households": [9999.0],
                        "renter_households": [9999.0],
                    }
                ),
            ],
            ignore_index=True,
        )

        result = build_tract_mediated_county_crosswalk(
            TRACT_CROSSWALK,
            duplicated_denominators,
            boundary_vintage="2025",
            county_vintage="2020",
            tract_vintage="2020",
            acs_vintage="2023",
        )

        row = result[(result["coc_id"] == "B") & (result["county_fips"] == "01001")].iloc[0]
        assert row["population_weight"] == pytest.approx(0.875)
        assert row["county_population_total"] == pytest.approx(400.0)
        assert row["denominator_tract_count"] == 2
        assert row["missing_denominator_tract_count"] == 0

    def test_non_numeric_required_denominator_is_treated_as_missing(self):
        denominators = ACS_TRACTS.copy().astype({"total_population": "object"})
        denominators.loc[denominators["tract_geoid"] == "01001000200", "total_population"] = "bad"

        with pytest.raises(ValueError, match="Tract-mediated denominator coverage is incomplete"):
            build_tract_mediated_county_crosswalk(
                TRACT_CROSSWALK,
                denominators,
                boundary_vintage="2025",
                county_vintage="2020",
                tract_vintage="2020",
                acs_vintage="2023",
            )

    def test_partial_tract_area_coverage_is_visible_in_county_diagnostics(self):
        partial = TRACT_CROSSWALK.copy()
        partial.loc[partial["tract_geoid"] == "01001000200", "area_share"] = 0.5
        partial.loc[partial["tract_geoid"] == "01001000200", "intersection_area"] = 50.0

        result = build_tract_mediated_county_crosswalk(
            partial,
            ACS_TRACTS,
            boundary_vintage="2025",
            county_vintage="2020",
            tract_vintage="2020",
            acs_vintage="2023",
        )

        county_rows = result[result["county_fips"] == "01001"]
        assert county_rows["county_area_coverage_ratio"].unique().tolist() == [pytest.approx(0.75)]
        assert county_rows["county_population_coverage_ratio"].unique().tolist() == [
            pytest.approx(0.625)
        ]

    def test_tract_and_denominator_geoids_are_zero_padded(self):
        tract_crosswalk = pd.DataFrame(
            {
                "coc_id": ["A"],
                "tract_geoid": [1001000100],
                "area_share": [1.0],
                "intersection_area": [100.0],
                "tract_area": [100.0],
            }
        )
        denominators = pd.DataFrame(
            {
                "tract_geoid": [1001000100],
                "total_population": [50.0],
            }
        )

        result = build_tract_mediated_county_crosswalk(
            tract_crosswalk,
            denominators,
            boundary_vintage="2025",
            county_vintage="2020",
            tract_vintage="2020",
            acs_vintage="2023",
        )

        row = result.iloc[0]
        assert row["county_fips"] == "01001"
        assert row["population_weight"] == pytest.approx(1.0)
        assert row["denominator_tract_coverage_ratio"] == pytest.approx(1.0)

    def test_missing_optional_household_columns_produce_nullable_weights(self):
        result = build_tract_mediated_county_crosswalk(
            TRACT_CROSSWALK,
            ACS_TRACTS[["tract_geoid", "total_population"]],
            boundary_vintage="2025",
            county_vintage="2020",
            tract_vintage="2020",
            acs_vintage="2023",
        )

        assert result["population_weight"].notna().any()
        assert result["household_weight"].isna().all()
        assert result["renter_household_weight"].isna().all()

    def test_missing_required_columns_raise_actionable_error(self):
        with pytest.raises(ValueError, match="tract_crosswalk missing required column"):
            build_tract_mediated_county_crosswalk(
                TRACT_CROSSWALK.drop(columns=["intersection_area"]),
                ACS_TRACTS,
                boundary_vintage="2025",
                county_vintage="2020",
                tract_vintage="2020",
                acs_vintage="2023",
            )

    def test_missing_required_denominator_rows_raise_actionable_error(self):
        with pytest.raises(
            ValueError,
            match=(
                "Tract-mediated denominator coverage is incomplete: "
                "B/01001: 1 of 2 tract"
            ),
        ):
            build_tract_mediated_county_crosswalk(
                TRACT_CROSSWALK,
                ACS_TRACTS[ACS_TRACTS["tract_geoid"] != "01001000200"],
                boundary_vintage="2025",
                county_vintage="2020",
                tract_vintage="2020",
                acs_vintage="2023",
            )

    def test_missing_denominator_rows_can_be_materialized_with_diagnostics(self):
        result = build_tract_mediated_county_crosswalk(
            TRACT_CROSSWALK,
            ACS_TRACTS[ACS_TRACTS["tract_geoid"] != "02001000100"],
            boundary_vintage="2025",
            county_vintage="2020",
            tract_vintage="2020",
            acs_vintage="2023",
            allow_incomplete_denominator_coverage=True,
        )

        row = result[(result["coc_id"] == "C") & (result["county_fips"] == "02001")].iloc[0]
        assert row["missing_denominator_tract_count"] == 1
        assert row["denominator_tract_coverage_ratio"] == 0
        assert pd.isna(row["population_weight"])

    def test_rejects_county_vintage_older_than_tract_vintage(self):
        with pytest.raises(
            ValueError,
            match="county_vintage 2010 is older than tract_vintage 2020",
        ):
            build_tract_mediated_county_crosswalk(
                TRACT_CROSSWALK,
                ACS_TRACTS,
                boundary_vintage="2025",
                county_vintage="2010",
                tract_vintage="2020",
                acs_vintage="2023",
            )

    def test_rejects_derived_county_fips_absent_from_expected_universe(self):
        with pytest.raises(ValueError, match="absent from the expected county-FIPS universe"):
            build_tract_mediated_county_crosswalk(
                TRACT_CROSSWALK,
                ACS_TRACTS,
                boundary_vintage="2025",
                county_vintage="2020",
                tract_vintage="2020",
                acs_vintage="2023",
                expected_county_fips=["01001"],
            )

    def test_decennial_denominator_weights_do_not_vary_by_analysis_year(self):
        result_2020 = build_decennial_fixture(2020)
        result_2024 = build_decennial_fixture(2024)

        weight_cols = ["coc_id", "county_fips", "population_weight"]
        pd.testing.assert_frame_equal(result_2020[weight_cols], result_2024[weight_cols])
        assert result_2020["acs_vintage"].isna().all()
        assert set(result_2020["denominator_source"]) == {"decennial"}
        assert set(result_2020["denominator_vintage"]) == {"2020"}

    def test_multi_county_multi_coc_weight_sums_match_coverage_diagnostics(self):
        result = build_fixture()

        county_sums = result.groupby("county_fips", dropna=False).agg(
            area_sum=("area_weight", lambda s: s.sum(min_count=1)),
            population_sum=("population_weight", lambda s: s.sum(min_count=1)),
            area_coverage=("county_area_coverage_ratio", "first"),
            population_coverage=("county_population_coverage_ratio", "first"),
            geo_count=("coc_id", "nunique"),
        )

        row_01001 = county_sums.loc["01001"]
        assert row_01001["geo_count"] == 2
        assert row_01001["area_sum"] == pytest.approx(row_01001["area_coverage"])
        assert row_01001["population_sum"] == pytest.approx(row_01001["population_coverage"])

        row_02001 = county_sums.loc["02001"]
        assert row_02001["geo_count"] == 1
        assert row_02001["area_sum"] == pytest.approx(row_02001["area_coverage"])
        assert pd.isna(row_02001["population_sum"])
        assert row_02001["population_coverage"] == pytest.approx(0.0)

    def test_selected_weighting_summary_reports_only_requested_modes(self):
        result = build_fixture()

        summary = summarize_tract_mediated_crosswalk(
            result,
            selected_weighting_modes=("population", "renter_household"),
        )

        assert summary["county_count"] == 2
        assert summary["selected_weight_columns"] == [
            "population_weight",
            "renter_household_weight",
        ]
        assert summary["available_weight_columns"] == [
            "area_weight",
            "population_weight",
            "household_weight",
            "renter_household_weight",
        ]
        assert summary["min_area_coverage_ratio"] == pytest.approx(1.0)
        assert summary["full_coverage_count"] == 2
        assert summary["population_weight_non_null_count"] == 2
        assert summary["population_weight_max"] == pytest.approx(0.875)
        assert summary["renter_household_weight_non_null_count"] == 2
        assert summary["renter_household_weight_max"] == pytest.approx(0.875)

    def test_save_embeds_provenance(self, tmp_path):
        result = build_fixture()

        output_path = save_tract_mediated_county_crosswalk(
            result,
            boundary_vintage="2025",
            county_vintage="2020",
            tract_vintage="2020",
            acs_vintage="2019-2023",
            output_dir=tmp_path,
        )

        assert output_path.name == ("xwalk_tract_mediated_county__A2023@B2025xC2020xT2020.parquet")
        provenance = read_provenance(output_path)
        assert provenance is not None
        assert provenance.weighting == "tract_mediated"
        assert provenance.extra["dataset_type"] == "tract_mediated_county_crosswalk"
        assert provenance.extra["denominator_source"] == "acs"
        assert provenance.extra["denominator_vintage"] == "2019-2023"
        assert provenance.extra["county_vintage_semantics"] == COUNTY_VINTAGE_SEMANTICS
        assert "renter_household_weight" in provenance.extra["weight_columns"]

    def test_save_decennial_denominator_uses_decennial_filename(self, tmp_path):
        result = build_decennial_fixture(2024)

        output_path = save_tract_mediated_county_crosswalk(
            result,
            boundary_vintage="2025",
            county_vintage="2020",
            tract_vintage="2020",
            denominator_source="decennial",
            denominator_vintage="2020",
            output_dir=tmp_path,
        )

        assert output_path.name == ("xwalk_tract_mediated_county__N2020@B2025xC2020xT2020.parquet")


class TestTractMediatedNaming:
    def test_filename_normalizes_acs_range(self):
        assert (
            tract_mediated_county_xwalk_filename("2025", "2020", "2020", "2019-2023")
            == "xwalk_tract_mediated_county__A2023@B2025xC2020xT2020.parquet"
        )

    def test_filename_supports_decennial_denominator(self):
        assert (
            tract_mediated_county_xwalk_filename(
                "2025",
                "2020",
                "2020",
                denominator_source="decennial",
                denominator_vintage="2020",
            )
            == "xwalk_tract_mediated_county__N2020@B2025xC2020xT2020.parquet"
        )
