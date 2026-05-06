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

from hhplab.naming import tract_mediated_county_xwalk_filename
from hhplab.provenance import read_provenance
from hhplab.xwalks.tract_mediated import (
    COUNTY_VINTAGE_SEMANTICS,
    build_tract_mediated_county_crosswalk,
    save_tract_mediated_county_crosswalk,
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

    def test_rejects_county_vintage_older_than_tract_vintage(self):
        with pytest.raises(ValueError, match="county_vintage 2010 is older than tract_vintage 2020"):
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
