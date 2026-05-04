"""Tests for MSA PIT aggregation from CoC-native PIT plus CoC->MSA crosswalks."""

from __future__ import annotations

import pandas as pd
import pytest

from hhplab.pit import aggregate_pit_to_msa, save_msa_pit
from hhplab.provenance import read_provenance

CROSSWALK_ROWS = [
    {
        "coc_id": "CO-100",
        "msa_id": "35620",
        "cbsa_code": "35620",
        "boundary_vintage": "2020",
        "county_vintage": "2020",
        "definition_version": "census_msa_2023",
        "allocation_method": "area",
        "share_column": "allocation_share",
        "allocation_share": 1.0,
    },
    {
        "coc_id": "CO-200",
        "msa_id": "35620",
        "cbsa_code": "35620",
        "boundary_vintage": "2020",
        "county_vintage": "2020",
        "definition_version": "census_msa_2023",
        "allocation_method": "area",
        "share_column": "allocation_share",
        "allocation_share": 0.5,
    },
    {
        "coc_id": "CO-200",
        "msa_id": "41180",
        "cbsa_code": "41180",
        "boundary_vintage": "2020",
        "county_vintage": "2020",
        "definition_version": "census_msa_2023",
        "allocation_method": "area",
        "share_column": "allocation_share",
        "allocation_share": 0.5,
    },
    {
        "coc_id": "CO-300",
        "msa_id": "41180",
        "cbsa_code": "41180",
        "boundary_vintage": "2020",
        "county_vintage": "2020",
        "definition_version": "census_msa_2023",
        "allocation_method": "area",
        "share_column": "allocation_share",
        "allocation_share": 1.0,
    },
]


@pytest.fixture
def msa_crosswalk() -> pd.DataFrame:
    return pd.DataFrame(CROSSWALK_ROWS)


@pytest.fixture
def coc_pit() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "coc_id": ["CO-100", "CO-200", "CO-300", "CO-100", "CO-200", "CO-300"],
            "pit_year": [2020, 2020, 2020, 2021, 2021, 2021],
            "pit_total": [100.0, 80.0, 60.0, 120.0, 100.0, 50.0],
            "pit_sheltered": [60.0, 40.0, 30.0, 70.0, 55.0, 25.0],
            "pit_unsheltered": [40.0, 40.0, 30.0, 50.0, 45.0, 25.0],
        }
    )


class TestAggregatePitToMsa:
    def test_output_columns(self, coc_pit: pd.DataFrame, msa_crosswalk: pd.DataFrame):
        result = aggregate_pit_to_msa(coc_pit, msa_crosswalk)
        assert "msa_id" in result.columns
        assert "coc_id" not in result.columns
        assert "boundary_vintage" in result.columns
        assert "county_vintage" in result.columns
        assert "definition_version" in result.columns

    def test_split_coc_allocation(self, coc_pit: pd.DataFrame, msa_crosswalk: pd.DataFrame):
        result = aggregate_pit_to_msa(coc_pit, msa_crosswalk)
        ny = result[(result["msa_id"] == "35620") & (result["year"] == 2020)].iloc[0]
        stl = result[(result["msa_id"] == "41180") & (result["year"] == 2020)].iloc[0]

        assert ny["pit_total"] == pytest.approx(140.0)
        assert ny["pit_sheltered"] == pytest.approx(80.0)
        assert ny["pit_unsheltered"] == pytest.approx(60.0)

        assert stl["pit_total"] == pytest.approx(100.0)
        assert stl["pit_sheltered"] == pytest.approx(50.0)
        assert stl["pit_unsheltered"] == pytest.approx(50.0)

    def test_multi_year_totals(self, coc_pit: pd.DataFrame, msa_crosswalk: pd.DataFrame):
        result = aggregate_pit_to_msa(coc_pit, msa_crosswalk)
        ny_2021 = result[(result["msa_id"] == "35620") & (result["year"] == 2021)].iloc[0]
        stl_2021 = result[(result["msa_id"] == "41180") & (result["year"] == 2021)].iloc[0]

        assert ny_2021["pit_total"] == pytest.approx(170.0)
        assert stl_2021["pit_total"] == pytest.approx(100.0)

    def test_missing_coc_reduces_coverage(self, msa_crosswalk: pd.DataFrame):
        partial_pit = pd.DataFrame(
            {
                "coc_id": ["CO-100", "CO-300"],
                "pit_year": [2020, 2020],
                "pit_total": [100.0, 60.0],
            }
        )
        result = aggregate_pit_to_msa(partial_pit, msa_crosswalk)
        ny = result[(result["msa_id"] == "35620") & (result["year"] == 2020)].iloc[0]
        stl = result[(result["msa_id"] == "41180") & (result["year"] == 2020)].iloc[0]

        assert ny["allocation_coverage_ratio"] == pytest.approx(2.0 / 3.0)
        assert "CO-200" in ny["missing_cocs"]
        assert stl["allocation_coverage_ratio"] == pytest.approx(2.0 / 3.0)
        assert "CO-200" in stl["missing_cocs"]

    def test_zero_coverage_rows_are_retained(self, msa_crosswalk: pd.DataFrame):
        empty_pit = pd.DataFrame(
            {
                "coc_id": ["ZZ-999"],
                "pit_year": [2020],
                "pit_total": [10.0],
            }
        )
        result = aggregate_pit_to_msa(empty_pit, msa_crosswalk)
        assert set(result["msa_id"]) == {"35620", "41180"}
        assert result["pit_total"].isna().all()
        assert (result["allocation_coverage_ratio"] == 0.0).all()

    def test_found_coc_tuples_do_not_use_pd_isna(
        self,
        coc_pit: pd.DataFrame,
        msa_crosswalk: pd.DataFrame,
        monkeypatch: pytest.MonkeyPatch,
    ):
        real_isna = pd.isna

        def strict_isna(value):
            if isinstance(value, tuple):
                raise ValueError("pd.isna called on tuple")
            return real_isna(value)

        monkeypatch.setattr("hhplab.pit.msa.pd.isna", strict_isna)

        result = aggregate_pit_to_msa(coc_pit, msa_crosswalk)

        assert set(result["msa_id"]) == {"35620", "41180"}

    def test_missing_crosswalk_columns_raise(self, coc_pit: pd.DataFrame):
        bad = pd.DataFrame({"coc_id": ["CO-100"], "msa_id": ["35620"]})
        with pytest.raises(ValueError, match="CoC-to-MSA crosswalk must have columns"):
            aggregate_pit_to_msa(coc_pit, bad)

    @pytest.mark.parametrize(
        ("column_name", "expected_dtype"),
        [
            ("pit_total", "Float64"),
            ("pit_sheltered", "Float64"),
            ("pit_unsheltered", "Float64"),
            ("covered_coc_count", "int64"),
            ("expected_coc_count", "int64"),
            ("allocation_share_sum", "float64"),
            ("expected_allocation_share_sum", "float64"),
            ("allocation_coverage_ratio", "float64"),
        ],
        ids=[
            "pit_total-Float64",
            "pit_sheltered-Float64",
            "pit_unsheltered-Float64",
            "covered_coc_count-int64",
            "expected_coc_count-int64",
            "allocation_share_sum-float64",
            "expected_allocation_share_sum-float64",
            "allocation_coverage_ratio-float64",
        ],
    )
    def test_output_dtypes(
        self,
        coc_pit: pd.DataFrame,
        msa_crosswalk: pd.DataFrame,
        column_name: str,
        expected_dtype: str,
    ):
        result = aggregate_pit_to_msa(coc_pit, msa_crosswalk)
        assert str(result[column_name].dtype) == expected_dtype


def test_save_msa_pit_embeds_expected_provenance(
    coc_pit: pd.DataFrame,
    msa_crosswalk: pd.DataFrame,
    tmp_path,
):
    result = aggregate_pit_to_msa(coc_pit, msa_crosswalk)
    written = save_msa_pit(
        result[result["year"] == 2020].reset_index(drop=True),
        pit_year=2020,
        definition_version="census_msa_2023",
        boundary_vintage="2020",
        county_vintage="2020",
        output_dir=tmp_path,
    )

    provenance = read_provenance(written)
    assert provenance is not None
    assert provenance.boundary_vintage == "2020"
    assert provenance.county_vintage == "2020"
    assert provenance.geo_type == "msa"
    assert provenance.definition_version == "census_msa_2023"
    assert provenance.weighting == "area"
    assert provenance.extra["dataset_type"] == "msa_pit"
    assert provenance.extra["pit_year"] == 2020
    assert provenance.extra["source_geometry"] == "coc"
    assert provenance.extra["share_column"] == "allocation_share"
    assert provenance.extra["allocation_method"] == "area"
