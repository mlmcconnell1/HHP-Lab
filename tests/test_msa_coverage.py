"""Tests for MSA-CoC area and population overlap coverage artifacts."""

from __future__ import annotations

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from hhplab.msa.coverage import (
    MSA_COC_COVERAGE_COLUMNS,
    build_msa_coc_coverage,
    read_msa_coc_coverage,
    save_msa_coc_coverage,
    select_primary_msa_for_cocs,
)
from hhplab.provenance import read_provenance
from hhplab.xwalks.county import ALBERS_EQUAL_AREA_CRS

COUNTY_ROWS = [
    ("36061", box(0, 0, 10, 10)),
    ("29510", box(10, 0, 20, 10)),
]

COC_ROWS = [
    ("CO-100", "Full left MSA", box(0, 0, 10, 10)),
    ("CO-200", "Split CoC", box(5, 0, 15, 10)),
]

MSA_MEMBERSHIP_ROWS = [
    ("35620", "35620", "Left MSA", "36061"),
    ("41180", "41180", "Right MSA", "29510"),
]

TRACT_ROWS = [
    ("36061000100", box(0, 0, 5, 10), 0),
    ("36061000200", box(5, 0, 10, 10), 100),
    ("29510000100", box(10, 0, 15, 10), 100),
    ("29510000200", box(15, 0, 20, 10), 0),
]

RANKING_ROWS = [
    ("35620", 2024, 100),
    ("41180", 2024, 100),
]


def _county_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"GEOID": [row[0] for row in COUNTY_ROWS]},
        geometry=[row[1] for row in COUNTY_ROWS],
        crs=ALBERS_EQUAL_AREA_CRS,
    )


def _coc_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "coc_id": [row[0] for row in COC_ROWS],
            "coc_name": [row[1] for row in COC_ROWS],
        },
        geometry=[row[2] for row in COC_ROWS],
        crs=ALBERS_EQUAL_AREA_CRS,
    )


def _msa_membership_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "msa_id": [row[0] for row in MSA_MEMBERSHIP_ROWS],
            "cbsa_code": [row[1] for row in MSA_MEMBERSHIP_ROWS],
            "msa_name": [row[2] for row in MSA_MEMBERSHIP_ROWS],
            "county_fips": [row[3] for row in MSA_MEMBERSHIP_ROWS],
        }
    )


def _ranking_df() -> pd.DataFrame:
    return pd.DataFrame(RANKING_ROWS, columns=["msa_id", "year", "population"])


def _tract_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"GEOID": [row[0] for row in TRACT_ROWS]},
        geometry=[row[1] for row in TRACT_ROWS],
        crs=ALBERS_EQUAL_AREA_CRS,
    )


def _acs5_population_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "GEOID": [row[0] for row in TRACT_ROWS],
            "year": [2023] * len(TRACT_ROWS),
            "total_population": [row[2] for row in TRACT_ROWS],
        }
    )


def _build_coverage(**overrides: object) -> pd.DataFrame:
    kwargs = {
        "year": 2024,
        "top_n": 2,
        "ranking_population_source": "pep",
        "ranking_reference_year": 2024,
        "boundary_vintage": "2025",
        "county_vintage": "2023",
        "definition_version": "census_msa_2023",
        "overlap_bases": ("area", "population"),
        "acs5_population_df": _acs5_population_df(),
        "tract_gdf": _tract_gdf(),
        "acs5_population_vintage": 2023,
    }
    kwargs.update(overrides)
    return build_msa_coc_coverage(
        _coc_gdf(),
        _county_gdf(),
        _msa_membership_df(),
        _ranking_df(),
        **kwargs,
    )


def test_coverage_schema_is_explicit() -> None:
    coverage = _build_coverage()

    assert list(coverage.columns) == list(MSA_COC_COVERAGE_COLUMNS)
    assert set(coverage["overlap_basis"]) == {"area", "population"}
    assert set(coverage["denominator_source"]) == {"geometry", "acs5"}


def test_population_overlap_can_differ_from_area_overlap() -> None:
    coverage = _build_coverage()

    area = coverage[
        (coverage["msa_id"] == "35620")
        & (coverage["coc_id"] == "CO-200")
        & (coverage["overlap_basis"] == "area")
    ].iloc[0]
    population = coverage[
        (coverage["msa_id"] == "35620")
        & (coverage["coc_id"] == "CO-200")
        & (coverage["overlap_basis"] == "population")
    ].iloc[0]

    assert area["msa_covered_by_coc_percent"] == pytest.approx(50.0)
    assert area["coc_contained_in_msa_percent"] == pytest.approx(50.0)
    assert population["msa_covered_by_coc_percent"] == pytest.approx(100.0)
    assert population["coc_contained_in_msa_percent"] == pytest.approx(50.0)
    assert population["denominator_vintage"] == "2023"
    assert population["denominator_column"] == "total_population"


def test_population_overlap_accepts_lowercase_geoid_tract_geometry() -> None:
    tracts = _tract_gdf().rename(columns={"GEOID": "geoid"})

    coverage = _build_coverage(tract_gdf=tracts)

    assert set(coverage["overlap_basis"]) == {"area", "population"}


@pytest.mark.parametrize(
    ("basis", "threshold", "expected_pairs"),
    [
        pytest.param("area", 0.75, {("35620", "CO-100")}, id="area-threshold"),
        pytest.param(
            "population",
            0.75,
            {("35620", "CO-100"), ("35620", "CO-200"), ("41180", "CO-200")},
            id="population-threshold",
        ),
    ],
)
def test_basis_specific_thresholds(
    basis: str,
    threshold: float,
    expected_pairs: set[tuple[str, str]],
) -> None:
    coverage = _build_coverage(
        overlap_bases=(basis,),
        min_msa_area_coverage_share=threshold if basis == "area" else None,
        min_msa_population_coverage_share=threshold if basis == "population" else None,
    )

    observed = set(zip(coverage["msa_id"], coverage["coc_id"], strict=True))
    assert observed == expected_pairs


def test_coc_area_containment_threshold_filters_area_rows() -> None:
    coverage = _build_coverage(
        overlap_bases=("area",),
        min_coc_area_containment_share=0.75,
    )

    observed = set(zip(coverage["msa_id"], coverage["coc_id"], strict=True))
    assert observed == {("35620", "CO-100")}
    assert coverage["coc_contained_in_msa_percent"].tolist() == pytest.approx([100.0])


def test_select_primary_msa_area_basis_is_deterministic() -> None:
    coverage = _build_coverage()

    primary = select_primary_msa_for_cocs(coverage, overlap_basis="area")

    assert list(primary.columns) == [
        "coc_id",
        "primary_msa_id",
        "primary_msa_name",
        "primary_msa_population",
        "primary_msa_overlap_basis",
        "primary_msa_coc_contained_percent",
        "primary_msa_covered_by_coc_percent",
    ]
    assert primary["coc_id"].tolist() == ["CO-100", "CO-200"]
    row = primary[primary["coc_id"] == "CO-200"].iloc[0]
    assert row["primary_msa_id"] == "35620"
    assert row["primary_msa_name"] == "Left MSA"
    assert pd.isna(row["primary_msa_population"])
    assert row["primary_msa_overlap_basis"] == "area"
    assert row["primary_msa_coc_contained_percent"] == pytest.approx(50.0)
    assert row["primary_msa_covered_by_coc_percent"] == pytest.approx(50.0)


def test_select_primary_msa_population_basis_can_differ_from_area() -> None:
    coverage = _build_coverage()

    primary = select_primary_msa_for_cocs(coverage, overlap_basis="population")

    row = primary[primary["coc_id"] == "CO-200"].iloc[0]
    assert row["primary_msa_id"] == "35620"
    assert row["primary_msa_population"] == pytest.approx(100.0)
    assert row["primary_msa_overlap_basis"] == "population"
    assert row["primary_msa_coc_contained_percent"] == pytest.approx(50.0)
    assert row["primary_msa_covered_by_coc_percent"] == pytest.approx(100.0)


def test_select_primary_msa_emits_null_rows_for_missing_overlap() -> None:
    coverage = _build_coverage()

    primary = select_primary_msa_for_cocs(
        coverage,
        coc_ids=("CO-100", "CO-200", "CO-999"),
    )

    row = primary[primary["coc_id"] == "CO-999"].iloc[0]
    assert pd.isna(row["primary_msa_id"])
    assert pd.isna(row["primary_msa_name"])
    assert pd.isna(row["primary_msa_population"])
    assert pd.isna(row["primary_msa_overlap_basis"])
    assert pd.isna(row["primary_msa_coc_contained_percent"])
    assert pd.isna(row["primary_msa_covered_by_coc_percent"])


def test_select_primary_msa_threshold_nulls_below_minimum() -> None:
    coverage = _build_coverage()

    primary = select_primary_msa_for_cocs(
        coverage,
        coc_ids=("CO-100", "CO-200"),
        min_coc_contained_share=0.75,
    )

    assert primary.loc[primary["coc_id"] == "CO-100", "primary_msa_id"].iloc[0] == "35620"
    assert pd.isna(primary.loc[primary["coc_id"] == "CO-200", "primary_msa_id"].iloc[0])


def test_select_primary_msa_accepts_area_crosswalk_shape() -> None:
    crosswalk = pd.DataFrame(
        {
            "coc_id": ["CO-100", "CO-100", "CO-200"],
            "msa_id": ["41180", "35620", "41180"],
            "allocation_share": [0.50, 0.50, 0.25],
        }
    )

    primary = select_primary_msa_for_cocs(
        crosswalk,
        coc_ids=("CO-100", "CO-200", "CO-999"),
    )

    assert primary.loc[primary["coc_id"] == "CO-100", "primary_msa_id"].iloc[0] == "35620"
    assert primary.loc[
        primary["coc_id"] == "CO-100",
        "primary_msa_coc_contained_percent",
    ].iloc[0] == pytest.approx(50.0)
    assert pd.isna(
        primary.loc[
            primary["coc_id"] == "CO-100",
            "primary_msa_covered_by_coc_percent",
        ].iloc[0]
    )
    assert pd.isna(primary.loc[primary["coc_id"] == "CO-999", "primary_msa_id"].iloc[0])


def test_population_overlap_requires_complete_acs5_denominators() -> None:
    incomplete = _acs5_population_df().iloc[:-1].copy()

    with pytest.raises(
        ValueError,
        match=(
            "ACS5 tract population denominator coverage is incomplete .* "
            "Run: hhplab ingest acs-population"
        ),
    ):
        _build_coverage(acs5_population_df=incomplete)


def test_zero_population_denominator_yields_null_percentage() -> None:
    population = _acs5_population_df()
    population.loc[population["GEOID"].isin(["29510000100", "29510000200"]), "total_population"] = 0

    coverage = _build_coverage(acs5_population_df=population, overlap_bases=("population",))
    row = coverage[
        (coverage["msa_id"] == "41180")
        & (coverage["coc_id"] == "CO-200")
        & (coverage["overlap_basis"] == "population")
    ].iloc[0]

    assert row["msa_denominator"] == 0
    assert pd.isna(row["msa_covered_by_coc_percent"])
    assert row["coc_contained_in_msa_percent"] == pytest.approx(0.0)


def test_save_coverage_writes_provenance(tmp_path) -> None:
    coverage = _build_coverage()
    path = save_msa_coc_coverage(
        coverage,
        tmp_path / "msa_coc_coverage.parquet",
        year=2024,
        boundary_vintage="2025",
        county_vintage="2023",
        definition_version="census_msa_2023",
        overlap_bases=("area", "population"),
        ranking_population_source="pep",
        ranking_reference_year=2024,
        top_n=2,
        acs5_population_vintage=2023,
        input_artifacts={"acs5_population": "acs5.parquet"},
    )

    provenance = read_provenance(path)
    assert provenance is not None
    assert provenance.boundary_vintage == "2025"
    assert provenance.county_vintage == "2023"
    assert provenance.acs_vintage == "2023"
    assert provenance.extra["dataset_type"] == "msa_coc_coverage"
    assert provenance.extra["overlap_bases"] == ["area", "population"]
    assert provenance.extra["row_count"] == len(coverage)
    assert provenance.extra["selection_diagnostics"]["selected_count"] == 2

    roundtrip = read_msa_coc_coverage(path)
    assert list(roundtrip.columns) == list(MSA_COC_COVERAGE_COLUMNS)
