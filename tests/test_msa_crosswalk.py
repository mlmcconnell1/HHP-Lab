"""Tests for CoC-to-MSA PIT allocation crosswalks."""

from __future__ import annotations

import json

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from hhplab.msa.crosswalk import (
    ALLOCATION_SHARE_TOLERANCE,
    COC_MSA_BLOCK_POPULATION_CROSSWALK_COLUMNS,
    COC_MSA_CROSSWALK_COLUMNS,
    _block_coc_msa_intersections,
    _block_geometry_denominator,
    _build_msa_geometry_from_counties,
    _coverage_share_from_denominator,
    _prepare_blocks,
    _summarize_block_denominator,
    _summarize_msa_block_denominator_from_membership,
    _validate_coverage_shares,
    aggregate_coc_to_msa_fractional_rollup,
    build_coc_msa_block_population_crosswalk,
    build_coc_msa_crosswalk,
    read_coc_msa_block_population_crosswalk,
    read_coc_msa_crosswalk,
    save_coc_msa_block_population_crosswalk,
    save_coc_msa_crosswalk,
    summarize_coc_msa_allocation,
)
from hhplab.provenance import read_provenance
from hhplab.xwalks.county import ALBERS_EQUAL_AREA_CRS

COUNTY_GEOMETRY_ROWS = [
    ("36061", box(0, 0, 10, 10)),
    ("29510", box(10, 0, 20, 10)),
]

COC_GEOMETRY_ROWS = [
    ("CO-100", box(0, 0, 10, 10)),
    ("CO-200", box(5, 0, 15, 10)),
    ("CO-300", box(10, 0, 15, 10)),
    ("CO-400", box(15, 0, 20, 10)),
    ("CO-900", box(25, 0, 30, 10)),
]

MSA_COUNTY_MEMBERSHIP_ROWS = [
    ("35620", "35620", "36061"),
    ("41180", "41180", "29510"),
]

# Truth table keyed by (coc_id, msa_id).
EXPECTED_ALLOCATION_SHARES: dict[tuple[str, str], float] = {
    ("CO-100", "35620"): 1.0,
    ("CO-200", "35620"): 0.5,
    ("CO-200", "41180"): 0.5,
    ("CO-300", "41180"): 1.0,
    ("CO-400", "41180"): 1.0,
}

EXPECTED_INTERSECTION_AREAS: dict[tuple[str, str], float] = {
    ("CO-100", "35620"): 100.0,
    ("CO-200", "35620"): 50.0,
    ("CO-200", "41180"): 50.0,
    ("CO-300", "41180"): 50.0,
    ("CO-400", "41180"): 50.0,
}

EXPECTED_UNALLOCATED_SHARE: dict[str, float] = {
    "CO-100": 0.0,
    "CO-200": 0.0,
    "CO-300": 0.0,
    "CO-400": 0.0,
}

BLOCK_GEOMETRY_ROWS = [
    ("360610001001000", box(0, 0, 10, 10)),
    ("295100001001000", box(10, 0, 20, 10)),
    ("060750001001000", box(20, 0, 30, 10)),
    ("170310001001000", box(30, 0, 40, 10)),
]

BLOCK_POPULATION_ROWS = [
    ("360610001001000", 100),
    ("295100001001000", 300),
    ("060750001001000", None),
    ("170310001001000", 0),
]

BLOCK_POPULATION_COC_ROWS = [
    ("CO-200", box(5, 0, 15, 10)),
    ("CO-600", box(20, 0, 25, 10)),
    ("CO-700", box(30, 0, 35, 10)),
]

BLOCK_POPULATION_COUNTY_ROWS = [
    ("36061", box(0, 0, 10, 10)),
    ("29510", box(10, 0, 20, 10)),
    ("06075", box(20, 0, 30, 10)),
    ("17031", box(30, 0, 40, 10)),
]

BLOCK_POPULATION_MEMBERSHIP_ROWS = [
    ("35620", "35620", "36061"),
    ("41180", "41180", "29510"),
    ("41860", "41860", "06075"),
    ("16980", "16980", "17031"),
]

CT_LEGACY_BLOCK_GEOID = "090010001001000"
CT_PLANNING_REGION_COUNTY_FIPS = "09110"
CT_BLOCK_POPULATION = 250
CT_BLOCK_POPULATION_COC_ID = "CT-503"
CT_BLOCK_POPULATION_MSA_ID = "14860"

EXPECTED_BLOCK_POPULATION_ROWS = {
    ("CO-200", "35620"): {
        "intersection_population": 50.0,
        "allocation_share": 0.25,
        "coc_population_denominator": 200.0,
        "msa_population_denominator": 100.0,
        "msa_population_coverage_share": 0.5,
        "missing_population_block_count": 0,
        "partial_coc_population_coverage": False,
    },
    ("CO-200", "41180"): {
        "intersection_population": 150.0,
        "allocation_share": 0.75,
        "coc_population_denominator": 200.0,
        "msa_population_denominator": 300.0,
        "msa_population_coverage_share": 0.5,
        "missing_population_block_count": 0,
        "partial_coc_population_coverage": False,
    },
    ("CO-600", "41860"): {
        "intersection_population": 0.0,
        "allocation_share": 0.0,
        "coc_population_denominator": 0.0,
        "msa_population_denominator": 0.0,
        "msa_population_coverage_share": 0.0,
        "missing_population_block_count": 1,
        "partial_coc_population_coverage": True,
    },
    ("CO-700", "16980"): {
        "intersection_population": 0.0,
        "allocation_share": 0.0,
        "coc_population_denominator": 0.0,
        "msa_population_denominator": 0.0,
        "msa_population_coverage_share": 0.0,
        "missing_population_block_count": 0,
        "partial_coc_population_coverage": False,
    },
}

# Input validation truth table:
# - missing CoC id column -> coc_gdf 'coc_id' error
# - missing CoC geometry column -> coc_gdf 'geometry' error
# - missing county geometry column -> county_gdf 'geometry' error
MISSING_INPUT_COLUMN_CASES = [
    pytest.param("coc", "coc_id", "coc_gdf must have 'coc_id' column", id="coc-id"),
    pytest.param(
        "coc",
        "geometry",
        "coc_gdf must have 'geometry' column",
        id="coc-geometry",
    ),
    pytest.param(
        "county",
        "geometry",
        "county_gdf must have 'geometry' column",
        id="county-geometry",
    ),
]

DENOMINATOR_GEOMETRY_ROWS = [
    ("G1", box(0, 0, 25, 10)),
    ("G2", box(15, 0, 30, 10)),
]

DENOMINATOR_BLOCK_ROWS = [
    ("B_FULL", box(0, 0, 10, 10), 100),
    ("B_PARTIAL", box(10, 0, 20, 10), 200),
    ("B_SHARED_FULL", box(20, 0, 25, 10), 300),
]

EXPECTED_DENOMINATOR_ROWS = {
    ("G1", "B_FULL"): {
        "denominator_intersection_area": 100.0,
        "allocated_population": 100.0,
    },
    ("G1", "B_PARTIAL"): {
        "denominator_intersection_area": 100.0,
        "allocated_population": 200.0,
    },
    ("G2", "B_PARTIAL"): {
        "denominator_intersection_area": 50.0,
        "allocated_population": 100.0,
    },
    ("G1", "B_SHARED_FULL"): {
        "denominator_intersection_area": 50.0,
        "allocated_population": 300.0,
    },
    ("G2", "B_SHARED_FULL"): {
        "denominator_intersection_area": 50.0,
        "allocated_population": 300.0,
    },
}


def _county_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"GEOID": [row[0] for row in COUNTY_GEOMETRY_ROWS]},
        geometry=[row[1] for row in COUNTY_GEOMETRY_ROWS],
        crs=ALBERS_EQUAL_AREA_CRS,
    )


def _coc_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"coc_id": [row[0] for row in COC_GEOMETRY_ROWS]},
        geometry=[row[1] for row in COC_GEOMETRY_ROWS],
        crs=ALBERS_EQUAL_AREA_CRS,
    )


def _msa_membership_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "msa_id": [row[0] for row in MSA_COUNTY_MEMBERSHIP_ROWS],
            "cbsa_code": [row[1] for row in MSA_COUNTY_MEMBERSHIP_ROWS],
            "county_fips": [row[2] for row in MSA_COUNTY_MEMBERSHIP_ROWS],
            "definition_version": ["census_msa_2023"] * len(MSA_COUNTY_MEMBERSHIP_ROWS),
        }
    )


def _block_population_coc_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"coc_id": [row[0] for row in BLOCK_POPULATION_COC_ROWS]},
        geometry=[row[1] for row in BLOCK_POPULATION_COC_ROWS],
        crs=ALBERS_EQUAL_AREA_CRS,
    )


def _block_population_county_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"GEOID": [row[0] for row in BLOCK_POPULATION_COUNTY_ROWS]},
        geometry=[row[1] for row in BLOCK_POPULATION_COUNTY_ROWS],
        crs=ALBERS_EQUAL_AREA_CRS,
    )


def _block_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"block_geoid": [row[0] for row in BLOCK_GEOMETRY_ROWS]},
        geometry=[row[1] for row in BLOCK_GEOMETRY_ROWS],
        crs=ALBERS_EQUAL_AREA_CRS,
    )


def _block_population_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "block_geoid": [row[0] for row in BLOCK_POPULATION_ROWS],
            "total_population": [row[1] for row in BLOCK_POPULATION_ROWS],
        }
    )


def _denominator_geometry_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"geometry_id": [row[0] for row in DENOMINATOR_GEOMETRY_ROWS]},
        geometry=[row[1] for row in DENOMINATOR_GEOMETRY_ROWS],
        crs=ALBERS_EQUAL_AREA_CRS,
    )


def _denominator_block_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"block_geoid": [row[0] for row in DENOMINATOR_BLOCK_ROWS]},
        geometry=[row[1] for row in DENOMINATOR_BLOCK_ROWS],
        crs=ALBERS_EQUAL_AREA_CRS,
    )


def _denominator_block_population_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "block_geoid": [row[0] for row in DENOMINATOR_BLOCK_ROWS],
            "total_population": [row[2] for row in DENOMINATOR_BLOCK_ROWS],
        }
    )


def _block_population_membership_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "msa_id": [row[0] for row in BLOCK_POPULATION_MEMBERSHIP_ROWS],
            "cbsa_code": [row[1] for row in BLOCK_POPULATION_MEMBERSHIP_ROWS],
            "county_fips": [row[2] for row in BLOCK_POPULATION_MEMBERSHIP_ROWS],
            "definition_version": ["census_msa_2023"] * len(BLOCK_POPULATION_MEMBERSHIP_ROWS),
        }
    )


def _overlay_block_geometry_denominator_reference(
    geometry_gdf: gpd.GeoDataFrame,
    blocks: gpd.GeoDataFrame,
) -> pd.DataFrame:
    intersections = gpd.overlay(
        geometry_gdf[["geometry_id", "geometry"]],
        blocks[["block_geoid", "total_population", "block_area", "geometry"]],
        how="intersection",
        keep_geom_type=False,
    )
    intersections = intersections.loc[~intersections.geometry.is_empty].copy()
    intersections["denominator_intersection_area"] = intersections.geometry.area
    intersections = intersections.loc[intersections["denominator_intersection_area"] > 0].copy()
    intersections["total_population"] = pd.to_numeric(
        intersections["total_population"],
        errors="coerce",
    )
    intersections["missing_population"] = intersections["total_population"].isna()
    intersections["allocated_population"] = (
        intersections["total_population"].fillna(0.0)
        * intersections["denominator_intersection_area"]
        / intersections["block_area"]
    )
    return intersections


def _overlay_block_coc_msa_intersections_reference(
    coc_blocks: gpd.GeoDataFrame,
    msa_gdf: gpd.GeoDataFrame,
) -> pd.DataFrame:
    intersections = gpd.overlay(
        coc_blocks[
            [
                "coc_id",
                "block_geoid",
                "total_population",
                "block_area",
                "missing_population",
                "geometry",
            ]
        ],
        msa_gdf[["msa_id", "cbsa_code", "geometry"]],
        how="intersection",
        keep_geom_type=False,
    )
    intersections = intersections.loc[~intersections.geometry.is_empty].copy()
    intersections["intersection_area"] = intersections.geometry.area
    intersections = intersections.loc[intersections["intersection_area"] > 0].copy()
    intersections["intersection_population"] = (
        intersections["total_population"].fillna(0.0)
        * intersections["intersection_area"]
        / intersections["block_area"]
    )
    return intersections


def _denominator_comparison_frame(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "geometry_id",
        "block_geoid",
        "total_population",
        "block_area",
        "denominator_intersection_area",
        "allocated_population",
        "missing_population",
    ]
    return df.loc[:, columns].sort_values(["geometry_id", "block_geoid"]).reset_index(drop=True)


def _msa_denominator_comparison_frame(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "msa_id",
        "msa_population_denominator",
        "msa_intersection_area",
        "msa_missing_population_block_count",
    ]
    return df.loc[:, columns].sort_values("msa_id").reset_index(drop=True)


def _block_coc_msa_intersections_comparison_frame(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["coc_id", "msa_id", "cbsa_code"], as_index=False)
        .agg(
            intersection_area=("intersection_area", "sum"),
            intersection_population=("intersection_population", "sum"),
            block_count=("block_geoid", "nunique"),
            missing_population_block_count=(
                "missing_population",
                lambda s: int(s.fillna(False).sum()),
            ),
        )
        .sort_values(["coc_id", "msa_id"])
        .reset_index(drop=True)
    )


@pytest.fixture
def coc_msa_crosswalk() -> pd.DataFrame:
    return build_coc_msa_crosswalk(
        _coc_gdf(),
        _county_gdf(),
        _msa_membership_df(),
        boundary_vintage="2025",
        county_vintage="2023",
        definition_version="census_msa_2023",
    )


def test_schema_is_explicit_and_auditable(coc_msa_crosswalk: pd.DataFrame):
    assert list(coc_msa_crosswalk.columns) == list(COC_MSA_CROSSWALK_COLUMNS)
    assert set(coc_msa_crosswalk["share_column"]) == {"allocation_share"}
    assert set(coc_msa_crosswalk["share_denominator"]) == {"coc_area"}


def test_fractional_rollup_aggregates_area_crosswalk_additive_measures(
    coc_msa_crosswalk: pd.DataFrame,
):
    source = pd.DataFrame(
        {
            "coc_id": ["CO-100", "CO-200", "CO-300", "CO-400"],
            "year": [2020, 2020, 2020, 2020],
            "pit_total": [100.0, 80.0, 60.0, 40.0],
            "pit_sheltered": [60.0, 40.0, 30.0, 20.0],
        }
    )

    result = aggregate_coc_to_msa_fractional_rollup(
        source,
        coc_msa_crosswalk,
        additive_measure_columns=("pit_total", "pit_sheltered"),
        source_dataset_id="pit_coc",
        native_msa_covariate_columns=("msa_population",),
    )

    ny = result[(result["msa_id"] == "35620") & (result["year"] == 2020)].iloc[0]
    stl = result[(result["msa_id"] == "41180") & (result["year"] == 2020)].iloc[0]
    assert ny["pit_total"] == pytest.approx(140.0)
    assert ny["pit_sheltered"] == pytest.approx(80.0)
    assert stl["pit_total"] == pytest.approx(140.0)
    assert stl["pit_sheltered"] == pytest.approx(70.0)
    assert ny["allocation_basis"] == "area"
    assert ny["denominator_source"] == "geometry_area"
    assert ny["source_additive_measure_columns"] == "pit_total,pit_sheltered"
    assert ny["native_msa_covariate_columns"] == "msa_population"
    assert ny["covered_coc_count"] == 2
    assert ny["missing_coc_count"] == 0
    assert ny["allocation_share_sum"] == pytest.approx(1.5)
    assert ny["expected_allocation_share_sum"] == pytest.approx(1.5)
    assert ny["coc_population_coverage_ratio"] == pytest.approx(1.0)
    assert ny["msa_population_coverage_ratio"] == pytest.approx(1.0)


def test_fractional_rollup_reports_missing_source_cocs(coc_msa_crosswalk: pd.DataFrame):
    source = pd.DataFrame(
        {
            "coc_id": ["CO-100"],
            "year": [2020],
            "pit_total": [100.0],
        }
    )

    result = aggregate_coc_to_msa_fractional_rollup(
        source,
        coc_msa_crosswalk,
        additive_measure_columns=("pit_total",),
        source_dataset_id="pit_coc",
    )

    ny = result[(result["msa_id"] == "35620") & (result["year"] == 2020)].iloc[0]
    stl = result[(result["msa_id"] == "41180") & (result["year"] == 2020)].iloc[0]
    assert ny["pit_total"] == pytest.approx(100.0)
    assert ny["missing_cocs"] == "CO-200"
    assert ny["missing_coc_count"] == 1
    assert ny["coc_population_coverage_ratio"] == pytest.approx(2.0 / 3.0)
    assert ny["msa_population_coverage_ratio"] == pytest.approx(2.0 / 3.0)
    assert pd.isna(stl["pit_total"])
    assert stl["missing_cocs"] == "CO-200,CO-300,CO-400"
    assert stl["coc_population_coverage_ratio"] == pytest.approx(0.0)
    assert stl["msa_population_coverage_ratio"] == pytest.approx(0.0)


def test_fractional_rollup_reports_unmapped_source_cocs(coc_msa_crosswalk: pd.DataFrame):
    source = pd.DataFrame(
        {
            "coc_id": ["CO-100", "CO-999-UNMAPPED"],
            "year": [2020, 2020],
            "pit_total": [100.0, 9999.0],
            "pit_sheltered": [60.0, 6000.0],
        }
    )

    result = aggregate_coc_to_msa_fractional_rollup(
        source,
        coc_msa_crosswalk[coc_msa_crosswalk["coc_id"] == "CO-100"],
        additive_measure_columns=("pit_total", "pit_sheltered"),
        source_dataset_id="pit_coc",
    )

    row = result.iloc[0]
    assert row["pit_total"] == pytest.approx(100.0)
    assert row["unmapped_source_coc_count"] == 1
    assert row["unmapped_source_cocs"] == "CO-999-UNMAPPED"
    assert json.loads(row["unmapped_source_additive_measure_totals"]) == {
        "pit_sheltered": 6000.0,
        "pit_total": 9999.0,
    }


def test_fractional_rollup_rejects_duplicate_source_coc_year_rows(
    coc_msa_crosswalk: pd.DataFrame,
):
    source = pd.DataFrame(
        {
            "coc_id": ["CO-100", "CO-100"],
            "year": [2020, 2020],
            "pit_total": [100.0, 100.0],
        }
    )

    with pytest.raises(
        ValueError,
        match=(
            "CoC source data must be unique by coc_id and year.*"
            "CO-100/2020.*Aggregate or deduplicate"
        ),
    ):
        aggregate_coc_to_msa_fractional_rollup(
            source,
            coc_msa_crosswalk,
            additive_measure_columns=("pit_total",),
            source_dataset_id="pit_coc",
        )


def test_fractional_rollup_thresholds_filter_crosswalk_rows(
    coc_msa_crosswalk: pd.DataFrame,
):
    source = pd.DataFrame(
        {
            "coc_id": ["CO-100", "CO-200", "CO-300", "CO-400"],
            "year": [2020, 2020, 2020, 2020],
            "pit_total": [100.0, 80.0, 60.0, 40.0],
        }
    )

    result = aggregate_coc_to_msa_fractional_rollup(
        source,
        coc_msa_crosswalk,
        additive_measure_columns=("pit_total",),
        source_dataset_id="pit_coc",
        min_allocation_share=0.75,
    )

    assert set(result["msa_id"]) == {"35620", "41180"}
    ny = result[(result["msa_id"] == "35620") & (result["year"] == 2020)].iloc[0]
    stl = result[(result["msa_id"] == "41180") & (result["year"] == 2020)].iloc[0]
    assert ny["pit_total"] == pytest.approx(100.0)
    assert stl["pit_total"] == pytest.approx(100.0)
    assert "CO-200" not in set(result["missing_cocs"].str.split(",").sum())


def test_block_population_rollup_matches_area_when_allocation_shares_match(
    coc_msa_crosswalk: pd.DataFrame,
):
    source = pd.DataFrame(
        {
            "coc_id": ["CO-100", "CO-200", "CO-300", "CO-400"],
            "year": [2020, 2020, 2020, 2020],
            "pit_total": [100.0, 80.0, 60.0, 40.0],
        }
    )
    block_like_crosswalk = coc_msa_crosswalk.copy()
    block_like_crosswalk["block_vintage"] = "2020"
    block_like_crosswalk["decennial_vintage"] = "2020"
    block_like_crosswalk["allocation_method"] = "block_population"
    block_like_crosswalk["share_denominator"] = "coc_population_denominator"
    block_like_crosswalk["intersection_population"] = (
        block_like_crosswalk["allocation_share"] * 100.0
    )
    block_like_crosswalk["coc_population_denominator"] = 100.0
    block_like_crosswalk["msa_population_denominator"] = 100.0
    block_like_crosswalk["coc_population_containment_share"] = block_like_crosswalk[
        "allocation_share"
    ]
    block_like_crosswalk["msa_population_coverage_share"] = block_like_crosswalk["allocation_share"]
    block_like_crosswalk["coc_intersection_area"] = block_like_crosswalk["intersection_area"]
    block_like_crosswalk["msa_intersection_area"] = block_like_crosswalk["intersection_area"]
    block_like_crosswalk["block_count"] = 1
    block_like_crosswalk["missing_population_block_count"] = 0
    block_like_crosswalk["zero_population_coc"] = False
    block_like_crosswalk["partial_coc_population_coverage"] = False
    block_like_crosswalk = block_like_crosswalk.loc[
        :,
        COC_MSA_BLOCK_POPULATION_CROSSWALK_COLUMNS,
    ]

    area = aggregate_coc_to_msa_fractional_rollup(
        source,
        coc_msa_crosswalk,
        additive_measure_columns=("pit_total",),
        source_dataset_id="pit_coc",
    )
    block_population = aggregate_coc_to_msa_fractional_rollup(
        source,
        block_like_crosswalk,
        additive_measure_columns=("pit_total",),
        source_dataset_id="pit_coc",
    )

    area_values = area[["msa_id", "year", "pit_total"]].reset_index(drop=True)
    block_values = block_population[["msa_id", "year", "pit_total"]].reset_index(drop=True)
    pd.testing.assert_frame_equal(block_values, area_values)
    assert block_population["allocation_basis"].unique().tolist() == ["block_population"]
    assert block_population["denominator_source"].unique().tolist() == [
        "pl_94_171_block_population"
    ]


def test_fractional_rollup_msa_population_coverage_uses_source_covered_cocs(
    coc_msa_crosswalk: pd.DataFrame,
):
    source = pd.DataFrame(
        {
            "coc_id": ["CO-100"],
            "year": [2020],
            "pit_total": [100.0],
        }
    )
    block_like_crosswalk = coc_msa_crosswalk[(coc_msa_crosswalk["msa_id"] == "35620")].copy()
    block_like_crosswalk["allocation_method"] = "block_population"
    block_like_crosswalk["msa_population_coverage_share"] = block_like_crosswalk["coc_id"].map(
        {"CO-100": 0.4, "CO-200": 0.6}
    )

    result = aggregate_coc_to_msa_fractional_rollup(
        source,
        block_like_crosswalk,
        additive_measure_columns=("pit_total",),
        source_dataset_id="pit_coc",
    )

    row = result.iloc[0]
    assert row["pit_total"] == pytest.approx(100.0)
    assert row["missing_cocs"] == "CO-200"
    assert row["msa_population_coverage_ratio"] == pytest.approx(0.4)


@pytest.fixture
def block_population_crosswalk() -> pd.DataFrame:
    return build_coc_msa_block_population_crosswalk(
        _block_population_coc_gdf(),
        _block_population_county_gdf(),
        _block_population_membership_df(),
        _block_gdf(),
        _block_population_df(),
        boundary_vintage="2025",
        county_vintage="2023",
        block_vintage="2020",
        decennial_vintage="2020",
        definition_version="census_msa_2023",
    )


def test_block_population_schema_is_explicit_and_auditable(
    block_population_crosswalk: pd.DataFrame,
):
    assert list(block_population_crosswalk.columns) == list(
        COC_MSA_BLOCK_POPULATION_CROSSWALK_COLUMNS
    )
    assert set(block_population_crosswalk["allocation_method"]) == {"block_population"}
    assert set(block_population_crosswalk["share_denominator"]) == {"coc_population_denominator"}


@pytest.mark.parametrize(
    ("coc_id", "msa_id"),
    list(EXPECTED_BLOCK_POPULATION_ROWS),
    ids=[f"{coc_id}-{msa_id}" for coc_id, msa_id in EXPECTED_BLOCK_POPULATION_ROWS],
)
def test_block_population_truth_table_allocations(
    block_population_crosswalk: pd.DataFrame,
    coc_id: str,
    msa_id: str,
):
    expected = EXPECTED_BLOCK_POPULATION_ROWS[(coc_id, msa_id)]
    row = block_population_crosswalk[
        (block_population_crosswalk["coc_id"] == coc_id)
        & (block_population_crosswalk["msa_id"] == msa_id)
    ].iloc[0]

    assert row["intersection_population"] == pytest.approx(expected["intersection_population"])
    assert row["allocation_share"] == pytest.approx(expected["allocation_share"])
    assert row["coc_population_denominator"] == pytest.approx(
        expected["coc_population_denominator"]
    )
    assert row["msa_population_denominator"] == pytest.approx(
        expected["msa_population_denominator"]
    )
    assert row["msa_population_coverage_share"] == pytest.approx(
        expected["msa_population_coverage_share"]
    )
    assert row["missing_population_block_count"] == expected["missing_population_block_count"]
    assert (
        bool(row["partial_coc_population_coverage"]) is expected["partial_coc_population_coverage"]
    )


def test_block_coc_msa_intersections_match_overlay_reference():
    coc = _block_population_coc_gdf()
    blocks = _prepare_blocks(_block_gdf(), _block_population_df())
    membership = _block_population_membership_df()
    county_gdf = _block_population_county_gdf()
    coc_blocks = _block_geometry_denominator(
        coc,
        blocks,
        left_id_col="coc_id",
        output_area_col="coc_intersection_area",
    )
    msa = _build_msa_geometry_from_counties(county_gdf, membership)

    actual = _block_coc_msa_intersections(coc_blocks, membership)
    expected = _overlay_block_coc_msa_intersections_reference(coc_blocks, msa)

    pd.testing.assert_frame_equal(
        _block_coc_msa_intersections_comparison_frame(actual),
        _block_coc_msa_intersections_comparison_frame(expected),
    )


def test_block_population_crosswalk_avoids_coc_msa_overlay(
    monkeypatch: pytest.MonkeyPatch,
):
    def fail_overlay(*args, **kwargs):
        raise AssertionError("block population CoC-MSA build should not call gpd.overlay")

    monkeypatch.setattr(gpd, "overlay", fail_overlay)

    result = build_coc_msa_block_population_crosswalk(
        _block_population_coc_gdf(),
        _block_population_county_gdf(),
        _block_population_membership_df(),
        _block_gdf(),
        _block_population_df(),
        boundary_vintage="2025",
        county_vintage="2023",
        block_vintage="2020",
        decennial_vintage="2020",
        definition_version="census_msa_2023",
    )

    assert set(zip(result["coc_id"], result["msa_id"], strict=True)) == set(
        EXPECTED_BLOCK_POPULATION_ROWS
    )


def test_block_population_crosswalk_bridges_ct_legacy_blocks_to_planning_regions():
    ct_box = box(0, 0, 10, 10)
    coc_gdf = gpd.GeoDataFrame(
        {"coc_id": [CT_BLOCK_POPULATION_COC_ID]},
        geometry=[ct_box],
        crs=ALBERS_EQUAL_AREA_CRS,
    )
    county_gdf = gpd.GeoDataFrame(
        {"GEOID": [CT_PLANNING_REGION_COUNTY_FIPS]},
        geometry=[ct_box],
        crs=ALBERS_EQUAL_AREA_CRS,
    )
    membership = pd.DataFrame(
        {
            "msa_id": [CT_BLOCK_POPULATION_MSA_ID],
            "cbsa_code": [CT_BLOCK_POPULATION_MSA_ID],
            "county_fips": [CT_PLANNING_REGION_COUNTY_FIPS],
            "definition_version": ["census_msa_2023"],
        }
    )
    block_gdf = gpd.GeoDataFrame(
        {"block_geoid": [CT_LEGACY_BLOCK_GEOID]},
        geometry=[ct_box],
        crs=ALBERS_EQUAL_AREA_CRS,
    )
    block_population = pd.DataFrame(
        {
            "block_geoid": [CT_LEGACY_BLOCK_GEOID],
            "total_population": [CT_BLOCK_POPULATION],
        }
    )

    result = build_coc_msa_block_population_crosswalk(
        coc_gdf,
        county_gdf,
        membership,
        block_gdf,
        block_population,
        boundary_vintage="2024",
        county_vintage="2023",
        block_vintage="2020",
        decennial_vintage="2020",
        definition_version="census_msa_2023",
    )

    row = result.iloc[0]
    assert row["coc_id"] == CT_BLOCK_POPULATION_COC_ID
    assert row["msa_id"] == CT_BLOCK_POPULATION_MSA_ID
    assert row["intersection_population"] == pytest.approx(CT_BLOCK_POPULATION)
    assert row["coc_population_denominator"] == pytest.approx(CT_BLOCK_POPULATION)
    assert row["msa_population_denominator"] == pytest.approx(CT_BLOCK_POPULATION)
    assert row["allocation_share"] == pytest.approx(1.0)
    assert row["msa_population_coverage_share"] == pytest.approx(1.0)


def test_block_population_crosswalk_rejects_unmatched_state_block_prefixes():
    coc_gdf = gpd.GeoDataFrame(
        {"coc_id": ["NY-999"]},
        geometry=[box(0, 0, 10, 10)],
        crs=ALBERS_EQUAL_AREA_CRS,
    )
    county_gdf = gpd.GeoDataFrame(
        {"GEOID": ["36062"]},
        geometry=[box(0, 0, 10, 10)],
        crs=ALBERS_EQUAL_AREA_CRS,
    )
    membership = pd.DataFrame(
        {
            "msa_id": ["99999"],
            "cbsa_code": ["99999"],
            "county_fips": ["36062"],
            "definition_version": ["test_msa"],
        }
    )
    block_gdf = gpd.GeoDataFrame(
        {"block_geoid": ["360610001001000"]},
        geometry=[box(0, 0, 10, 10)],
        crs=ALBERS_EQUAL_AREA_CRS,
    )
    block_population = pd.DataFrame({"block_geoid": ["360610001001000"], "total_population": [100]})

    with pytest.raises(
        ValueError,
        match=(
            "Block GEOID county prefixes do not match MSA county membership.*"
            "36 blocks=36061 membership=36062"
        ),
    ):
        build_coc_msa_block_population_crosswalk(
            coc_gdf,
            county_gdf,
            membership,
            block_gdf,
            block_population,
            boundary_vintage="2024",
            county_vintage="2023",
            block_vintage="2020",
            decennial_vintage="2020",
            definition_version="test_msa",
        )


def test_msa_population_coverage_share_zeroes_unusable_denominators():
    coverage = _coverage_share_from_denominator(
        pd.Series([100.0, 100.0, 100.0, None]),
        pd.Series([200.0, 0.0, None, 100.0]),
    )

    assert coverage.tolist() == [0.5, 0.0, 0.0, 0.0]


def test_validate_coverage_shares_rejects_non_finite_values():
    crosswalk = pd.DataFrame(
        {
            "coc_id": ["CO-100"],
            "msa_id": ["99999"],
            "msa_population_coverage_share": [float("inf")],
        }
    )

    with pytest.raises(
        ValueError,
        match="Computed coverage share outside the allowed finite range",
    ):
        _validate_coverage_shares(crosswalk, ("msa_population_coverage_share",))


def test_block_geometry_denominator_matches_overlay_reference():
    geometry_gdf = _denominator_geometry_gdf()
    blocks = _prepare_blocks(_denominator_block_gdf(), _denominator_block_population_df())

    actual = _block_geometry_denominator(
        geometry_gdf,
        blocks,
        left_id_col="geometry_id",
        output_area_col="denominator_intersection_area",
    )
    expected = _overlay_block_geometry_denominator_reference(geometry_gdf, blocks)

    pd.testing.assert_frame_equal(
        _denominator_comparison_frame(actual),
        _denominator_comparison_frame(expected),
    )


@pytest.mark.parametrize(
    ("geometry_id", "block_geoid"),
    list(EXPECTED_DENOMINATOR_ROWS),
    ids=[f"{geometry_id}-{block_geoid}" for geometry_id, block_geoid in EXPECTED_DENOMINATOR_ROWS],
)
def test_block_geometry_denominator_truth_table(
    geometry_id: str,
    block_geoid: str,
):
    expected = EXPECTED_DENOMINATOR_ROWS[(geometry_id, block_geoid)]
    result = _block_geometry_denominator(
        _denominator_geometry_gdf(),
        _prepare_blocks(_denominator_block_gdf(), _denominator_block_population_df()),
        left_id_col="geometry_id",
        output_area_col="denominator_intersection_area",
    )
    row = result[
        (result["geometry_id"] == geometry_id) & (result["block_geoid"] == block_geoid)
    ].iloc[0]

    assert row["denominator_intersection_area"] == pytest.approx(
        expected["denominator_intersection_area"]
    )
    assert row["allocated_population"] == pytest.approx(expected["allocated_population"])


def test_block_geometry_denominator_avoids_full_overlay(monkeypatch: pytest.MonkeyPatch):
    def fail_overlay(*args, **kwargs):
        raise AssertionError("_block_geometry_denominator should not call gpd.overlay")

    monkeypatch.setattr(gpd, "overlay", fail_overlay)

    result = _block_geometry_denominator(
        _denominator_geometry_gdf(),
        _prepare_blocks(_denominator_block_gdf(), _denominator_block_population_df()),
        left_id_col="geometry_id",
        output_area_col="denominator_intersection_area",
    )

    assert set(zip(result["geometry_id"], result["block_geoid"], strict=True)) == set(
        EXPECTED_DENOMINATOR_ROWS
    )


def test_msa_block_denominator_from_membership_matches_overlay_reference():
    membership = pd.DataFrame(
        {
            "msa_id": ["99999", "99999", "99999", "88888"],
            "cbsa_code": ["99999", "99999", "99999", "88888"],
            "county_fips": ["36061", "29510", "06075", "17031"],
        }
    )
    county_gdf = _block_population_county_gdf()
    blocks = _prepare_blocks(_block_gdf(), _block_population_df())
    msa = _build_msa_geometry_from_counties(county_gdf, membership)
    overlay_intersections = _block_geometry_denominator(
        msa,
        blocks,
        left_id_col="msa_id",
        output_area_col="msa_intersection_area",
    )
    expected = _summarize_block_denominator(
        overlay_intersections,
        id_col="msa_id",
        denominator_col="msa_population_denominator",
        area_col="msa_intersection_area",
        output_area_col="msa_intersection_area",
        missing_col="msa_missing_population_block_count",
    )

    actual = _summarize_msa_block_denominator_from_membership(blocks, membership)

    pd.testing.assert_frame_equal(
        _msa_denominator_comparison_frame(actual),
        _msa_denominator_comparison_frame(expected),
    )


def test_block_population_summary_flags_zero_and_missing_population(
    block_population_crosswalk: pd.DataFrame,
):
    missing_row = block_population_crosswalk[
        (block_population_crosswalk["coc_id"] == "CO-600")
        & (block_population_crosswalk["msa_id"] == "41860")
    ].iloc[0]
    zero_row = block_population_crosswalk[
        (block_population_crosswalk["coc_id"] == "CO-700")
        & (block_population_crosswalk["msa_id"] == "16980")
    ].iloc[0]

    assert bool(missing_row["zero_population_coc"]) is True
    assert missing_row["missing_population_block_count"] == 1
    assert bool(missing_row["partial_coc_population_coverage"]) is True
    assert bool(zero_row["zero_population_coc"]) is True
    assert zero_row["missing_population_block_count"] == 0
    assert bool(zero_row["partial_coc_population_coverage"]) is False


def test_fractional_rollup_preserves_block_population_diagnostics(
    block_population_crosswalk: pd.DataFrame,
):
    source = pd.DataFrame(
        {
            "coc_id": ["CO-200", "CO-700"],
            "year": [2020, 2020],
            "pit_total": [80.0, 40.0],
        }
    )

    result = aggregate_coc_to_msa_fractional_rollup(
        source,
        block_population_crosswalk,
        additive_measure_columns=("pit_total",),
        source_dataset_id="pit_coc",
        min_msa_population_coverage_share=0.5,
    )

    ny = result[(result["msa_id"] == "35620") & (result["year"] == 2020)].iloc[0]
    stl = result[(result["msa_id"] == "41180") & (result["year"] == 2020)].iloc[0]
    assert ny["pit_total"] == pytest.approx(20.0)
    assert stl["pit_total"] == pytest.approx(60.0)
    assert ny["allocation_basis"] == "block_population"
    assert ny["denominator_source"] == "pl_94_171_block_population"
    assert ny["block_vintage"] == "2020"
    assert ny["decennial_vintage"] == "2020"
    assert ny["msa_population_coverage_ratio"] == pytest.approx(0.5)
    assert ny["min_msa_population_coverage_share"] == pytest.approx(0.5)
    assert ny["zero_population_coc_count"] == 0
    assert ny["missing_population_block_count"] == 0


def test_fractional_rollup_population_containment_threshold_keeps_high_share(
    block_population_crosswalk: pd.DataFrame,
):
    source = pd.DataFrame(
        {
            "coc_id": ["CO-200", "CO-600", "CO-700"],
            "year": [2020, 2020, 2020],
            "pit_total": [80.0, 20.0, 40.0],
        }
    )
    threshold_crosswalk = block_population_crosswalk.copy()
    high_share = (threshold_crosswalk["coc_id"] == "CO-200") & (
        threshold_crosswalk["msa_id"] == "41180"
    )
    threshold_crosswalk.loc[high_share, "coc_population_containment_share"] = 0.95

    result = aggregate_coc_to_msa_fractional_rollup(
        source,
        threshold_crosswalk,
        additive_measure_columns=("pit_total",),
        source_dataset_id="pit_coc",
        min_coc_population_containment_share=0.9,
    )

    assert result[["msa_id", "year"]].to_dict(orient="records") == [
        {"msa_id": "41180", "year": 2020}
    ]
    row = result.iloc[0]
    assert row["pit_total"] == pytest.approx(60.0)
    assert row["coc_count"] == 1
    assert row["covered_coc_count"] == 1
    assert row["missing_coc_count"] == 0
    assert row["min_coc_population_containment_share"] == pytest.approx(0.9)


def test_block_population_save_read_roundtrip_preserves_schema_and_provenance(
    block_population_crosswalk: pd.DataFrame,
    tmp_path,
):
    output_dir = tmp_path / "curated" / "xwalks"
    written = save_coc_msa_block_population_crosswalk(
        block_population_crosswalk,
        boundary_vintage="2025",
        county_vintage="2023",
        block_vintage="2020",
        decennial_vintage="2020",
        definition_version="census_msa_2023",
        output_dir=output_dir,
    )

    roundtrip = read_coc_msa_block_population_crosswalk(
        "2025",
        "census_msa_2023",
        "2023",
        "2020",
        "2020",
        base_dir=tmp_path,
    )

    assert list(roundtrip.columns) == list(COC_MSA_BLOCK_POPULATION_CROSSWALK_COLUMNS)
    pd.testing.assert_frame_equal(roundtrip, block_population_crosswalk)

    provenance = read_provenance(written)
    assert provenance is not None
    assert provenance.boundary_vintage == "2025"
    assert provenance.county_vintage == "2023"
    assert provenance.geo_type == "msa"
    assert provenance.definition_version == "census_msa_2023"
    assert provenance.weighting == "block_population"
    assert provenance.extra["allocation_basis"] == "block_population"
    assert provenance.extra["block_vintage"] == "2020"
    assert provenance.extra["decennial_vintage"] == "2020"


@pytest.mark.parametrize(
    ("coc_id", "msa_id", "expected_share", "expected_area"),
    [
        (
            coc_id,
            msa_id,
            EXPECTED_ALLOCATION_SHARES[(coc_id, msa_id)],
            EXPECTED_INTERSECTION_AREAS[(coc_id, msa_id)],
        )
        for coc_id, msa_id in EXPECTED_ALLOCATION_SHARES
    ],
    ids=[f"{coc_id}-{msa_id}" for coc_id, msa_id in EXPECTED_ALLOCATION_SHARES],
)
def test_truth_table_allocations(
    coc_msa_crosswalk: pd.DataFrame,
    coc_id: str,
    msa_id: str,
    expected_share: float,
    expected_area: float,
):
    row = coc_msa_crosswalk[
        (coc_msa_crosswalk["coc_id"] == coc_id) & (coc_msa_crosswalk["msa_id"] == msa_id)
    ].iloc[0]
    assert row["allocation_share"] == pytest.approx(expected_share)
    assert row["intersection_area"] == pytest.approx(expected_area)


def test_null_intersection_coc_is_excluded(coc_msa_crosswalk: pd.DataFrame):
    assert "CO-900" not in set(coc_msa_crosswalk["coc_id"])


@pytest.mark.parametrize(
    ("input_name", "column", "expected_error"),
    MISSING_INPUT_COLUMN_CASES,
)
def test_missing_required_input_columns_raise_clear_errors(
    input_name: str,
    column: str,
    expected_error: str,
):
    coc_gdf = _coc_gdf()
    county_gdf = _county_gdf()
    if input_name == "coc":
        coc_gdf = coc_gdf.drop(columns=[column])
    else:
        county_gdf = county_gdf.drop(columns=[column])

    with pytest.raises(ValueError, match=expected_error):
        build_coc_msa_crosswalk(
            coc_gdf,
            county_gdf,
            _msa_membership_df(),
            boundary_vintage="2025",
            county_vintage="2023",
            definition_version="census_msa_2023",
        )


def test_lowercase_county_geoid_column_is_normalized():
    expected = build_coc_msa_crosswalk(
        _coc_gdf(),
        _county_gdf(),
        _msa_membership_df(),
        boundary_vintage="2025",
        county_vintage="2023",
        definition_version="census_msa_2023",
    )
    lowercase_counties = _county_gdf().rename(columns={"GEOID": "geoid"})

    actual = build_coc_msa_crosswalk(
        _coc_gdf(),
        lowercase_counties,
        _msa_membership_df(),
        boundary_vintage="2025",
        county_vintage="2023",
        definition_version="census_msa_2023",
    )

    pd.testing.assert_frame_equal(actual, expected)


def test_allocation_summary_flags_unallocated_share(coc_msa_crosswalk: pd.DataFrame):
    summary = summarize_coc_msa_allocation(coc_msa_crosswalk)
    assert set(summary["coc_id"]) == set(EXPECTED_UNALLOCATED_SHARE)
    for coc_id, expected_unallocated in EXPECTED_UNALLOCATED_SHARE.items():
        row = summary[summary["coc_id"] == coc_id].iloc[0]
        assert row["unallocated_share"] == pytest.approx(expected_unallocated)


def test_save_read_roundtrip_preserves_schema_and_provenance(
    coc_msa_crosswalk: pd.DataFrame,
    tmp_path,
):
    output_dir = tmp_path / "curated" / "xwalks"
    written = save_coc_msa_crosswalk(
        coc_msa_crosswalk,
        boundary_vintage="2025",
        county_vintage="2023",
        definition_version="census_msa_2023",
        output_dir=output_dir,
    )

    roundtrip = read_coc_msa_crosswalk(
        "2025",
        "census_msa_2023",
        "2023",
        base_dir=tmp_path,
    )

    assert list(roundtrip.columns) == list(COC_MSA_CROSSWALK_COLUMNS)
    pd.testing.assert_frame_equal(roundtrip, coc_msa_crosswalk)

    provenance = read_provenance(written)
    assert provenance is not None
    assert provenance.boundary_vintage == "2025"
    assert provenance.county_vintage == "2023"
    assert provenance.geo_type == "msa"
    assert provenance.definition_version == "census_msa_2023"
    assert provenance.weighting == "area"
    assert provenance.extra["dataset_type"] == "coc_msa_crosswalk"
    assert provenance.extra["share_column"] == "allocation_share"
    assert provenance.extra["share_denominator"] == "coc_area"


def test_read_coc_msa_crosswalk_missing_file_is_actionable(tmp_path):
    with pytest.raises(
        FileNotFoundError,
        match=(
            r"CoC-to-MSA crosswalk artifact not found .* "
            r"Run: hhplab generate msa-xwalk --boundary 2025 "
            r"--definition-version census_msa_2023 --counties 2023"
        ),
    ):
        read_coc_msa_crosswalk(
            "2025",
            "census_msa_2023",
            "2023",
            base_dir=tmp_path,
        )


def test_invalid_allocation_share_raises_clear_error(monkeypatch: pytest.MonkeyPatch):
    invalid_county_crosswalk = pd.DataFrame(
        {
            "coc_id": ["CO-999"],
            "boundary_vintage": ["2025"],
            "county_fips": ["36061"],
            "area_share": [1.0],
            "intersection_area": [110.0],
            "county_area": [110.0],
            "coc_area": [100.0],
        }
    )

    monkeypatch.setattr(
        "hhplab.msa.crosswalk.build_county_crosswalk",
        lambda *args, **kwargs: invalid_county_crosswalk,
    )

    with pytest.raises(
        ValueError,
        match=(
            r"Computed allocation_share outside the allowed range .* "
            r"Offending rows: CO-999->35620=1\.100000000"
        ),
    ):
        build_coc_msa_crosswalk(
            _coc_gdf(),
            _county_gdf(),
            _msa_membership_df().iloc[[0]].copy(),
            boundary_vintage="2025",
            county_vintage="2023",
            definition_version="census_msa_2023",
        )


def test_inconsistent_coc_area_raises_clear_error(monkeypatch: pytest.MonkeyPatch):
    inconsistent_county_crosswalk = pd.DataFrame(
        {
            "coc_id": ["CO-999", "CO-999"],
            "boundary_vintage": ["2025", "2025"],
            "county_fips": ["36061", "29510"],
            "area_share": [0.5, 0.5],
            "intersection_area": [50.0, 50.0],
            "county_area": [100.0, 100.0],
            "coc_area": [100.0, 100.1],
        }
    )

    monkeypatch.setattr(
        "hhplab.msa.crosswalk.build_county_crosswalk",
        lambda *args, **kwargs: inconsistent_county_crosswalk,
    )

    with pytest.raises(
        ValueError,
        match=(
            r"CoC-to-county crosswalk produced inconsistent coc_area values .* "
            r"Offending CoCs: CO-999: min=100\.000000000, max=100\.100000000"
        ),
    ):
        build_coc_msa_crosswalk(
            _coc_gdf(),
            _county_gdf(),
            _msa_membership_df(),
            boundary_vintage="2025",
            county_vintage="2023",
            definition_version="census_msa_2023",
        )


def test_allocation_summary_rejects_total_above_one_plus_tolerance():
    crosswalk = pd.DataFrame(
        {
            "coc_id": ["CO-999"],
            "allocation_share": [1.0 + (ALLOCATION_SHARE_TOLERANCE * 2)],
        }
    )

    with pytest.raises(
        ValueError,
        match=r"Computed allocation_share_sum outside the allowed range .* Offending CoCs: CO-999=",
    ):
        summarize_coc_msa_allocation(crosswalk)


def test_missing_membership_county_raises_actionable_error():
    membership = _msa_membership_df()
    membership.loc[len(membership)] = {
        "msa_id": "99999",
        "cbsa_code": "99999",
        "county_fips": "99998",
        "definition_version": "census_msa_2023",
    }
    with pytest.raises(ValueError, match="Run: hhplab ingest tiger --year 2023 --type counties"):
        build_coc_msa_crosswalk(
            _coc_gdf(),
            _county_gdf(),
            membership,
            boundary_vintage="2025",
            county_vintage="2023",
            definition_version="census_msa_2023",
        )


def test_empty_intersection_emits_warning_and_marks_result(caplog: pytest.LogCaptureFixture):
    with caplog.at_level("WARNING"):
        crosswalk = build_coc_msa_crosswalk(
            gpd.GeoDataFrame(
                {"coc_id": ["CO-999"]},
                geometry=[box(100, 100, 110, 110)],
                crs=ALBERS_EQUAL_AREA_CRS,
            ),
            _county_gdf(),
            _msa_membership_df(),
            boundary_vintage="2025",
            county_vintage="2023",
            definition_version="census_msa_2023",
        )

    assert crosswalk.empty
    assert "No CoC-to-county intersections were found" in crosswalk.attrs["warning"]
    assert "geometry mismatch or CRS issue" in crosswalk.attrs["warning"]
    assert "No CoC-to-county intersections were found" in caplog.text


def test_empty_membership_join_emits_warning_and_marks_result(caplog: pytest.LogCaptureFixture):
    coc_gdf = gpd.GeoDataFrame(
        {"coc_id": ["CO-100", "CO-200"]},
        geometry=[box(0, 0, 10, 10), box(10, 0, 20, 10)],
        crs=ALBERS_EQUAL_AREA_CRS,
    )
    county_gdf = gpd.GeoDataFrame(
        {"GEOID": ["36061", "29510", "01001"]},
        geometry=[
            box(0, 0, 10, 10),
            box(10, 0, 20, 10),
            box(30, 0, 40, 10),
        ],
        crs=ALBERS_EQUAL_AREA_CRS,
    )
    membership = pd.DataFrame(
        {
            "msa_id": ["99999"],
            "cbsa_code": ["99999"],
            "county_fips": ["01001"],
            "definition_version": ["census_msa_2023"],
        }
    )

    with caplog.at_level("WARNING"):
        crosswalk = build_coc_msa_crosswalk(
            coc_gdf,
            county_gdf,
            membership,
            boundary_vintage="2025",
            county_vintage="2023",
            definition_version="census_msa_2023",
        )

    assert crosswalk.empty
    assert "none matched the MSA county membership artifact" in crosswalk.attrs["warning"]
    assert "Tried county_fips: 29510, 36061." in crosswalk.attrs["warning"]
    assert "MSA counties by msa_id: 99999=[01001]." in crosswalk.attrs["warning"]
    assert "none matched the MSA county membership artifact" in caplog.text
