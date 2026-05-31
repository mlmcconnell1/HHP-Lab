"""Tests for block-weighted CoC Urban Area attribution.

Truth table for fixture geometries:

| CoC | Designed case | Blocks | Total | Urban | Fraction | Coverage |
| --- | ------------- | ------ | ----- | ----- | -------- | -------- |
| A | full urban + half of shared urban block | U1 + 50% U2 | 140 | 140 | 1.0 | 1.0 |
| B | half shared urban block + rural block | 50% U2 + R1 | 100 | 40 | 0.4 | 1.0 |
| C | zero-population urban block | Z1 | 0 | 0 | NA | 1.0 |
| D | missing-denominator rural block | M1 | 0 | 0 | NA | 0.0 |
"""

from __future__ import annotations

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import MultiPolygon, box

from hhplab.schema.columns import COC_URBAN_AREA_DETAIL_COLUMNS, COC_URBAN_FRACTION_COLUMNS
from hhplab.xwalks.county import ALBERS_EQUAL_AREA_CRS
from hhplab.xwalks.urban_fraction import build_coc_urban_fraction

COC_EXPECTATIONS = {
    "A": {
        "coc_total_population": 140.0,
        "coc_urban_population": 140.0,
        "coc_rural_population": 0.0,
        "urban_population_fraction": 1.0,
        "block_count": 2,
        "urban_block_count": 2,
        "rural_block_count": 0,
        "missing_denominator_block_count": 0,
        "population_coverage_ratio": 1.0,
    },
    "B": {
        "coc_total_population": 100.0,
        "coc_urban_population": 40.0,
        "coc_rural_population": 60.0,
        "urban_population_fraction": 0.4,
        "block_count": 2,
        "urban_block_count": 1,
        "rural_block_count": 1,
        "missing_denominator_block_count": 0,
        "population_coverage_ratio": 1.0,
    },
    "C": {
        "coc_total_population": 0.0,
        "coc_urban_population": 0.0,
        "coc_rural_population": 0.0,
        "urban_population_fraction": pd.NA,
        "block_count": 1,
        "urban_block_count": 1,
        "rural_block_count": 0,
        "missing_denominator_block_count": 0,
        "population_coverage_ratio": 1.0,
    },
    "D": {
        "coc_total_population": 0.0,
        "coc_urban_population": 0.0,
        "coc_rural_population": 0.0,
        "urban_population_fraction": pd.NA,
        "block_count": 1,
        "urban_block_count": 0,
        "rural_block_count": 1,
        "missing_denominator_block_count": 1,
        "population_coverage_ratio": 0.0,
    },
}

DETAIL_EXPECTATIONS = {
    ("A", "UA1"): 100.0,
    ("A", "UA2"): 40.0,
    ("B", "UA2"): 40.0,
    ("C", "UA2"): 0.0,
}


def coc_fixture() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "coc_id": ["A", "B", "C", "D"],
            "geometry": [
                box(0, 0, 10, 10),
                box(10, 0, 20, 10),
                box(20, 0, 30, 10),
                box(30, 0, 40, 10),
            ],
        },
        geometry="geometry",
        crs=ALBERS_EQUAL_AREA_CRS,
    )


def block_fixture() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "block_geoid": ["U1", "U2", "R1", "Z1", "M1"],
            "total_population": [100, 80, 60, 0, pd.NA],
            "geometry": [
                box(0, 0, 10, 10),
                box(8, 0, 12, 10),
                box(12, 0, 20, 10),
                box(20, 0, 30, 10),
                box(30, 0, 40, 10),
            ],
        },
        geometry="geometry",
        crs=ALBERS_EQUAL_AREA_CRS,
    )


def urban_area_fixture() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "urban_area_geoid": ["UA1", "UA2"],
            "urban_area_name": ["Fixture Full Urban Area", "Fixture Shared Urban Area"],
            "geometry": [
                box(-1, -1, 7, 11),
                MultiPolygon([box(7, -1, 13, 11), box(19, -1, 31, 11)]),
            ],
        },
        geometry="geometry",
        crs=ALBERS_EQUAL_AREA_CRS,
    )


def build_fixture_outputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    return build_coc_urban_fraction(
        coc_fixture(),
        block_fixture(),
        urban_area_fixture(),
        boundary_vintage=2025,
        urban_area_vintage=2020,
        block_vintage=2020,
        decennial_vintage=2020,
    )


def test_build_coc_urban_fraction_outputs_canonical_columns() -> None:
    summary, detail = build_fixture_outputs()

    assert list(summary.columns) == list(COC_URBAN_FRACTION_COLUMNS)
    assert list(detail.columns) == list(COC_URBAN_AREA_DETAIL_COLUMNS)


@pytest.mark.parametrize("coc_id", list(COC_EXPECTATIONS), ids=list(COC_EXPECTATIONS))
def test_build_coc_urban_fraction_matches_truth_table(coc_id: str) -> None:
    summary, _detail = build_fixture_outputs()
    row = summary.set_index("coc_id").loc[coc_id]

    for column, expected in COC_EXPECTATIONS[coc_id].items():
        if pd.isna(expected):
            assert pd.isna(row[column])
        elif isinstance(expected, float):
            assert row[column] == pytest.approx(expected)
        else:
            assert row[column] == expected

    assert row["boundary_vintage"] == 2025
    assert row["urban_area_vintage"] == 2020
    assert row["block_vintage"] == 2020
    assert row["decennial_vintage"] == 2020
    assert row["denominator_source"] == "pl_94_171_block_population"
    assert row["allocation_method"] == "block_area_intersection"
    assert row["classification_method"] == "block_representative_point_in_urban_area"


@pytest.mark.parametrize(
    "key",
    list(DETAIL_EXPECTATIONS),
    ids=[f"{coc_id}-{ua_id}" for coc_id, ua_id in DETAIL_EXPECTATIONS],
)
def test_build_coc_urban_fraction_preserves_urban_area_detail(key: tuple[str, str]) -> None:
    _summary, detail = build_fixture_outputs()
    coc_id, urban_area_geoid = key
    row = detail.set_index(["coc_id", "urban_area_geoid"]).loc[key]

    assert row["urban_population"] == pytest.approx(DETAIL_EXPECTATIONS[key])
    assert row["total_population"] == pytest.approx(DETAIL_EXPECTATIONS[key])
    assert row["urban_area_name"].startswith("Fixture")
    assert row["source"] == "coc_urban_area_detail"


def test_build_coc_urban_fraction_reports_missing_required_columns() -> None:
    coc = coc_fixture().drop(columns=["coc_id"])

    with pytest.raises(ValueError, match="coc_gdf missing required column"):
        build_coc_urban_fraction(
            coc,
            block_fixture(),
            urban_area_fixture(),
            boundary_vintage=2025,
            urban_area_vintage=2020,
            block_vintage=2020,
            decennial_vintage=2020,
        )
