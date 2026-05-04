"""Tests for CoC-to-MSA PIT allocation crosswalks."""

from __future__ import annotations

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from hhplab.msa.crosswalk import (
    ALLOCATION_SHARE_TOLERANCE,
    COC_MSA_CROSSWALK_COLUMNS,
    build_coc_msa_crosswalk,
    read_coc_msa_crosswalk,
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
