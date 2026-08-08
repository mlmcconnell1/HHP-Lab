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

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import MultiPolygon, box
from typer.testing import CliRunner

import hhplab.artifacts.naming.naming as naming
from hhplab.cli.main import app
from hhplab.geographies.xwalks.county import ALBERS_EQUAL_AREA_CRS
from hhplab.geographies.xwalks.urban_fraction import build_coc_urban_fraction
from hhplab.schema.columns import COC_URBAN_AREA_DETAIL_COLUMNS, COC_URBAN_FRACTION_COLUMNS
from hhplab.storage.provenance import read_provenance

runner = CliRunner()

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
            "state_abbrev": ["CO", "CO", "CO", "CO"],
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
            "state_fips": ["08", "08", "08", "08", "08"],
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


def test_build_coc_urban_fraction_uses_spatial_index_candidate_pairs(monkeypatch) -> None:
    """Block/CoC intersections should not route through full GeoPandas overlay."""
    from hhplab.geographies.xwalks import urban_fraction

    monkeypatch.setattr(
        urban_fraction.gpd,
        "overlay",
        lambda *_args, **_kwargs: pytest.fail("build should not call gpd.overlay"),
    )

    summary, detail = build_fixture_outputs()

    assert summary["coc_id"].tolist() == ["A", "B", "C", "D"]
    assert detail.set_index(["coc_id", "urban_area_geoid"]).loc[("A", "UA1")][
        "urban_population"
    ] == pytest.approx(100.0)


def test_build_coc_urban_fraction_skips_intersections_for_covered_blocks(monkeypatch) -> None:
    """Blocks wholly covered by one CoC should not pay polygon intersection cost."""
    from hhplab.geographies.xwalks import urban_fraction

    coc = gpd.GeoDataFrame(
        {"coc_id": ["ALL"], "geometry": [box(-1, -1, 41, 11)]},
        geometry="geometry",
        crs=ALBERS_EQUAL_AREA_CRS,
    )

    monkeypatch.setattr(
        urban_fraction.gpd.GeoSeries,
        "intersection",
        lambda *_args, **_kwargs: pytest.fail(
            "fully covered blocks should not call polygon intersection"
        ),
    )

    summary, detail = build_coc_urban_fraction(
        coc,
        block_fixture(),
        urban_area_fixture(),
        boundary_vintage=2025,
        urban_area_vintage=2020,
        block_vintage=2020,
        decennial_vintage=2020,
    )

    row = summary.set_index("coc_id").loc["ALL"]
    assert row["coc_total_population"] == pytest.approx(240.0)
    assert row["block_count"] == 5
    assert detail["urban_population"].sum() == pytest.approx(180.0)


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


def test_urban_area_detail_groups_by_geoid_when_names_vary() -> None:
    urban_areas = urban_area_fixture()
    urban_areas["urban_area_geoid"] = "UA-MERGED"
    urban_areas["urban_area_name"] = ["Fixture Merged Area", "Fixture Merged Area Alias"]

    _summary, detail = build_coc_urban_fraction(
        coc_fixture(),
        block_fixture(),
        urban_areas,
        boundary_vintage=2025,
        urban_area_vintage=2020,
        block_vintage=2020,
        decennial_vintage=2020,
    )

    row = detail.set_index(["coc_id", "urban_area_geoid"]).loc[("A", "UA-MERGED")]
    assert row["urban_area_name"] == "Fixture Merged Area"
    assert row["urban_population"] == pytest.approx(140.0)
    assert row["total_population"] == pytest.approx(140.0)
    assert row["block_count"] == 2
    assert (
        detail.loc[
            (detail["coc_id"] == "A") & (detail["urban_area_geoid"] == "UA-MERGED")
        ].shape[0]
        == 1
    )


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


@pytest.mark.parametrize(
    ("coc", "blocks"),
    [
        pytest.param(coc_fixture().iloc[0:0].copy(), block_fixture(), id="empty-coc"),
        pytest.param(coc_fixture(), block_fixture().iloc[0:0].copy(), id="empty-blocks"),
    ],
)
def test_build_coc_urban_fraction_empty_inputs_return_canonical_empty_frames(
    coc: gpd.GeoDataFrame,
    blocks: gpd.GeoDataFrame,
) -> None:
    summary, detail = build_coc_urban_fraction(
        coc,
        blocks,
        urban_area_fixture(),
        boundary_vintage=2025,
        urban_area_vintage=2020,
        block_vintage=2020,
        decennial_vintage=2020,
    )

    assert summary.empty
    assert detail.empty
    assert list(summary.columns) == list(COC_URBAN_FRACTION_COLUMNS)
    assert list(detail.columns) == list(COC_URBAN_AREA_DETAIL_COLUMNS)


def _patch_curated_dir(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(
        "hhplab.cli.build_cmds.urban_fraction.curated_dir",
        lambda name: tmp_path / name,
    )


def _write_cli_inputs(tmp_path, *, split_block_geometry: bool = True) -> tuple[Path, Path]:
    for subdir in ("coc_boundaries", "tiger", "census", "measures"):
        (tmp_path / subdir).mkdir(parents=True, exist_ok=True)

    coc_fixture().to_parquet(
        tmp_path / "coc_boundaries" / naming.coc_base_filename("2025"),
        index=False,
    )
    urban_area_fixture().to_parquet(
        tmp_path / "tiger" / naming.urban_area_filename(2020),
        index=False,
    )
    block_path = tmp_path / "census" / naming.pl_block_population_filename(2020, 2020)
    geometry_path = tmp_path / "tiger" / naming.block_geometry_filename(2020)
    blocks = block_fixture()
    if split_block_geometry:
        pd.DataFrame(blocks.drop(columns=["geometry"])).to_parquet(block_path, index=False)
        blocks[["block_geoid", "state_fips", "geometry"]].to_parquet(
            geometry_path,
            index=False,
        )
    else:
        blocks.to_parquet(block_path, index=False)
    return block_path, geometry_path


def test_urban_fraction_cli_dry_run_success_json(monkeypatch, tmp_path) -> None:
    _patch_curated_dir(monkeypatch, tmp_path)
    _block_path, geometry_path = _write_cli_inputs(tmp_path)

    result = runner.invoke(
        app,
        [
            "build",
            "urban-fraction",
            "--boundary",
            "2025",
            "--urban-area-vintage",
            "2020",
            "--decennial",
            "2020",
            "--block-geometry",
            str(geometry_path),
            "--dry-run",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["action"] == "dry_run"
    assert payload["will_write"] is False
    assert all(status["exists"] for status in payload["inputs"].values())


def test_urban_fraction_cli_dry_run_discovers_canonical_block_geometry(
    monkeypatch,
    tmp_path,
) -> None:
    """Preflight succeeds without --block-geometry when canonical block geometry exists."""
    _patch_curated_dir(monkeypatch, tmp_path)
    _block_path, geometry_path = _write_cli_inputs(tmp_path)

    result = runner.invoke(
        app,
        [
            "build",
            "urban-fraction",
            "--boundary",
            "2025",
            "--urban-area-vintage",
            "2020",
            "--decennial",
            "2020",
            "--dry-run",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["inputs"]["block_geometry"]["path"] == str(geometry_path)
    assert payload["inputs"]["block_geometry"]["exists"] is True


def test_urban_fraction_geometry_preflight_uses_parquet_metadata(
    monkeypatch,
    tmp_path,
) -> None:
    """Geometry discovery does not materialize large block GeoParquet artifacts."""
    from hhplab.cli.build_cmds import urban_fraction as build_urban_fraction_cli

    _patch_curated_dir(monkeypatch, tmp_path)
    _block_path, geometry_path = _write_cli_inputs(tmp_path)
    monkeypatch.setattr(
        build_urban_fraction_cli.gpd,
        "read_parquet",
        lambda _path: pytest.fail("preflight should inspect metadata only"),
    )

    assert build_urban_fraction_cli._geoparquet_has_geometry(geometry_path) is True


def test_urban_fraction_cli_dry_run_accepts_integrated_block_geometry(
    monkeypatch,
    tmp_path,
) -> None:
    """Preflight keeps accepting PL block files with embedded GeoParquet geometry."""
    _patch_curated_dir(monkeypatch, tmp_path)
    block_path, geometry_path = _write_cli_inputs(tmp_path, split_block_geometry=False)
    geometry_path.unlink(missing_ok=True)

    result = runner.invoke(
        app,
        [
            "build",
            "urban-fraction",
            "--boundary",
            "2025",
            "--urban-area-vintage",
            "2020",
            "--decennial",
            "2020",
            "--dry-run",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["inputs"]["block_geometry"]["path"] == str(block_path)
    assert payload["inputs"]["block_geometry"]["has_geometry"] is True


def test_urban_fraction_cli_missing_inputs_json(monkeypatch, tmp_path) -> None:
    _patch_curated_dir(monkeypatch, tmp_path)

    result = runner.invoke(
        app,
        [
            "build",
            "urban-fraction",
            "--boundary",
            "2025",
            "--urban-area-vintage",
            "2020",
            "--decennial",
            "2020",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert payload["error"] == "missing_inputs"
    assert set(payload["missing_inputs"]) == {
        "coc_boundaries",
        "urban_areas",
        "pl_blocks",
        "block_geometry",
    }
    assert "hhplab ingest urban-areas --year 2020" in payload["commands"]["urban_areas"]
    assert "hhplab ingest block-geometry --year 2020" in payload["commands"]["block_geometry"]


def test_urban_fraction_cli_fixture_build_json(monkeypatch, tmp_path) -> None:
    _patch_curated_dir(monkeypatch, tmp_path)
    _block_path, geometry_path = _write_cli_inputs(tmp_path)

    result = runner.invoke(
        app,
        [
            "build",
            "urban-fraction",
            "--boundary",
            "2025",
            "--urban-area-vintage",
            "2020",
            "--decennial",
            "2020",
            "--block-geometry",
            str(geometry_path),
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["rows"] == 4
    assert payload["coc_count"] == 4
    assert payload["total_population"] == pytest.approx(240.0)
    assert payload["urban_population"] == pytest.approx(180.0)
    assert payload["min_fraction"] == pytest.approx(0.4)
    assert payload["max_fraction"] == pytest.approx(1.0)
    assert payload["diagnostics"]["missing_denominator_block_count"] == 1
    summary = pd.read_parquet(payload["artifact"])
    assert list(summary.columns) == list(COC_URBAN_FRACTION_COLUMNS)
    provenance = read_provenance(payload["artifact"])
    assert provenance is not None
    assert provenance.extra["dataset_type"] == "coc_urban_fraction"
    assert provenance.extra["block_geometry_artifact"] == str(geometry_path)


def test_urban_fraction_cli_fixture_build_discovers_canonical_block_geometry(
    monkeypatch,
    tmp_path,
) -> None:
    """End-to-end fixture build succeeds with separate canonical block geometry."""
    _patch_curated_dir(monkeypatch, tmp_path)
    _block_path, _geometry_path = _write_cli_inputs(tmp_path)

    result = runner.invoke(
        app,
        [
            "build",
            "urban-fraction",
            "--boundary",
            "2025",
            "--urban-area-vintage",
            "2020",
            "--decennial",
            "2020",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["rows"] == 4
    assert payload["diagnostics"]["missing_denominator_block_count"] == 1


@pytest.mark.parametrize(
    "include_state_abbrev",
    [
        pytest.param(True, id="with-state-abbrev"),
        pytest.param(False, id="without-state-abbrev"),
    ],
)
def test_chunked_urban_fraction_includes_cross_state_blocks(
    tmp_path,
    include_state_abbrev: bool,
) -> None:
    """Chunking must include every block state intersecting a CoC geometry."""
    from hhplab.cli.build_cmds import urban_fraction as build_urban_fraction_cli

    pl_blocks_path = tmp_path / "pl_blocks.parquet"
    block_geometry_path = tmp_path / "block_geometry.parquet"
    coc_data = {
        "coc_id": ["X"],
        "geometry": [box(0, 0, 20, 10)],
    }
    if include_state_abbrev:
        coc_data["state_abbrev"] = ["CO"]
    coc = gpd.GeoDataFrame(
        coc_data,
        geometry="geometry",
        crs=ALBERS_EQUAL_AREA_CRS,
    )
    blocks = gpd.GeoDataFrame(
        {
            "block_geoid": ["080010001001000", "200010001001000"],
            "state_fips": ["08", "20"],
            "total_population": [100, 50],
            "geometry": [box(0, 0, 10, 10), box(10, 0, 20, 10)],
        },
        geometry="geometry",
        crs=ALBERS_EQUAL_AREA_CRS,
    )
    urban_areas = gpd.GeoDataFrame(
        {
            "urban_area_geoid": ["UA1"],
            "urban_area_name": ["Fixture Cross-State Urban Area"],
            "geometry": [box(-1, -1, 21, 11)],
        },
        geometry="geometry",
        crs=ALBERS_EQUAL_AREA_CRS,
    )
    pd.DataFrame(blocks.drop(columns=["geometry"])).to_parquet(pl_blocks_path, index=False)
    blocks[["block_geoid", "state_fips", "geometry"]].to_parquet(
        block_geometry_path,
        index=False,
    )

    summary, detail, chunk_count = build_urban_fraction_cli._build_coc_urban_fraction_chunked(
        coc,
        urban_areas,
        pl_blocks_path=pl_blocks_path,
        block_geometry_path=block_geometry_path,
        boundary_vintage=2025,
        urban_area_vintage=2020,
        block_vintage=2020,
        decennial_vintage=2020,
        progress=False,
    )

    row = summary.set_index("coc_id").loc["X"]
    assert chunk_count == 2
    assert summary["coc_id"].tolist() == ["X"]
    assert row["coc_total_population"] == pytest.approx(150.0)
    assert row["coc_urban_population"] == pytest.approx(150.0)
    assert row["block_count"] == 2
    assert detail.set_index(["coc_id", "urban_area_geoid"]).loc[("X", "UA1")][
        "urban_population"
    ] == pytest.approx(150.0)


def test_urban_fraction_cli_fixture_build_accepts_integrated_block_geometry(
    monkeypatch,
    tmp_path,
) -> None:
    """End-to-end fixture build succeeds when PL block GeoParquet embeds geometry."""
    _patch_curated_dir(monkeypatch, tmp_path)
    block_path, geometry_path = _write_cli_inputs(tmp_path, split_block_geometry=False)
    geometry_path.unlink(missing_ok=True)

    result = runner.invoke(
        app,
        [
            "build",
            "urban-fraction",
            "--boundary",
            "2025",
            "--urban-area-vintage",
            "2020",
            "--decennial",
            "2020",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["rows"] == 4
    assert payload["diagnostics"]["missing_denominator_block_count"] == 1
    provenance = read_provenance(payload["artifact"])
    assert provenance is not None
    assert provenance.extra["block_geometry_artifact"] == str(block_path)


def test_load_block_inputs_reraises_non_missing_geo_metadata_value_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Corrupt GeoParquet errors must not fall back to population-only loading."""
    from hhplab.cli.build_cmds import urban_fraction as build_urban_fraction_cli

    block_path, geometry_path = _write_cli_inputs(tmp_path)

    def raise_corrupt_parquet(_path: Path) -> gpd.GeoDataFrame:
        raise ValueError("column type mismatch")

    monkeypatch.setattr(build_urban_fraction_cli.gpd, "read_parquet", raise_corrupt_parquet)
    monkeypatch.setattr(
        build_urban_fraction_cli.pd,
        "read_parquet",
        lambda _path: pytest.fail("non-sentinel GeoPandas errors must not fall back"),
    )

    with pytest.raises(ValueError, match="column type mismatch"):
        build_urban_fraction_cli._load_block_inputs(block_path, geometry_path)


def test_urban_fraction_cli_errors_on_incomplete_block_geometry(monkeypatch, tmp_path) -> None:
    """Population rows without matching geometry fail with actionable coverage diagnostics."""
    _patch_curated_dir(monkeypatch, tmp_path)
    _block_path, geometry_path = _write_cli_inputs(tmp_path)
    geometry = gpd.read_parquet(geometry_path)
    geometry = geometry.loc[geometry["block_geoid"] != "M1"].copy()
    geometry.to_parquet(geometry_path, index=False)

    result = runner.invoke(
        app,
        [
            "build",
            "urban-fraction",
            "--boundary",
            "2025",
            "--urban-area-vintage",
            "2020",
            "--decennial",
            "2020",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert "Block geometry coverage is incomplete" in payload["error"]
    assert "M1" in payload["error"]


def test_urban_fraction_cli_writes_null_rows_when_boundary_coc_has_no_block_coverage(
    monkeypatch,
    tmp_path,
) -> None:
    """Partial builds preserve unsupported CoCs with null denominator-derived values."""
    _patch_curated_dir(monkeypatch, tmp_path)
    _block_path, geometry_path = _write_cli_inputs(tmp_path)
    boundary_path = tmp_path / "coc_boundaries" / naming.coc_base_filename("2025")
    coc = gpd.read_parquet(boundary_path)
    missing_coc = gpd.GeoDataFrame(
        {
            "coc_id": ["AS-500"],
            "state_abbrev": ["AS"],
            "geometry": [box(100, 100, 110, 110)],
        },
        geometry="geometry",
        crs=ALBERS_EQUAL_AREA_CRS,
    )
    pd.concat([coc, missing_coc], ignore_index=True).to_parquet(boundary_path, index=False)

    result = runner.invoke(
        app,
        [
            "build",
            "urban-fraction",
            "--boundary",
            "2025",
            "--urban-area-vintage",
            "2020",
            "--decennial",
            "2020",
            "--block-geometry",
            str(geometry_path),
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["diagnostics"]["missing_summary_coc_count"] == 1
    assert payload["diagnostics"]["missing_summary_coc_ids"] == ["AS-500"]

    summary = pd.read_parquet(payload["artifact"])
    row = summary.set_index("coc_id").loc["AS-500"]
    assert pd.isna(row["coc_total_population"])
    assert pd.isna(row["coc_urban_population"])
    assert pd.isna(row["coc_rural_population"])
    assert pd.isna(row["urban_population_fraction"])
    assert row["block_count"] == 0
    assert row["urban_block_count"] == 0
    assert row["rural_block_count"] == 0
    assert pd.isna(row["missing_denominator_block_count"])
    assert pd.isna(row["population_coverage_ratio"])


def test_load_block_inputs_for_state_requires_actionable_state_fips_geometry(
    tmp_path,
) -> None:
    """Legacy block geometry without state_fips fails before Arrow emits an unknown-column error."""
    from hhplab.cli.build_cmds import urban_fraction as build_urban_fraction_cli

    pl_blocks_path, geometry_path = _write_cli_inputs(tmp_path)
    geometry = gpd.read_parquet(geometry_path).drop(columns=["state_fips"])
    geometry.to_parquet(geometry_path, index=False)

    with pytest.raises(ValueError, match="missing state_fips column"):
        build_urban_fraction_cli._load_block_inputs_for_state(
            pl_blocks_path,
            geometry_path,
            state_fips="08",
        )


def test_urban_fraction_cli_artifact_exists_json(monkeypatch, tmp_path) -> None:
    _patch_curated_dir(monkeypatch, tmp_path)
    _block_path, geometry_path = _write_cli_inputs(tmp_path)
    summary_path = (
        tmp_path
        / "measures"
        / naming.coc_urban_fraction_filename(2025, 2020, 2020, 2020)
    )
    pd.DataFrame({"coc_id": ["A"]}).to_parquet(summary_path, index=False)

    result = runner.invoke(
        app,
        [
            "build",
            "urban-fraction",
            "--boundary",
            "2025",
            "--urban-area-vintage",
            "2020",
            "--decennial",
            "2020",
            "--block-geometry",
            str(geometry_path),
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["error"] == "artifact_exists"
    assert payload["output_exists"] is True


def test_urban_fraction_cli_force_overwrites_existing_artifacts(monkeypatch, tmp_path) -> None:
    _patch_curated_dir(monkeypatch, tmp_path)
    _block_path, geometry_path = _write_cli_inputs(tmp_path)
    summary_path = (
        tmp_path
        / "measures"
        / naming.coc_urban_fraction_filename(2025, 2020, 2020, 2020)
    )
    detail_path = (
        tmp_path
        / "measures"
        / naming.coc_urban_area_detail_filename(2025, 2020, 2020, 2020)
    )
    pd.DataFrame({"stale": [True]}).to_parquet(summary_path, index=False)
    pd.DataFrame({"stale": [True]}).to_parquet(detail_path, index=False)

    result = runner.invoke(
        app,
        [
            "build",
            "urban-fraction",
            "--boundary",
            "2025",
            "--urban-area-vintage",
            "2020",
            "--decennial",
            "2020",
            "--block-geometry",
            str(geometry_path),
            "--force",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["output_exists"] is True
    assert payload["rows"] == 4
    assert list(pd.read_parquet(summary_path).columns) == list(COC_URBAN_FRACTION_COLUMNS)
    assert list(pd.read_parquet(detail_path).columns) == list(COC_URBAN_AREA_DETAIL_COLUMNS)


def test_urban_fraction_cli_human_output_is_concise(monkeypatch, tmp_path) -> None:
    _patch_curated_dir(monkeypatch, tmp_path)
    _block_path, geometry_path = _write_cli_inputs(tmp_path)

    result = runner.invoke(
        app,
        [
            "build",
            "urban-fraction",
            "--boundary",
            "2025",
            "--urban-area-vintage",
            "2020",
            "--decennial",
            "2020",
            "--block-geometry",
            str(geometry_path),
        ],
    )

    assert result.exit_code == 0
    assert "Processing block chunk 1: state_fips=08, cocs=4" in result.output
    assert "Loaded 5 block rows." in result.output
    assert "Built CoC urban fraction artifact." in result.output
    assert "Rows: 4" in result.output
