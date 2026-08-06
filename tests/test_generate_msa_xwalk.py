"""Tests for `hhplab generate msa-xwalk`."""

from __future__ import annotations

import inspect
import json
import warnings
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box
from typer.testing import CliRunner

from hhplab.cli.generate.msa_xwalk import _concat_block_population_state_shards, generate_msa_xwalk
from hhplab.cli.main import app
from hhplab.geographies.msa.crosswalk import (
    ALLOCATION_SHARE_TOLERANCE,
    COC_MSA_BLOCK_POPULATION_CROSSWALK_COLUMNS,
)
from hhplab.geographies.msa.msa_definitions import DELINEATION_FILE_YEAR
from hhplab.registry.schema import RegistryEntry

runner = CliRunner()


def _boundary_registry_entry(tmp_path: Path) -> RegistryEntry:
    return RegistryEntry(
        boundary_vintage="2025",
        source="hud_exchange",
        ingested_at=pd.Timestamp("2026-04-30T00:00:00Z").to_pydatetime(),
        path=tmp_path / "data" / "curated" / "coc_boundaries" / "coc__B2025.parquet",
        feature_count=1,
        hash_of_file="abc",
    )


def _write_test_inputs(tmp_path: Path) -> None:
    boundaries_dir = tmp_path / "data" / "curated" / "coc_boundaries"
    tiger_dir = tmp_path / "data" / "curated" / "tiger"
    msa_dir = tmp_path / "data" / "curated" / "msa"
    boundaries_dir.mkdir(parents=True, exist_ok=True)
    tiger_dir.mkdir(parents=True, exist_ok=True)
    msa_dir.mkdir(parents=True, exist_ok=True)

    gpd.GeoDataFrame(
        {"coc_id": ["CO-100"]},
        geometry=[box(0, 0, 10, 10)],
        crs="EPSG:4326",
    ).to_parquet(boundaries_dir / "coc__B2025.parquet")

    gpd.GeoDataFrame(
        {"GEOID": ["36061"]},
        geometry=[box(0, 0, 10, 10)],
        crs="EPSG:4326",
    ).to_parquet(tiger_dir / "counties__C2023.parquet")

    pd.DataFrame(
        {
            "msa_id": ["35620"],
            "cbsa_code": ["35620"],
            "county_fips": ["36061"],
            "county_name": ["New York County"],
            "state_name": ["New York"],
            "central_outlying": ["Central"],
            "definition_version": ["census_msa_2023"],
        }
    ).to_parquet(msa_dir / "msa_county_membership__census_msa_2023.parquet")


def _write_block_population_inputs(tmp_path: Path) -> None:
    tiger_dir = tmp_path / "data" / "curated" / "tiger"
    census_dir = tmp_path / "data" / "curated" / "census"
    tiger_dir.mkdir(parents=True, exist_ok=True)
    census_dir.mkdir(parents=True, exist_ok=True)

    block_geoid = "360610001001000"
    gpd.GeoDataFrame(
        {"block_geoid": [block_geoid], "state_fips": ["36"]},
        geometry=[box(0, 0, 10, 10)],
        crs="EPSG:4326",
    ).to_parquet(tiger_dir / "blocks__K2020.parquet")
    pd.DataFrame(
        {"block_geoid": [block_geoid], "state_fips": ["36"], "total_population": [100]}
    ).to_parquet(census_dir / "pl_blocks__N2020xK2020.parquet")


def _write_boundary_input(tmp_path: Path) -> None:
    boundaries_dir = tmp_path / "data" / "curated" / "coc_boundaries"
    boundaries_dir.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame(
        {"coc_id": ["CO-100"]},
        geometry=[box(0, 0, 10, 10)],
        crs="EPSG:4326",
    ).to_parquet(boundaries_dir / "coc__B2025.parquet")


def _setup_no_boundary_registry(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> list[str]:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("hhplab.cli.generate.msa_xwalk.latest_vintage", lambda: None)
    return ["generate", "msa-xwalk", "--counties", "2023", "--json"]


def _setup_missing_boundary_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> list[str]:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.list_boundaries",
        lambda: [_boundary_registry_entry(tmp_path)],
    )
    return ["generate", "msa-xwalk", "--boundary", "2025", "--counties", "2023", "--json"]


def _setup_missing_county_geometry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> list[str]:
    monkeypatch.chdir(tmp_path)
    _write_boundary_input(tmp_path)
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.list_boundaries",
        lambda: [_boundary_registry_entry(tmp_path)],
    )
    return ["generate", "msa-xwalk", "--boundary", "2025", "--counties", "2023", "--json"]


GENERATE_MSA_XWALK_ERROR_CASES = [
    pytest.param(
        _setup_no_boundary_registry,
        "No boundary vintages found in the registry",
        "hhplab ingest boundaries --source hud_exchange --vintage <year>",
        id="empty-registry",
    ),
    pytest.param(
        _setup_missing_boundary_file,
        "Boundary file not found",
        "hhplab ingest boundaries --source hud_exchange --vintage 2025",
        id="missing-boundary-file",
    ),
    pytest.param(
        _setup_missing_county_geometry,
        "County geometry file not found",
        "hhplab ingest tiger --year 2023 --type counties",
        id="missing-county-geometry",
    ),
]


def test_generate_msa_xwalk_json(monkeypatch, tmp_path: Path):
    _write_test_inputs(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.list_boundaries",
        lambda: [
            RegistryEntry(
                boundary_vintage="2025",
                source="hud_exchange",
                ingested_at=pd.Timestamp("2026-04-30T00:00:00Z").to_pydatetime(),
                path=tmp_path / "data" / "curated" / "coc_boundaries" / "coc__B2025.parquet",
                feature_count=1,
                hash_of_file="abc",
            )
        ],
    )
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.latest_vintage",
        lambda: "2025",
    )

    result = runner.invoke(
        app,
        ["generate", "msa-xwalk", "--boundary", "2025", "--counties", "2023", "--json"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["rows"] == 1
    assert payload["coc_count"] == 1
    assert payload["msa_count"] == 1
    assert "state_sharded" not in payload
    assert payload["artifact"].endswith("msa_coc_xwalk__B2025xMcensus_msa_2023xC2023.parquet")


def test_generate_msa_xwalk_json_block_population(monkeypatch, tmp_path: Path):
    _write_test_inputs(tmp_path)
    _write_block_population_inputs(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.list_boundaries",
        lambda: [_boundary_registry_entry(tmp_path)],
    )
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.latest_vintage",
        lambda: "2025",
    )

    result = runner.invoke(
        app,
        [
            "generate",
            "msa-xwalk",
            "--boundary",
            "2025",
            "--counties",
            "2023",
            "--allocation-basis",
            "block_population",
            "--blocks",
            "2020",
            "--decennial",
            "2020",
            "--json",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["allocation_basis"] == "block_population"
    assert payload["state_sharded"] is True
    assert payload["state_shard_count"] == 1
    assert payload["failed_state_shards"] == []
    assert Path(payload["state_shard_dir"]).exists()
    assert payload["rows"] == 1
    assert payload["artifact"].endswith(
        "msa_coc_xwalk__N2020@B2025xMcensus_msa_2023xC2023xK2020__basis-block_population.parquet"
    )
    crosswalk = pd.read_parquet(payload["artifact"])
    assert crosswalk["msa_population_denominator"].tolist() == [100.0]


def test_generate_msa_xwalk_state_shards_do_not_read_national_blocks(
    monkeypatch,
    tmp_path: Path,
):
    _write_test_inputs(tmp_path)
    _write_block_population_inputs(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.list_boundaries",
        lambda: [_boundary_registry_entry(tmp_path)],
    )
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.latest_vintage",
        lambda: "2025",
    )
    original_gpd_read = gpd.read_parquet
    original_pd_read = pd.read_parquet

    def guard_gpd_read(path, *args, **kwargs):
        if Path(path).name == "blocks__K2020.parquet":
            assert kwargs.get("filters") == [("state_fips", "==", "36")]
        return original_gpd_read(path, *args, **kwargs)

    def guard_pd_read(path, *args, **kwargs):
        if Path(path).name == "pl_blocks__N2020xK2020.parquet":
            assert kwargs.get("filters") == [("state_fips", "==", "36")]
        return original_pd_read(path, *args, **kwargs)

    monkeypatch.setattr("hhplab.cli.generate.msa_xwalk.gpd.read_parquet", guard_gpd_read)
    monkeypatch.setattr("hhplab.cli.generate.msa_xwalk.pd.read_parquet", guard_pd_read)

    result = runner.invoke(
        app,
        [
            "generate",
            "msa-xwalk",
            "--boundary",
            "2025",
            "--counties",
            "2023",
            "--allocation-basis",
            "block_population",
            "--blocks",
            "2020",
            "--decennial",
            "2020",
            "--json",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0


def test_generate_msa_xwalk_json_block_population_can_opt_out_of_state_shards(
    monkeypatch,
    tmp_path: Path,
):
    _write_test_inputs(tmp_path)
    _write_block_population_inputs(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.list_boundaries",
        lambda: [_boundary_registry_entry(tmp_path)],
    )
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.latest_vintage",
        lambda: "2025",
    )

    result = runner.invoke(
        app,
        [
            "generate",
            "msa-xwalk",
            "--boundary",
            "2025",
            "--counties",
            "2023",
            "--allocation-basis",
            "block_population",
            "--blocks",
            "2020",
            "--decennial",
            "2020",
            "--no-state-shards",
            "--json",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["allocation_basis"] == "block_population"
    assert "state_sharded" not in payload
    assert payload["rows"] == 1


def test_generate_msa_xwalk_json_block_population_oom_error_is_actionable(
    monkeypatch,
    tmp_path: Path,
):
    _write_test_inputs(tmp_path)
    _write_block_population_inputs(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.list_boundaries",
        lambda: [_boundary_registry_entry(tmp_path)],
    )
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.latest_vintage",
        lambda: "2025",
    )

    def raise_memory_error(*args, **kwargs):
        raise MemoryError("simulated OOM")

    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk._build_block_population_state_shards",
        raise_memory_error,
    )

    result = runner.invoke(
        app,
        [
            "generate",
            "msa-xwalk",
            "--boundary",
            "2025",
            "--counties",
            "2023",
            "--allocation-basis",
            "block_population",
            "--blocks",
            "2020",
            "--decennial",
            "2020",
            "--json",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert "ran out of memory" in payload["error"]
    assert "--reuse-shards" in payload["error"]
    assert "--no-state-shards" in payload["error"]


def test_generate_msa_xwalk_json_block_population_state_shards_alabama_smoke(
    monkeypatch,
    tmp_path: Path,
):
    boundaries_dir = tmp_path / "data" / "curated" / "coc_boundaries"
    tiger_dir = tmp_path / "data" / "curated" / "tiger"
    msa_dir = tmp_path / "data" / "curated" / "msa"
    census_dir = tmp_path / "data" / "curated" / "census"
    boundaries_dir.mkdir(parents=True, exist_ok=True)
    tiger_dir.mkdir(parents=True, exist_ok=True)
    msa_dir.mkdir(parents=True, exist_ok=True)
    census_dir.mkdir(parents=True, exist_ok=True)

    block_geoid = "010010001001000"
    gpd.GeoDataFrame(
        {"coc_id": ["AL-500"]},
        geometry=[box(0, 0, 10, 10)],
        crs="EPSG:4326",
    ).to_parquet(boundaries_dir / "coc__B2025.parquet")
    gpd.GeoDataFrame(
        {"GEOID": ["01001"]},
        geometry=[box(0, 0, 10, 10)],
        crs="EPSG:4326",
    ).to_parquet(tiger_dir / "counties__C2023.parquet")
    gpd.GeoDataFrame(
        {"block_geoid": [block_geoid], "state_fips": ["01"]},
        geometry=[box(0, 0, 10, 10)],
        crs="EPSG:4326",
    ).to_parquet(tiger_dir / "blocks__K2020.parquet")
    pd.DataFrame(
        {"block_geoid": [block_geoid], "state_fips": ["01"], "total_population": [125]}
    ).to_parquet(census_dir / "pl_blocks__N2020xK2020.parquet")
    pd.DataFrame(
        {
            "msa_id": ["33860"],
            "cbsa_code": ["33860"],
            "county_fips": ["01001"],
            "county_name": ["Autauga County"],
            "state_name": ["Alabama"],
            "central_outlying": ["Central"],
            "definition_version": ["census_msa_2023"],
        }
    ).to_parquet(msa_dir / "msa_county_membership__census_msa_2023.parquet")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.list_boundaries",
        lambda: [_boundary_registry_entry(tmp_path)],
    )
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.latest_vintage",
        lambda: "2025",
    )

    result = runner.invoke(
        app,
        [
            "generate",
            "msa-xwalk",
            "--boundary",
            "2025",
            "--counties",
            "2023",
            "--allocation-basis",
            "block_population",
            "--blocks",
            "2020",
            "--decennial",
            "2020",
            "--state-shards",
            "--json",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["state_sharded"] is True
    assert payload["state_shard_count"] == 1
    assert payload["failed_state_shards"] == []
    shard_dir = Path(payload["state_shard_dir"])
    assert (shard_dir / f"{Path(payload['artifact']).stem}__state-01.parquet").exists()
    crosswalk = pd.read_parquet(payload["artifact"])
    assert list(crosswalk.columns) == list(COC_MSA_BLOCK_POPULATION_CROSSWALK_COLUMNS)
    assert crosswalk["msa_population_denominator"].tolist() == [125.0]


def test_block_population_state_shard_concat_zeroes_unusable_msa_denominator():
    shard = pd.DataFrame(
        {
            "coc_id": ["CO-100"],
            "msa_id": ["99999"],
            "cbsa_code": ["99999"],
            "boundary_vintage": ["2025"],
            "county_vintage": ["2023"],
            "block_vintage": ["2020"],
            "decennial_vintage": ["2020"],
            "definition_version": ["test_msa"],
            "allocation_method": ["block_population"],
            "share_column": ["allocation_share"],
            "share_denominator": ["coc_population_denominator"],
            "allocation_share": [1.0],
            "intersection_population": [100.0],
            "coc_population_denominator": [100.0],
            "msa_population_denominator": [0.0],
            "coc_population_containment_share": [1.0],
            "msa_population_coverage_share": [0.0],
            "intersection_area": [10.0],
            "coc_intersection_area": [10.0],
            "msa_intersection_area": [0.0],
            "block_count": [1],
            "missing_population_block_count": [0],
            "zero_population_coc": [False],
            "partial_coc_population_coverage": [False],
            "_shard_state_fips": ["99"],
        }
    )

    result = _concat_block_population_state_shards([shard])

    assert result["msa_population_coverage_share"].tolist() == [0.0]


@pytest.mark.parametrize(
    ("setup_case", "expected_problem", "expected_action"),
    GENERATE_MSA_XWALK_ERROR_CASES,
)
def test_generate_msa_xwalk_json_error_paths_are_actionable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    setup_case,
    expected_problem: str,
    expected_action: str,
):
    args = setup_case(monkeypatch, tmp_path)

    result = runner.invoke(app, args, catch_exceptions=False)

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert expected_problem in payload["error"]
    assert expected_action in payload["error"]


def test_generate_msa_xwalk_uses_shared_partial_allocation_tolerance(
    monkeypatch,
    tmp_path: Path,
):
    _write_test_inputs(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.list_boundaries",
        lambda: [
            RegistryEntry(
                boundary_vintage="2025",
                source="hud_exchange",
                ingested_at=pd.Timestamp("2026-04-30T00:00:00Z").to_pydatetime(),
                path=tmp_path / "data" / "curated" / "coc_boundaries" / "coc__B2025.parquet",
                feature_count=1,
                hash_of_file="abc",
            )
        ],
    )
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.latest_vintage",
        lambda: "2025",
    )
    monkeypatch.setattr(
        "hhplab.geographies.msa.crosswalk.build_coc_msa_crosswalk",
        lambda *args, **kwargs: pd.DataFrame(
            {
                "coc_id": ["CO-100"],
                "msa_id": ["35620"],
                "allocation_share": [1.0],
            }
        ),
    )
    monkeypatch.setattr(
        "hhplab.geographies.msa.crosswalk.summarize_coc_msa_allocation",
        lambda crosswalk: pd.DataFrame(
            {
                "coc_id": ["CO-100", "CO-200"],
                "allocation_share_sum": [
                    1.0 - (ALLOCATION_SHARE_TOLERANCE / 2.0),
                    1.0 - (ALLOCATION_SHARE_TOLERANCE * 2.0),
                ],
                "unallocated_share": [
                    ALLOCATION_SHARE_TOLERANCE / 2.0,
                    ALLOCATION_SHARE_TOLERANCE * 2.0,
                ],
            }
        ),
    )

    result = runner.invoke(
        app,
        ["generate", "msa-xwalk", "--boundary", "2025", "--counties", "2023", "--json", "--force"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["partially_allocated_cocs"] == 1


def test_generate_msa_xwalk_uses_shared_county_default_year():
    counties_param = inspect.signature(generate_msa_xwalk).parameters["counties"]

    assert counties_param.default == DELINEATION_FILE_YEAR


def test_generate_msa_xwalk_missing_membership_is_actionable(monkeypatch, tmp_path: Path):
    boundaries_dir = tmp_path / "data" / "curated" / "coc_boundaries"
    tiger_dir = tmp_path / "data" / "curated" / "tiger"
    boundaries_dir.mkdir(parents=True, exist_ok=True)
    tiger_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(tmp_path)

    gpd.GeoDataFrame(
        {"coc_id": ["CO-100"]},
        geometry=[box(0, 0, 10, 10)],
        crs="EPSG:4326",
    ).to_parquet(boundaries_dir / "coc__B2025.parquet")
    gpd.GeoDataFrame(
        {"GEOID": ["36061"]},
        geometry=[box(0, 0, 10, 10)],
        crs="EPSG:4326",
    ).to_parquet(tiger_dir / "counties__C2023.parquet")

    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.list_boundaries",
        lambda: [
            RegistryEntry(
                boundary_vintage="2025",
                source="hud_exchange",
                ingested_at=pd.Timestamp("2026-04-30T00:00:00Z").to_pydatetime(),
                path=tmp_path / "data" / "curated" / "coc_boundaries" / "coc__B2025.parquet",
                feature_count=1,
                hash_of_file="abc",
            )
        ],
    )

    result = runner.invoke(
        app,
        ["generate", "msa-xwalk", "--boundary", "2025", "--counties", "2023", "--json"],
        catch_exceptions=False,
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert "Run: hhplab generate msa --definition-version census_msa_2023" in payload["error"]


def test_generate_msa_xwalk_json_surfaces_empty_intersection_warning(
    monkeypatch,
    tmp_path: Path,
):
    boundaries_dir = tmp_path / "data" / "curated" / "coc_boundaries"
    tiger_dir = tmp_path / "data" / "curated" / "tiger"
    msa_dir = tmp_path / "data" / "curated" / "msa"
    boundaries_dir.mkdir(parents=True, exist_ok=True)
    tiger_dir.mkdir(parents=True, exist_ok=True)
    msa_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(tmp_path)

    gpd.GeoDataFrame(
        {"coc_id": ["CO-100"]},
        geometry=[box(100, 100, 110, 110)],
        crs="EPSG:4326",
    ).to_parquet(boundaries_dir / "coc__B2025.parquet")
    gpd.GeoDataFrame(
        {"GEOID": ["36061"]},
        geometry=[box(0, 0, 10, 10)],
        crs="EPSG:4326",
    ).to_parquet(tiger_dir / "counties__C2023.parquet")
    pd.DataFrame(
        {
            "msa_id": ["35620"],
            "cbsa_code": ["35620"],
            "county_fips": ["36061"],
            "definition_version": ["census_msa_2023"],
        }
    ).to_parquet(msa_dir / "msa_county_membership__census_msa_2023.parquet")

    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.list_boundaries",
        lambda: [
            RegistryEntry(
                boundary_vintage="2025",
                source="hud_exchange",
                ingested_at=pd.Timestamp("2026-04-30T00:00:00Z").to_pydatetime(),
                path=boundaries_dir / "coc__B2025.parquet",
                feature_count=1,
                hash_of_file="abc",
            )
        ],
    )
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.latest_vintage",
        lambda: "2025",
    )

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        result = runner.invoke(
            app,
            ["generate", "msa-xwalk", "--boundary", "2025", "--counties", "2023", "--json"],
            catch_exceptions=False,
        )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["rows"] == 0
    assert "warning" in payload
    assert "No CoC-to-county intersections were found" in payload["warning"]


def test_generate_msa_xwalk_json_surfaces_empty_membership_join_warning(
    monkeypatch,
    tmp_path: Path,
):
    boundaries_dir = tmp_path / "data" / "curated" / "coc_boundaries"
    tiger_dir = tmp_path / "data" / "curated" / "tiger"
    msa_dir = tmp_path / "data" / "curated" / "msa"
    boundaries_dir.mkdir(parents=True, exist_ok=True)
    tiger_dir.mkdir(parents=True, exist_ok=True)
    msa_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(tmp_path)

    gpd.GeoDataFrame(
        {"coc_id": ["CO-100"]},
        geometry=[box(0, 0, 10, 10)],
        crs="EPSG:4326",
    ).to_parquet(boundaries_dir / "coc__B2025.parquet")
    gpd.GeoDataFrame(
        {"GEOID": ["36061", "01001"]},
        geometry=[box(0, 0, 10, 10), box(20, 0, 30, 10)],
        crs="EPSG:4326",
    ).to_parquet(tiger_dir / "counties__C2023.parquet")
    pd.DataFrame(
        {
            "msa_id": ["99999"],
            "cbsa_code": ["99999"],
            "county_fips": ["01001"],
            "definition_version": ["census_msa_2023"],
        }
    ).to_parquet(msa_dir / "msa_county_membership__census_msa_2023.parquet")

    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.list_boundaries",
        lambda: [
            RegistryEntry(
                boundary_vintage="2025",
                source="hud_exchange",
                ingested_at=pd.Timestamp("2026-04-30T00:00:00Z").to_pydatetime(),
                path=boundaries_dir / "coc__B2025.parquet",
                feature_count=1,
                hash_of_file="abc",
            )
        ],
    )
    monkeypatch.setattr(
        "hhplab.cli.generate.msa_xwalk.latest_vintage",
        lambda: "2025",
    )

    result = runner.invoke(
        app,
        ["generate", "msa-xwalk", "--boundary", "2025", "--counties", "2023", "--json"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["rows"] == 0
    assert "warning" in payload
    assert "none matched the MSA county membership artifact" in payload["warning"]
    assert "Tried county_fips: 36061." in payload["warning"]
    assert "MSA counties by msa_id: 99999=[01001]." in payload["warning"]
