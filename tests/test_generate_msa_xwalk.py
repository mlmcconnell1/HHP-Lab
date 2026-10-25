"""Tests for `hhplab generate msa-xwalk`."""

from __future__ import annotations

import inspect
import json
import warnings
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from shapely.geometry import box
from typer.testing import CliRunner

from hhplab.artifacts.curated.policy import validate_curated_layout
from hhplab.cli.generate.msa_xwalk import (
    STATE_SHARD_IDENTITY_VERSION,
    _concat_block_population_state_shards,
    _shard_identity_matches,
    _shard_input_identity,
    generate_msa_xwalk,
)
from hhplab.cli.main import app
from hhplab.geographies.msa.crosswalk import (
    ALLOCATION_SHARE_TOLERANCE,
    COC_MSA_BLOCK_POPULATION_CROSSWALK_COLUMNS,
)
from hhplab.geographies.msa.msa_definitions import DELINEATION_FILE_YEAR
from hhplab.geographies.xwalks.county import ALBERS_EQUAL_AREA_CRS
from hhplab.registry.schema import RegistryEntry
from hhplab.storage.provenance import (
    PROVENANCE_KEY,
    ProvenanceBlock,
    read_provenance,
    write_parquet_with_provenance,
)

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
        geometry=[box(*BASE_COC_BOX)],
        crs="EPSG:4326",
    ).to_parquet(boundaries_dir / "coc__B2025.parquet")

    gpd.GeoDataFrame(
        {"GEOID": [COUNTY_FIPS]},
        geometry=[box(*BASE_COUNTY_BOX)],
        crs="EPSG:4326",
    ).to_parquet(tiger_dir / "counties__C2023.parquet")

    pd.DataFrame(
        {
            "msa_id": [BASE_MSA_ID],
            "cbsa_code": [BASE_CBSA_CODE],
            "county_fips": [COUNTY_FIPS],
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

    block_geoid = BLOCK_GEOID
    gpd.GeoDataFrame(
        {"block_geoid": [block_geoid], "state_fips": [STATE_FIPS_36]},
        geometry=[box(*BASE_BLOCK_BOX)],
        crs="EPSG:4326",
    ).to_parquet(tiger_dir / "blocks__K2020.parquet")
    pd.DataFrame(
        {
            "block_geoid": [block_geoid],
            "state_fips": [STATE_FIPS_36],
            "total_population": [BASE_BLOCK_POPULATION],
        }
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


BLOCK_POPULATION_ARGS = [
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
]


def _setup_block_population_generate(monkeypatch, tmp_path: Path) -> None:
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


def test_generate_msa_xwalk_block_population_shards_use_raw_cache_and_curated_stays_valid(
    monkeypatch,
    tmp_path: Path,
):
    """State shards live under the raw cache; curated layout stays valid without cleanup."""
    _setup_block_population_generate(monkeypatch, tmp_path)

    result = runner.invoke(app, BLOCK_POPULATION_ARGS, catch_exceptions=False)

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["state_sharded"] is True

    artifact_stem = Path(payload["artifact"]).stem
    expected_shard_dir = (
        tmp_path
        / "data"
        / "raw"
        / "msa_xwalk_state_shards"
        / f"{artifact_stem}__state_shards"
    )
    assert Path(payload["state_shard_dir"]) == expected_shard_dir
    shard_file = expected_shard_dir / f"{artifact_stem}__state-36.parquet"
    assert shard_file.exists()

    # The legacy nested location under curated/xwalks must not be recreated.
    legacy_location = (
        tmp_path
        / "data"
        / "curated"
        / "xwalks"
        / f"{artifact_stem}__state_shards"
    )
    assert not legacy_location.exists()

    # Curated layout validates while shard files remain retained (no cleanup needed).
    violations = validate_curated_layout(tmp_path / "data" / "curated")
    assert violations == []
    assert shard_file.exists()


def test_generate_msa_xwalk_reuse_shards_reads_from_raw_cache(
    monkeypatch,
    tmp_path: Path,
):
    """--reuse-shards reads the retained shards from the raw cache location."""
    _setup_block_population_generate(monkeypatch, tmp_path)

    first = runner.invoke(app, BLOCK_POPULATION_ARGS, catch_exceptions=False)
    assert first.exit_code == 0
    first_payload = json.loads(first.stdout)
    shard_dir = Path(first_payload["state_shard_dir"])
    artifact_stem = Path(first_payload["artifact"]).stem
    shard_file = shard_dir / f"{artifact_stem}__state-36.parquet"
    assert shard_file.exists()
    original_shard = pd.read_parquet(shard_file)
    original_shard_bytes = shard_file.read_bytes()
    retained_other_state = shard_dir / f"{artifact_stem}__state-99.parquet"
    retained_other_state.write_bytes(original_shard_bytes)

    shard_reads: list[Path] = []
    original_pd_read = pd.read_parquet

    def tracking_read(path, *args, **kwargs):
        if str(path).startswith(str(shard_dir)):
            shard_reads.append(Path(path))
        return original_pd_read(path, *args, **kwargs)

    monkeypatch.setattr("hhplab.cli.generate.msa_xwalk.pd.read_parquet", tracking_read)

    second = runner.invoke(
        app,
        [*BLOCK_POPULATION_ARGS, "--force", "--reuse-shards"],
        catch_exceptions=False,
    )
    assert second.exit_code == 0
    second_payload = json.loads(second.stdout)
    assert second_payload["reused_state_shards"] is True
    assert second_payload["rows"] == 1
    assert second_payload["artifact"] == first_payload["artifact"]
    assert second_payload["state_shard_count"] == 1
    assert second_payload["state_shard_files_on_disk"] == 2
    assert retained_other_state.read_bytes() == original_shard_bytes

    # The shard file is actually reused from the raw cache location, unchanged
    # byte-for-byte (no rewrite happened on the reuse path).
    assert shard_reads == [shard_file]
    assert shard_file.read_bytes() == original_shard_bytes
    assert pd.read_parquet(shard_file).equals(original_shard)


# ---------------------------------------------------------------------------
# State-shard input identity fixtures
# ---------------------------------------------------------------------------
# Named fixture constants so expectations derive from the same values that
# build the on-disk inputs (derive, don't duplicate).
BASE_BLOCK_POPULATION = 100
CHANGED_BLOCK_POPULATION = 250
THIRD_BLOCK_POPULATION = 500
BASE_COC_BOX = (0, 0, 10, 10)
CHANGED_COC_BOX = (5, 5, 15, 15)
BASE_COUNTY_BOX = (0, 0, 10, 10)
CHANGED_COUNTY_BOX = (5, 5, 15, 15)
BASE_BLOCK_BOX = (0, 0, 10, 10)
CHANGED_BLOCK_BOX = (5, 5, 15, 15)
BASE_MSA_ID = "35620"
CHANGED_MSA_ID = "99999"
BASE_CBSA_CODE = "35620"
CHANGED_CBSA_CODE = "99999"
BLOCK_GEOID = "360610001001000"
COUNTY_FIPS = "36061"
STATE_FIPS_36 = "36"


def _projected_area(box_coords: tuple) -> float:
    """Projected area of a named EPSG:4326 box in the pipeline's equal-area CRS."""
    return float(
        gpd.GeoSeries([box(*box_coords)], crs="EPSG:4326")
        .to_crs(ALBERS_EQUAL_AREA_CRS)
        .area.iloc[0]
    )


def _projected_overlap_area(left_box: tuple, right_box: tuple) -> float:
    """Projected overlap area of two named EPSG:4326 boxes."""
    left = gpd.GeoSeries([box(*left_box)], crs="EPSG:4326").to_crs(
        ALBERS_EQUAL_AREA_CRS
    )
    right = gpd.GeoSeries([box(*right_box)], crs="EPSG:4326").to_crs(
        ALBERS_EQUAL_AREA_CRS
    )
    return float(left.intersection(right, align=False).area.iloc[0])


def _rewrite_coc_boundary(tmp_path: Path, *, box_coords: tuple = CHANGED_COC_BOX) -> Path:
    path = tmp_path / "data" / "curated" / "coc_boundaries" / "coc__B2025.parquet"
    gpd.GeoDataFrame(
        {"coc_id": ["CO-100"]},
        geometry=[box(*box_coords)],
        crs="EPSG:4326",
    ).to_parquet(path)
    return path


def _rewrite_county_geometry(
    tmp_path: Path,
    *,
    box_coords: tuple = CHANGED_COUNTY_BOX,
) -> Path:
    path = tmp_path / "data" / "curated" / "tiger" / "counties__C2023.parquet"
    gpd.GeoDataFrame(
        {"GEOID": [COUNTY_FIPS]},
        geometry=[box(*box_coords)],
        crs="EPSG:4326",
    ).to_parquet(path)
    return path


def _rewrite_msa_membership(
    tmp_path: Path,
    *,
    msa_id: str = CHANGED_MSA_ID,
    cbsa_code: str = CHANGED_CBSA_CODE,
) -> Path:
    path = (
        tmp_path
        / "data"
        / "curated"
        / "msa"
        / "msa_county_membership__census_msa_2023.parquet"
    )
    pd.DataFrame(
        {
            "msa_id": [msa_id],
            "cbsa_code": [cbsa_code],
            "county_fips": [COUNTY_FIPS],
            "county_name": ["New York County"],
            "state_name": ["New York"],
            "central_outlying": ["Central"],
            "definition_version": ["census_msa_2023"],
        }
    ).to_parquet(path)
    return path


def _rewrite_block_geometry(
    tmp_path: Path,
    *,
    box_coords: tuple = CHANGED_BLOCK_BOX,
) -> Path:
    path = tmp_path / "data" / "curated" / "tiger" / "blocks__K2020.parquet"
    gpd.GeoDataFrame(
        {"block_geoid": [BLOCK_GEOID], "state_fips": [STATE_FIPS_36]},
        geometry=[box(*box_coords)],
        crs="EPSG:4326",
    ).to_parquet(path)
    return path


def _rewrite_block_population(
    tmp_path: Path,
    *,
    population: int = CHANGED_BLOCK_POPULATION,
) -> Path:
    path = (
        tmp_path
        / "data"
        / "curated"
        / "census"
        / "pl_blocks__N2020xK2020.parquet"
    )
    pd.DataFrame(
        {
            "block_geoid": [BLOCK_GEOID],
            "state_fips": [STATE_FIPS_36],
            "total_population": [population],
        }
    ).to_parquet(path)
    return path


def _assert_block_population_changed(first: pd.DataFrame, second: pd.DataFrame) -> None:
    assert first["msa_population_denominator"].tolist() == [
        float(BASE_BLOCK_POPULATION)
    ]
    assert second["msa_population_denominator"].tolist() == [
        float(CHANGED_BLOCK_POPULATION)
    ]
    assert second["intersection_population"].tolist() == [
        pytest.approx(float(CHANGED_BLOCK_POPULATION))
    ]
    assert second["allocation_share"].tolist() == [pytest.approx(1.0)]


def _assert_coc_boundary_changed(first: pd.DataFrame, second: pd.DataFrame) -> None:
    block_area = _projected_area(BASE_BLOCK_BOX)
    expected_first_area = _projected_overlap_area(BASE_COC_BOX, BASE_BLOCK_BOX)
    expected_second_area = _projected_overlap_area(CHANGED_COC_BOX, BASE_BLOCK_BOX)
    expected_second_denom = BASE_BLOCK_POPULATION * expected_second_area / block_area
    assert float(first["intersection_area"].iloc[0]) == pytest.approx(
        expected_first_area, rel=1e-6
    )
    assert float(second["intersection_area"].iloc[0]) == pytest.approx(
        expected_second_area, rel=1e-6
    )
    assert first["coc_population_denominator"].tolist() == [
        pytest.approx(float(BASE_BLOCK_POPULATION))
    ]
    assert second["coc_population_denominator"].tolist() == [
        pytest.approx(expected_second_denom)
    ]
    assert second["intersection_population"].tolist() == [
        pytest.approx(expected_second_denom)
    ]
    assert second["msa_population_denominator"].tolist() == [
        float(BASE_BLOCK_POPULATION)
    ]
    assert second["allocation_share"].tolist() == [pytest.approx(1.0)]


def _assert_county_geometry_rebuilt_equal(
    first: pd.DataFrame,
    second: pd.DataFrame,
) -> None:
    # This fixture assigns its block by county GEOID, so moving the county
    # polygon leaves its arithmetic unchanged. The caller separately checks
    # that the changed source identity triggered a rebuild.
    numeric_columns = [
        "intersection_area",
        "intersection_population",
        "block_count",
        "coc_population_denominator",
        "coc_intersection_area",
        "msa_population_denominator",
        "msa_intersection_area",
        "allocation_share",
        "coc_population_containment_share",
        "msa_population_coverage_share",
    ]
    for column in numeric_columns:
        assert second[column].tolist() == pytest.approx(
            first[column].tolist(), rel=1e-6
        )
    assert second["msa_id"].tolist() == first["msa_id"].tolist()



def _assert_block_geometry_changed(first: pd.DataFrame, second: pd.DataFrame) -> None:
    new_block_area = _projected_area(CHANGED_BLOCK_BOX)
    expected_overlap = _projected_overlap_area(BASE_COC_BOX, CHANGED_BLOCK_BOX)
    expected_population = BASE_BLOCK_POPULATION * expected_overlap / new_block_area
    assert float(second["intersection_area"].iloc[0]) == pytest.approx(
        expected_overlap, rel=1e-6
    )
    assert second["intersection_population"].tolist() == [
        pytest.approx(expected_population)
    ]
    assert second["msa_population_coverage_share"].tolist() == [
        pytest.approx(expected_population / BASE_BLOCK_POPULATION)
    ]
    assert first["intersection_population"].tolist() == [
        pytest.approx(float(BASE_BLOCK_POPULATION))
    ]


def _assert_membership_changed(first: pd.DataFrame, second: pd.DataFrame) -> None:
    assert first["msa_id"].tolist() == [BASE_MSA_ID]
    assert second["msa_id"].tolist() == [CHANGED_MSA_ID]
    assert second["cbsa_code"].tolist() == [CHANGED_CBSA_CODE]
    assert second["msa_population_denominator"].tolist() == [
        float(BASE_BLOCK_POPULATION)
    ]


CHANGED_SOURCE_CASES = [
    pytest.param(
        "coc_boundary",
        _rewrite_coc_boundary,
        _assert_coc_boundary_changed,
        id="coc-boundary",
    ),
    pytest.param(
        "county_geometry",
        _rewrite_county_geometry,
        _assert_county_geometry_rebuilt_equal,
        id="county-geometry",
    ),
    pytest.param(
        "msa_membership",
        _rewrite_msa_membership,
        _assert_membership_changed,
        id="msa-membership",
    ),
    pytest.param(
        "block_geometry",
        _rewrite_block_geometry,
        _assert_block_geometry_changed,
        id="block-geometry",
    ),
    pytest.param(
        "block_population",
        _rewrite_block_population,
        _assert_block_population_changed,
        id="block-population",
    ),
]


@pytest.mark.parametrize("source_key,rewrite,assert_result", CHANGED_SOURCE_CASES)
def test_generate_msa_xwalk_reuse_shards_changed_source_forces_rebuild(
    source_key: str,
    rewrite,
    assert_result,
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Changing any single source at the same path must invalidate shard reuse."""
    _setup_block_population_generate(monkeypatch, tmp_path)

    first = runner.invoke(app, BLOCK_POPULATION_ARGS, catch_exceptions=False)
    assert first.exit_code == 0
    first_payload = json.loads(first.stdout)
    assert first_payload["status"] == "ok"
    shard_dir = Path(first_payload["state_shard_dir"])
    artifact_stem = Path(first_payload["artifact"]).stem
    shard_file = shard_dir / f"{artifact_stem}__state-{STATE_FIPS_36}.parquet"
    assert shard_file.exists()
    original_shard_bytes = shard_file.read_bytes()
    first_crosswalk = pd.read_parquet(first_payload["artifact"])

    rewrite(tmp_path)

    second = runner.invoke(
        app,
        [*BLOCK_POPULATION_ARGS, "--force", "--reuse-shards"],
        catch_exceptions=False,
    )
    assert second.exit_code == 0
    second_payload = json.loads(second.stdout)
    assert second_payload["reused_state_shards"] is False
    assert second_payload["state_shards_reused"] == 0
    assert second_payload["state_shards_built"] == 1
    assert second_payload["state_shards_preserved"] == 1

    retained = sorted(shard_dir.glob(f"{shard_file.name}.sha256_*"))
    assert len(retained) == 1
    assert retained[0].read_bytes() == original_shard_bytes

    second_crosswalk = pd.read_parquet(second_payload["artifact"])
    assert_result(first_crosswalk, second_crosswalk)


def _write_shard_without_provenance(path: Path, frame: pd.DataFrame) -> None:
    frame.to_parquet(path, index=False)


def _write_shard_with_malformed_provenance(path: Path, frame: pd.DataFrame) -> None:
    table = pa.Table.from_pandas(frame, preserve_index=False)
    table = table.replace_schema_metadata({PROVENANCE_KEY: b"not-json\x00"})
    pq.write_table(table, path)


LEGACY_SHARD_WRITERS = [
    pytest.param(_write_shard_without_provenance, id="missing-identity"),
    pytest.param(_write_shard_with_malformed_provenance, id="malformed-identity"),
]


@pytest.mark.parametrize("writer", LEGACY_SHARD_WRITERS)
def test_generate_msa_xwalk_reuse_shards_missing_or_malformed_identity_not_reused(
    writer,
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Shards without a valid recorded identity are never reused and preserved."""
    _setup_block_population_generate(monkeypatch, tmp_path)

    first = runner.invoke(app, BLOCK_POPULATION_ARGS, catch_exceptions=False)
    assert first.exit_code == 0
    first_payload = json.loads(first.stdout)
    shard_dir = Path(first_payload["state_shard_dir"])
    artifact_stem = Path(first_payload["artifact"]).stem
    shard_file = shard_dir / f"{artifact_stem}__state-{STATE_FIPS_36}.parquet"
    assert shard_file.exists()

    original = pd.read_parquet(shard_file)
    fake = original.copy()
    fake["msa_population_denominator"] = 999.0
    writer(shard_file, fake)
    fake_bytes = shard_file.read_bytes()

    second = runner.invoke(
        app,
        [*BLOCK_POPULATION_ARGS, "--force", "--reuse-shards"],
        catch_exceptions=False,
    )
    assert second.exit_code == 0
    payload = json.loads(second.stdout)
    assert payload["reused_state_shards"] is False
    assert payload["state_shards_reused"] == 0
    assert payload["state_shards_built"] == 1
    assert payload["state_shards_preserved"] == 1

    retained = sorted(shard_dir.glob(f"{shard_file.name}.sha256_*"))
    assert len(retained) == 1
    assert retained[0].read_bytes() == fake_bytes

    rebuilt = pd.read_parquet(shard_file)
    assert rebuilt["msa_population_denominator"].tolist() == [
        float(BASE_BLOCK_POPULATION)
    ]
    assert rebuilt["msa_population_denominator"].tolist() != [999.0]


def test_shard_identity_binds_parameters_and_fingerprints(tmp_path: Path) -> None:
    """A shard identity must bind parameters and source fingerprints, not paths."""
    fingerprints = {
        "coc_boundary": {"sha256": "a" * 64, "crs": "EPSG:4326"},
        "county_geometry": {"sha256": "b" * 64, "crs": "EPSG:4326"},
        "msa_county_membership": {"sha256": "c" * 64},
        "block_geometry": {"sha256": "d" * 64},
        "block_population": {"sha256": "e" * 64},
    }

    def identity(**overrides):
        params = dict(
            boundary_vintage="2025",
            county_vintage="2023",
            block_vintage="2020",
            decennial_vintage="2020",
            definition_version="census_msa_2023",
        )
        params.update(overrides)
        return _shard_input_identity(
            state_fips=STATE_FIPS_36,
            fingerprints=fingerprints,
            **params,
        )

    base = identity()
    assert base["identity_version"] == STATE_SHARD_IDENTITY_VERSION
    shard_path = tmp_path / "shard.parquet"
    write_parquet_with_provenance(
        pd.DataFrame(columns=list(COC_MSA_BLOCK_POPULATION_CROSSWALK_COLUMNS)),
        shard_path,
        ProvenanceBlock(
            extra={
                "dataset_type": "msa_xwalk_state_shard",
                "identity": base,
            }
        ),
    )

    assert _shard_identity_matches(shard_path, base) is True
    assert _shard_identity_matches(shard_path, identity(definition_version="other_def")) is False
    assert _shard_identity_matches(shard_path, identity(boundary_vintage="2024")) is False
    assert _shard_identity_matches(shard_path, identity(county_vintage="2010")) is False
    assert _shard_identity_matches(shard_path, identity(block_vintage="2010")) is False
    assert _shard_identity_matches(shard_path, identity(decennial_vintage="2010")) is False

    changed_fingerprints = {
        **fingerprints,
        "block_population": {"sha256": "f" * 64},
    }
    changed_fp_identity = _shard_input_identity(
        state_fips=STATE_FIPS_36,
        fingerprints=changed_fingerprints,
        boundary_vintage="2025",
        county_vintage="2023",
        block_vintage="2020",
        decennial_vintage="2020",
        definition_version="census_msa_2023",
    )
    assert _shard_identity_matches(shard_path, changed_fp_identity) is False


def test_generate_msa_xwalk_repeated_force_rebuild_preserves_each_stale_version(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Repeated force rebuilds preserve every stale shard version, none deleted."""
    _setup_block_population_generate(monkeypatch, tmp_path)

    def run():
        result = runner.invoke(
            app,
            [*BLOCK_POPULATION_ARGS, "--force", "--reuse-shards"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        shard_dir = Path(payload["state_shard_dir"])
        stem = Path(payload["artifact"]).stem
        shard_file = shard_dir / f"{stem}__state-{STATE_FIPS_36}.parquet"
        return shard_dir, shard_file

    shard_dir, shard_file = run()
    first_bytes = shard_file.read_bytes()

    _rewrite_block_population(tmp_path, population=CHANGED_BLOCK_POPULATION)
    shard_dir, shard_file = run()
    second_bytes = shard_file.read_bytes()
    assert second_bytes != first_bytes

    _rewrite_block_population(tmp_path, population=THIRD_BLOCK_POPULATION)
    shard_dir, shard_file = run()
    third_bytes = shard_file.read_bytes()
    assert third_bytes != second_bytes

    retained = sorted(shard_dir.glob(f"{shard_file.name}.sha256_*"))
    assert len(retained) == 2
    retained_bytes = [path.read_bytes() for path in retained]
    assert first_bytes in retained_bytes
    assert second_bytes in retained_bytes
    assert pd.read_parquet(shard_file)["msa_population_denominator"].tolist() == [
        float(THIRD_BLOCK_POPULATION)
    ]


def test_generate_msa_xwalk_reuse_shards_never_materializes_national_block_files(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Reuse must not load national block geometry/population files into memory."""
    _setup_block_population_generate(monkeypatch, tmp_path)

    first = runner.invoke(app, BLOCK_POPULATION_ARGS, catch_exceptions=False)
    assert first.exit_code == 0

    block_geometry_input = tmp_path / "data" / "curated" / "tiger" / "blocks__K2020.parquet"
    block_population_input = (
        tmp_path
        / "data"
        / "curated"
        / "census"
        / "pl_blocks__N2020xK2020.parquet"
    )
    national_block_paths = {str(block_geometry_input), str(block_population_input)}

    original_gpd_read = gpd.read_parquet
    original_pd_read = pd.read_parquet

    def guarded_gpd_read(path, *args, **kwargs):
        if str(path) in national_block_paths and "filters" not in kwargs:
            raise AssertionError(
                f"national block geometry was materialized without filters: {path}"
            )
        return original_gpd_read(path, *args, **kwargs)

    def guarded_pd_read(path, *args, **kwargs):
        if str(path) in national_block_paths and "filters" not in kwargs:
            raise AssertionError(
                f"national block population was materialized without filters: {path}"
            )
        return original_pd_read(path, *args, **kwargs)

    monkeypatch.setattr("hhplab.cli.generate.msa_xwalk.gpd.read_parquet", guarded_gpd_read)
    monkeypatch.setattr("hhplab.cli.generate.msa_xwalk.pd.read_parquet", guarded_pd_read)

    second = runner.invoke(
        app,
        [*BLOCK_POPULATION_ARGS, "--force", "--reuse-shards"],
        catch_exceptions=False,
    )
    assert second.exit_code == 0
    payload = json.loads(second.stdout)
    assert payload["reused_state_shards"] is True
    assert payload["state_shards_reused"] == 1


def test_block_population_state_shard_writes_provenance_identity(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Built shards embed a provenance identity binding all inputs and parameters."""
    _setup_block_population_generate(monkeypatch, tmp_path)

    result = runner.invoke(app, BLOCK_POPULATION_ARGS, catch_exceptions=False)
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    stem = Path(payload["artifact"]).stem
    shard_file = Path(payload["state_shard_dir"]) / (
        f"{stem}__state-{STATE_FIPS_36}.parquet"
    )

    provenance = read_provenance(shard_file)
    assert provenance is not None
    assert provenance.extra["dataset_type"] == "msa_xwalk_state_shard"
    identity = provenance.extra["identity"]
    assert identity["identity_version"] == STATE_SHARD_IDENTITY_VERSION
    assert identity["state_fips"] == STATE_FIPS_36
    assert identity["parameters"]["definition_version"] == "census_msa_2023"
    assert gpd.GeoSeries(crs=identity["inputs"]["coc_boundary"]["crs"]).crs.to_epsg() == 4326
    assert gpd.GeoSeries(crs=identity["inputs"]["county_geometry"]["crs"]).crs.to_epsg() == 4326
    assert len(identity["inputs"]["block_population"]["sha256"]) == 64
    assert len(identity["inputs"]["block_geometry"]["sha256"]) == 64


def _shard_metadata_readable_but_data_broken(path: Path) -> bool:
    """True when provenance/schema stay readable but row data cannot be read."""
    try:
        provenance = read_provenance(path)
        schema_names = list(pq.read_schema(path).names)
    except Exception:
        return False
    if provenance is None or schema_names != list(
        COC_MSA_BLOCK_POPULATION_CROSSWALK_COLUMNS
    ):
        return False
    try:
        pd.read_parquet(path)
    except Exception:
        return True
    return False


def test_generate_msa_xwalk_reuse_shards_corrupt_data_pages_not_reused(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A metadata-valid shard with unreadable data pages is preserved and rebuilt."""
    _setup_block_population_generate(monkeypatch, tmp_path)

    first = runner.invoke(app, BLOCK_POPULATION_ARGS, catch_exceptions=False)
    assert first.exit_code == 0
    first_payload = json.loads(first.stdout)
    shard_dir = Path(first_payload["state_shard_dir"])
    artifact_stem = Path(first_payload["artifact"]).stem
    shard_file = shard_dir / f"{artifact_stem}__state-{STATE_FIPS_36}.parquet"
    assert shard_file.exists()

    original_bytes = shard_file.read_bytes()
    # A zero byte terminates the Thrift page header before its required fields.
    # Use the recorded page offset so the footer and provenance stay intact.
    page_offset = pq.read_metadata(shard_file).row_group(0).column(0).data_page_offset
    candidate = bytearray(original_bytes)
    candidate[page_offset] = 0
    shard_file.write_bytes(candidate)
    assert _shard_metadata_readable_but_data_broken(shard_file)
    corrupted_bytes = shard_file.read_bytes()
    assert corrupted_bytes != original_bytes

    second = runner.invoke(
        app,
        [*BLOCK_POPULATION_ARGS, "--force", "--reuse-shards"],
        catch_exceptions=False,
    )
    assert second.exit_code == 0
    payload = json.loads(second.stdout)
    assert payload["reused_state_shards"] is False
    assert payload["state_shards_reused"] == 0
    assert payload["state_shards_built"] == 1
    assert payload["state_shards_preserved"] == 1

    retained = sorted(shard_dir.glob(f"{shard_file.name}.sha256_*"))
    assert len(retained) == 1
    assert retained[0].read_bytes() == corrupted_bytes

    rebuilt = pd.read_parquet(shard_file)
    assert rebuilt["msa_population_denominator"].tolist() == [
        float(BASE_BLOCK_POPULATION)
    ]
