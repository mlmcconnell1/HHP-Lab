"""Containment-list builder tests.

Truth table for the polygon fixtures:

| Case | Container | Candidate | Share denominator | Expected share |
|------|-----------|-----------|-------------------|----------------|
| CoC-county full | COC-A | 001 | county area | 1.00 |
| CoC-county half | COC-A | 002 | county area | 0.50 |
| MSA-CoC full | MSA-1 | COC-A | CoC area | 1.00 |
| MSA-CoC half | MSA-1 | COC-B | CoC area | 0.50 |
"""

from __future__ import annotations

import json

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
import pytest
from shapely.geometry import box

from hhplab.naming import coc_base_path, county_path, msa_county_membership_path
from hhplab.recipe.executor import execute_recipe
from hhplab.recipe.executor_containment import (
    ALBERS_EQUAL_AREA_CRS,
    CONTAINMENT_COLUMNS,
    build_containment_list,
)
from hhplab.recipe.executor_manifest import resolve_pipeline_artifacts
from hhplab.recipe.loader import load_recipe
from hhplab.recipe.manifest import read_manifest
from hhplab.recipe.recipe_schema import ContainmentSpec

CRS = ALBERS_EQUAL_AREA_CRS
COUNTY_FIXTURES = {
    "001": box(0, 0, 10, 10),
    "002": box(10, 0, 20, 10),
}
COC_FIXTURES = {
    "COC-A": box(0, 0, 15, 10),
    "COC-B": box(5, 0, 15, 10),
}
MSA_MEMBERSHIP = pd.DataFrame(
    {
        "msa_id": ["MSA-1"],
        "cbsa_code": ["MSA-1"],
        "county_fips": ["001"],
    }
)


def _county_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "GEOID": list(COUNTY_FIXTURES),
            "geometry": list(COUNTY_FIXTURES.values()),
        },
        crs=CRS,
    )


def _coc_gdf(*, ids: list[str] | None = None) -> gpd.GeoDataFrame:
    selected = ids or list(COC_FIXTURES)
    return gpd.GeoDataFrame(
        {
            "coc_id": selected,
            "geometry": [COC_FIXTURES[coc_id] for coc_id in selected],
        },
        crs=CRS,
    )


def _containment_spec(
    container_type: str,
    candidate_type: str,
    *,
    selector_ids: list[str] | None = None,
    candidate_selector_ids: list[str] | None = None,
    min_share: float = 0.0,
    denominator: str = "candidate_area",
) -> ContainmentSpec:
    container = {"type": container_type, "vintage": 2023}
    if container_type == "msa":
        container["source"] = "test_msa_v1"
    candidate_vintage = 2025 if candidate_type == "coc" else 2023
    return ContainmentSpec.model_validate(
        {
            "container": container,
            "candidate": {"type": candidate_type, "vintage": candidate_vintage},
            "selector_ids": selector_ids,
            "candidate_selector_ids": candidate_selector_ids,
            "min_share": min_share,
            "denominator": denominator,
        }
    )


def test_coc_county_containment_filters_inclusively_and_sorts() -> None:
    spec = _containment_spec(
        "coc",
        "county",
        selector_ids=["COC-A"],
        min_share=0.5,
    )

    containment = build_containment_list(
        spec,
        coc_gdf=_coc_gdf(ids=["COC-A"]),
        county_gdf=_county_gdf(),
    )

    assert list(containment.columns) == list(CONTAINMENT_COLUMNS)
    assert containment["candidate_id"].tolist() == ["001", "002"]
    assert containment["container_id"].tolist() == ["COC-A", "COC-A"]
    assert containment["contained_share"].tolist() == pytest.approx([1.0, 0.5])
    assert (containment["candidate_area"] > 0).all()
    assert (containment["container_area"] > 0).all()


def test_coc_county_containment_applies_candidate_selector() -> None:
    spec = _containment_spec(
        "coc",
        "county",
        selector_ids=["COC-A"],
        candidate_selector_ids=["002"],
    )

    containment = build_containment_list(
        spec,
        coc_gdf=_coc_gdf(ids=["COC-A"]),
        county_gdf=_county_gdf(),
    )

    assert containment["candidate_id"].tolist() == ["002"]
    assert containment["contained_share"].tolist() == pytest.approx([0.5])


def test_coc_county_containment_honors_container_area_denominator() -> None:
    spec = _containment_spec(
        "coc",
        "county",
        selector_ids=["COC-A"],
        candidate_selector_ids=["002"],
        denominator="container_area",
    )

    containment = build_containment_list(
        spec,
        coc_gdf=_coc_gdf(ids=["COC-A"]),
        county_gdf=_county_gdf(),
    )

    assert containment["candidate_id"].tolist() == ["002"]
    assert containment["contained_share"].tolist() == pytest.approx([1 / 3])


def test_msa_coc_containment_uses_coc_area_denominator() -> None:
    spec = _containment_spec(
        "msa",
        "coc",
        selector_ids=["MSA-1"],
        min_share=0.5,
    )

    containment = build_containment_list(
        spec,
        coc_gdf=_coc_gdf(),
        county_gdf=_county_gdf(),
        msa_county_membership=MSA_MEMBERSHIP,
    )

    assert containment["candidate_id"].tolist() == ["COC-A", "COC-B"]
    assert containment["container_id"].tolist() == ["MSA-1", "MSA-1"]
    assert containment["contained_share"].tolist() == pytest.approx([2 / 3, 0.5])
    assert (containment["candidate_area"] > 0).all()
    assert (containment["container_area"] > 0).all()
    assert containment["definition_version"].unique().tolist() == ["test_msa_v1"]


@pytest.mark.parametrize(
    ("selector_field", "selector_value", "match"),
    [
        pytest.param(
            "selector_ids",
            ["missing-container"],
            "container selector_ids did not match",
            id="missing-container",
        ),
        pytest.param(
            "candidate_selector_ids",
            ["missing-candidate"],
            "candidate_selector_ids did not match",
            id="missing-candidate",
        ),
    ],
)
def test_containment_rejects_missing_selectors(
    selector_field: str,
    selector_value: list[str],
    match: str,
) -> None:
    kwargs = {selector_field: selector_value}
    spec = _containment_spec("coc", "county", **kwargs)

    with pytest.raises(ValueError, match=match):
        build_containment_list(
            spec,
            coc_gdf=_coc_gdf(ids=["COC-A"]),
            county_gdf=_county_gdf(),
        )


def test_containment_missing_geometry_error_is_actionable() -> None:
    spec = _containment_spec("coc", "county")

    with pytest.raises(ValueError, match="Missing CoC boundary geometry.*Run:"):
        build_containment_list(spec, county_gdf=_county_gdf())


def test_execute_recipe_persists_containment_output(tmp_path) -> None:
    coc_file = coc_base_path("2025", tmp_path / "data")
    county_file = county_path("2023", tmp_path / "data")
    coc_file.parent.mkdir(parents=True, exist_ok=True)
    county_file.parent.mkdir(parents=True, exist_ok=True)
    _coc_gdf(ids=["COC-A"]).to_parquet(coc_file)
    _county_gdf().to_parquet(county_file)

    recipe = load_recipe(
        {
            "version": 1,
            "name": "containment-executor-test",
            "universe": {"years": [2020]},
            "targets": [
                {
                    "id": "coc_county_containment",
                    "geometry": {"type": "coc", "vintage": 2025},
                    "outputs": ["containment"],
                    "containment_spec": {
                        "container": {"type": "coc", "vintage": 2025},
                        "candidate": {"type": "county", "vintage": 2023},
                        "selector_ids": ["COC-A"],
                        "min_share": 0.5,
                    },
                }
            ],
            "datasets": {},
            "transforms": [],
            "pipelines": [
                {
                    "id": "main",
                    "target": "coc_county_containment",
                    "steps": [],
                }
            ],
        }
    )

    results = execute_recipe(recipe, project_root=tmp_path, quiet=True)

    assert results[0].success
    assert [step.step_kind for step in results[0].steps] == ["persist_containment"]
    artifacts = resolve_pipeline_artifacts(recipe, "main", project_root=tmp_path)
    containment_path = tmp_path / artifacts["containment_path"]
    manifest_path = tmp_path / artifacts["manifest_path"]
    assert containment_path.exists()
    assert manifest_path.exists()

    containment = pd.read_parquet(containment_path)
    assert containment["candidate_id"].tolist() == ["001", "002"]
    assert containment["contained_share"].tolist() == pytest.approx([1.0, 0.5])

    metadata = pq.read_metadata(containment_path).metadata or {}
    provenance = json.loads(metadata[b"hhplab_provenance"])
    assert provenance["containment"]["row_count"] == 2
    assert provenance["containment_spec"]["container"]["type"] == "coc"

    manifest = read_manifest(manifest_path)
    assert manifest.output_path == artifacts["containment_path"]
    assert {asset.role for asset in manifest.assets} == {"geometry"}


def test_execute_recipe_persists_msa_coc_containment_output(tmp_path) -> None:
    coc_file = coc_base_path("2025", tmp_path / "data")
    county_file = county_path("2023", tmp_path / "data")
    membership_file = msa_county_membership_path("test_msa_v1", tmp_path / "data")
    coc_file.parent.mkdir(parents=True, exist_ok=True)
    county_file.parent.mkdir(parents=True, exist_ok=True)
    membership_file.parent.mkdir(parents=True, exist_ok=True)
    _coc_gdf().to_parquet(coc_file)
    _county_gdf().to_parquet(county_file)
    MSA_MEMBERSHIP.to_parquet(membership_file)

    recipe = load_recipe(
        {
            "version": 1,
            "name": "msa-containment-executor-test",
            "universe": {"years": [2020]},
            "targets": [
                {
                    "id": "msa_coc_containment",
                    "geometry": {"type": "msa", "source": "test_msa_v1"},
                    "outputs": ["containment"],
                    "containment_spec": {
                        "container": {
                            "type": "msa",
                            "vintage": 2023,
                            "source": "test_msa_v1",
                        },
                        "candidate": {"type": "coc", "vintage": 2025},
                        "selector_ids": ["MSA-1"],
                        "min_share": 0.51,
                    },
                }
            ],
            "datasets": {},
            "transforms": [],
            "pipelines": [
                {
                    "id": "main",
                    "target": "msa_coc_containment",
                    "steps": [],
                }
            ],
        }
    )

    results = execute_recipe(recipe, project_root=tmp_path, quiet=True)

    assert results[0].success
    assert [step.step_kind for step in results[0].steps] == ["persist_containment"]
    artifacts = resolve_pipeline_artifacts(recipe, "main", project_root=tmp_path)
    containment_path = tmp_path / artifacts["containment_path"]
    manifest_path = tmp_path / artifacts["manifest_path"]
    assert containment_path.exists()
    assert manifest_path.exists()

    containment = pd.read_parquet(containment_path)
    assert list(containment.columns) == list(CONTAINMENT_COLUMNS)
    assert containment["container_id"].tolist() == ["MSA-1"]
    assert containment["candidate_id"].tolist() == ["COC-A"]
    assert containment["contained_share"].tolist() == pytest.approx([2 / 3])

    metadata = pq.read_metadata(containment_path).metadata or {}
    provenance = json.loads(metadata[b"hhplab_provenance"])
    assert provenance["containment"]["row_count"] == 1
    assert provenance["containment_spec"]["container"]["type"] == "msa"
    assert provenance["containment_spec"]["container"]["source"] == "test_msa_v1"

    manifest = read_manifest(manifest_path)
    assert manifest.output_path == artifacts["containment_path"]
    assert {asset.role for asset in manifest.assets} == {"geometry"}


def test_execute_recipe_persists_panel_and_containment_outputs(tmp_path) -> None:
    coc_file = coc_base_path("2025", tmp_path / "data")
    county_file = county_path("2023", tmp_path / "data")
    pit_file = tmp_path / "data" / "pit.parquet"
    coc_file.parent.mkdir(parents=True, exist_ok=True)
    county_file.parent.mkdir(parents=True, exist_ok=True)
    _coc_gdf(ids=["COC-A"]).to_parquet(coc_file)
    _county_gdf().to_parquet(county_file)
    pd.DataFrame(
        {
            "coc_id": ["COC-A"],
            "year": [2020],
            "pit_total": [42],
        }
    ).to_parquet(pit_file)

    recipe = load_recipe(
        {
            "version": 1,
            "name": "panel-containment-executor-test",
            "universe": {"years": [2020]},
            "targets": [
                {
                    "id": "coc_panel_and_containment",
                    "geometry": {"type": "coc", "vintage": 2025},
                    "outputs": ["panel", "containment"],
                    "containment_spec": {
                        "container": {"type": "coc", "vintage": 2025},
                        "candidate": {"type": "county", "vintage": 2023},
                        "selector_ids": ["COC-A"],
                        "min_share": 0.5,
                    },
                }
            ],
            "datasets": {
                "pit": {
                    "provider": "hud",
                    "product": "pit",
                    "version": 1,
                    "native_geometry": {"type": "coc"},
                    "path": "data/pit.parquet",
                    "years": {"years": [2020]},
                },
            },
            "transforms": [],
            "pipelines": [
                {
                    "id": "main",
                    "target": "coc_panel_and_containment",
                    "steps": [
                        {
                            "resample": {
                                "dataset": "pit",
                                "to_geometry": {"type": "coc", "vintage": 2025},
                                "method": "identity",
                                "measures": ["pit_total"],
                            }
                        },
                        {
                            "join": {
                                "datasets": ["pit"],
                                "join_on": ["geo_id", "year"],
                            }
                        },
                    ],
                }
            ],
        }
    )

    results = execute_recipe(recipe, project_root=tmp_path, quiet=True)

    assert results[0].success
    assert [step.step_kind for step in results[0].steps][-2:] == [
        "persist",
        "persist_containment",
    ]
    artifacts = resolve_pipeline_artifacts(recipe, "main", project_root=tmp_path)
    panel_path = tmp_path / artifacts["panel_path"]
    containment_path = tmp_path / artifacts["containment_path"]
    panel_manifest_path = tmp_path / artifacts["manifest_path"]
    containment_manifest_path = tmp_path / artifacts["containment_manifest_path"]
    assert panel_path.exists()
    assert containment_path.exists()
    assert panel_manifest_path.exists()
    assert containment_manifest_path.exists()

    panel = pd.read_parquet(panel_path)
    assert panel["pit_total"].tolist() == [42]
    containment = pd.read_parquet(containment_path)
    assert containment["candidate_id"].tolist() == ["001", "002"]
    assert containment["contained_share"].tolist() == pytest.approx([1.0, 0.5])
