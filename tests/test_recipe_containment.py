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
    MSA_COC_MEMBERSHIP_COLUMNS,
    build_containment_list,
    build_msa_coc_membership,
)
from hhplab.recipe.executor_msa_coc_panel import (
    _first_available,
    build_msa_coc_containment_spec,
)
from hhplab.recipe.executor_manifest import resolve_pipeline_artifacts
from hhplab.recipe.loader import load_recipe
from hhplab.recipe.manifest import read_manifest
from hhplab.recipe.recipe_schema import ContainmentSpec, MsaCocPanelSpec
from hhplab.schema.columns import MSA_COC_PANEL_COLUMNS

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


@pytest.mark.parametrize(
    "frame,candidates,expected",
    [
        (
            pd.DataFrame({"value": ["1.5", "bad", None]}),
            ["value"],
            [1.5, pd.NA, pd.NA],
        ),
        (
            pd.DataFrame({"other": [1, 2, 3]}),
            ["missing"],
            [pd.NA, pd.NA, pd.NA],
        ),
    ],
    ids=["column-present", "column-missing"],
)
def test_first_available_returns_consistent_nullable_float_dtype(
    frame: pd.DataFrame,
    candidates: list[str],
    expected: list[object],
) -> None:
    result = _first_available(frame, candidates)

    assert str(result.dtype) == "Float64"
    assert result.tolist() == expected


def test_msa_coc_containment_spec_builds_with_validated_selector_ids() -> None:
    panel_spec = MsaCocPanelSpec(
        top_n=1,
        ranking_population_source="acs5",
        ranking_reference_year=2020,
        containment_min_share=0.5,
        coc_boundary_vintage=2025,
        msa_definition_version="census_msa_2023",
        msa_population_source="acs5",
        unemployment_source="acs5",
    )

    spec = build_msa_coc_containment_spec(panel_spec, selector_ids=["MSA-1"])

    assert spec.selector_ids == ["MSA-1"]
    with pytest.raises(ValueError, match="blank"):
        build_msa_coc_containment_spec(panel_spec, selector_ids=[" "])


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


def test_msa_coc_membership_reuses_containment_threshold_and_metadata() -> None:
    spec = _containment_spec(
        "msa",
        "coc",
        selector_ids=["MSA-1"],
        min_share=0.66,
    )

    membership = build_msa_coc_membership(
        spec,
        coc_gdf=_coc_gdf(),
        county_gdf=_county_gdf(),
        msa_county_membership=MSA_MEMBERSHIP,
    )

    assert list(membership.columns) == list(MSA_COC_MEMBERSHIP_COLUMNS)
    assert membership["msa_id"].tolist() == ["MSA-1"]
    assert membership["coc_id"].tolist() == ["COC-A"]
    assert membership["contained_share"].tolist() == pytest.approx([2 / 3])
    assert membership["containment_denominator"].tolist() == ["candidate_area"]
    assert membership["coc_boundary_vintage"].tolist() == [2025]
    assert membership["msa_definition_version"].tolist() == ["test_msa_v1"]
    assert membership["container_type"].tolist() == ["msa"]
    assert membership["container_id"].tolist() == ["MSA-1"]
    assert membership["candidate_type"].tolist() == ["coc"]
    assert membership["candidate_id"].tolist() == ["COC-A"]
    assert (membership["intersection_area"] > 0).all()
    assert (membership["coc_area"] > 0).all()
    assert (membership["msa_area"] > 0).all()


def test_msa_coc_membership_returns_empty_contract_for_no_matches() -> None:
    spec = _containment_spec(
        "msa",
        "coc",
        selector_ids=["MSA-1"],
        min_share=0.99,
    )

    membership = build_msa_coc_membership(
        spec,
        coc_gdf=_coc_gdf(),
        county_gdf=_county_gdf(),
        msa_county_membership=MSA_MEMBERSHIP,
    )

    assert membership.empty
    assert list(membership.columns) == list(MSA_COC_MEMBERSHIP_COLUMNS)


def test_msa_coc_membership_rejects_non_msa_coc_pair() -> None:
    spec = _containment_spec("coc", "county")

    with pytest.raises(ValueError, match="requires containment geometry pair 'msa -> coc'"):
        build_msa_coc_membership(
            spec,
            coc_gdf=_coc_gdf(),
            county_gdf=_county_gdf(),
        )


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


def test_execute_recipe_containment_filter_keeps_selected_panel_candidates(tmp_path) -> None:
    coc_file = coc_base_path("2025", tmp_path / "data")
    county_file = county_path("2023", tmp_path / "data")
    membership_file = msa_county_membership_path("test_msa_v1", tmp_path / "data")
    pit_file = tmp_path / "data" / "pit.parquet"
    coc_file.parent.mkdir(parents=True, exist_ok=True)
    county_file.parent.mkdir(parents=True, exist_ok=True)
    membership_file.parent.mkdir(parents=True, exist_ok=True)
    _coc_gdf().to_parquet(coc_file)
    _county_gdf().to_parquet(county_file)
    MSA_MEMBERSHIP.to_parquet(membership_file)
    pd.DataFrame(
        {
            "coc_id": ["COC-A", "COC-B"],
            "year": [2020, 2020],
            "pit_total": [42, 7],
        }
    ).to_parquet(pit_file)

    recipe = load_recipe(
        {
            "version": 1,
            "name": "panel-containment-filter-test",
            "universe": {"years": [2020]},
            "targets": [
                {
                    "id": "filtered_coc_panel",
                    "geometry": {"type": "coc", "vintage": 2025},
                    "outputs": ["panel"],
                    "selector_ids": ["COC-A", "COC-B"],
                    "containment_filter": {
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
                    "target": "filtered_coc_panel",
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
    artifacts = resolve_pipeline_artifacts(recipe, "main", project_root=tmp_path)
    panel_path = tmp_path / artifacts["panel_path"]
    panel = pd.read_parquet(panel_path)
    assert panel["geo_id"].tolist() == ["COC-A"]
    assert panel["pit_total"].tolist() == [42]

    metadata = pq.read_metadata(panel_path).metadata or {}
    provenance = json.loads(metadata[b"hhplab_provenance"])
    containment_filter = provenance["containment_filter"]
    assert containment_filter["candidate_count"] == 1
    assert containment_filter["panel_rows_before"] == 2
    assert containment_filter["panel_rows_after"] == 1
    assert containment_filter["spec"]["selector_ids"] == ["MSA-1"]
    target_selector = provenance["target_selector"]
    assert target_selector["selector_ids"] == ["COC-A", "COC-B"]
    assert target_selector["selected_count"] == 2


def test_execute_recipe_persists_msa_coc_panel_output(tmp_path) -> None:
    definition_version = "test_msa_2023_v1"
    coc_file = coc_base_path("2025", tmp_path / "data")
    county_file = county_path("2023", tmp_path / "data")
    membership_file = msa_county_membership_path(definition_version, tmp_path / "data")
    pit_file = tmp_path / "data" / "pit.parquet"
    coc_population_file = tmp_path / "data" / "coc_population.parquet"
    msa_acs_file = tmp_path / "data" / "msa_acs.parquet"
    coc_file.parent.mkdir(parents=True, exist_ok=True)
    county_file.parent.mkdir(parents=True, exist_ok=True)
    membership_file.parent.mkdir(parents=True, exist_ok=True)
    _coc_gdf().to_parquet(coc_file)
    _county_gdf().to_parquet(county_file)
    MSA_MEMBERSHIP.to_parquet(membership_file)
    pd.DataFrame(
        {
            "coc_id": ["COC-A", "COC-B", "COC-A", "COC-B"],
            "year": [2020, 2020, 2021, 2021],
            "pit_total": [42, 7, 45, 9],
            "pit_sheltered": [40, 5, 41, 6],
            "pit_unsheltered": [2, 2, 4, 3],
        }
    ).to_parquet(pit_file)
    pd.DataFrame(
        {
            "coc_id": ["COC-A", "COC-B", "COC-A", "COC-B"],
            "year": [2020, 2020, 2021, 2021],
            "population": [1000, 500, 1010, 505],
        }
    ).to_parquet(coc_population_file)
    pd.DataFrame(
        {
            "msa_id": ["MSA-1", "MSA-1"],
            "year": [2020, 2021],
            "total_population": [10_000, 10_100],
            "median_gross_rent": [1500.0, 1525.0],
            "vacancy_rate": [0.05, 0.04],
            "poverty_rate": [0.12, 0.11],
            "median_household_income": [80_000.0, 81_000.0],
            "rent_burden_30_plus": [0.31, 0.30],
            "civilian_labor_force": [5000.0, 5050.0],
            "unemployed_count": [250.0, 202.0],
        }
    ).to_parquet(msa_acs_file)

    recipe = load_recipe(
        {
            "version": 1,
            "name": "msa-coc-panel-executor-test",
            "universe": {"years": [2020, 2021]},
            "targets": [
                {
                    "id": "msa_coc_panel",
                    "geometry": {"type": "coc", "vintage": 2025},
                    "outputs": ["panel"],
                    "msa_coc_panel": {
                        "top_n": 1,
                        "ranking_population_source": "acs5",
                        "ranking_reference_year": 2020,
                        "containment_min_share": 0.5,
                        "containment_denominator": "candidate_area",
                        "coc_boundary_vintage": 2025,
                        "msa_definition_version": definition_version,
                        "msa_population_source": "acs5",
                        "unemployment_source": "acs5",
                        "output_aliases": {"msa_population": "msa_population_acs5_alias"},
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
                    "years": {"years": [2020, 2021]},
                    "geo_column": "coc_id",
                },
                "coc_population": {
                    "provider": "census",
                    "product": "pep",
                    "version": 1,
                    "native_geometry": {"type": "coc"},
                    "path": "data/coc_population.parquet",
                    "years": {"years": [2020, 2021]},
                    "geo_column": "coc_id",
                },
                "msa_acs": {
                    "provider": "census",
                    "product": "acs5",
                    "version": 1,
                    "native_geometry": {"type": "msa"},
                    "path": "data/msa_acs.parquet",
                    "years": {"years": [2020, 2021]},
                    "geo_column": "msa_id",
                },
            },
            "transforms": [],
            "pipelines": [
                {
                    "id": "main",
                    "target": "msa_coc_panel",
                    "steps": [
                        {
                            "resample": {
                                "dataset": "pit",
                                "to_geometry": {"type": "coc", "vintage": 2025},
                                "method": "identity",
                                "measures": ["pit_total", "pit_sheltered", "pit_unsheltered"],
                            }
                        },
                        {
                            "resample": {
                                "dataset": "coc_population",
                                "to_geometry": {"type": "coc", "vintage": 2025},
                                "method": "identity",
                                "measures": ["population"],
                            }
                        },
                        {
                            "resample": {
                                "dataset": "msa_acs",
                                "to_geometry": {"type": "msa", "source": definition_version},
                                "method": "identity",
                                "measures": [
                                    "total_population",
                                    "median_gross_rent",
                                    "vacancy_rate",
                                    "poverty_rate",
                                    "median_household_income",
                                    "rent_burden_30_plus",
                                    "civilian_labor_force",
                                    "unemployed_count",
                                ],
                            }
                        },
                        {
                            "join": {
                                "datasets": ["pit", "coc_population"],
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
    assert results[0].steps[-1].step_kind == "persist"
    artifacts = resolve_pipeline_artifacts(recipe, "main", project_root=tmp_path)
    panel_path = tmp_path / artifacts["panel_path"]
    assert panel_path.name == "panel__msa-coc__Y2020-2021@B2025xMtestmsa2023v1.parquet"
    panel = pd.read_parquet(panel_path)
    assert list(panel.columns[: len(MSA_COC_PANEL_COLUMNS)]) == MSA_COC_PANEL_COLUMNS
    assert panel[["msa_id", "coc_id", "year"]].to_dict(orient="records") == [
        {"msa_id": "MSA-1", "coc_id": "COC-A", "year": 2020},
        {"msa_id": "MSA-1", "coc_id": "COC-A", "year": 2021},
        {"msa_id": "MSA-1", "coc_id": "COC-B", "year": 2020},
        {"msa_id": "MSA-1", "coc_id": "COC-B", "year": 2021},
    ]
    grain_columns = ["msa_id", "coc_id", "year"]
    assert not panel.duplicated(grain_columns).any()
    assert (panel.groupby(grain_columns, dropna=False).size() == 1).all()
    assert panel["msa_population"].tolist() == [10_000, 10_100, 10_000, 10_100]
    assert panel["msa_population_acs5_alias"].tolist() == [10_000, 10_100, 10_000, 10_100]
    assert panel["msa_unemployment"].tolist() == pytest.approx([0.05, 0.04, 0.05, 0.04])
    assert panel["coc_population"].tolist() == [1000, 1010, 500, 505]
    assert panel["pit_total"].tolist() == [42, 45, 7, 9]

    metadata = pq.read_metadata(panel_path).metadata or {}
    provenance = json.loads(metadata[b"hhplab_provenance"])
    assert provenance["msa_coc_panel"]["selected_msa_ids"] == ["MSA-1"]
    assert provenance["msa_coc_panel"]["msa_population_source"] == "acs5"
    assert provenance["msa_coc_panel"]["unemployment_source"] == "acs5"


def test_execute_recipe_target_selector_filters_panel_rows(tmp_path) -> None:
    coc_file = coc_base_path("2025", tmp_path / "data")
    pit_file = tmp_path / "data" / "pit.parquet"
    coc_file.parent.mkdir(parents=True, exist_ok=True)
    _coc_gdf().to_parquet(coc_file)
    pd.DataFrame(
        {
            "coc_id": ["COC-A", "COC-B"],
            "year": [2020, 2020],
            "pit_total": [42, 7],
        }
    ).to_parquet(pit_file)

    recipe = load_recipe(
        {
            "version": 1,
            "name": "panel-target-selector-test",
            "universe": {"years": [2020]},
            "targets": [
                {
                    "id": "selected_coc_panel",
                    "geometry": {"type": "coc", "vintage": 2025},
                    "outputs": ["panel"],
                    "selector_ids": ["COC-B"],
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
                    "target": "selected_coc_panel",
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
    artifacts = resolve_pipeline_artifacts(recipe, "main", project_root=tmp_path)
    panel_path = tmp_path / artifacts["panel_path"]
    panel = pd.read_parquet(panel_path)
    assert panel["geo_id"].tolist() == ["COC-B"]
    assert panel["pit_total"].tolist() == [7]

    metadata = pq.read_metadata(panel_path).metadata or {}
    provenance = json.loads(metadata[b"hhplab_provenance"])
    target_selector = provenance["target_selector"]
    assert target_selector["selector_ids"] == ["COC-B"]
    assert target_selector["selected_count"] == 1
    assert target_selector["panel_rows_before"] == 2
    assert target_selector["panel_rows_after"] == 1
