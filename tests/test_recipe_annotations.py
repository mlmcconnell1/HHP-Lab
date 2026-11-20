"""Annotation propagation through recipe preparation primitives."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from hhplab.artifacts.naming.naming import coc_base_path
from hhplab.geographies.xwalks.boundary_annotations import (
    B2010_COVERAGE_ANNOTATION,
    bind_boundary_annotations,
    bind_tract_mediated_annotations,
)
from hhplab.geographies.xwalks.tract_mediated import (
    build_tract_mediated_county_crosswalk,
    save_tract_mediated_county_crosswalk,
)
from hhplab.recipe.cache import RecipeCache
from hhplab.recipe.executor import (
    ExecutionContext,
    _execute_join,
    _execute_resample,
    _resample_allocate,
    derived_measure_required_columns,
    persistence,
)
from hhplab.recipe.executor import panel as recipe_panel
from hhplab.recipe.executor.annotations import (
    bind_crosswalk_aggregate_contributors,
    bind_source_aggregate_contributors,
    frame_column_map,
    propagate_annotations,
)
from hhplab.recipe.executor.core import ExecutorError
from hhplab.recipe.executor.inputs import _apply_temporal_filter
from hhplab.recipe.loader import load_recipe
from hhplab.recipe.planner import ExecutionPlan, JoinTask, ResampleTask
from hhplab.recipe.recipe_schema import TemporalFilter
from hhplab.recipe.schema_common import GeometryRef
from hhplab.results.runner import run_results
from hhplab.schema.contracts import (
    frame_annotation_contract,
    serialize_annotation_contract,
)
from hhplab.storage.config import StorageConfig
from hhplab.storage.provenance import ProvenanceBlock, write_parquet_with_provenance

# The retained fixture row has RTS=0: excluding RTS=1 keeps the row, but its
# classification caveat must still apply. Projected reason/context notes stay unused.
PREPARATION_VALUES = {"pit_total": 12, "context_value": 4, "right_to_shelter": 0}


def _record(
    annotation_id: str,
    column: str,
    *,
    public_text: str | None = None,
) -> dict[str, object]:
    record: dict[str, object] = {
        "id": annotation_id,
        "revision": 1,
        "category": "caveat",
        "summary": f"Caveat for {column}",
        "text": f"The {column} value has a known limitation.",
        "scope": {"columns": [column]},
    }
    if public_text is not None:
        record["public_text"] = public_text
    return record


def _annotated_frame(
    *,
    geography: str,
    measure: str,
    value: float,
    annotation_id: str,
    annotation_column: str,
    annotation_source: str,
    public_text: str | None = None,
) -> pd.DataFrame:
    values = {"coc_id": [geography], "year": [2020], measure: [value]}
    if annotation_column not in values:
        values[annotation_column] = [1]
    frame = pd.DataFrame(values)
    frame.attrs["annotations"] = serialize_annotation_contract(
        [_record(annotation_id, annotation_column, public_text=public_text)],
        bindings={
            f"{annotation_id}#r1": {
                "columns": {annotation_column: [[geography, 2020]]},
            },
        },
        sources={
            f"{annotation_id}#r1": [
                {"sha256": hashlib.sha256(annotation_source.encode()).hexdigest(),
                 "vintages": {"geo_type": "coc"}},
            ],
        },
    )
    return frame


def test_join_projection_alias_and_persistence_retain_exact_bindings(tmp_path: Path) -> None:
    left = _annotated_frame(
        geography="COC-1",
        measure="pit_total",
        value=12,
        annotation_id="rts-caveat",
        annotation_column="pit_total",
        annotation_source="rts-source",
    )
    right = _annotated_frame(
        geography="COC-1",
        measure="income",
        value=42000,
        annotation_id="income-note",
        annotation_column="income",
        annotation_source="income-source",
    )
    joined = left.merge(right.drop(columns=["coc_id", "year"]), left_index=True, right_index=True)
    # The policy projects away an annotated exclusion flag while retaining the record.
    joined = joined.drop(columns=["pit_total", "income"])
    joined["pit_rate"] = [0.2]
    joined = propagate_annotations(
        joined,
        [
            (left, frame_column_map(left, joined, aliases={"pit_total": "pit_rate"})),
            (right, frame_column_map(right, joined)),
        ],
        output_geo_column="coc_id",
    )

    contract = frame_annotation_contract(joined)
    assert contract is not None
    assert [record["id"] for record in contract["records"]] == [
        "income-note", "rts-caveat",
    ]
    assert contract["bindings"]["rts-caveat#r1"]["columns"] == {
        "pit_rate": [["COC-1", 2020]],
    }
    assert contract["bindings"]["income-note#r1"]["columns"] == {}
    assert len(contract["sources"]["rts-caveat#r1"]) == 1

    output_path = tmp_path / "prepared.parquet"
    write_parquet_with_provenance(
        joined,
        output_path,
        ProvenanceBlock(geo_type="coc"),
    )
    loaded = RecipeCache().read_parquet(output_path)
    persisted = frame_annotation_contract(loaded)
    assert persisted == contract


def test_annual_crosswalk_annotation_maps_contributors_to_msa_years() -> None:
    key = "hud-annual-wilmington-redraw#r1"
    contract = serialize_annotation_contract(
        [_record("hud-annual-wilmington-redraw", "allocation_share")],
        bindings={key: {"columns": {
            "allocation_share": [["NC-506", 2023]],
        }}},
    )
    contributors = pd.DataFrame({
        "coc_id": ["NC-506", "NC-500"],
        "msa_id": ["48900", "48900"],
        "year": [2019, 2020],
    })
    output = pd.DataFrame({
        "msa_id": ["48900", "48900"],
        "year": [2019, 2020],
        "population": [10, 11],
        "independent_denominator": [100, 101],
    })
    result = bind_crosswalk_aggregate_contributors(
        output, contract,
        {"population": ("allocation_share", contributors),
         "independent_denominator": ("county_population", contributors)},
        target_geo_column="msa_id", output_geo_column="msa_id",
    )
    assert frame_annotation_contract(result)["bindings"][key]["columns"] == {
        "population": [["48900", 2019]],
    }


def test_v1_identity_and_join_output_is_readable_without_redeclaration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recipe = load_recipe(
        {
            "version": 1,
            "name": "annotation-preparation",
            "universe": {"range": "2020-2020"},
            "targets": [{
                "id": "target",
                "geometry": {"type": "coc", "vintage": 2025},
                "panel_policy": {
                    "column_aliases": {"pit_total": "pit_rate"},
                    "output_columns": [
                        "coc_id", "geo_id", "year", "pit_rate", "boundary_changed",
                        "right_to_shelter",
                    ],
                },
            }],
            "datasets": {
                dataset_id: {
                    "provider": "fixture",
                    "product": "annotated-table",
                    "version": 1,
                    "native_geometry": {"type": "coc"},
                    "years": "2020-2020",
                    "path": f"data/{dataset_id}.parquet",
                }
                for dataset_id in ("rts", "context")
            },
            "pipelines": [{"id": "main", "target": "target", "steps": []}],
        }
    )
    ctx = ExecutionContext(project_root=tmp_path, recipe=recipe, quiet=True)
    rts_frame = _annotated_frame(
        geography="COC-1",
        measure="pit_total",
        value=PREPARATION_VALUES["pit_total"],
        annotation_id="rts-caveat",
        annotation_column="pit_total",
        annotation_source="rts-source",
        public_text="The PIT result has a public methodology caveat.",
    )
    rts_frame["excluded_reason"] = "masked source row"
    rts_frame["right_to_shelter"] = PREPARATION_VALUES["right_to_shelter"]
    rts_frame.attrs["annotations"] = serialize_annotation_contract(
        [
            _record("rts-caveat", "pit_total", public_text=(
                "The PIT result has a public methodology caveat."
            )),
            _record("rts-exclusion", "excluded_reason"),
            _record("rts-baseline", "right_to_shelter", public_text=(
                "The shelter-right classification is provisional."
            )),
        ],
        bindings={
            "rts-caveat#r1": {"columns": {"pit_total": [["COC-1", 2020]]}},
            "rts-exclusion#r1": {
                "columns": {"excluded_reason": [["COC-1", 2020]]},
            },
            "rts-baseline#r1": {
                "columns": {"right_to_shelter": [["COC-1", 2020]]},
            },
        },
    )
    annotated_inputs = [
        (
            "rts",
            rts_frame,
            ["pit_total", "right_to_shelter"],
        ),
        (
            "context",
            _annotated_frame(
                geography="COC-1",
                measure="context_value",
                value=PREPARATION_VALUES["context_value"],
                annotation_id="context-note",
                annotation_column="context_value",
                annotation_source="context-source",
            ),
            ["context_value"],
        ),
    ]
    for dataset_id, frame, measures in annotated_inputs:
        input_path = tmp_path / "data" / f"{dataset_id}.parquet"
        write_parquet_with_provenance(
            frame, input_path, ProvenanceBlock(geo_type="coc"),
        )
        result = _execute_resample(
            ResampleTask(
                dataset_id=dataset_id,
                year=2020,
                input_path=str(input_path.relative_to(tmp_path)),
                effective_geometry=GeometryRef(type="coc"),
                method="identity",
                transform_id=None,
                to_geometry=GeometryRef(type="coc"),
                measures=measures,
            ),
            ctx,
        )
        assert result.success, result.error

    joined = _execute_join(
        JoinTask(
            datasets=["rts", "context"],
            join_on=["geo_id", "year"],
            year=2020,
        ),
        ctx,
    )
    assert joined.success, joined.error
    prepared = ctx.intermediates[("__joined__", 2020)]
    assert "results_content_sha256" not in prepared.attrs
    assert "results_source_parquet_bytes" not in prepared.attrs
    contract = frame_annotation_contract(prepared)
    assert contract is not None
    assert contract["bindings"]["rts-caveat#r1"]["columns"] == {
        "pit_total": [["COC-1", 2020]],
    }
    assert contract["bindings"]["context-note#r1"]["columns"] == {
        "context_value": [["COC-1", 2020]],
    }
    assert contract["bindings"]["rts-exclusion#r1"]["columns"] == {}
    rts_sha256 = hashlib.sha256(
        (tmp_path / "data" / "rts.parquet").read_bytes(),
    ).hexdigest()
    assert any(
        source["sha256"] == rts_sha256
        and source["vintages"].get("geo_type") == "coc"
        for source in contract["sources"]["rts-caveat#r1"]
    )

    prepared["boundary_changed"] = False
    ctx.intermediates[("__joined__", 2020)] = prepared
    output_path = tmp_path / "data" / "prepared.parquet"
    monkeypatch.setattr(
        recipe_panel,
        "_add_recipe_coc_names",
        lambda panel, **_kwargs: panel,
    )
    monkeypatch.setattr(
        recipe_panel,
        "_add_recipe_coc_population_density",
        lambda panel, **_kwargs: panel,
    )
    monkeypatch.setattr(
        persistence,
        "_resolve_panel_output_file",
        lambda *_args, **_kwargs: output_path,
    )
    monkeypatch.setattr(persistence, "_build_provenance", lambda *_args: {"extra": {}})
    monkeypatch.setattr(persistence, "_build_manifest", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(persistence, "write_manifest", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        persistence,
        "collect_conformance_flags",
        lambda **_kwargs: SimpleNamespace(
            measure_columns=["pit_total", "context_value"],
            acs_products=[],
            include_zori=False,
            include_laus=False,
        ),
    )
    monkeypatch.setattr(
        persistence,
        "run_conformance",
        lambda *_args, **_kwargs: SimpleNamespace(
            to_dict=lambda: {}, summary=lambda: "synthetic conformance",
        ),
    )
    result = persistence.persist_outputs(ExecutionPlan(pipeline_id="main"), ctx)
    assert result.success, result.error
    reloaded = RecipeCache().read_parquet(output_path)
    persisted_contract = frame_annotation_contract(reloaded)
    assert persisted_contract is not None
    assert persisted_contract["bindings"]["rts-caveat#r1"]["columns"] == {
        "pit_rate": [["COC-1", 2020]],
    }
    assert persisted_contract["bindings"]["context-note#r1"]["columns"] == {}
    assert persisted_contract["bindings"]["rts-exclusion#r1"]["columns"] == {}
    assert persisted_contract["bindings"]["rts-baseline#r1"]["columns"] == {
        "right_to_shelter": [["COC-1", 2020]],
    }
    assert reloaded["pit_rate"].tolist() == [PREPARATION_VALUES["pit_total"]]
    assert reloaded["right_to_shelter"].tolist() == [PREPARATION_VALUES["right_to_shelter"]]
    assert "results_content_sha256" in reloaded.attrs

    results_recipe = load_recipe(
        {
            "version": 2,
            "id": "prepared-annotation-report",
            "title": "Prepared annotation report",
            "authors": ["test"],
            "universe": {"range": "2020-2020"},
            "targets": [{"id": "target", "geometry": {"type": "coc", "vintage": 2025}}],
            "datasets": {},
            "pipelines": [{"id": "build", "target": "target", "steps": []}],
            "panels": {
                "panel": {
                    "ref": {"pipeline": "build", "output": "panel"},
                    "geo_key": "geo_id",
                    "time_key": "year",
                },
            },
            "annotations": [],
            "masks": [],
            "analyses": [{
                "id": "describe",
                "method": "descriptive",
                "input": "panels.panel",
                "sample": {"geographies": {
                    "exclude": {"column": "right_to_shelter", "equals": 1},
                }},
                "columns": ["pit_rate"],
                "stats": ["n", "mean"],
            }],
            "results": {
                "bundle_dir": "results/{id}/{run_id}",
                "include_panels": ["panel"],
                "report": {"title": "Prepared annotation report"},
            },
        }
    )
    run_input = json.dumps(
        results_recipe.model_dump(mode="json", by_alias=True, exclude_none=True),
        sort_keys=True,
    ).encode()
    private_result = run_results(
        results_recipe,
        original_recipe_bytes=run_input,
        outputs_root=tmp_path / "private-results",
        panel_frames={"panel": reloaded},
        private=True,
    )
    private_bundle = Path(private_result["bundle_path"])
    private_annotations = json.loads((private_bundle / "annotations.json").read_bytes())
    assert "rts-caveat" in {record["id"] for record in private_annotations["records"]}
    assert "rts-exclusion" in {record["id"] for record in private_annotations["records"]}
    applied = {record["id"] for record in private_annotations["applied"]}
    assert "rts-baseline" in applied
    assert "rts-exclusion" not in applied
    assert "Caveat for pit_total" in (private_bundle / "report" / "index.html").read_text()

    public_result = run_results(
        results_recipe,
        original_recipe_bytes=run_input,
        outputs_root=tmp_path / "public-results",
        panel_frames={"panel": reloaded},
        private=False,
    )
    public_bundle = Path(public_result["bundle_path"])
    public_annotations = (public_bundle / "annotations.json").read_text()
    assert "The PIT result has a public methodology caveat." in public_annotations
    assert "The shelter-right classification is provisional." in (
        public_bundle / "report" / "index.html"
    ).read_text()
    assert "The pit_total value has a known limitation." not in public_annotations
    public_sample = public_bundle / "samples" / "describe@base.parquet"
    assert b"The pit_total value has a known limitation." not in public_sample.read_bytes()
    with pytest.raises(
        ValueError,
        match="Public sample artifact omits detailed annotation lineage",
    ):
        RecipeCache().read_parquet(public_sample)

    single_source = ctx.intermediates[("rts", 2020)].copy()
    single_source["boundary_changed"] = False
    single_ctx = ExecutionContext(project_root=tmp_path, recipe=recipe, quiet=True)
    single_ctx.intermediates[("__joined__", 2020)] = single_source
    single_output = tmp_path / "data" / "prepared-single-source.parquet"
    monkeypatch.setattr(
        persistence,
        "_resolve_panel_output_file",
        lambda *_args, **_kwargs: single_output,
    )
    single_result = persistence.persist_outputs(ExecutionPlan(pipeline_id="main"), single_ctx)
    assert single_result.success, single_result.error
    single_loaded = RecipeCache().read_parquet(single_output)
    single_contract = frame_annotation_contract(single_loaded)
    assert single_contract is not None
    assert any(
        source["sha256"] == rts_sha256
        for source in single_contract["sources"]["rts-caveat#r1"]
    )


def test_out_of_scope_rows_do_not_receive_bindings() -> None:
    source = _annotated_frame(
        geography="COC-1",
        measure="pit_total",
        value=12,
        annotation_id="scoped-note",
        annotation_column="pit_total",
        annotation_source="rts-source",
    )
    output = pd.DataFrame(
        {"geo_id": ["COC-2"], "year": [2020], "pit_total": [0]},
    )
    rebound = propagate_annotations(
        output,
        [(source, {"pit_total": "pit_total"})],
    )
    contract = frame_annotation_contract(rebound)
    assert contract is not None
    assert contract["bindings"]["scoped-note#r1"]["columns"] == {}


def test_empty_annotated_source_without_geo_key_retains_immutable_record() -> None:
    source = _annotated_frame(
        geography="COC-1",
        measure="pit_total",
        value=12,
        annotation_id="unused-empty-source",
        annotation_column="pit_total",
        annotation_source="empty-source",
    ).iloc[0:0].drop(columns=["coc_id", "year"])
    output = pd.DataFrame(
        {"geo_id": ["COC-2"], "year": [2020], "pit_total": [0]},
    )
    rebound = propagate_annotations(
        output,
        [(source, {"pit_total": "pit_total"})],
    )
    contract = frame_annotation_contract(rebound)
    assert contract is not None
    assert [record["id"] for record in contract["records"]] == ["unused-empty-source"]
    assert contract["bindings"]["unused-empty-source#r1"]["columns"] == {}


def test_join_tracks_duplicate_measure_columns_across_three_sources() -> None:
    recipe = load_recipe(
        {
            "version": 1,
            "name": "annotation-three-way-join",
            "universe": {"range": "2020-2020"},
            "targets": [{"id": "target", "geometry": {"type": "coc"}}],
            "datasets": {
                dataset_id: {
                    "provider": "fixture",
                    "product": "annotated-table",
                    "version": 1,
                    "native_geometry": {"type": "coc"},
                    "years": "2020-2020",
                    "path": f"data/{dataset_id}.parquet",
                }
                for dataset_id in ("first", "second", "third")
            },
            "pipelines": [{"id": "main", "target": "target", "steps": []}],
        }
    )
    ctx = ExecutionContext(project_root=Path("."), recipe=recipe, quiet=True)
    for dataset_id in ("first", "second", "third"):
        frame = _annotated_frame(
            geography="COC-1",
            measure="shared_measure",
            value=1,
            annotation_id=f"{dataset_id}-note",
            annotation_column="shared_measure",
            annotation_source=dataset_id,
        ).rename(columns={"coc_id": "geo_id"})
        ctx.intermediates[(dataset_id, 2020)] = propagate_annotations(
            frame,
            [(frame, {"shared_measure": "shared_measure"})],
        )

    result = _execute_join(
        JoinTask(
            datasets=["first", "second", "third"],
            join_on=["geo_id", "year"],
            year=2020,
        ),
        ctx,
    )
    assert result.success, result.error
    joined = ctx.intermediates[("__joined__", 2020)]
    contract = frame_annotation_contract(joined)
    assert contract is not None
    assert contract["bindings"]["first-note#r1"]["columns"] == {
        "shared_measure_x": [["COC-1", 2020]],
    }
    assert contract["bindings"]["second-note#r1"]["columns"] == {
        "shared_measure_y": [["COC-1", 2020]],
    }
    assert contract["bindings"]["third-note#r1"]["columns"] == {
        "shared_measure": [["COC-1", 2020]],
    }


def test_annotated_preparation_requires_scope_keys() -> None:
    source = _annotated_frame(
        geography="COC-1",
        measure="pit_total",
        value=12,
        annotation_id="missing-key-note",
        annotation_column="pit_total",
        annotation_source="rts-source",
    ).drop(columns=["year"])
    with pytest.raises(ValueError, match="no year key"):
        propagate_annotations(
            pd.DataFrame({"geo_id": ["COC-1"], "year": [2020], "pit_total": [12]}),
            [(source, {"pit_total": "pit_total"})],
        )


def test_annotation_revision_conflict_fails_closed() -> None:
    left = _annotated_frame(
        geography="COC-1",
        measure="pit_total",
        value=12,
        annotation_id="shared-note",
        annotation_column="pit_total",
        annotation_source="source-a",
    )
    right = _annotated_frame(
        geography="COC-1",
        measure="income",
        value=42000,
        annotation_id="shared-note",
        annotation_column="income",
        annotation_source="source-b",
    )
    right.attrs["annotations"]["records"][0]["text"] = "Conflicting immutable text."
    output = pd.DataFrame(
        {"geo_id": ["COC-1"], "year": [2020], "pit_total": [12], "income": [42000]},
    )
    with pytest.raises(ValueError, match="conflicting annotation content"):
        propagate_annotations(
            output,
            [(left, {"pit_total": "pit_total"}), (right, {"income": "income"})],
        )


def test_point_in_time_filter_retains_only_matching_annotated_cells() -> None:
    source = pd.DataFrame(
        {
            "coc_id": ["COC-1", "COC-2", "COC-1"],
            "year": [2020, 2020, 2021],
            "month": [1, 2, 2],
            "pit_total": [12, 14, 16],
        }
    )
    source.attrs["annotations"] = serialize_annotation_contract(
        [_record("january-only", "pit_total")],
        bindings={"january-only#r1": {
            "columns": {
                "pit_total": [
                    ["COC-1", 2020], ["COC-2", 2020], ["COC-1", 2021],
                ],
            },
        }},
    )
    filtered = _apply_temporal_filter(
        source,
        TemporalFilter(column="month", method="point_in_time", month=1),
        2020,
        "rts",
    )
    assert filtered["pit_total"].tolist() == [12]
    contract = frame_annotation_contract(filtered)
    assert contract is not None
    assert contract["bindings"]["january-only#r1"]["columns"] == {
        "pit_total": [["COC-1", 2020]],
    }


@pytest.mark.parametrize(
    ("geo_column", "year_column", "geometry", "geography"),
    [
        ("metro_id", None, "metro", "M-1"),
        ("pit_geo_id", "pit_year", "coc", "COC-1"),
    ],
    ids=["native-metro-id", "custom-pit-keys"],
)
def test_identity_resample_uses_resolved_custom_geography_and_year_keys(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    geo_column: str,
    year_column: str | None,
    geometry: str,
    geography: str,
) -> None:
    row: dict[str, object] = {
        geo_column: geography,
        "pit_total": 9,
    }
    if year_column is not None:
        row[year_column] = 2020
    else:
        row["year"] = 2020
    frame = pd.DataFrame([row])
    frame.attrs["annotations"] = serialize_annotation_contract(
        [_record("resolved-key-note", "pit_total")],
        bindings={
            "resolved-key-note#r1": {
                "columns": {"pit_total": [[geography, 2020]]},
            },
        },
    )
    input_path = tmp_path / "data" / "source.parquet"
    write_parquet_with_provenance(frame, input_path, ProvenanceBlock(geo_type=geometry))
    recipe = load_recipe(
        {
            "version": 1,
            "name": "annotation-custom-keys",
            "universe": {"range": "2020-2020"},
            "targets": [{"id": "target", "geometry": {"type": geometry}}],
            "datasets": {
                "source": {
                    "provider": "fixture",
                    "product": "annotated-table",
                    "version": 1,
                    "native_geometry": {"type": geometry},
                    "years": "2020-2020",
                    "path": "data/source.parquet",
                },
            },
            "pipelines": [{"id": "main", "target": "target", "steps": []}],
        }
    )
    task = ResampleTask(
        dataset_id="source",
        year=2020,
        input_path="data/source.parquet",
        effective_geometry=GeometryRef(type=geometry),
        method="identity",
        transform_id=None,
        to_geometry=GeometryRef(type=geometry),
        measures=["pit_total"],
        year_column=year_column,
        geo_column=geo_column if geo_column != "metro_id" else None,
    )
    ctx = ExecutionContext(project_root=tmp_path, recipe=recipe, quiet=True)
    result = _execute_resample(task, ctx)
    assert result.success, result.error
    contract = frame_annotation_contract(ctx.intermediates[("source", 2020)])
    assert contract is not None
    assert contract["bindings"]["resolved-key-note#r1"]["columns"] == {
        "pit_total": [[geography, 2020]],
    }
    canonical_input = ctx.intermediates[("source", 2020)].copy()
    canonical_input["boundary_changed"] = False
    ctx.intermediates[("__joined__", 2020)] = canonical_input
    if geometry == "coc":
        monkeypatch.setattr(
            recipe_panel, "_add_recipe_coc_names", lambda panel, **_kwargs: panel,
        )
        monkeypatch.setattr(
            recipe_panel,
            "_add_recipe_coc_population_density",
            lambda panel, **_kwargs: panel,
        )
    else:
        monkeypatch.setattr(
            recipe_panel, "_add_recipe_metro_metadata", lambda panel, **_kwargs: panel,
        )
    assembled = recipe_panel.assemble_panel(ExecutionPlan(pipeline_id="main"), ctx)
    assert isinstance(assembled, recipe_panel.AssembledPanel)
    final_contract = frame_annotation_contract(assembled.panel)
    assert final_contract is not None
    final_geo = "coc_id" if geometry == "coc" else "metro_id"
    assert final_contract["bindings"]["resolved-key-note#r1"]["columns"] == {
        "pit_total": [[geography, 2020]],
    }
    assert final_geo in assembled.panel.columns


@pytest.mark.parametrize("lag", [1, 2], ids=["acs-reference-year", "unreviewed-lag"])
def test_annotated_acs1_lag_preserves_exact_year_bindings(tmp_path: Path, lag: int) -> None:
    source_year = 2020
    panel_year = source_year + lag
    frame = pd.DataFrame({
        "metro_id": ["35620"], "acs1_vintage": [source_year], "total_population": [100],
    })
    frame.attrs["annotations"] = serialize_annotation_contract(
        [_record("lagged-source-note", "total_population")],
        bindings={"lagged-source-note#r1": {
            "columns": {"total_population": [["35620", source_year]]},
        }},
    )
    path = tmp_path / "data" / "source.parquet"
    write_parquet_with_provenance(frame, path, ProvenanceBlock(geo_type="metro"))
    recipe = load_recipe({
        "version": 1, "name": "annotation-lagged-source",
        "universe": {"years": [panel_year]},
        "targets": [{"id": "target", "geometry": {"type": "metro"}}],
        "datasets": {"source": {
            "provider": "census", "product": "acs1", "version": 1,
            "native_geometry": {"type": "metro"}, "path": "data/source.parquet",
        }},
        "pipelines": [{"id": "main", "target": "target", "steps": []}],
    })
    ctx = ExecutionContext(project_root=tmp_path, recipe=recipe, quiet=True)
    result = _execute_resample(
        ResampleTask(
            dataset_id="source", year=panel_year, input_path="data/source.parquet",
            effective_geometry=GeometryRef(type="metro"), method="identity",
            transform_id=None, to_geometry=GeometryRef(type="metro"),
            measures=["total_population"], year_column="acs1_vintage",
        ),
        ctx,
    )
    if lag == 2:
        assert not result.success
        assert "annotation year mapping" in result.error
        return
    assert result.success, result.error
    prepared = ctx.intermediates[("source", panel_year)]
    assert prepared["acs1_vintage"].tolist() == [source_year]
    contract = frame_annotation_contract(prepared)
    assert contract["bindings"]["lagged-source-note#r1"]["columns"] == {
        "total_population": [["35620", panel_year]],
    }
    assert hashlib.sha256(path.read_bytes()).hexdigest() in {
        entry["sha256"] for entry in contract["sources"]["lagged-source-note#r1"]
    }


def test_annotated_aggregate_binds_exact_source_cells_and_persists(tmp_path: Path) -> None:
    input_path = tmp_path / "data" / "rts.parquet"
    frame = _annotated_frame(
        geography="01001",
        measure="pit_total",
        value=12,
        annotation_id="aggregate-note",
        annotation_column="pit_total",
        annotation_source="rts-source",
    )
    frame = frame.rename(columns={"coc_id": "county_fips"})
    frame = pd.concat([
        frame,
        pd.DataFrame({
            "county_fips": ["01003", "01005"], "year": [2020, 2020],
            "pit_total": [0.0, None],
        }),
    ], ignore_index=True)
    frame.attrs["annotations"] = serialize_annotation_contract(
        [
            _record("aggregate-note", "pit_total"),
            _record("zero-weight-note", "pit_total"),
            _record("null-measure-note", "pit_total"),
        ],
        bindings={
            "aggregate-note#r1": {"columns": {"pit_total": [["01001", 2020]]}},
            "zero-weight-note#r1": {"columns": {"pit_total": [["01003", 2020]]}},
            "null-measure-note#r1": {"columns": {"pit_total": [["01005", 2020]]}},
        },
    )
    write_parquet_with_provenance(frame, input_path, ProvenanceBlock(geo_type="county"))
    xwalk_path = tmp_path / "data" / "county-to-coc.parquet"
    pd.DataFrame({
        "county_fips": ["01001", "01003", "01005"],
        "coc_id": ["COC-1", "COC-1", "COC-1"], "area_share": [1.0, 0.0, 1.0],
    }).to_parquet(xwalk_path)
    recipe = load_recipe(
        {
            "version": 1,
            "name": "annotation-aggregate",
            "universe": {"range": "2020-2020"},
            "targets": [{"id": "target", "geometry": {"type": "coc"}}],
            "datasets": {
                "rts": {
                    "provider": "fixture",
                    "product": "annotated-table",
                    "version": 1,
                    "native_geometry": {"type": "county"},
                    "years": "2020-2020",
                    "path": "data/rts.parquet",
                },
            },
            "transforms": [{
                "id": "county-to-coc", "type": "crosswalk",
                "from": {"type": "county"}, "to": {"type": "coc"},
                "spec": {"weighting": {"scheme": "area"}},
            }],
            "pipelines": [{"id": "main", "target": "target", "steps": []}],
        }
    )
    ctx = ExecutionContext(
        project_root=tmp_path, recipe=recipe, quiet=True,
        transform_paths={"county-to-coc": xwalk_path},
    )
    result = _execute_resample(
        ResampleTask(
            dataset_id="rts",
            year=2020,
            input_path="data/rts.parquet",
            effective_geometry=GeometryRef(type="county"),
            method="aggregate",
            transform_id="county-to-coc",
            to_geometry=GeometryRef(type="coc"),
            measures=["pit_total"],
        ),
        ctx,
    )
    assert result.success, result.error
    prepared = ctx.intermediates[("rts", 2020)]
    contract = frame_annotation_contract(prepared)
    assert contract is not None
    assert contract["bindings"]["aggregate-note#r1"]["columns"] == {
        "pit_total": [["COC-1", 2020]],
    }
    assert contract["bindings"]["zero-weight-note#r1"]["columns"] == {}
    assert contract["bindings"]["null-measure-note#r1"]["columns"] == {}
    assert hashlib.sha256(input_path.read_bytes()).hexdigest() in {
        source["sha256"] for source in contract["sources"]["aggregate-note#r1"]
    }
    prepared_path = tmp_path / "data" / "prepared.parquet"
    write_parquet_with_provenance(prepared, prepared_path, ProvenanceBlock(geo_type="coc"))
    loaded = RecipeCache().read_parquet(prepared_path)
    assert frame_annotation_contract(loaded) == contract
    report_recipe = load_recipe({
        "version": 2, "id": "v1-aggregate-report", "title": "v1 aggregate report",
        "authors": ["test"], "universe": {"years": [2020]},
        "targets": [{"id": "target", "geometry": {"type": "coc"}}],
        "datasets": {}, "pipelines": [{"id": "build", "target": "target", "steps": []}],
        "panels": {"panel": {"ref": {"pipeline": "build", "output": "panel"},
                              "geo_key": "geo_id", "time_key": "year"}},
        "analyses": [{"id": "describe", "method": "descriptive", "input": "panels.panel",
                       "sample": {}, "columns": ["pit_total"], "stats": ["n", "mean"]}],
        "results": {"bundle_dir": "results/{id}/{run_id}", "include_panels": ["panel"],
                    "report": {"title": "v1 aggregate report"}},
    })
    bundle = run_results(
        report_recipe,
        original_recipe_bytes=report_recipe.model_dump_json(
            by_alias=True, exclude_none=True,
        ).encode(),
        outputs_root=tmp_path / "results",
        panel_frames={"panel": loaded},
        private=True,
    )
    assert "Caveat for pit_total" in (
        Path(bundle["bundle_path"]) / "report" / "index.html"
    ).read_text()


def test_annotated_allocate_binds_only_observed_positive_weight_cells() -> None:
    source = _annotated_frame(
        geography="14460", measure="population", value=25,
        annotation_id="allocation-note", annotation_column="population",
        annotation_source="allocation-source",
    ).rename(columns={"coc_id": "geo_id"})
    source["year"] = 2020
    crosswalk = pd.DataFrame({
        "msa_id": ["14460", "14460", "14460", "14500"],
        "coc_id": ["COC-1", "COC-2", "COC-4", "COC-3"],
        "area_share": [0.4, 0.6, 0.0, 1.0],
    })
    result = _resample_allocate(source, crosswalk, ResampleTask(
        dataset_id="population", year=2020, input_path="population.parquet",
        effective_geometry=GeometryRef(type="msa"), method="allocate",
        transform_id="msa-to-coc", to_geometry=GeometryRef(type="coc"),
        measures=["population"], geo_column="geo_id",
    ))
    contract = frame_annotation_contract(result)
    assert contract is not None
    assert contract["bindings"]["allocation-note#r1"]["columns"] == {
        "population": [["COC-1", 2020], ["COC-2", 2020]],
    }


def test_annotated_allocate_rejects_duplicate_output_cells() -> None:
    source = _annotated_frame(
        geography="14460", measure="population", value=25,
        annotation_id="allocation-note", annotation_column="population",
        annotation_source="allocation-source",
    ).rename(columns={"coc_id": "geo_id"})
    source["year"] = 2020
    crosswalk = pd.DataFrame({
        "msa_id": ["14460", "14460"],
        "coc_id": ["COC-1", "COC-1"],
        "area_share": [0.4, 0.6],
    })
    with pytest.raises(ExecutorError, match="duplicate output geography/year"):
        _resample_allocate(source, crosswalk, ResampleTask(
            dataset_id="population", year=2020, input_path="population.parquet",
            effective_geometry=GeometryRef(type="msa"), method="allocate",
            transform_id="msa-to-coc", to_geometry=GeometryRef(type="coc"),
            measures=["population"], geo_column="geo_id",
        ))


def test_annotated_identity_derived_measure_binds_actual_source_dependencies(
    tmp_path: Path,
) -> None:
    config = {
        "type": "quantile_from_distribution",
        "distribution_family": "household_income",
        "quantile": 0.25,
        "diagnostic_output_column": "income_diagnostic",
    }
    source_columns = derived_measure_required_columns(
        config, available_columns={"household_income_total"},
    )
    source = pd.DataFrame({
        "coc_id": ["COC-1"], "year": [2020], "household_income_total": [100.0],
        **{column: [0.0] for column in source_columns if column != "household_income_total"},
    })
    annotated_column = source_columns[1]
    source[annotated_column] = 100.0
    source.attrs["annotations"] = serialize_annotation_contract(
        [_record("income-bin-note", annotated_column)],
        bindings={"income-bin-note#r1": {"columns": {
            annotated_column: [["COC-1", 2020]],
        }}},
    )
    input_path = tmp_path / "data" / "income.parquet"
    write_parquet_with_provenance(source, input_path, ProvenanceBlock(geo_type="coc"))
    recipe = load_recipe({
        "version": 1, "name": "identity-derived-note",
        "universe": {"range": "2020-2020"},
        "targets": [{"id": "target", "geometry": {"type": "coc"}}],
        "datasets": {"income": {
            "provider": "fixture", "product": "income", "version": 1,
            "native_geometry": {"type": "coc"}, "years": "2020-2020",
            "path": "data/income.parquet",
        }},
        "pipelines": [{"id": "main", "target": "target", "steps": []}],
    })
    ctx = ExecutionContext(project_root=tmp_path, recipe=recipe, quiet=True)
    result = _execute_resample(ResampleTask(
        dataset_id="income", year=2020, input_path="data/income.parquet",
        effective_geometry=GeometryRef(type="coc"), method="identity",
        transform_id=None, to_geometry=GeometryRef(type="coc"),
        measures=["household_income_total"],
        derived_measures={"bottom_quartile_income": config},
    ), ctx)
    assert result.success, result.error
    contract = frame_annotation_contract(ctx.intermediates[("income", 2020)])
    assert contract["bindings"]["income-bin-note#r1"]["columns"] == {
        "bottom_quartile_income": [["COC-1", 2020]],
        "income_diagnostic": [["COC-1", 2020]],
    }


def test_aggregate_contributors_resolve_legacy_and_bounded_uncertainty() -> None:
    source = pd.DataFrame({
        "county_fips": ["01001", "01003", "01005"],
        "year": [2020, 2020, 2020],
        "estimate": [10.0, 0.0, None],
    })
    global_note = _record("global-uncertainty", "estimate")
    out_of_scope_note = _record("out-of-scope", "estimate")
    source.attrs["annotations"] = serialize_annotation_contract(
        [global_note, out_of_scope_note],
        bindings={
            "global-uncertainty#r1": {"columns": {}},
            "out-of-scope#r1": {"columns": {
                "estimate": [["01003", 2020], ["01005", 2020]],
            }},
        },
        uncertain_ids=["global-uncertainty#r1", "out-of-scope#r1"],
    )
    output = pd.DataFrame({"geo_id": ["COC-1"], "year": [2020], "estimate": [10.0]})
    contributors = pd.DataFrame({
        "county_fips": ["01001"], "year": [2020],
        "output_geo": ["COC-1"], "output_year": [2020],
    })
    rebound = bind_source_aggregate_contributors(
        output, source, {"estimate": ("estimate", contributors)},
        source_geo_column="county_fips", source_year_column="year",
        output_geo_column="output_geo", output_year_column="output_year",
    )
    contract = frame_annotation_contract(rebound)
    assert contract is not None
    assert contract["bindings"]["global-uncertainty#r1"]["columns"] == {
        "estimate": [["COC-1", 2020]],
    }
    assert contract["uncertain_ids"] == ["global-uncertainty#r1"]
    assert contract["bindings"]["out-of-scope#r1"]["columns"] == {}


def test_aggregate_contributors_map_legacy_scope_to_exact_cells() -> None:
    source = pd.DataFrame({
        "county_fips": ["01001"], "year": [2020], "estimate": [10.0],
    })
    source.attrs["annotations"] = [_record("legacy-note", "estimate")]
    output = pd.DataFrame({"geo_id": ["COC-1"], "year": [2020], "estimate": [10.0]})
    contributors = pd.DataFrame({
        "county_fips": ["01001"], "year": [2020],
        "output_geo": ["COC-1"], "output_year": [2020],
    })
    rebound = bind_source_aggregate_contributors(
        output, source, {"estimate": ("estimate", contributors)},
        source_geo_column="county_fips", source_year_column="year",
        output_geo_column="output_geo", output_year_column="output_year",
    )
    contract = frame_annotation_contract(rebound)
    assert contract["bindings"]["legacy-note#r1"]["columns"] == {
        "estimate": [["COC-1", 2020]],
    }
    bad_contributors = contributors.assign(output_geo="COC-OTHER")
    with pytest.raises(ValueError, match="outside the exact output cells"):
        bind_source_aggregate_contributors(
            pd.DataFrame({"geo_id": ["COC-1"], "year": [2020], "estimate": [10.0]}),
            source,
            {"estimate": ("estimate", bad_contributors)},
            source_geo_column="county_fips", source_year_column="year",
            output_geo_column="output_geo", output_year_column="output_year",
        )


@pytest.mark.parametrize(
    ("method", "expected"),
    [("calendar_mean", 15.0), ("calendar_median", 15.0)],
    ids=["calendar-mean", "calendar-median"],
)
def test_annotated_calendar_aggregation_rebinds_exact_output_cells(method, expected) -> None:
    source = pd.DataFrame({
        "coc_id": ["COC-1", "COC-1"], "year": [2020, 2020],
        "month": [1, 7], "estimate": [10.0, 20.0],
    })
    source.attrs["annotations"] = serialize_annotation_contract(
        [_record("calendar-note", "estimate")],
        bindings={"calendar-note#r1": {"columns": {
            "estimate": [["COC-1", 2020]],
        }}},
    )
    filtered = _apply_temporal_filter(
        source, TemporalFilter(type="temporal", column="month", method=method),
        2020, "calendar-source", geo_column="coc_id",
    )
    assert filtered["estimate"].item() == expected
    contract = frame_annotation_contract(filtered)
    assert contract["bindings"]["calendar-note#r1"]["columns"] == {
        "estimate": [["COC-1", 2020]],
    }


@pytest.mark.parametrize(
    ("month", "output_year", "expected"),
    [(1, 2020, 15.0), (10, 2019, 12.5)],
    ids=["previous-calendar-year", "following-calendar-year"],
)
def test_interpolation_binds_both_source_endpoints(month, output_year, expected) -> None:
    source = pd.DataFrame({
        "coc_id": ["COC-1", "COC-1", "COC-1"], "year": [2019, 2020, 2021],
        "date": pd.to_datetime(["2019-07-01", "2020-07-01", "2021-07-01"]),
        "estimate": [10.0, 20.0, None],
    })
    source.attrs["annotations"] = serialize_annotation_contract(
        [_record("first-endpoint", "estimate"), _record("second-endpoint", "estimate")],
        bindings={
            "first-endpoint#r1": {"columns": {"estimate": [["COC-1", 2019]]}},
            "second-endpoint#r1": {"columns": {"estimate": [["COC-1", 2020]]}},
        },
    )
    filtered = _apply_temporal_filter(
        source,
        TemporalFilter(type="temporal", column="date", method="interpolate_to_month", month=month),
        output_year, "interpolated-source", geo_column="coc_id",
    )
    estimates = filtered.set_index("year")["estimate"]
    assert estimates[output_year] == expected
    assert estimates[output_year - 1] == 10.0
    assert pd.isna(estimates[output_year + 1])
    contract = frame_annotation_contract(filtered)
    first_bindings = contract["bindings"]["first-endpoint#r1"]["columns"]["estimate"]
    assert {tuple(cell) for cell in first_bindings} == {
        ("COC-1", output_year - 1), ("COC-1", output_year),
    }
    assert contract["bindings"]["second-endpoint#r1"]["columns"]["estimate"] == [
        ["COC-1", output_year],
    ]


@pytest.mark.parametrize("annotated_crosswalk", [False, True], ids=["legacy", "generated"])
def test_b2010_coverage_flows_from_weights_to_joined_panel(
    tmp_path: Path, annotated_crosswalk: bool,
) -> None:
    asset_root = tmp_path / "data"
    boundary_path = coc_base_path("2010", asset_root)
    write_parquet_with_provenance(
        pd.DataFrame({"coc_id": ["CO-1"]}), boundary_path,
        ProvenanceBlock(boundary_vintage="2010", extra={
            "coverage_status": "partial_unresolved", "coverage_reference": "native_2010_listing",
        }),
    )
    xwalk_path = asset_root / "weights.parquet"
    weights = pd.DataFrame({"county_fips": ["01001"], "coc_id": ["CO-1"], "area_share": [0.5]})
    if annotated_crosswalk:
        bind_boundary_annotations(weights, boundary_path, "2010", ("area_share",))
    write_parquet_with_provenance(weights, xwalk_path, ProvenanceBlock(boundary_vintage="2010"))
    pd.DataFrame({"county_fips": ["01001"], "year": [2012], "estimate": [12]}).to_parquet(
        asset_root / "input.parquet"
    )
    recipe = load_recipe({
        "version": 1,
        "name": "b2010-coverage",
        "universe": {"years": [2012]},
        "targets": [{"id": "target", "geometry": {"type": "coc", "vintage": 2010}}],
        "datasets": {"source": {
            "provider": "fixture", "product": "estimate", "version": 1,
            "native_geometry": {"type": "county"}, "years": "2012-2012",
            "path": "data/input.parquet",
        }},
        "transforms": [{
            "id": "weights", "type": "crosswalk", "from": {"type": "county"},
            "to": {"type": "coc", "vintage": 2010}, "spec": {"weighting": {"scheme": "area"}},
        }],
        "pipelines": [{"id": "main", "target": "target", "steps": []}],
    })
    ctx = ExecutionContext(
        project_root=tmp_path, recipe=recipe, quiet=True,
        storage_config=StorageConfig(asset_store_root=asset_root, output_root=tmp_path / "output"),
        transform_paths={"weights": xwalk_path},
    )
    result = _execute_resample(ResampleTask(
        dataset_id="source", year=2012, input_path="data/input.parquet",
        effective_geometry=GeometryRef(type="county"), method="aggregate", transform_id="weights",
        to_geometry=GeometryRef(type="coc", vintage=2010), measures=["estimate"],
    ), ctx)
    assert result.success, result.error
    ctx.intermediates[("control", 2012)] = pd.DataFrame({
        "geo_id": ["CO-1", "CO-2"], "year": [2012, 2012], "control": [1, 2],
    })
    joined = _execute_join(JoinTask(datasets=["source", "control"], join_on=["geo_id", "year"],
                                    year=2012), ctx)
    assert joined.success, joined.error
    panel = ctx.intermediates[("__joined__", 2012)]
    panel_path = tmp_path / "panel.parquet"
    write_parquet_with_provenance(panel, panel_path, ProvenanceBlock(geo_type="coc"))
    loaded = RecipeCache().read_parquet(panel_path)
    assert loaded.loc[loaded["geo_id"] == "CO-1", "estimate"].item() == 6
    contract = frame_annotation_contract(loaded)
    key = f"{B2010_COVERAGE_ANNOTATION}#r1"
    assert contract["records"][0]["scope"]["years"] == [2010]
    assert contract["uncertain_ids"] == [key]
    assert contract["bindings"][key]["columns"] == {"estimate": [["CO-1", 2012]]}
    assert {source["sha256"] for source in contract["sources"][key]} == {
        hashlib.sha256(path.read_bytes()).hexdigest() for path in (xwalk_path, boundary_path)
    }


def test_annual_hud_crosswalk_aggregates_and_persists_exact_contributors(
    tmp_path: Path,
) -> None:
    boundary_vintage = "HUDAnnual_2023"
    boundary_path = coc_base_path(boundary_vintage, tmp_path / "data")
    key = "hud-annual-fixture#r1"
    boundary = pd.DataFrame({"coc_id": ["NC-506"], "geometry": ["polygon"]})
    write_parquet_with_provenance(
        boundary,
        boundary_path,
        ProvenanceBlock(
            boundary_vintage=boundary_vintage,
            geo_type="coc",
            extra={"annotations": serialize_annotation_contract(
                [_record("hud-annual-fixture", "geometry")],
                bindings={key: {"columns": {"geometry": [["NC-506", 2023]]}}},
            )},
        ),
    )
    xwalk_path = tmp_path / "data" / "weights.parquet"
    weights = pd.DataFrame({
        "county_fips": ["37001", "37003", "37005", "37007"],
        "coc_id": ["NC-506", "NC-506", "NC-506", "NC-506"],
        "area_share": [0.25, 0.75, 0.0, 0.5],
        "pop_share": [0.9, 0.1, 0.0, 0.3],
        "county_population": [100, 200, 300, 400],
    })
    bind_boundary_annotations(
        weights, boundary_path, boundary_vintage,
        ("area_share", "pop_share"),
    )
    write_parquet_with_provenance(
        weights, xwalk_path, ProvenanceBlock(boundary_vintage=boundary_vintage)
    )
    input_path = tmp_path / "data" / "input.parquet"
    pd.DataFrame({
        "county_fips": ["37001", "37003", "37005", "37007"],
        "year": [2019, 2019, 2019, 2019],
        "estimate": [10.0, 30.0, 100.0, None],
        "independent_denominator": [1.0, 2.0, 3.0, 4.0],
    }).to_parquet(input_path)
    recipe = load_recipe({
        "version": 1,
        "name": "annual-hud-crosswalk",
        "universe": {"years": [2019]},
        "targets": [{"id": "target", "geometry": {
            "type": "coc", "vintage": boundary_vintage,
        }}],
        "datasets": {"source": {
            "provider": "fixture", "product": "estimate", "version": 1,
            "native_geometry": {"type": "county"}, "years": "2019-2019",
            "path": "data/input.parquet",
        }},
        "transforms": [{
            "id": "weights", "type": "crosswalk",
            "from": {"type": "county"},
            "to": {"type": "coc", "vintage": boundary_vintage},
            "spec": {"weighting": {"scheme": "area"}},
        }],
        "pipelines": [{"id": "main", "target": "target", "steps": []}],
    })
    ctx = ExecutionContext(
        project_root=tmp_path, recipe=recipe, quiet=True,
        storage_config=StorageConfig(
            asset_store_root=tmp_path / "data", output_root=tmp_path / "output",
        ),
        transform_paths={"weights": xwalk_path},
    )
    result = _execute_resample(ResampleTask(
        dataset_id="source", year=2019, input_path="data/input.parquet",
        effective_geometry=GeometryRef(type="county"), method="aggregate",
        transform_id="weights", to_geometry=GeometryRef(
            type="coc", vintage=boundary_vintage,
        ), measures=["estimate"],
    ), ctx)

    assert result.success, result.error
    panel = ctx.intermediates[("source", 2019)]
    panel_path = tmp_path / "data" / "annual-panel.parquet"
    write_parquet_with_provenance(panel, panel_path, ProvenanceBlock(geo_type="coc"))
    loaded = RecipeCache().read_parquet(panel_path)
    assert loaded["estimate"].item() == pytest.approx(25.0)
    contract = frame_annotation_contract(loaded)
    assert contract is not None
    assert contract["bindings"][key]["columns"] == {"estimate": [["NC-506", 2019]]}
    assert "independent_denominator" not in contract["bindings"][key]["columns"]
    assert {source["sha256"] for source in contract["sources"][key]} == {
        hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (boundary_path, xwalk_path)
    }

    report_recipe = load_recipe({
        "version": 2, "id": "annual-geometry-report", "title": "Annual geometry report",
        "authors": ["test"], "universe": {"years": [2019]},
        "targets": [{"id": "target", "geometry": {"type": "coc", "vintage": boundary_vintage}}],
        "datasets": {}, "pipelines": [{"id": "build", "target": "target", "steps": []}],
        "panels": {"panel": {"ref": {"pipeline": "build", "output": "panel"},
                             "geo_key": "geo_id", "time_key": "year"}},
        "analyses": [{"id": "describe", "method": "descriptive", "input": "panels.panel",
                      "sample": {}, "columns": ["estimate"], "stats": ["n", "mean"]}],
        "results": {"bundle_dir": "results/{id}/{run_id}", "include_panels": ["panel"],
                    "report": {"title": "Annual geometry report"}},
    })
    bundle = run_results(
        report_recipe,
        original_recipe_bytes=report_recipe.model_dump_json(by_alias=True, exclude_none=True)
        .encode(),
        outputs_root=tmp_path / "results",
        panel_frames={"panel": loaded},
        private=True,
    )
    report = Path(bundle["bundle_path"]) / "report" / "index.html"
    assert "Caveat for geometry" in report.read_text()

    weighted_ctx = ExecutionContext(
        project_root=tmp_path, recipe=recipe, quiet=True,
        storage_config=StorageConfig(
            asset_store_root=tmp_path / "data", output_root=tmp_path / "output",
        ),
        transform_paths={"weights": xwalk_path},
    )
    weighted_result = _execute_resample(ResampleTask(
        dataset_id="source", year=2019, input_path="data/input.parquet",
        effective_geometry=GeometryRef(type="county"), method="aggregate",
        transform_id="weights", to_geometry=GeometryRef(
            type="coc", vintage=boundary_vintage,
        ), measures=["estimate"], measure_aggregations={"estimate": "weighted_mean"},
    ), weighted_ctx)
    assert weighted_result.success, weighted_result.error
    weighted_panel = weighted_ctx.intermediates[("source", 2019)]
    assert weighted_panel["estimate"].item() == pytest.approx(12.0)
    weighted_contract = frame_annotation_contract(weighted_panel)
    assert weighted_contract is not None
    assert weighted_contract["bindings"][key]["columns"] == {
        "estimate": [["NC-506", 2019]],
    }

    bad_xwalk_path = tmp_path / "data" / "unverified-weights.parquet"
    bad_weights = weights.copy()
    bad_weights.attrs["annotations"] = serialize_annotation_contract(
        [_record("unverified-annual-note", "area_share")],
        bindings={"unverified-annual-note#r1": {"columns": {
            "area_share": [["NC-506", 2023]],
        }}},
        sources={"unverified-annual-note#r1": [{
            "sha256": hashlib.sha256(b"unverified").hexdigest(),
            "vintages": {"boundary_vintage": boundary_vintage},
        }]},
    )
    write_parquet_with_provenance(
        bad_weights, bad_xwalk_path,
        ProvenanceBlock(boundary_vintage=boundary_vintage),
    )
    bad_ctx = ExecutionContext(
        project_root=tmp_path, recipe=recipe, quiet=True,
        storage_config=StorageConfig(
            asset_store_root=tmp_path / "data", output_root=tmp_path / "output",
        ),
        transform_paths={"weights": bad_xwalk_path},
    )
    rejected = _execute_resample(ResampleTask(
        dataset_id="source", year=2019, input_path="data/input.parquet",
        effective_geometry=GeometryRef(type="county"), method="aggregate",
        transform_id="weights", to_geometry=GeometryRef(
            type="coc", vintage=boundary_vintage,
        ), measures=["estimate"],
    ), bad_ctx)
    assert not rejected.success
    assert "no verified source identity" in rejected.error


def test_geometry_ref_accepts_only_qualified_annual_coc_vintages() -> None:
    assert GeometryRef(type="coc", vintage="2023").vintage == 2023
    assert GeometryRef(type="coc", vintage="HUDAnnual_2023").vintage == "HUDAnnual_2023"
    with pytest.raises(ValueError, match="only for CoC"):
        GeometryRef(type="county", vintage="HUDAnnual_2023")


@pytest.mark.parametrize("boundary_vintage", ["HUDAnnual_2023", "2010"])
def test_tract_mediated_county_weights_preserve_boundary_annotations(
    tmp_path: Path, boundary_vintage: str,
) -> None:
    boundary_path = coc_base_path(boundary_vintage, tmp_path / "data")
    annual_key = "annual-tract-note#r1"
    archive_key = f"{B2010_COVERAGE_ANNOTATION}#r1"
    write_parquet_with_provenance(
        pd.DataFrame({"coc_id": ["NC-506"], "geometry": ["polygon"]}),
        boundary_path,
        ProvenanceBlock(
            boundary_vintage=boundary_vintage,
            extra=(
                {"coverage_status": "partial_unresolved",
                 "coverage_reference": "native_2010_listing"}
                if boundary_vintage == "2010" else
                {"annotations": serialize_annotation_contract(
                    [_record("annual-tract-note", "geometry")],
                    bindings={annual_key: {"columns": {"geometry": [["NC-506", 2023]]}}},
                )}
            ),
        ),
    )
    tract_path = tmp_path / "data" / "tract-xwalk.parquet"
    tract_xwalk = pd.DataFrame({
        "coc_id": ["NC-506"],
        "tract_geoid": ["37001000100"],
        "area_share": [0.5],
        "intersection_area": [5.0],
        "tract_area": [10.0],
    })
    bind_boundary_annotations(
        tract_xwalk, boundary_path, boundary_vintage,
        ("area_share", "intersection_area"),
    )
    write_parquet_with_provenance(
        tract_xwalk,
        tract_path,
        ProvenanceBlock(boundary_vintage=boundary_vintage),
    )
    county_weights = build_tract_mediated_county_crosswalk(
        tract_xwalk,
        pd.DataFrame({
            "tract_geoid": ["37001000100"],
            "total_population": [100],
            "total_households": [40],
            "renter_households": [20],
        }),
        boundary_vintage=boundary_vintage,
        county_vintage=2020,
        tract_vintage=2020,
        acs_vintage="2016-2020",
    )
    bind_tract_mediated_annotations(county_weights, tract_path, boundary_vintage)
    output_path = save_tract_mediated_county_crosswalk(
        county_weights,
        boundary_vintage=boundary_vintage,
        county_vintage=2020,
        tract_vintage=2020,
        acs_vintage="2016-2020",
        output_dir=tmp_path / "data" / "xwalks",
    )
    loaded = RecipeCache().read_parquet(output_path)
    contract = frame_annotation_contract(loaded)
    assert contract is not None
    key = archive_key if boundary_vintage == "2010" else annual_key
    assert contract["bindings"][key]["columns"] == ({} if boundary_vintage == "2010" else {
        column: [["NC-506", 2023]]
        for family in ("area", "population", "household", "renter_household")
        for column in (f"{family}_weight", f"{family}_denominator", f"geo_{family}_total")
    })
    assert hashlib.sha256(tract_path.read_bytes()).hexdigest() in {
        source["sha256"] for source in contract["sources"][key]
    }
