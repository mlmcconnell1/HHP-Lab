"""Annotation propagation through recipe preparation primitives."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from hhplab.recipe.cache import RecipeCache
from hhplab.recipe.executor import (
    ExecutionContext,
    _execute_join,
    _execute_resample,
    persistence,
)
from hhplab.recipe.executor import panel as recipe_panel
from hhplab.recipe.executor.annotations import (
    frame_column_map,
    propagate_annotations,
)
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


def test_annotated_lagged_source_cannot_silently_lose_year_bindings(tmp_path: Path) -> None:
    source_year, panel_year = 2020, 2021
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
    result = _execute_resample(
        ResampleTask(
            dataset_id="source", year=panel_year, input_path="data/source.parquet",
            effective_geometry=GeometryRef(type="metro"), method="identity",
            transform_id=None, to_geometry=GeometryRef(type="metro"),
            measures=["total_population"], year_column="acs1_vintage",
        ),
        ExecutionContext(project_root=tmp_path, recipe=recipe, quiet=True),
    )
    assert not result.success
    assert "annotation year mapping" in result.error


def test_annotated_aggregation_is_actionably_rejected(tmp_path: Path) -> None:
    input_path = tmp_path / "data" / "rts.parquet"
    frame = _annotated_frame(
        geography="COC-1",
        measure="pit_total",
        value=12,
        annotation_id="aggregate-note",
        annotation_column="pit_total",
        annotation_source="rts-source",
    )
    write_parquet_with_provenance(frame, input_path, ProvenanceBlock(geo_type="county"))
    recipe = load_recipe(
        {
            "version": 1,
            "name": "annotation-aggregate-rejection",
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
            "pipelines": [{"id": "main", "target": "target", "steps": []}],
        }
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
        ExecutionContext(project_root=tmp_path, recipe=recipe, quiet=True),
    )
    assert not result.success
    assert "exact source contributors are mapped" in result.error
