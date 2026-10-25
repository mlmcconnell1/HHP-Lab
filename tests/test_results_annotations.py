"""Annotation provenance: data-scoped caveats/adjustments flow through runs.

These tests cover the RecipeV2 ``annotations`` lineage: plan identity, replay
aggregation (applied keys, per-analysis keys, unused records, conflicts),
derived-column propagation, and bundle/package embedding (``annotations.json``
plus report and README notes) under both private and public distribution.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from hhplab.recipe.cache import RecipeCache
from hhplab.recipe.derive import apply_masks, extract_bindings_from_sidecar
from hhplab.recipe.loader import load_recipe
from hhplab.recipe.results_plan import plan_results
from hhplab.results.runner import replay_results, run_results
from hhplab.results.verify import verify_bundle
from hhplab.schema.contracts import (
    DataAnnotationSpec,
    frame_annotation_contract,
    parse_annotation_contract,
    serialize_annotation_contract,
)
from hhplab.storage.provenance import (
    ProvenanceBlock,
    read_provenance,
    write_parquet_with_provenance,
)

PANEL = pd.DataFrame(
    {
        "geo_id": ["a", "a", "b", "b"],
        "year": [2020, 2021, 2020, 2021],
        "x": [1.0, 2.0, 3.0, 4.0],
        "y": [5.0, 6.0, 7.0, 8.0],
    }
)
PANEL.attrs["measures"] = {
    column: {"units": "count", "aggregation_semantics": "extensive"}
    for column in ("x", "y", "z")
}

ANNOTATIONS = [
    {
        "id": "a-x-2020",
        "revision": 1,
        "category": "caveat",
        "summary": "X 2020 caveat",
        "text": "X values for 2020 are provisional.",
        "evidence": [{"name": "caveat doc", "ref": "docs/caveat.md"}],
        "scope": {"columns": ["x"], "years": [2020]},
    },
    {
        "id": "b-mask-y",
        "revision": 1,
        "category": "adjustment",
        "summary": "Masked Y adjustment",
        "text": "Y is adjusted where masked.",
        "applies_to_masks": ["m1"],
        "scope": {"columns": ["y"], "years": [2021]},
    },
    {
        "id": "c-unused",
        "revision": 2,
        "category": "other",
        "summary": "Unused annotation",
        "text": "Does not touch any sampled year.",
        "scope": {"columns": ["y"], "years": [2019]},
    },
]

MASK = {"id": "m1", "columns": ["y"], "years": [2021], "reason": "mask y 2021"}
DERIVE = {"id": "z", "op": "sum", "of": ["x", "y"]}


def _recipe(**overrides: object):
    base: dict[str, object] = {
        "version": 2,
        "id": "annotation-run",
        "title": "Annotation run",
        "authors": ["test"],
        "universe": {"range": "2020-2021"},
        "targets": [{"id": "target", "geometry": {"type": "coc", "vintage": 2025}}],
        "datasets": {},
        "pipelines": [{"id": "build", "target": "target", "steps": []}],
        "panels": {
            "panel": {
                "ref": {"pipeline": "build", "output": "panel"},
                "geo_key": "geo_id",
                "time_key": "year",
            }
        },
        "masks": [MASK],
        "annotations": ANNOTATIONS,
        "derive": [DERIVE],
        "analyses": [
            {
                "id": "fit",
                "method": "regression",
                "input": "panels.panel",
                "sample": {},
                "outcome": "y",
                "predictors": ["x"],
                "inference": {"method": "hc1"},
            },
            {
                "id": "fit2020",
                "method": "regression",
                "input": "panels.panel",
                "sample": {"years": {"from": 2020, "to": 2020}},
                "outcome": "y",
                "predictors": ["x"],
                "inference": {"method": "hc1"},
            },
            {
                "id": "fitz",
                "method": "descriptive",
                "input": "panels.panel",
                "sample": {},
                "columns": ["z"],
                "stats": ["n", "mean"],
            },
        ],
        "results": {
            "bundle_dir": "results/{id}/{run_id}",
            "include_panels": ["panel"],
            "report": {"title": "Annotation run"},
        },
    }
    base.update(overrides)
    return load_recipe(base)


def _recipe_dict(recipe) -> dict[str, object]:
    return recipe.model_dump(mode="json", by_alias=True, exclude_none=True)


def _panel_with_origin(records: list[dict]) -> pd.DataFrame:
    """Clone PANEL carrying persisted data-origin annotation records in attrs."""
    frame = PANEL.copy(deep=True)
    frame.attrs["annotations"] = [dict(record) for record in records]
    return frame


def _replay(recipe, *, panels: dict[str, pd.DataFrame] | None = None):
    plan = plan_results(recipe, original_recipe_bytes=b"annotation recipe bytes")
    frames = {"panel": PANEL} if panels is None else panels
    return replay_results(
        {"recipe": _recipe_dict(recipe), "plan": plan.to_dict()},
        {"spec_hash": plan.plan_sha256},
        frames,
    )


def test_plan_identity_includes_annotations_component() -> None:
    plan = plan_results(_recipe(), original_recipe_bytes=b"identity bytes")
    assert "annotations" in plan.component_hashes
    records = plan.to_dict()["annotations"]
    assert {item["id"] for item in records} == {"a-x-2020", "b-mask-y", "c-unused"}
    changed = _recipe(annotations=ANNOTATIONS[:-1])
    other = plan_results(changed, original_recipe_bytes=b"identity bytes")
    assert plan.plan_sha256 != other.plan_sha256
    assert plan.canonical_plan != other.canonical_plan


def test_replay_reports_applied_per_analysis_and_derived_keys() -> None:
    replay = _replay(_recipe())
    annotations = replay["annotations"]
    applied = {(item["id"], item["revision"]) for item in annotations["applied"]}
    assert ("a-x-2020", 1) in applied
    assert ("b-mask-y", 1) in applied
    assert ("c-unused", 2) not in applied
    assert annotations["unused"] == [{"id": "c-unused", "revision": 2}]
    assert annotations["conflicts"] == []
    assert {item["id"] for item in annotations["records"]} == {
        "a-x-2020",
        "b-mask-y",
        "c-unused",
    }

    per = annotations["per_analysis"]
    fit2020_keys = {
        (item["id"], item["revision"]) for item in per["fit2020@base"]["keys"]
    }
    assert ("a-x-2020", 1) in fit2020_keys
    assert ("b-mask-y", 1) not in fit2020_keys

    fitz_keys = {(item["id"], item["revision"]) for item in per["fitz@base"]["keys"]}
    assert ("a-x-2020", 1) in fitz_keys
    assert ("b-mask-y", 1) in fitz_keys

    assert per["fit2020@base"]["counts"]["a-x-2020#r1"] == 2
    assert per["fitz@base"]["counts"]["b-mask-y#r1"] == 2


def test_conflicting_annotations_are_reported_not_silently_resolved(tmp_path) -> None:
    # The panel artifact carries revision 1; the recipe declares revision 2 of
    # the same annotation id without a supersedes chain. Both active revisions
    # must be surfaced as a conflict -- never silently resolved.
    inherited = ANNOTATIONS[0]
    revised = {
        **ANNOTATIONS[0],
        "revision": 2,
        "summary": "X 2020 caveat (revised)",
    }
    recipe = _recipe(annotations=[revised])
    panels = {"panel": _panel_with_origin([inherited])}
    replay = _replay(recipe, panels=panels)
    annotations = replay["annotations"]
    assert len(annotations["conflicts"]) == 1
    conflict = annotations["conflicts"][0]
    assert conflict["id"] == "a-x-2020"
    assert conflict["revisions"] == [1, 2]
    applied = {
        (item["id"], item["revision"]) for item in annotations["applied"]
    }
    assert ("a-x-2020", 1) in applied
    assert ("a-x-2020", 2) in applied
    assert annotations["origins"]["a-x-2020#r1"] == "artifact"
    assert annotations["origins"]["a-x-2020#r2"] == "declared"
    assert annotations["inherited"] == [
        {"panel": "panel", "id": "a-x-2020", "revision": 1}
    ]

    result = run_results(
        recipe,
        original_recipe_bytes=json.dumps(_recipe_dict(recipe), sort_keys=True).encode(),
        outputs_root=tmp_path,
        panel_frames=panels,
        private=True,
    )
    bundle = Path(result["bundle_path"])
    packaged = json.loads((bundle / "annotations.json").read_bytes())
    assert packaged["conflicts"] == annotations["conflicts"]
    report = (bundle / "report" / "index.html").read_text()
    assert "X 2020 caveat" in report
    assert "X 2020 caveat (revised)" in report


def test_superseded_revision_reports_active_only_in_report_notes(tmp_path) -> None:
    # Artifact carries revision 1; recipe declares revision 2 that supersedes
    # it. No conflict, but only the active revision 2 is reported as a note.
    inherited = ANNOTATIONS[0]
    revised = {
        **ANNOTATIONS[0],
        "revision": 2,
        "supersedes": 1,
        "summary": "X 2020 caveat (revised)",
        "text": "Updated after audit.",
    }
    recipe = _recipe(annotations=[revised])
    panels = {"panel": _panel_with_origin([inherited])}
    replay = _replay(recipe, panels=panels)
    annotations = replay["annotations"]
    assert annotations["conflicts"] == []
    assert annotations["origins"]["a-x-2020#r1"] == "artifact"
    assert annotations["origins"]["a-x-2020#r2"] == "declared"
    applied = {item["revision"] for item in annotations["applied"] if item["id"] == "a-x-2020"}
    assert applied == {1, 2}

    result = run_results(
        recipe,
        original_recipe_bytes=json.dumps(_recipe_dict(recipe), sort_keys=True).encode(),
        outputs_root=tmp_path,
        panel_frames=panels,
        private=True,
    )
    bundle = Path(result["bundle_path"])
    report = (bundle / "report" / "index.html").read_text()
    assert "X 2020 caveat (revised)" in report
    assert "Updated after audit." in report
    assert "provisional" not in report
    readme = (bundle / "README.md").read_text()
    assert "X 2020 caveat (revised)" in readme
    assert "provisional" not in readme


def test_replay_is_deterministic_across_identical_inputs() -> None:
    first = _replay(_recipe())
    second = _replay(_recipe())
    assert first["annotations"] == second["annotations"]
    assert first["effective_annotations"] == second["effective_annotations"]
    assert first["inherited_annotations"] == second["inherited_annotations"]


def test_replay_rejects_spec_hash_mismatch_after_annotation_change() -> None:
    recipe = _recipe()
    plan = plan_results(recipe, original_recipe_bytes=b"annotation recipe bytes")
    resolved = {"recipe": _recipe_dict(recipe), "plan": plan.to_dict()}
    with pytest.raises(ValueError):
        replay_results(resolved, {"spec_hash": "deadbeef"}, {"panel": PANEL})


@pytest.mark.parametrize("private", [True, False])
def test_run_bundle_embeds_annotations_package_and_notes(tmp_path, private: bool) -> None:
    recipe = _recipe()
    source = json.dumps(_recipe_dict(recipe), sort_keys=True).encode()
    result = run_results(
        recipe,
        original_recipe_bytes=source,
        outputs_root=tmp_path,
        panel_frames={"panel": PANEL},
        private=private,
    )
    bundle = Path(result["bundle_path"])
    annotations_path = bundle / "annotations.json"
    assert annotations_path.is_file()
    annotations = json.loads(annotations_path.read_bytes())
    if not private:
        assert annotations["distribution_status"] == "withheld"
        assert "per_analysis" not in annotations
        assert "provisional" not in json.dumps(annotations)
        for relative in ("report/index.html", "README.md"):
            assert "Data caveat details withheld" in (bundle / relative).read_text()
        for sample in (bundle / "samples").glob("*.parquet"):
            assert b"provisional" not in sample.read_bytes()
            assert b"a-x-2020" not in sample.read_bytes()
            with pytest.raises(ValueError, match="Public sample artifact"):
                RecipeCache().read_parquet(sample)
        return
    applied_ids = {item["id"] for item in annotations["applied"]}
    assert {"a-x-2020", "b-mask-y"} <= applied_ids
    assert "c-unused" not in applied_ids
    assert annotations["conflicts"] == []
    records = {item["id"]: item for item in annotations["records"]}
    assert records["a-x-2020"]["evidence"][0]["ref"] == "docs/caveat.md"

    manifest = json.loads((bundle / "manifest.json").read_bytes())
    assert "annotations.json" in {item["path"] for item in manifest["files"]}

    report = (bundle / "report" / "index.html").read_text()
    assert "Annotation notes" in report
    assert "X 2020 caveat" in report
    assert "provisional" in report
    readme = (bundle / "README.md").read_text()
    assert "Notes" in readme
    assert "X 2020 caveat" in readme
    assert "docs/caveat.md" in readme


BOSTON_SOURCE_COUNTS = {"MA-510": 10, "MA-513": 20, "MA-516": 30, "MA-518": 40}
BOSTON_2010_NOTE = {
    "id": "boston-2010-shared-jurisdiction", "revision": 1,
    "category": "geographic_approximation", "summary": "Boston 2010 boundary treatment",
    "text": "The four CoCs and their shared polygon are assigned to Boston MSA 14460.",
    "treatment": "Combine four source PIT/HIC totals without allocation within the shared polygon.",
    "uncertainty": (
        "Collective count coverage and absence of duplication have not been "
        "independently verified; "
        "this does not certify national B2010 completeness."
    ),
    "evidence": [{"name": "Historical audit", "ref": "docs/historical-coc-2010-acquisition.md"}],
    "scope": {"panels": ["original"], "columns": ["pit_total"], "years": [2010],
              "geographies": {"include": ["14460"]}},
}


def _boston_recipe(*, panel="panel", year=2011, derive=True, operation="lag"):
    return _recipe(
        id=f"boston-{panel}-{year}", universe={"range": "2010-2012"},
        annotations=[], masks=[],
        panels={panel: {"ref": {"pipeline": "build", "output": "panel"},
                        "geo_key": "geo_id", "time_key": "year"}},
        derive=[{"id": "lag_pit", "op": operation, "of": "pit_total", "years": 1}]
        if derive else [],
        analyses=[{"id": "summary", "method": "descriptive", "input": f"panels.{panel}",
                   "sample": {"years": {"from": year, "to": year}},
                   "columns": ["lag_pit"], "stats": ["n", "mean"]}],
        results={"bundle_dir": "results/{id}/{run_id}", "include_panels": [panel],
                 "report": {"title": "Boston caveat fixture"}},
    )


@pytest.mark.parametrize("operation", ["lag", "diff"], ids=["lagged", "change-from-baseline"])
def test_boston_origin_survives_two_recipes_renaming_and_relocated_replay(tmp_path, operation):
    total = sum(BOSTON_SOURCE_COUNTS.values())
    source = pd.DataFrame({"geo_id": ["14460"] * 3, "year": [2010, 2011, 2012],
                           "pit_total": [total, total + 1, total + 2]})
    source_path = tmp_path / "authoritative.parquet"
    write_parquet_with_provenance(source, source_path, ProvenanceBlock(
        boundary_vintage="2010", extra={"annotations": [BOSTON_2010_NOTE]},
    ))
    cached = RecipeCache().read_parquet(source_path)
    cached.attrs["measures"] = {
        "pit_total": {"units": "count", "aggregation_semantics": "extensive"}
    }
    recipe = _boston_recipe(operation=operation)
    result = run_results(recipe, original_recipe_bytes=json.dumps(_recipe_dict(recipe)).encode(),
                         outputs_root=tmp_path, panel_frames={"panel": cached}, private=True)
    bundle = Path(result["bundle_path"])
    saved_path = bundle / "derived/panel.parquet"
    saved = RecipeCache().read_parquet(saved_path)
    assert "pit_total" not in saved.columns
    contract = frame_annotation_contract(saved)
    key = f"{BOSTON_2010_NOTE['id']}#r1"
    cells = contract["bindings"][key]["columns"]["lag_pit"]
    assert ["14460", 2011] in cells
    assert ["14460", 2012] not in cells
    assert any(entry["vintages"].get("boundary_vintage") == "2010"
               for entry in contract["sources"][key])
    assert saved.loc[saved.year == 2011, "lag_pit"].item() == (total if operation == "lag" else 1)
    second = _boston_recipe(panel="renamed", derive=False)
    reused = _replay(second, panels={"renamed": saved})
    assert reused["panel_errors"] == {}
    assert reused["annotations"]["applied"] == [{"id": BOSTON_2010_NOTE["id"], "revision": 1}]
    reused_sources = reused["effective_annotations"]["renamed"]["sources"][key]
    assert all(source in reused_sources for source in contract["sources"][key])
    unaffected = _replay(_boston_recipe(panel="renamed", year=2012, derive=False),
                         panels={"renamed": saved})
    assert unaffected["annotations"]["applied"] == []
    report = (bundle / "report/index.html").read_text()
    assert "national B2010 completeness" in report
    relocated = tmp_path / "relocated"
    shutil.copytree(bundle, relocated)
    verification = verify_bundle(relocated)
    assert verification.integrity == "pass"
    assert verification.differences == ()
    assert verification.reproducibility in {"pass", "partial"}
    assert read_provenance(source_path).boundary_vintage == "2010"


def test_exact_bindings_do_not_form_a_cartesian_product():
    note = {**ANNOTATIONS[0], "scope": {}}
    state = apply_masks(PANEL, [], panel_name="panel", geo_key="geo_id", time_key="year")
    sidecar = state["annotation_provenance"]
    key = (note["id"], 1)
    sidecar.at[0, "x"] = (key,)
    sidecar.at[3, "y"] = (key,)
    bindings = extract_bindings_from_sidecar(sidecar, PANEL, geo_key="geo_id", time_key="year")
    source = PANEL.copy()
    source.attrs["annotations"] = serialize_annotation_contract([note], bindings=bindings)
    recipe = _recipe(annotations=[], masks=[], derive=[], analyses=[{
        "id": "x2021", "method": "descriptive", "input": "panels.panel",
        "columns": ["x"], "stats": ["n"], "sample": {"years": [2021]},
    }])
    assert _replay(recipe, panels={"panel": source})["annotations"]["applied"] == []


def test_projecting_away_uncertain_candidates_does_not_make_the_note_global(tmp_path):
    key = "a-x-2020#r1"
    source = PANEL.copy()
    source.attrs["annotations"] = serialize_annotation_contract(
        [ANNOTATIONS[0]], bindings={key: {"columns": {"x": [["a", 2020]]}}},
        uncertain_ids=[key],
    )
    analysis = [{"id": "summary", "method": "descriptive", "input": "panels.panel",
                 "sample": {}, "columns": ["z"], "stats": ["n", "mean"]}]
    recipe = _recipe(annotations=[], masks=[], derive=[{"id": "z", "op": "sum", "of": ["y", "y"]}],
                     analyses=analysis)
    result = run_results(recipe, original_recipe_bytes=json.dumps(_recipe_dict(recipe)).encode(),
                         outputs_root=tmp_path, panel_frames={"panel": source}, private=True)
    saved = RecipeCache().read_parquet(Path(result["bundle_path"]) / "derived/panel.parquet")
    assert frame_annotation_contract(saved)["uncertain_ids"] == []
    second = _recipe(annotations=[], masks=[], derive=[], analyses=analysis)
    assert _replay(second, panels={"panel": saved})["annotations"]["applied"] == []


@pytest.mark.parametrize("value", [
    None, False, "bad", {"unexpected": []},
    {"schema_version": True, "records": []},
    {"schema_version": 1.0, "records": []},
    {"schema_version": 2, "records": []},
    {"schema_version": 1, "records": [], "bindings": False},
    {"schema_version": 1, "records": [], "uncertain_ids": None},
    {"schema_version": 1, "records": [], "uncertain_ids": ["dangling#r1"]},
    [{**ANNOTATIONS[0], "revision": True}],
    [{**ANNOTATIONS[0], "revision": 1.0}],
    [{**ANNOTATIONS[0], "supersedes": 1}],
], ids=["null", "bool", "string", "mapping", "bool-version", "float-version", "future-version",
        "false-bindings", "null-uncertain", "dangling", "bool-revision", "float-revision", "self"])
def test_malformed_present_metadata_fails(value):
    frame = PANEL.copy()
    frame.attrs["annotations"] = value
    with pytest.raises(ValueError):
        frame_annotation_contract(frame)


@pytest.mark.parametrize("evidence", [
    {"ref": "/private/file"}, {"ref": "../private"}, {"ref": "C:\\private"},
    {"ref": "file:///private", "kind": "url"},
    {"ref": "https://user:secret@example.org/x", "kind": "url"},
    {"ref": "docs/a.md", "sha256": "a" * 64 + "\n"},
], ids=["absolute", "traversal", "windows", "scheme", "credentials", "hash-newline"])
def test_evidence_requires_portable_refs_and_exact_digests(evidence):
    with pytest.raises(ValueError):
        DataAnnotationSpec.model_validate({
            **ANNOTATIONS[0], "evidence": [{"name": "test", **evidence}],
        })


def test_conflicting_identity_and_alternate_containers_fail():
    from hhplab.results.runner import _merge_annotation_records

    with pytest.raises(ValueError, match="conflict"):
        _merge_annotation_records(
            [ANNOTATIONS[0]], [("artifact", {**ANNOTATIONS[0], "text": "changed"})],
        )
    frame = _panel_with_origin([ANNOTATIONS[0]])
    frame.attrs["results_panel_provenance"] = {"extra": {"annotations": []}}
    with pytest.raises(ValueError, match="inconsistent"):
        frame_annotation_contract(frame)


def test_unused_superseder_does_not_hide_contributing_revision():
    from hhplab.results.runner import _report_notes

    old = ANNOTATIONS[0]
    newer = {**old, "revision": 2, "supersedes": 1, "scope": {"years": [2021]}}
    notes = _report_notes([old, newer], {"a": {"keys": [(old["id"], 1)]}})
    assert [note["revision"] for note in notes] == [1]


def test_narrower_superseder_keeps_both_contributing_revisions_visible():
    from hhplab.results.runner import _report_notes

    old = ANNOTATIONS[0]
    newer = {**old, "revision": 2, "supersedes": 1, "scope": {"years": [2021]}}
    notes = _report_notes([old, newer], {"pooled": {"keys": [(old["id"], 1), (old["id"], 2)]}})
    assert [note["revision"] for note in notes] == [1, 2]


def test_frozen_base_membership_retains_its_own_annotation_dependencies():
    from hhplab.analysis.sample import resolve_sample
    from tests.test_results_samples import SAMPLE_FRAME, plan

    frame = SAMPLE_FRAME.copy()
    note = {**ANNOTATIONS[0], "scope": {"columns": ["weight"]}}
    state = apply_masks(frame, [], panel_name="cbsa", geo_key="geo_id", time_key="year",
                        annotations=[note])
    base = resolve_sample(frame, plan(), annotation_provenance=state["annotation_provenance"])
    variant = resolve_sample(
        frame, plan(base_membership_key="synthetic@base"), base_membership=base,
    )
    assert variant["annotations"]["keys"] == ((note["id"], 1),)
    assert variant["annotations"]["counts"] == base["annotations"]["counts"]


def test_cache_metadata_only_change_invalidates_even_with_same_stat(tmp_path):
    path = tmp_path / "cache.parquet"
    cache = RecipeCache()
    first = {**ANNOTATIONS[0], "text": "A"}
    second = {**ANNOTATIONS[0], "text": "B"}
    write_parquet_with_provenance(PANEL, path, ProvenanceBlock(
        created_at="fixed", extra={"annotations": [first]},
    ))
    stat = path.stat()
    before = cache.file_identity(path)
    assert frame_annotation_contract(cache.read_parquet(path))["records"][0]["text"] == "A"
    write_parquet_with_provenance(PANEL, path, ProvenanceBlock(
        created_at="fixed", extra={"annotations": [second]},
    ))
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    assert path.stat().st_size == stat.st_size
    assert frame_annotation_contract(cache.read_parquet(path))["records"][0]["text"] == "B"
    assert cache.file_identity(path).sha256 != before.sha256
    with pytest.raises(ValueError, match="v1 preparation"):
        cache.read_parquet(path, reject_annotations=True)


@pytest.mark.parametrize("edit", ["filter", "value"], ids=["row-filter", "changed-value"])
def test_external_edits_cannot_reuse_original_annotated_source_bytes(tmp_path, edit):
    path = tmp_path / "source.parquet"
    write_parquet_with_provenance(PANEL, path, ProvenanceBlock(
        extra={"annotations": [ANNOTATIONS[0]]},
    ))
    frame = RecipeCache().read_parquet(path)
    frame.attrs["measures"] = PANEL.attrs["measures"]
    if edit == "filter":
        frame = frame.loc[frame.year == 2021].copy()
    else:
        frame.loc[0, "x"] = 999
    replay = _replay(_recipe(annotations=[], masks=[]), panels={"panel": frame})
    assert "source DataFrame changed" in replay["panel_errors"]["panel"]


def test_restricted_public_bundle_never_exposes_note_details(tmp_path):
    from hhplab.results.bundle import BundleWriter
    from hhplab.schema.redistribution import SourceRedistribution
    from tests.test_results_bundle import _payload

    restricted = SourceRedistribution("restricted", "restricted")

    note = {**ANNOTATIONS[0], "id": "PRIVATE_NOTE", "text": "PRIVATE_SOURCE_VALUE",
            "evidence": [{"name": "PRIVATE_EVIDENCE", "ref": "private/evidence.txt"}]}
    annotations = {"records": [note], "applied": [{"id": note["id"], "revision": 1}],
                   "per_analysis": {"secret-analysis": {"counts": {"PRIVATE_NOTE#r1": 99}}}}
    payload = _payload(source_terms={"panel-source": restricted, "aggregate-source": restricted})
    payload = replace(payload, annotations=annotations,
                      report_payload={**payload.report_payload, "notes": [note]})
    BundleWriter(tmp_path / "public").write(payload)
    for relative in ("annotations.json", "report/index.html", "README.md"):
        text = (tmp_path / "public" / relative).read_text()
        assert "PRIVATE_" not in text
        assert "secret-analysis" not in text
        assert "private/evidence.txt" not in text
    private_payload = replace(payload, private=True)
    BundleWriter(tmp_path / "private").write(private_payload)
    assert "PRIVATE_SOURCE_VALUE" in (tmp_path / "private/annotations.json").read_text()


def _refresh_inventory(bundle):
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    for entry in manifest["files"]:
        content = (bundle / entry["path"]).read_bytes()
        entry["bytes"] = len(content)
        entry["sha256"] = hashlib.sha256(content).hexdigest()
    encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    manifest_path.write_bytes(encoded)
    (bundle / "manifest.sha256").write_text(
        hashlib.sha256(encoded).hexdigest() + "  manifest.json\n"
    )


@pytest.mark.parametrize(
    "artifact", ["annotations.json", "derived/panel.parquet", "samples/fitz@base.parquet"],
    ids=["structured-text", "derived-binding", "sample-binding"],
)
def test_replay_rejects_semantic_tampering_with_consistent_inventory(tmp_path, artifact):
    import pyarrow.parquet as pq

    from hhplab.storage.provenance import PROVENANCE_KEY

    recipe = _recipe()
    result = run_results(recipe, original_recipe_bytes=json.dumps(_recipe_dict(recipe)).encode(),
                         outputs_root=tmp_path, panel_frames={"panel": PANEL}, private=True)
    bundle = Path(result["bundle_path"])
    baseline = verify_bundle(bundle)
    assert baseline.differences == ()
    path = bundle / artifact
    if artifact.endswith(".json"):
        payload = json.loads(path.read_bytes())
        payload["records"][0]["text"] = "Changed scientific limitation"
        path.write_text(json.dumps(payload))
    else:
        table = pq.read_table(path)
        metadata = dict(table.schema.metadata)
        provenance = json.loads(metadata[PROVENANCE_KEY])
        contract = provenance["extra"]["annotations"]
        contract["uncertain_ids"] = []
        contract["bindings"] = {
            f"{record['id']}#r{record['revision']}": {"columns": {}}
            for record in contract["records"]
        }
        metadata[PROVENANCE_KEY] = json.dumps(provenance).encode()
        pq.write_table(table.replace_schema_metadata(metadata), path)
    _refresh_inventory(bundle)
    changed = verify_bundle(bundle)
    assert changed.integrity == "pass"
    assert changed.reproducibility == "fail"
    assert any(item["path"].startswith(artifact) for item in changed.differences)


def test_public_sample_notice_is_verified_even_with_withheld_inputs(tmp_path):
    import pyarrow.parquet as pq

    from hhplab.storage.provenance import PROVENANCE_KEY

    recipe = _recipe()
    result = run_results(recipe, original_recipe_bytes=json.dumps(_recipe_dict(recipe)).encode(),
                         outputs_root=tmp_path, panel_frames={"panel": PANEL}, private=False)
    bundle = Path(result["bundle_path"])
    baseline = verify_bundle(bundle)
    assert baseline.integrity == "pass"
    assert baseline.reproducibility == "partial"
    assert baseline.differences == ()
    path = bundle / "samples/fitz@base.parquet"
    table = pq.read_table(path)
    metadata = dict(table.schema.metadata)
    provenance = json.loads(metadata[PROVENANCE_KEY])
    provenance["extra"].pop("annotation_status")
    metadata[PROVENANCE_KEY] = json.dumps(provenance).encode()
    pq.write_table(table.replace_schema_metadata(metadata), path)
    _refresh_inventory(bundle)
    changed = verify_bundle(bundle)
    assert changed.integrity == "pass"
    assert changed.reproducibility == "fail"
    assert any(item["reason"] == "annotation_notice_differs" for item in changed.differences)


def test_exclusion_caveat_is_reported_but_out_of_range_is_not():
    replay = _replay(_recipe())
    notes = replay["annotations"]["per_analysis"]
    assert "b-mask-y" in {item["id"] for item in notes["fit@base"]["keys"]}
    assert "b-mask-y" not in {item["id"] for item in notes["fit2020@base"]["keys"]}


def test_unused_measure_on_an_excluded_row_does_not_add_a_note():
    note = {**ANNOTATIONS[0], "scope": {"columns": ["x"], "years": [2021]}}
    recipe = _recipe(annotations=[note], analyses=[{
        "id": "fit", "method": "regression", "input": "panels.panel", "sample": {},
        "outcome": "y", "predictors": ["x"], "inference": {"method": "hc1"},
    }])
    assert _replay(recipe)["annotations"]["applied"] == []


@pytest.mark.parametrize("binding", [
    {"columns": ["x"], "cells": [["a", 2020]]},
    {"columns": {"x": [["a", True]]}},
    {"columns": None},
], ids=["cartesian-shape", "bool-year", "null-columns"])
def test_malformed_bindings_are_rejected(binding):
    key = "a-x-2020#r1"
    with pytest.raises(ValueError):
        parse_annotation_contract({"schema_version": 1, "records": [ANNOTATIONS[0]],
                                   "bindings": {key: binding}})


def test_explicit_public_explanation_does_not_expose_evidence():
    from hhplab.results.bundle import _distribution_safe_annotations

    note = {**ANNOTATIONS[0], "public_text": "Public-safe approximation explanation."}
    safe = _distribution_safe_annotations(
        {"records": [note], "applied": [{"id": note["id"], "revision": 1}]},
        private=False, withheld=[{"path": "panels/panel.parquet"}],
    )
    assert safe["public_notes"][0]["text"] == note["public_text"]
    assert "docs/caveat.md" not in json.dumps(safe)
    assert note["id"] not in json.dumps(safe)


def test_identical_declared_and_inherited_records_deduplicate_with_both_origins() -> None:
    inherited = ANNOTATIONS[0]
    recipe = _recipe(annotations=[ANNOTATIONS[0]])
    panels = {"panel": _panel_with_origin([inherited])}
    replay = _replay(recipe, panels=panels)
    annotations = replay["annotations"]
    assert annotations["conflicts"] == []
    assert annotations["inherited"] == [{"id": "a-x-2020", "revision": 1, "panel": "panel"}]
    assert annotations["origin_sources"]["a-x-2020#r1"] == ["artifact", "declared"]
    assert annotations["origins"]["a-x-2020#r1"] == "declared"
    assert len(annotations["records"]) == 1
    assert annotations["records"][0]["id"] == "a-x-2020"
    assert annotations["records"][0]["revision"] == 1


def test_inherited_annotations_discovered_from_persisted_artifact_provenance() -> None:
    # Persisted bundle provenance form: results_panel_provenance.extra.annotations.
    # The record is not declared in the recipe, so it must be inherited.
    inherited = {
        "id": "d-unused",
        "revision": 1,
        "category": "other",
        "summary": "Persisted origin annotation",
        "text": "Carried over from an earlier panel artifact.",
        "scope": {"columns": ["y"], "years": [2019]},
    }
    frame = PANEL.copy(deep=True)
    frame.attrs["results_panel_provenance"] = {
        "dataset_type": "results_input_panel",
        "extra": {"annotations": [inherited]},
    }
    replay = _replay(_recipe(), panels={"panel": frame})
    annotations = replay["annotations"]
    records = {(item["id"], item["revision"]) for item in annotations["records"]}
    assert ("d-unused", 1) in records
    assert annotations["origins"]["d-unused#r1"] == "artifact"
    assert any(
        item["id"] == "d-unused" and item["panel"] == "panel"
        for item in annotations["inherited"]
    )
    # The inherited record does not touch any sampled year -> unused.
    assert {"id": "d-unused", "revision": 1} in annotations["unused"]
