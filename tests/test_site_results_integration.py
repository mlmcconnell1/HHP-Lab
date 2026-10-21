"""Synthetic acceptance for site-year bundling and offline replay.

The fixture freezes three synthetic MSA sites at threshold 0.95, with one
excluded candidate and one incomplete 2021 outcome. These values exercise the
software contract only; they are not research parameters or empirical results.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest
import yaml

from hhplab.artifacts.naming.naming import msa_coc_sites_filename
from hhplab.recipe.executor.inputs import resolve_site_universe_files
from hhplab.recipe.loader import load_recipe
from hhplab.recipe.results_plan import plan_results
from hhplab.results.bundle import write_bundle
from hhplab.results.runner import run_results
from hhplab.results.verify import verify_bundle
from hhplab.schema.redistribution import (
    PUBLIC_DATA_TERMS,
    RedistributionStatus,
    SourceRedistribution,
)
from hhplab.storage.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.storage.run_ledger import RunLedger

ARTIFACT_ID = "overlap_samples"
THRESHOLDS = (0.90, 0.95)
YEARS = (2020, 2021)
FROZEN_VINTAGE = "2024"
SELECTED_MSAS = ("MSA-A", "MSA-B", "MSA-C")
EXCLUDED_MSA = "MSA-D"
HIGH_THRESHOLD_MEMBERSHIPS = (
    ("MSA-A", "CO-1"),
    ("MSA-B", "CO-3"),
    ("MSA-C", "CO-4"),
)
LOW_THRESHOLD_MEMBERSHIPS = (
    ("MSA-A", "CO-1"),
    ("MSA-A", "CO-2"),
    ("MSA-B", "CO-3"),
    ("MSA-C", "CO-4"),
)
OUTCOME_ROWS = (
    ("CO-1", 2020, 10, 20),
    ("CO-1", 2021, 11, 22),
    ("CO-2", 2020, 5, 7),
    ("CO-2", 2021, 6, 8),
    ("CO-3", 2020, 30, 40),
    ("CO-3", 2021, 31, 42),
    ("CO-4", 2020, 50, 60),
    ("CO-4", 2021, 51, None),
    ("CO-9", 2020, 1, 1),
    ("CO-9", 2021, 1, 1),
)
PRODUCER_PARAMETERS = {
    "thresholds": list(THRESHOLDS),
    "coc_boundary_vintage": 2024,
    "msa_definition_version": "census_msa_2023",
}


def _write_site_artifact(root: Path) -> tuple[Path, Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    sites_path = root / msa_coc_sites_filename(ARTIFACT_ID, "sites")
    memberships_path = root / msa_coc_sites_filename(ARTIFACT_ID, "memberships")
    summary_path = root / msa_coc_sites_filename(ARTIFACT_ID, "summary")

    site_rows = [
        {
            "threshold": threshold,
            "msa_id": msa_id,
            "eligible": msa_id != EXCLUDED_MSA,
            "reason": "eligible" if msa_id != EXCLUDED_MSA else "below_threshold",
        }
        for threshold in THRESHOLDS
        for msa_id in (*SELECTED_MSAS, EXCLUDED_MSA)
    ]
    selected_by_threshold = {
        0.90: LOW_THRESHOLD_MEMBERSHIPS,
        0.95: HIGH_THRESHOLD_MEMBERSHIPS,
    }
    membership_rows = [
        {
            "threshold": threshold,
            "msa_id": msa_id,
            "coc_id": coc_id,
            "site_eligible": msa_id != EXCLUDED_MSA,
        }
        for threshold, pairs in selected_by_threshold.items()
        for msa_id, coc_id in (*pairs, (EXCLUDED_MSA, "CO-9"))
    ]
    common = {
        "dataset_type": "msa_coc_sites",
        "parameters": PRODUCER_PARAMETERS,
        "inputs": {"crosswalk_path": "synthetic/crosswalk.parquet"},
        "rule": "synthetic fixture uses threshold-selected whole CoCs",
    }
    for path, frame in (
        (sites_path, pd.DataFrame(site_rows)),
        (memberships_path, pd.DataFrame(membership_rows)),
    ):
        write_parquet_with_provenance(
            frame,
            path,
            ProvenanceBlock(
                geo_type="msa",
                boundary_vintage=FROZEN_VINTAGE,
                definition_version="census_msa_2023",
                extra=common,
            ),
        )
    summary_path.write_text(
        json.dumps(
            {
                "schema_version": "msa_coc_sites/1",
                "parameters": PRODUCER_PARAMETERS,
                "samples": [],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return sites_path, memberships_path, summary_path


def _write_evidence(root: Path, *, threshold: float, membership_identity: str) -> Path:
    path = root / "coc-boundary-compatibility.json"
    memberships = HIGH_THRESHOLD_MEMBERSHIPS if threshold == 0.95 else LOW_THRESHOLD_MEMBERSHIPS
    records = [
        {
            "source_panel": "coc_outcomes",
            "frozen_coc_id": coc_id,
            "year": year,
            "frozen_boundary_vintage": FROZEN_VINTAGE,
            "source_boundary_vintage": FROZEN_VINTAGE,
            "status": "compatible",
            "reason": "Synthetic fixture marks this pair as reviewed compatible.",
            "evidence_ref": f"audit://synthetic/{coc_id}/{year}",
        }
        for _msa_id, coc_id in memberships
        for year in YEARS
    ]
    path.write_text(
        json.dumps(
            {
                "schema_version": "coc_boundary_compatibility/1",
                "evidence_id": f"synthetic-boundary-{threshold:.2f}",
                "artifact_id": ARTIFACT_ID,
                "threshold": threshold,
                "membership_identity": membership_identity,
                "frozen_boundary_vintage": FROZEN_VINTAGE,
                "records": records,
            },
            sort_keys=True,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def _recipe_data(*, threshold: float, recipe_id: str) -> dict:
    return {
        "version": 2,
        "id": recipe_id,
        "title": "Synthetic site-year integration acceptance",
        "authors": ["HHP-Lab test fixture"],
        "universe": {"range": "2020-2021"},
        "targets": [{"id": "coc-outcomes", "geometry": {"type": "coc", "vintage": 2024}}],
        "datasets": {},
        "pipelines": [{"id": "build_coc_outcomes", "target": "coc-outcomes", "steps": []}],
        "panels": {
            "coc_outcomes": {
                "ref": {"pipeline": "build_coc_outcomes", "output": "panel"},
                "geo_key": "coc_id",
                "time_key": "year",
            },
            "site_year": {
                "geo_key": "msa_id",
                "time_key": "year",
                "site_year": {
                    "universe": "site_universe",
                    "boundary_evidence": {"path": "site-inputs/coc-boundary-compatibility.json"},
                    "outcomes": {
                        "pit": {"panel": "coc_outcomes", "measures": ["pit_sheltered"]},
                        "hic": {"panel": "coc_outcomes", "measures": ["hic_total_beds"]},
                    },
                },
            },
        },
        "site_universe": {
            "source": {
                "artifact_id": ARTIFACT_ID,
                "sites_path": f"site-inputs/{msa_coc_sites_filename(ARTIFACT_ID, 'sites')}",
                "memberships_path": (
                    f"site-inputs/{msa_coc_sites_filename(ARTIFACT_ID, 'memberships')}"
                ),
            },
            "threshold": threshold,
            "site_key": "msa_id",
        },
        "sample_groups": {
            "pooled_site_years": {
                "sample": {
                    "years": {"from": 2020, "to": 2021},
                    "require": ["pit_sheltered", "hic_total_beds"],
                }
            },
            "site_year_2020": {
                "sample": {
                    "years": {"from": 2020, "to": 2020},
                    "require": ["pit_sheltered", "hic_total_beds"],
                }
            },
            "site_year_2021": {
                "sample": {
                    "years": {"from": 2021, "to": 2021},
                    "require": ["pit_sheltered", "hic_total_beds"],
                }
            },
        },
        "analyses": [
            {
                "id": analysis_id,
                "method": "correlation",
                "input": "panels.site_year",
                "sample": {"group": group_id},
                "columns": ["pit_sheltered", "hic_total_beds"],
                "kind": "pearson",
            }
            for analysis_id, group_id in (
                ("pooled_correlation", "pooled_site_years"),
                ("correlation_2020", "site_year_2020"),
                ("correlation_2021", "site_year_2021"),
            )
        ],
        "results": {
            "bundle_dir": "results/{id}/{run_id}",
            "include_panels": ["coc_outcomes", "site_year"],
            "report": {"title": "Synthetic site-year integration"},
        },
    }


def _write_recipe(root: Path, *, threshold: float, recipe_id: str) -> tuple[Path, bytes]:
    path = root / f"{recipe_id}.yaml"
    raw = yaml.safe_dump(_recipe_data(threshold=threshold, recipe_id=recipe_id), sort_keys=False)
    path.write_text(raw, encoding="utf-8")
    return path, raw.encode("utf-8")


def _outcome_panel() -> pd.DataFrame:
    frame = pd.DataFrame(
        OUTCOME_ROWS,
        columns=("coc_id", "year", "pit_sheltered", "hic_total_beds"),
    )
    frame.attrs["results_panel_provenance"] = {"target_geometry": {"type": "coc", "vintage": 2024}}
    return frame


def test_predecessor_counts_replay_with_panel_specific_operations(tmp_path: Path) -> None:
    """CO-1 PIT is preaggregated; HIC needs CO-1 + CO-2, even on replay."""
    input_root = tmp_path / "site-inputs"
    sites_path, memberships_path, _ = _write_site_artifact(input_root)
    resolution = resolve_site_universe_files(
        artifact_id=ARTIFACT_ID,
        threshold=0.95,
        sites_path=sites_path,
        memberships_path=memberships_path,
    )
    evidence_path = _write_evidence(
        input_root, threshold=0.95, membership_identity=resolution.membership_identity
    )
    evidence = json.loads(evidence_path.read_text())
    records = []
    for panel in ("pit_counts", "hic_counts"):
        for original in evidence["records"]:
            coc_id = original["frozen_coc_id"]
            record = {
                **original,
                "source_panel": panel,
                "source_coc_ids": [coc_id],
                "count_operation": "single",
                "geometry_status": "compatible",
            }
            if coc_id == "CO-1":
                record.update(
                    status="reconciled",
                    count_operation="preaggregated" if panel == "pit_counts" else "sum",
                    count_evidence_ref=f"audit://synthetic/{panel}/predecessor-counts",
                    approval_ref="review://synthetic/predecessor-union",
                    reconciliation_basis="administrative_predecessor_union",
                    geometry_status="different_unresolved",
                    geometry_evidence_ref="audit://synthetic/perimeter-difference",
                    geometry_residual="Synthetic perimeter residual retained explicitly.",
                )
                if panel == "pit_counts":
                    record["preaggregated_source_coc_ids"] = ["CO-1", "CO-2"]
                else:
                    record["source_coc_ids"] = ["CO-1", "CO-2"]
            records.append(record)
    evidence.update(schema_version="coc_boundary_compatibility/2", records=records)
    evidence_path.write_text(json.dumps(evidence))

    data = _recipe_data(threshold=0.95, recipe_id="predecessor-replay")
    original_panel = data["panels"].pop("coc_outcomes")
    data["panels"].update(pit_counts=original_panel, hic_counts=dict(original_panel))
    data["pipelines"].append({"id": "build_hic_counts", "target": "coc-outcomes", "steps": []})
    data["panels"]["hic_counts"]["ref"] = {"pipeline": "build_hic_counts", "output": "panel"}
    outcomes = data["panels"]["site_year"]["site_year"]["outcomes"]
    outcomes["pit"]["panel"] = "pit_counts"
    outcomes["hic"]["panel"] = "hic_counts"
    data["results"]["include_panels"] = ["pit_counts", "hic_counts", "site_year"]
    recipe_path = tmp_path / "predecessor-replay.yaml"
    recipe_bytes = yaml.safe_dump(data).encode()
    recipe_path.write_bytes(recipe_bytes)
    recipe = load_recipe(recipe_path)
    plan = plan_results(recipe, original_recipe_bytes=recipe_bytes)
    outputs_root = tmp_path / "outputs"
    RunLedger(outputs_root).create_lock(
        recipe_id=recipe.id,
        recipe_sha256=hashlib.sha256(recipe_bytes).hexdigest(),
        components=plan.component_hashes,
        spec_hash=plan.plan_sha256,
    )
    run = run_results(
        recipe,
        original_recipe_bytes=recipe_bytes,
        outputs_root=outputs_root,
        panel_frames={"pit_counts": _outcome_panel(), "hic_counts": _outcome_panel()},
        private=True,
    )
    assert run["lock_status"]["status"] == "matched"
    assert run["statuses"]["execution_status"] == "complete"
    bundle = Path(run["bundle_path"])
    panel = pd.read_parquet(bundle / "panels/site_year.parquet").set_index(["msa_id", "year"])
    source = _outcome_panel().set_index(["coc_id", "year"])
    assert panel.loc[("MSA-A", 2020), "pit_sheltered"] == source.loc[
        ("CO-1", 2020), "pit_sheltered"
    ]
    assert panel.loc[("MSA-A", 2020), "hic_total_beds"] == (
        source.loc[("CO-1", 2020), "hic_total_beds"]
        + source.loc[("CO-2", 2020), "hic_total_beds"]
    )
    input_root.rename(tmp_path / "unavailable-site-inputs")
    replay = verify_bundle(bundle)
    assert replay.integrity == "pass"
    assert replay.reproducibility == "pass", replay.to_dict()
    assert replay.differences == ()


def test_site_year_bundle_replays_from_bundled_inputs_and_rejects_tampering(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "site-inputs"
    sites_path, memberships_path, _summary_path = _write_site_artifact(input_root)
    low_threshold = resolve_site_universe_files(
        artifact_id=ARTIFACT_ID,
        threshold=0.90,
        sites_path=sites_path,
        memberships_path=memberships_path,
    )
    high_threshold = resolve_site_universe_files(
        artifact_id=ARTIFACT_ID,
        threshold=0.95,
        sites_path=sites_path,
        memberships_path=memberships_path,
    )
    assert low_threshold.memberships == LOW_THRESHOLD_MEMBERSHIPS
    assert high_threshold.memberships == HIGH_THRESHOLD_MEMBERSHIPS
    assert low_threshold.membership_identity != high_threshold.membership_identity

    _write_evidence(
        input_root,
        threshold=0.95,
        membership_identity=high_threshold.membership_identity,
    )
    recipe_path, recipe_bytes = _write_recipe(
        tmp_path, threshold=0.95, recipe_id="site-year-integration"
    )
    recipe = load_recipe(recipe_path)
    plan = plan_results(recipe, original_recipe_bytes=recipe_bytes)

    _write_evidence(
        input_root,
        threshold=0.90,
        membership_identity=low_threshold.membership_identity,
    )
    low_recipe_path, low_recipe_bytes = _write_recipe(
        tmp_path, threshold=0.90, recipe_id="site-year-integration-threshold-090"
    )
    low_recipe = load_recipe(low_recipe_path)
    low_plan = plan_results(low_recipe, original_recipe_bytes=low_recipe_bytes)
    _write_evidence(
        input_root,
        threshold=0.95,
        membership_identity=high_threshold.membership_identity,
    )
    high_sample_context = plan.analyses[0].group_context_key
    low_sample_context = low_plan.analyses[0].group_context_key
    assert high_sample_context != low_sample_context
    assert plan.component_hashes["site_universe"] != low_plan.component_hashes["site_universe"]

    outputs_root = tmp_path / "outputs"
    ledger = RunLedger(outputs_root)
    ledger.create_lock(
        recipe_id=recipe.id,
        recipe_sha256=hashlib.sha256(recipe_bytes).hexdigest(),
        components=plan.component_hashes,
        spec_hash=plan.plan_sha256,
    )

    run = run_results(
        recipe,
        original_recipe_bytes=recipe_bytes,
        outputs_root=outputs_root,
        panel_frames={"coc_outcomes": _outcome_panel()},
        private=True,
    )
    assert run["statuses"]["execution_status"] == "complete"
    assert run["lock_status"]["status"] == "matched"

    origin_bundle = Path(run["bundle_path"])
    relocated_bundle = tmp_path / "relocated-bundle"
    shutil.copytree(origin_bundle, relocated_bundle)
    origin_bundle.rename(tmp_path / "unavailable-origin-bundle")

    manifest_path = relocated_bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    site_index = manifest["site_universes"][ARTIFACT_ID]
    expected_names = {
        "sites": msa_coc_sites_filename(ARTIFACT_ID, "sites"),
        "memberships": msa_coc_sites_filename(ARTIFACT_ID, "memberships"),
        "summary": msa_coc_sites_filename(ARTIFACT_ID, "summary"),
        "boundary_evidence": f"boundary_evidence__{ARTIFACT_ID}.json",
    }
    inventory_paths = {record["path"] for record in manifest["files"]}
    for key, basename in expected_names.items():
        assert Path(site_index[key]).name == basename
        assert site_index[key] in inventory_paths

    resolved = json.loads(
        (relocated_bundle / "recipe" / "resolved.json").read_text(encoding="utf-8")
    )
    site_resolution = resolved["plan"]["site_universe"]
    assert site_resolution["selected_msa_ids"] == list(SELECTED_MSAS)
    assert site_resolution["eligible_site_count"] == len(SELECTED_MSAS)
    assert site_resolution["excluded_site_count"] == 1
    assert site_resolution["membership_identity"] == high_threshold.membership_identity

    input_root.rename(tmp_path / "unavailable-site-inputs")
    verified = verify_bundle(relocated_bundle)
    assert verified.integrity == "pass"
    assert verified.reproducibility == "pass"
    assert verified.differences == ()

    execution = json.loads(
        (relocated_bundle / "recipe" / "execution.json").read_text(encoding="utf-8")
    )
    sample_paths = execution["samples"]
    pooled = pd.read_parquet(relocated_bundle / sample_paths["pooled_correlation@base"])
    pooled_by_year = pooled.groupby("year")["included"].agg(["sum", "count"])
    assert pooled_by_year.loc[2020].to_dict() == {"sum": 3, "count": 3}
    assert pooled_by_year.loc[2021].to_dict() == {"sum": 2, "count": 3}
    per_year_frames = {
        year: pd.read_parquet(relocated_bundle / sample_paths[f"correlation_{year}@base"])
        for year in YEARS
    }
    for year, expected_included in ((2020, 3), (2021, 2)):
        by_year = per_year_frames[year].groupby("year")["included"].agg(["sum", "count"])
        assert by_year.loc[year].to_dict() == {"sum": expected_included, "count": 3}

    for artifact_key in ("sites", "boundary_evidence"):
        tampered_bundle = tmp_path / f"tampered-{artifact_key}"
        shutil.copytree(relocated_bundle, tampered_bundle)
        tampered_path = tampered_bundle / site_index[artifact_key]
        with tampered_path.open("ab") as stream:
            stream.write(b"tampered")
        report = verify_bundle(tampered_bundle)
        assert report.integrity == "fail"
        assert report.reproducibility == "not_run"


@pytest.mark.parametrize("withheld_scope", ("source_panel", "site_universe"))
def test_withheld_site_inputs_replay_as_partial(
    tmp_path: Path, monkeypatch, withheld_scope: str
) -> None:
    input_root = tmp_path / "site-inputs"
    sites_path, memberships_path, _summary_path = _write_site_artifact(input_root)
    resolution = resolve_site_universe_files(
        artifact_id=ARTIFACT_ID,
        threshold=0.95,
        sites_path=sites_path,
        memberships_path=memberships_path,
    )
    _write_evidence(
        input_root,
        threshold=0.95,
        membership_identity=resolution.membership_identity,
    )
    recipe_path, recipe_bytes = _write_recipe(
        tmp_path, threshold=0.95, recipe_id="site-year-withheld-source"
    )
    recipe = load_recipe(recipe_path)
    plan = plan_results(recipe, original_recipe_bytes=recipe_bytes)
    outputs_root = tmp_path / "withheld-outputs"
    RunLedger(outputs_root).create_lock(
        recipe_id=recipe.id,
        recipe_sha256=hashlib.sha256(recipe_bytes).hexdigest(),
        components=plan.component_hashes,
        spec_hash=plan.plan_sha256,
    )

    original_write_bundle = write_bundle
    observed_panel_sources: set[str] = set()

    def write_with_source_panel_withheld(bundle_dir, payload):
        terms = {source: PUBLIC_DATA_TERMS for source in payload.source_terms}
        if withheld_scope == "source_panel":
            panel_artifact = next(
                artifact
                for artifact in payload.artifacts
                if artifact.path == "panels/coc_outcomes.parquet"
            )
            observed_panel_sources.update(panel_artifact.lineage.sources)
            withheld_sources = panel_artifact.lineage.sources
        else:
            withheld_sources = (ARTIFACT_ID,)
        for source in withheld_sources:
            terms[source] = SourceRedistribution(
                RedistributionStatus.RESTRICTED,
                RedistributionStatus.ALLOWED,
            )
        return original_write_bundle(
            bundle_dir,
            replace(payload, private=False, source_terms=terms),
        )

    monkeypatch.setattr("hhplab.results.runner.write_bundle", write_with_source_panel_withheld)
    run = run_results(
        recipe,
        original_recipe_bytes=recipe_bytes,
        outputs_root=outputs_root,
        panel_frames={"coc_outcomes": _outcome_panel()},
        private=False,
    )
    if withheld_scope == "source_panel":
        assert observed_panel_sources
    bundle = Path(run["bundle_path"])
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    included_paths = {record["path"] for record in manifest["files"]}
    site_index = manifest["site_universes"][ARTIFACT_ID]
    if withheld_scope == "source_panel":
        assert "panels/coc_outcomes.parquet" not in included_paths
        assert not site_index.get("withheld", False)
        assert all(
            site_index[key] in included_paths
            for key in ("sites", "memberships", "summary", "boundary_evidence")
        )
        assert "report/index.html" in included_paths
    else:
        assert site_index.get("withheld", False)
        indexed_paths = {
            site_index[key] for key in ("sites", "memberships", "summary", "boundary_evidence")
        }
        assert indexed_paths.isdisjoint(included_paths)
        withheld_doc = json.loads((bundle / "withheld.json").read_text(encoding="utf-8"))
        withheld_paths = {item["path"] for item in withheld_doc["withheld"]}
        assert indexed_paths <= withheld_paths

    report = verify_bundle(bundle)
    assert report.integrity == "pass"
    assert report.reproducibility == "partial", report.to_dict()
    assert report.errors == ()
    assert report.differences == ()
    if withheld_scope == "source_panel":
        assert "unavailable_inputs" in report.partial_reasons
        unavailable_paths = {item.get("path") for item in report.unavailable}
        assert "panels/coc_outcomes.parquet" in unavailable_paths
        assert "panels/site_year.parquet" in unavailable_paths
        assert "analysis:pooled_correlation@base" in unavailable_paths
        assert "analysis:correlation_2020@base" in unavailable_paths
        assert "analysis:correlation_2021@base" in unavailable_paths
    else:
        assert "site_universe_inputs_withheld" in report.partial_reasons
        unavailable_paths = {item.get("path") for item in report.unavailable}
        assert f"site_universes:{ARTIFACT_ID}" in unavailable_paths
        assert "panels/site_year.parquet" in unavailable_paths
        assert "analysis:pooled_correlation@base" in unavailable_paths
        assert "analysis:correlation_2020@base" in unavailable_paths
        assert "analysis:correlation_2021@base" in unavailable_paths
        assert "samples/pooled_correlation@base.parquet" in unavailable_paths
        assert "samples/correlation_2020@base.parquet" in unavailable_paths
        assert "samples/correlation_2021@base.parquet" in unavailable_paths
        dependencies = {
            item.get("path"): item
            for item in report.unavailable
            if item.get("path")
            in {
                "panels/site_year.parquet",
                "analysis:pooled_correlation@base",
                "analysis:correlation_2020@base",
                "analysis:correlation_2021@base",
                "samples/pooled_correlation@base.parquet",
                "samples/correlation_2020@base.parquet",
                "samples/correlation_2021@base.parquet",
            }
        }
        assert all(item.get("artifact_id") == ARTIFACT_ID for item in dependencies.values())
