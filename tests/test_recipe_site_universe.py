"""Focused tests for threshold-selected site-universe inputs."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from hhplab.artifacts.naming.naming import msa_coc_sites_filename
from hhplab.recipe.executor.core import ExecutorError
from hhplab.recipe.executor.inputs import (
    resolve_site_universe_files,
    resolve_site_universe_source,
)
from hhplab.recipe.loader import load_recipe
from hhplab.recipe.results_plan import plan_results
from hhplab.recipe.schema_results import SiteUniverseSource
from hhplab.storage.provenance import ProvenanceBlock, write_parquet_with_provenance

ARTIFACT_ID = "overlap_samples"
BOUNDARY_VINTAGE = "2024"
PARAMETERS = {
    "thresholds": [0.9, 0.95, 0.99],
    "coc_boundary_vintage": 2024,
    "msa_definition_version": "census_msa_2023",
}


def _write_source(
    root: Path,
    *,
    thresholds: list[float] | None = None,
    sites: pd.DataFrame | None = None,
    memberships: pd.DataFrame | None = None,
    schema_version: str = "msa_coc_sites/1",
    membership_definition_version: str = "census_msa_2023",
    site_definition_version: str | None = None,
) -> tuple[Path, Path, Path]:
    thresholds = thresholds or [0.9, 0.95, 0.99]
    sites_path = root / msa_coc_sites_filename(ARTIFACT_ID, "sites")
    memberships_path = root / msa_coc_sites_filename(ARTIFACT_ID, "memberships")
    summary_path = root / msa_coc_sites_filename(ARTIFACT_ID, "summary")
    if sites is None:
        sites = pd.DataFrame(
            [
                {
                    "threshold": threshold,
                    "msa_id": msa_id,
                    "eligible": eligible,
                    "reason": "eligible" if eligible else "below",
                }
                for threshold in thresholds
                for msa_id, eligible in (("MSA1", True), ("MSA2", False))
            ]
        )
    if memberships is None:
        memberships = pd.DataFrame(
            [
                {
                    "threshold": threshold,
                    "msa_id": msa_id,
                    "coc_id": coc_id,
                    "site_eligible": eligible,
                }
                for threshold in thresholds
                for msa_id, coc_id, eligible in (
                    ("MSA1", "COC1", True),
                    ("MSA2", "COC2", False),
                )
            ]
        )
    common = {
        "dataset_type": "msa_coc_sites",
        "parameters": {
            **PARAMETERS,
            "thresholds": thresholds,
            "msa_definition_version": membership_definition_version,
        },
        "inputs": {"crosswalk_path": "inputs/crosswalk.parquet"},
        "rule": "site selection is based on block population containment",
    }
    for path, frame, definition_version in (
        (sites_path, sites, site_definition_version or membership_definition_version),
        (memberships_path, memberships, membership_definition_version),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        provenance = ProvenanceBlock(
            geo_type="msa",
            boundary_vintage=BOUNDARY_VINTAGE,
            definition_version=definition_version,
            extra=common,
        )
        write_parquet_with_provenance(frame, path, provenance)
    summary_path.write_text(
        json.dumps(
            {
                "schema_version": schema_version,
                "parameters": common["parameters"],
                "samples": [],
            }
        )
    )
    return sites_path, memberships_path, summary_path


@pytest.mark.parametrize("threshold", [0.9, 0.95, 0.99])
def test_resolves_explicit_supported_threshold(tmp_path: Path, threshold: float) -> None:
    sites_path, memberships_path, _ = _write_source(tmp_path)

    result = resolve_site_universe_files(
        artifact_id=ARTIFACT_ID,
        threshold=threshold,
        sites_path=sites_path,
        memberships_path=memberships_path,
    )

    assert result.threshold == threshold
    assert result.selected_msa_ids == ("MSA1",)
    assert result.memberships == (("MSA1", "COC1"),)
    assert result.eligible_site_count == 1
    assert result.excluded_site_count == 1
    assert len(result.membership_identity) == 64


def test_rejects_threshold_absent_from_producer(tmp_path: Path) -> None:
    sites_path, memberships_path, _ = _write_source(tmp_path, thresholds=[0.9])

    with pytest.raises(ExecutorError, match="absent from producer summary"):
        resolve_site_universe_files(
            artifact_id=ARTIFACT_ID,
            threshold=0.95,
            sites_path=sites_path,
            memberships_path=memberships_path,
        )


def test_rejects_unknown_schema_and_mismatched_table_parameters(tmp_path: Path) -> None:
    sites_path, memberships_path, _ = _write_source(
        tmp_path, schema_version="msa_coc_sites/99"
    )
    with pytest.raises(ExecutorError, match="unsupported schema_version"):
        resolve_site_universe_files(
            artifact_id=ARTIFACT_ID,
            threshold=0.9,
            sites_path=sites_path,
            memberships_path=memberships_path,
        )

    sites_path, memberships_path, _ = _write_source(
        tmp_path,
        site_definition_version="different_msa_definition",
    )
    with pytest.raises(ExecutorError, match="definition version disagrees"):
        resolve_site_universe_files(
            artifact_id=ARTIFACT_ID,
            threshold=0.9,
            sites_path=sites_path,
            memberships_path=memberships_path,
        )


def test_rejects_duplicate_membership_and_malformed_flags_or_ids(tmp_path: Path) -> None:
    duplicate = pd.DataFrame(
        [
            {"threshold": 0.9, "msa_id": "MSA1", "coc_id": "COC1", "site_eligible": True},
            {"threshold": 0.9, "msa_id": "MSA1", "coc_id": "COC1", "site_eligible": True},
        ]
    )
    sites_path, memberships_path, _ = _write_source(
        tmp_path, thresholds=[0.9], memberships=duplicate
    )
    with pytest.raises(ExecutorError, match="duplicate membership key"):
        resolve_site_universe_files(
            artifact_id=ARTIFACT_ID,
            threshold=0.9,
            sites_path=sites_path,
            memberships_path=memberships_path,
        )

    malformed = duplicate.iloc[:1].copy()
    malformed["coc_id"] = None
    sites_path, memberships_path, _ = _write_source(
        tmp_path, thresholds=[0.9], memberships=malformed
    )
    with pytest.raises(ExecutorError, match="identifiers must be nonempty strings"):
        resolve_site_universe_files(
            artifact_id=ARTIFACT_ID,
            threshold=0.9,
            sites_path=sites_path,
            memberships_path=memberships_path,
        )

    malformed["coc_id"] = "COC1"
    malformed["site_eligible"] = "false"
    sites_path, memberships_path, _ = _write_source(
        tmp_path, thresholds=[0.9], memberships=malformed
    )
    with pytest.raises(ExecutorError, match="flags must be nonmissing booleans"):
        resolve_site_universe_files(
            artifact_id=ARTIFACT_ID,
            threshold=0.9,
            sites_path=sites_path,
            memberships_path=memberships_path,
        )


def test_rejects_changed_fingerprints_and_inconsistent_site_flags(tmp_path: Path) -> None:
    sites_path, memberships_path, _ = _write_source(tmp_path, thresholds=[0.9])
    planned = resolve_site_universe_files(
        artifact_id=ARTIFACT_ID,
        threshold=0.9,
        sites_path=sites_path,
        memberships_path=memberships_path,
    )
    sites = pd.read_parquet(sites_path)
    sites.loc[sites.msa_id == "MSA1", "eligible"] = False
    _write_source(tmp_path, thresholds=[0.9], sites=sites)

    with pytest.raises(ExecutorError, match="fingerprints changed"):
        resolve_site_universe_files(
            artifact_id=ARTIFACT_ID,
            threshold=0.9,
            sites_path=sites_path,
            memberships_path=memberships_path,
            expected_fingerprints=planned.fingerprints,
        )

    with pytest.raises(ExecutorError, match="eligibility flags disagree"):
        resolve_site_universe_files(
            artifact_id=ARTIFACT_ID,
            threshold=0.9,
            sites_path=sites_path,
            memberships_path=memberships_path,
        )


def test_empty_selection_is_explicit_and_shared_coc_is_rejected(tmp_path: Path) -> None:
    sites = pd.DataFrame(
        [{"threshold": 0.9, "msa_id": "MSA1", "eligible": False, "reason": "below"}]
    )
    memberships = pd.DataFrame(
        [{"threshold": 0.9, "msa_id": "MSA1", "coc_id": "COC1", "site_eligible": False}]
    )
    sites_path, memberships_path, _ = _write_source(
        tmp_path, thresholds=[0.9], sites=sites, memberships=memberships
    )
    result = resolve_site_universe_files(
        artifact_id=ARTIFACT_ID,
        threshold=0.9,
        sites_path=sites_path,
        memberships_path=memberships_path,
    )
    assert result.eligible_site_count == 0
    assert result.selected_msa_ids == ()
    assert result.memberships == ()

    shared_memberships = pd.DataFrame(
        [
            {"threshold": 0.9, "msa_id": "MSA1", "coc_id": "COC1", "site_eligible": True},
            {"threshold": 0.9, "msa_id": "MSA2", "coc_id": "COC1", "site_eligible": True},
        ]
    )
    shared_sites = pd.DataFrame(
        [
            {"threshold": 0.9, "msa_id": msa_id, "eligible": True, "reason": "eligible"}
            for msa_id in ("MSA1", "MSA2")
        ]
    )
    sites_path, memberships_path, _ = _write_source(
        tmp_path,
        thresholds=[0.9],
        sites=shared_sites,
        memberships=shared_memberships,
    )
    with pytest.raises(ExecutorError, match="belongs to multiple selected MSAs"):
        resolve_site_universe_files(
            artifact_id=ARTIFACT_ID,
            threshold=0.9,
            sites_path=sites_path,
            memberships_path=memberships_path,
        )


def test_resolves_verified_bundle_site_universe_index(tmp_path: Path) -> None:
    bundle_root = tmp_path / "site-bundle"
    sites_path, memberships_path, summary_path = _write_source(
        bundle_root, thresholds=[0.9]
    )
    paths = {
        "sites": sites_path,
        "memberships": memberships_path,
        "summary": summary_path,
    }
    relative = {key: f"artifacts/{path.name}" for key, path in paths.items()}
    for path in paths.values():
        target = bundle_root / "artifacts" / path.name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(path.read_bytes())
    inventory = [
        {
            "path": relative[key],
            "sha256": hashlib.sha256((bundle_root / relative[key]).read_bytes()).hexdigest(),
            "bytes": (bundle_root / relative[key]).stat().st_size,
            "role": "input",
        }
        for key in paths
    ]
    manifest = {
        "files": inventory,
        "site_universes": {
            ARTIFACT_ID: {
                "schema_version": "msa_coc_sites/1",
                **relative,
            }
        },
    }
    manifest_bytes = json.dumps(manifest, sort_keys=True).encode("utf-8")
    (bundle_root / "manifest.json").write_bytes(manifest_bytes)
    manifest_digest = hashlib.sha256(manifest_bytes).hexdigest()
    (bundle_root / "manifest.sha256").write_text(f"{manifest_digest}  manifest.json\n")
    source = SiteUniverseSource.model_validate(
        {"artifact_id": ARTIFACT_ID, "bundle_path": "site-bundle"}
    )

    result = resolve_site_universe_source(
        source=source,
        threshold=0.9,
        source_base_dir=tmp_path,
    )

    assert result.selected_msa_ids == ("MSA1",)
    assert result.manifest_fingerprint == manifest_digest

    bundled_sites = bundle_root / relative["sites"]
    bundled_sites.write_bytes(b"tampered after manifest creation")
    with pytest.raises(ExecutorError, match="differs from the manifest inventory"):
        resolve_site_universe_source(
            source=source,
            threshold=0.9,
            source_base_dir=tmp_path,
        )


def test_results_plan_lists_exact_inputs_membership_and_boundary_evidence(
    tmp_path: Path,
) -> None:
    sites_path, memberships_path, _ = _write_source(tmp_path, thresholds=[0.9])
    resolved = resolve_site_universe_files(
        artifact_id=ARTIFACT_ID,
        threshold=0.9,
        sites_path=sites_path,
        memberships_path=memberships_path,
    )
    evidence_path = tmp_path / "boundary-evidence.json"
    evidence_path.write_text(
        json.dumps(
            {
                "schema_version": "coc_boundary_compatibility/1",
                "evidence_id": "fixture-evidence",
                "artifact_id": ARTIFACT_ID,
                "threshold": 0.9,
                "membership_identity": resolved.membership_identity,
                "frozen_boundary_vintage": BOUNDARY_VINTAGE,
                "records": [
                    {
                        "source_panel": "source",
                        "frozen_coc_id": "COC1",
                        "year": 2020,
                        "frozen_boundary_vintage": BOUNDARY_VINTAGE,
                        "source_boundary_vintage": "2020",
                        "status": "compatible",
                        "reason": "Evidence reviewed against the source panel vintage.",
                        "evidence_ref": "audit://fixture/COC1/2020",
                    }
                ],
            }
        )
    )
    recipe = load_recipe(
        {
            "version": 2,
            "id": "site-universe-plan",
            "title": "Site universe plan",
            "authors": ["HHP-Lab"],
            "universe": {"years": [2020]},
            "targets": [{"id": "coc-target", "geometry": {"type": "coc", "vintage": 2020}}],
            "datasets": {},
            "pipelines": [{"id": "coc-build", "target": "coc-target", "steps": []}],
            "panels": {
                "source": {
                    "ref": {"pipeline": "coc-build", "output": "panel"},
                    "geo_key": "coc_id",
                    "time_key": "year",
                },
                "site_year": {
                    "geo_key": "msa_id",
                    "time_key": "year",
                    "site_year": {
                        "universe": "site_universe",
                        "boundary_evidence": {"path": evidence_path.name},
                        "outcomes": {
                            "pit": {"panel": "source", "measures": ["pit_total"]},
                            "hic": {"panel": "source", "measures": ["hic_total_beds"]},
                        },
                    }
                },
            },
            "site_universe": {
                "source": {
                    "artifact_id": ARTIFACT_ID,
                    "sites_path": sites_path.name,
                    "memberships_path": memberships_path.name,
                },
                "threshold": 0.9,
                "site_key": "msa_id",
            },
            "analyses": [
                {
                    "id": "corr",
                    "method": "correlation",
                    "input": "panels.site_year",
                    "sample": {"years": {"from": 2020, "to": 2020}},
                    "columns": ["pit_total", "hic_total_beds"],
                }
            ],
            "results": {
                "bundle_dir": "results/{id}/{run_id}",
                "include_panels": ["site_year"],
                "report": {"title": "Site universe results"},
            },
        },
        source_base_dir=tmp_path,
    )

    plan = plan_results(recipe, original_recipe_bytes=b"site-universe recipe")
    assert plan.site_universe is not None
    assert plan.site_universe["memberships"] == (("MSA1", "COC1"),)
    assert plan.site_universe["eligible_site_count"] == 1
    assert plan.site_universe["excluded_site_count"] == 1
    assert plan.site_universe["boundary_evidence"]["validation"]["evidence_id"] == (
        "fixture-evidence"
    )
    assert plan.component_hashes["site_universe"]
    assert str(tmp_path).encode() not in plan.canonical_plan
    assert plan.is_ready
    assert not any(finding.is_blocking for finding in plan.preflight_findings)
