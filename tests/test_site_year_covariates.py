"""Native MSA/year covariate joins onto frozen whole-CoC site outcomes."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from hhplab.artifacts.naming.naming import msa_coc_sites_filename
from hhplab.geographies.coc.coc_validate import validate_coc_boundary_compatibility
from hhplab.recipe.executor.core import ExecutorError
from hhplab.recipe.executor.msa_coc_panel import (
    SiteYearCovariateBinding,
    SiteYearSourceBinding,
    _normalize_site_year_covariates,
    assemble_site_year_panel,
)
from hhplab.recipe.loader import RecipeLoadError, load_recipe
from hhplab.recipe.results_plan import ResultsPlanningError, plan_results
from hhplab.results.runner import (
    _panel_input_bytes,
    _validate_site_covariate_provenance,
    run_results,
)
from hhplab.results.verify import _dependency_exclusions, verify_bundle
from hhplab.schema.contracts import serialize_annotation_contract
from hhplab.sources.hud.pit.observation import COUNT_TYPES_SHELTERED_ONLY
from hhplab.storage.provenance import (
    ProvenanceBlock,
    read_provenance,
    write_parquet_with_provenance,
)
from hhplab.storage.run_ledger import RunLedger

ARTIFACT_ID = "site_year_covariate_sites"
YEARS = (2020, 2021)
FROZEN_VINTAGE = "2024"
SELECTED_MSAS = ("MSA-A", "MSA-B", "MSA-C")
HIGH_THRESHOLD_MEMBERSHIPS = (
    ("MSA-A", "CO-1"),
    ("MSA-B", "CO-3"),
    ("MSA-C", "CO-4"),
)

ANNOTATION_ID = "right-to-shelter-provisional-source-baseline"
ANNOTATION_TEXT = (
    "Uncovered populations in listed jurisdictions use the reviewed provisional baseline."
)
RTS_ROWS = tuple(
    (msa_id, year, int(msa_id == "MSA-B"))
    for msa_id in SELECTED_MSAS
    for year in YEARS
)


def _write_site_artifact(root: Path) -> tuple[Path, Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    sites_path = root / msa_coc_sites_filename(ARTIFACT_ID, "sites")
    memberships_path = root / msa_coc_sites_filename(ARTIFACT_ID, "memberships")
    summary_path = root / msa_coc_sites_filename(ARTIFACT_ID, "summary")
    parameters = {
        "thresholds": [0.95],
        "coc_boundary_vintage": 2024,
        "msa_definition_version": "census_msa_2023",
    }
    common = {
        "dataset_type": "msa_coc_sites",
        "parameters": parameters,
        "inputs": {"crosswalk_path": "synthetic/crosswalk.parquet"},
    }
    write_parquet_with_provenance(
        pd.DataFrame(
            [
                {"threshold": 0.95, "msa_id": msa_id, "eligible": True}
                for msa_id in SELECTED_MSAS
            ]
        ),
        sites_path,
        ProvenanceBlock(
            geo_type="msa",
            boundary_vintage=FROZEN_VINTAGE,
            definition_version="census_msa_2023",
            extra=common,
        ),
    )
    write_parquet_with_provenance(
        pd.DataFrame(
            [
                {"threshold": 0.95, "msa_id": msa_id, "coc_id": coc_id,
                 "site_eligible": True}
                for msa_id, coc_id in HIGH_THRESHOLD_MEMBERSHIPS
            ]
        ),
        memberships_path,
        ProvenanceBlock(
            geo_type="msa",
            boundary_vintage=FROZEN_VINTAGE,
            definition_version="census_msa_2023",
            extra=common,
        ),
    )
    summary_path.write_text(
        json.dumps(
            {"schema_version": "msa_coc_sites/1", "parameters": parameters, "samples": []}
        ),
        encoding="utf-8",
    )
    return sites_path, memberships_path, summary_path


def _write_evidence(root: Path, *, membership_identity: str) -> Path:
    path = root / "coc-boundary-compatibility.json"
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
        for _msa_id, coc_id in HIGH_THRESHOLD_MEMBERSHIPS
        for year in YEARS
    ]
    path.write_text(
        json.dumps(
            {
                "schema_version": "coc_boundary_compatibility/1",
                "evidence_id": "synthetic-covariate-boundary",
                "artifact_id": ARTIFACT_ID,
                "threshold": 0.95,
                "membership_identity": membership_identity,
                "frozen_boundary_vintage": FROZEN_VINTAGE,
                "records": records,
            }
        ),
        encoding="utf-8",
    )
    return path


def _recipe_data(*, recipe_id: str) -> dict:
    return {
        "version": 2,
        "id": recipe_id,
        "title": "Synthetic site-year covariate integration",
        "authors": ["HHP-Lab test fixture"],
        "universe": {"range": "2020-2021"},
        "targets": [{"id": "coc-outcomes", "geometry": {"type": "coc", "vintage": 2024}}],
        "datasets": {},
        "pipelines": [{"id": "build_coc_outcomes", "target": "coc-outcomes", "steps": []}],
        "panels": {
            "coc_outcomes": {
                "ref": {"pipeline": "build_coc_outcomes", "output": "panel"},
                "geo_key": "coc_id", "time_key": "year",
            },
            "site_year": {
                "geo_key": "msa_id", "time_key": "year",
                "site_year": {
                    "universe": "site_universe",
                    "boundary_evidence": {"path": "coc-boundary-compatibility.json"},
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
                "sites_path": msa_coc_sites_filename(ARTIFACT_ID, "sites"),
                "memberships_path": msa_coc_sites_filename(ARTIFACT_ID, "memberships"),
            },
            "threshold": 0.95,
            "site_key": "msa_id",
        },
        "sample_groups": {
            "pooled_site_years": {
                "sample": {
                    "years": {"from": 2020, "to": 2021},
                    "require": ["pit_sheltered", "hic_total_beds", "right_to_shelter"],
                    "geographies": {
                        "exclude": {"column": "right_to_shelter", "equals": 1}
                    },
                }
            }
        },
        "analyses": [
            {
                "id": "pooled_correlation", "method": "correlation",
                "input": "panels.site_year", "sample": {"group": "pooled_site_years"},
                "columns": ["pit_sheltered", "hic_total_beds"], "kind": "pearson",
            }
        ],
        "results": {
            "bundle_dir": "results/{id}/{run_id}",
            "include_panels": ["coc_outcomes", "site_year"],
            "report": {"title": "Synthetic site-year covariate integration"},
        },
    }


def _outcome_panel() -> pd.DataFrame:
    frame = pd.DataFrame(
        [(coc, year, 10 + year % 10, 20 + year % 10)
         for _msa, coc in HIGH_THRESHOLD_MEMBERSHIPS
         for year in YEARS],
        columns=("coc_id", "year", "pit_sheltered", "hic_total_beds"),
    )
    frame["count_types"] = COUNT_TYPES_SHELTERED_ONLY
    frame.attrs["results_panel_provenance"] = {
        "target_geometry": {"type": "coc", "vintage": 2024}
    }
    return frame


def _boundary_validation():
    evidence_payload = {
        "schema_version": "coc_boundary_compatibility/1",
        "evidence_id": "synthetic-covariate-test",
        "artifact_id": ARTIFACT_ID,
        "threshold": 0.95,
        "membership_identity": "synthetic-membership",
        "frozen_boundary_vintage": FROZEN_VINTAGE,
        "records": [
            {
                "source_panel": "coc_outcomes",
                "frozen_coc_id": coc_id,
                "year": year,
                "frozen_boundary_vintage": FROZEN_VINTAGE,
                "source_boundary_vintage": FROZEN_VINTAGE,
                "status": "compatible",
                "reason": "synthetic fixture",
                "evidence_ref": f"audit://{coc_id}/{year}",
            }
            for _msa_id, coc_id in HIGH_THRESHOLD_MEMBERSHIPS
            for year in YEARS
        ],
    }
    return validate_coc_boundary_compatibility(
        evidence_payload,
        artifact_id=ARTIFACT_ID,
        threshold=0.95,
        membership_identity="synthetic-membership",
        frozen_boundary_vintage=FROZEN_VINTAGE,
        frozen_memberships=HIGH_THRESHOLD_MEMBERSHIPS,
        requested_years=YEARS,
        source_panel_vintages={
            ("coc_outcomes", year): FROZEN_VINTAGE for year in YEARS
        },
    )


def _annotated_covariate_panel(rows=RTS_ROWS) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=("msa_id", "year", "right_to_shelter"))
    frame.attrs["results_panel_provenance"] = {
        "target_geometry": {"type": "msa", "source": "census_msa_2023"},
        "definition_version": "census_msa_2023",
    }
    frame.attrs["annotations"] = serialize_annotation_contract(
        [
            {
                "id": ANNOTATION_ID,
                "revision": 1,
                "category": "caveat",
                "summary": "RTS baseline coding includes provisional none decisions.",
                "text": ANNOTATION_TEXT,
                "public_text": ANNOTATION_TEXT,
                "scope": {"columns": ["right_to_shelter"]},
            }
        ],
        bindings={
            f"{ANNOTATION_ID}#r1": {
                "columns": {
                    "right_to_shelter": [
                        [msa_id, year] for msa_id, year, _value in rows
                    ]
                }
            }
        },
    )
    return frame


def _covariate_recipe_data() -> dict:
    data = _recipe_data(recipe_id="site-year-rts-covariates")
    data["targets"].append(
        {
            "id": "msa-reference",
            "geometry": {"type": "msa", "source": "census_msa_2023"},
        }
    )
    data["pipelines"].append(
        {"id": "build_msa_reference", "target": "msa-reference", "steps": []}
    )
    data["panels"]["msa_reference"] = {
        "ref": {"pipeline": "build_msa_reference", "output": "panel"},
        "geo_key": "msa_id",
        "time_key": "year",
    }
    data["panels"]["site_year"]["site_year"]["covariates"] = [
        {
            "panel": "msa_reference",
            "columns": ["right_to_shelter"],
            "require_match": True,
        }
    ]
    return data


def _run_covariate_fixture(
    tmp_path: Path, covariates: pd.DataFrame, *, private: bool = True
):
    input_root = tmp_path / "site-inputs"
    sites_path, memberships_path, _summary = _write_site_artifact(input_root)
    # Boundary evidence is bound to the calculated membership identity.
    from hhplab.recipe.executor.inputs import resolve_site_universe_files

    resolution = resolve_site_universe_files(
        artifact_id=ARTIFACT_ID,
        threshold=0.95,
        sites_path=sites_path,
        memberships_path=memberships_path,
    )
    evidence_path = _write_evidence(
        input_root, membership_identity=resolution.membership_identity
    )
    data = _covariate_recipe_data()
    data["panels"]["site_year"]["site_year"]["boundary_evidence"]["path"] = str(
        evidence_path
    )
    data["site_universe"]["source"]["sites_path"] = str(sites_path)
    data["site_universe"]["source"]["memberships_path"] = str(memberships_path)
    recipe_path = tmp_path / "site-year-rts-covariates.yaml"
    recipe_bytes = yaml.safe_dump(data, sort_keys=False).encode()
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
        panel_frames={
            "coc_outcomes": _outcome_panel(),
            "msa_reference": covariates,
        },
        private=private,
    )
    return run, plan, recipe


@pytest.mark.parametrize("private", (True, False), ids=("private", "public"))
def test_runner_joins_covariates_and_verifies_portable_annotated_bundle(
    tmp_path: Path, private: bool
) -> None:
    run, plan, recipe = _run_covariate_fixture(
        tmp_path, _annotated_covariate_panel(), private=private
    )

    assert run["statuses"]["execution_status"] == "complete"
    assert plan.site_universe["msa_definition_version"] == "census_msa_2023"
    assert set(plan.site_universe["site_year_source_panels"]) == {
        "coc_outcomes",
        "msa_reference",
    }
    bundle = Path(run["bundle_path"])
    annotation_key = f"{ANNOTATION_ID}#r1"
    if private:
        site_panel = pd.read_parquet(bundle / "panels/site_year.parquet")
        assert site_panel[["msa_id", "year"]].to_records(index=False).tolist() == [
            (msa_id, year) for msa_id in sorted(SELECTED_MSAS) for year in YEARS
        ]
        expected = pd.DataFrame(RTS_ROWS, columns=("msa_id", "year", "right_to_shelter"))
        actual = site_panel[["msa_id", "year", "right_to_shelter"]]
        pd.testing.assert_frame_equal(
            actual.reset_index(drop=True), expected.reset_index(drop=True), check_dtype=False
        )
        provenance = read_provenance(bundle / "panels/site_year.parquet")
        assert provenance is not None
        contract = provenance.extra["annotations"]
        assert set(provenance.extra["site_year_panel"]["source_panels"]) == {
            "coc_outcomes",
            "msa_reference",
        }
        assert contract["bindings"][annotation_key]["columns"]["right_to_shelter"] == [
            [msa_id, year] for msa_id, year, _value in RTS_ROWS
        ]
    else:
        assert not (bundle / "panels/site_year.parquet").exists()
        withheld = json.loads((bundle / "withheld.json").read_text(encoding="utf-8"))
        assert any(item["path"] == "panels/site_year.parquet" for item in withheld["withheld"])
    sample_paths = sorted((bundle / "samples").glob("*.parquet"))
    assert len(sample_paths) == 1
    sampled = pd.read_parquet(sample_paths[0])
    included = sampled.loc[sampled["included"].astype(bool)]
    assert set(included["msa_id"]) == {"MSA-A", "MSA-C"}
    assert set(included["year"]) == set(YEARS)
    if private:
        sample_provenance = read_provenance(sample_paths[0])
        assert sample_provenance is not None
        assert annotation_key in sample_provenance.extra["annotations"]["uncertain_ids"]
    else:
        sample_provenance = read_provenance(sample_paths[0])
        assert sample_provenance is not None
        assert "annotation_status" in sample_provenance.extra
    verification = verify_bundle(bundle)
    assert verification.integrity == "pass"
    expected_reproducibility = "pass" if private else "partial"
    assert verification.reproducibility == expected_reproducibility, verification.to_dict()
    resolved = {
        "plan": plan.to_dict(),
        "recipe": recipe.model_dump(mode="json", by_alias=True, exclude_none=True),
    }
    dependency = _dependency_exclusions(
        resolved, [{"path": "panels/msa_reference.parquet"}]
    )
    assert "site_year" in dependency.panels


@pytest.mark.parametrize(
    "rows, message",
    [
        (RTS_ROWS[:-1], "missing 1 required"),
        ((*RTS_ROWS, RTS_ROWS[0]), "duplicate normalized key"),
        (((None, 2020, 0), *RTS_ROWS[1:]), "null or empty msa_id"),
        (((" MSA-A", 2020, 0), *RTS_ROWS[1:]), "leading or trailing whitespace"),
    ],
    ids=(
        "missing-required-match", "duplicate-normalized-key", "null-geo-key",
        "whitespace-geo-key",
    ),
)
def test_executor_rejects_invalid_covariate_keys(rows, message: str) -> None:
    memberships = HIGH_THRESHOLD_MEMBERSHIPS
    boundary = _boundary_validation()
    covariate_frame = pd.DataFrame(
        rows, columns=("msa_id", "year", "right_to_shelter")
    )
    with pytest.raises(ExecutorError, match=message):
        assemble_site_year_panel(
            frozen_memberships=memberships,
            requested_years=YEARS,
            source_panels={
                "coc_outcomes": _outcome_panel(),
                "msa_reference": covariate_frame,
            },
            outcome_bindings={
                "pit": SiteYearSourceBinding("coc_outcomes", ("pit_sheltered",)),
                "hic": SiteYearSourceBinding("coc_outcomes", ("hic_total_beds",)),
            },
            covariate_bindings=(
                SiteYearCovariateBinding(
                    "msa_reference", ("right_to_shelter",), require_match=True
                ),
            ),
            boundary_compatibility=boundary,
            artifact_id=ARTIFACT_ID,
            threshold=0.95,
            membership_identity="synthetic-membership",
            frozen_boundary_vintage="2024",
            source_panel_fingerprints={
                "coc_outcomes": "outcome-fingerprint",
                "msa_reference": "covariate-fingerprint",
            },
        )


def test_optional_covariate_match_preserves_missing_as_null() -> None:
    rows = RTS_ROWS[:-1]
    result = assemble_site_year_panel(
        frozen_memberships=HIGH_THRESHOLD_MEMBERSHIPS,
        requested_years=YEARS,
        source_panels={
            "coc_outcomes": _outcome_panel(),
            "msa_reference": pd.DataFrame(
                rows, columns=("msa_id", "year", "right_to_shelter")
            ),
        },
        outcome_bindings={
            "pit": SiteYearSourceBinding("coc_outcomes", ("pit_sheltered",)),
            "hic": SiteYearSourceBinding("coc_outcomes", ("hic_total_beds",)),
        },
        covariate_bindings=(
            SiteYearCovariateBinding(
                "msa_reference", ("right_to_shelter",), require_match=False
            ),
        ),
        boundary_compatibility=_boundary_validation(),
        artifact_id=ARTIFACT_ID,
        threshold=0.95,
        membership_identity="synthetic-membership",
        frozen_boundary_vintage=FROZEN_VINTAGE,
        source_panel_fingerprints={
            "coc_outcomes": "outcome-fingerprint",
            "msa_reference": "covariate-fingerprint",
        },
    )

    missing = result.panel.loc[
        (result.panel.msa_id == "MSA-C") & (result.panel.year == 2021),
        "right_to_shelter",
    ]
    assert len(missing) == 1 and pd.isna(missing.iloc[0])


def test_recipe_rejects_non_msa_covariate_source(tmp_path: Path) -> None:
    data = _covariate_recipe_data()
    data["targets"][1]["geometry"] = {"type": "county", "source": "census_msa_2023"}
    recipe_path = tmp_path / "wrong-geometry.yaml"
    recipe_path.write_text(yaml.safe_dump(data), encoding="utf-8")

    with pytest.raises(RecipeLoadError, match="canonical MSA geometry"):
        load_recipe(recipe_path)


def test_planner_rejects_msa_definition_mismatch(tmp_path: Path) -> None:
    from hhplab.recipe.executor.inputs import resolve_site_universe_files

    input_root = tmp_path / "site-inputs"
    sites_path, memberships_path, _summary = _write_site_artifact(input_root)
    resolution = resolve_site_universe_files(
        artifact_id=ARTIFACT_ID,
        threshold=0.95,
        sites_path=sites_path,
        memberships_path=memberships_path,
    )
    evidence_path = _write_evidence(
        input_root, membership_identity=resolution.membership_identity
    )
    data = _covariate_recipe_data()
    data["targets"][1]["geometry"]["source"] = "census_msa_2010"
    data["site_universe"]["source"]["sites_path"] = str(sites_path)
    data["site_universe"]["source"]["memberships_path"] = str(memberships_path)
    data["panels"]["site_year"]["site_year"]["boundary_evidence"]["path"] = str(
        evidence_path
    )
    recipe_path = tmp_path / "wrong-definition.yaml"
    recipe_bytes = yaml.safe_dump(data).encode()
    recipe_path.write_bytes(recipe_bytes)
    recipe = load_recipe(recipe_path)

    with pytest.raises(ResultsPlanningError, match="does not match frozen site-universe"):
        plan_results(recipe, original_recipe_bytes=recipe_bytes)


def test_runtime_rejects_subset_definition_provenance() -> None:
    frame = pd.DataFrame(RTS_ROWS, columns=("msa_id", "year", "right_to_shelter"))
    frame.attrs["results_panel_provenance"] = {
        "target_geometry": {
            "type": "msa",
            "source": "census_msa_2023",
            "subset_profile_definition_version": "subset-v1",
        }
    }

    with pytest.raises(ValueError, match="cannot use an MSA subset profile"):
        _validate_site_covariate_provenance(
            "msa_reference", frame, {"msa_definition_version": "census_msa_2023"}
        )


def test_executor_rejects_covariate_column_collision() -> None:
    boundary = _boundary_validation()
    with pytest.raises(ExecutorError, match="collide with output columns"):
        assemble_site_year_panel(
            frozen_memberships=HIGH_THRESHOLD_MEMBERSHIPS,
            requested_years=YEARS,
            source_panels={
                "coc_outcomes": _outcome_panel(),
                "msa_reference": _annotated_covariate_panel(),
            },
            outcome_bindings={
                "pit": SiteYearSourceBinding("coc_outcomes", ("pit_sheltered",)),
                "hic": SiteYearSourceBinding("coc_outcomes", ("hic_total_beds",)),
            },
            covariate_bindings=(
                SiteYearCovariateBinding("msa_reference", ("pit_sheltered",)),
            ),
            boundary_compatibility=boundary,
            artifact_id=ARTIFACT_ID,
            threshold=0.95,
            membership_identity="synthetic-membership",
            frozen_boundary_vintage="2024",
            source_panel_fingerprints={
                "coc_outcomes": "outcome-fingerprint",
                "msa_reference": "covariate-fingerprint",
            },
        )


def test_covariate_binding_normalizes_panel_and_requires_string_columns() -> None:
    with pytest.raises(ExecutorError, match="may be bound only once"):
        _normalize_site_year_covariates(
            (
                SiteYearCovariateBinding("msa_reference", ("right_to_shelter",)),
                SiteYearCovariateBinding(" msa_reference ", ("another_flag",)),
            )
        )
    with pytest.raises(ExecutorError, match="selected columns must be strings"):
        _normalize_site_year_covariates(
            (SiteYearCovariateBinding("msa_reference", (None,)),)
        )


def test_annotated_source_bytes_allow_list_roundtrip_and_reject_scalar_mutation() -> None:
    frame = pd.DataFrame(
        {"msa_id": ["MSA-A"], "year": [2020], "right_to_shelter": [0],
         "member_ids": [["CO-1"]]}
    )
    annotation = {
        "id": ANNOTATION_ID,
        "revision": 1,
        "category": "caveat",
        "summary": "Provisional source classification.",
        "text": ANNOTATION_TEXT,
        "public_text": ANNOTATION_TEXT,
        "scope": {"columns": ["right_to_shelter"]},
    }
    annotation_key = f"{ANNOTATION_ID}#r1"
    frame.attrs["annotations"] = serialize_annotation_contract(
        [annotation],
        bindings={
            annotation_key: {"columns": {"right_to_shelter": [["MSA-A", 2020]]}}
        },
    )
    source_bytes = _panel_input_bytes(frame, dataset_type="results_input_panel")
    frame.attrs["results_source_parquet_bytes"] = source_bytes

    assert _panel_input_bytes(frame, dataset_type="results_input_panel") == source_bytes
    mutated = frame.copy(deep=True)
    mutated.loc[0, "right_to_shelter"] = 1
    with pytest.raises(ValueError, match="changed after reading its source bytes"):
        _panel_input_bytes(mutated, dataset_type="results_input_panel")
