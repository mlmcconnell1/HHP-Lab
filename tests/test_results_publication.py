"""Offline publication acceptance with synthetic values and frozen permission evidence."""

from __future__ import annotations

import csv
import hashlib
import json
import xml.etree.ElementTree as et
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from urllib.parse import parse_qsl

import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.recipe.loader import load_recipe
from hhplab.recipe.results_plan import plan_results
from hhplab.results import publish as publisher
from hhplab.results.bundle import BundleArtifact, read_bundle, write_bundle
from hhplab.results.lineage import ArtifactLineage
from hhplab.results.runner import run_results
from hhplab.results.verify import VerificationReport
from hhplab.schema.redistribution import (
    UNKNOWN_TERMS,
    ArtifactScope,
    PermissionSource,
    PermissionTermsOrigin,
)
from hhplab.schema.results import LEGACY_BUNDLE_FORMAT_VERSION, ResultError, canonical_bytes
from hhplab.storage.run_ledger import RunLedger

SOURCE_YEARS = (2020, 2021, 2022)
SYNTHETIC_SLOPES = {
    "supports_claim": 1.0,
    "no_meaningful_effect": 0.0,
    "inconclusive": 0.5,
}
RECIPE_CONFIGURATION = {
    "version": 2,
    "id": "publication-test",
    "title": "Synthetic publication",
    "authors": ["HHP-Lab"],
    "universe": {"range": "2020-2022"},
    "targets": [{"id": "target", "geometry": {"type": "coc", "vintage": 2024}}],
    "datasets": {
        "source": {
            "provider": "hud",
            "product": "pit",
            "version": 1,
            "native_geometry": {"type": "coc", "vintage": 2024},
        }
    },
    "pipelines": [
        {"id": "build", "target": "target", "steps": [{"kind": "join", "datasets": ["source"]}]}
    ],
    "panels": {
        "panel": {
            "ref": {"pipeline": "build", "output": "panel"},
            "geo_key": "geo_id",
            "time_key": "year",
        }
    },
    "parameters": {"width": 0.15},
    "analyses": [
        {
            "id": "fit",
            "method": "regression",
            "input": "panels.panel",
            "sample": {},
            "outcome": "y",
            "predictors": ["x"],
            "inference": {"method": "hc1"},
        }
    ],
    "findings": [
        {
            "id": "effect",
            "claim": "A change in x is associated with a similar change in y.",
            "criteria_source": {"estimate": {"analysis": "fit", "term": "x"}},
            "reference_values": [0.0, 0.5, 1.0],
            "bands": [
                {
                    "label": "similar",
                    "public_verdict": "supports_claim",
                    "equivalent_to": {"value": 1.0, "margin": {"param": "width"}},
                },
                {
                    "label": "near_zero",
                    "public_verdict": "no_meaningful_effect",
                    "equivalent_to": {"value": 0.0, "margin": {"param": "width"}},
                },
            ],
        }
    ],
    "results": {
        "bundle_dir": "results/{id}/{run_id}",
        "include_panels": ["panel"],
        "report": {"title": "Synthetic publication"},
    },
}


def _run_bundle(
    root, verdict, *, configuration=None, expected_execution="complete", alternate_year=False,
    extra_panels=None,
    **options,
) -> Path:
    slope = SYNTHETIC_SLOPES[verdict]
    records = []
    for year in SOURCE_YEARS:
        alternate = alternate_year and year == SOURCE_YEARS[-1]
        for site in range(12):
            x = (site - 5.5) * (0.2 if alternate else 1)
            noise = ((site + year) % 7 - 3) / (100 if alternate_year else 13)
            records.append({
                "geo_id": f"s-{site:02}",
                "year": year,
                "x": x,
                "y": (0 if alternate else slope) * x + noise,
            })
    panel = pd.DataFrame(records)
    panel.attrs["measures"] = {"x": {"unit": "beds"}, "y": {"unit": "people"}}
    configuration = configuration or RECIPE_CONFIGURATION
    original = yaml.safe_dump(configuration).encode()
    recipe = load_recipe(configuration)
    result = run_results(
        recipe,
        outputs_root=root,
        original_recipe_bytes=original,
        panel_frames={"panel": panel, **(extra_panels or {})},
        **options,
    )
    assert result["statuses"]["execution_status"] == expected_execution
    return Path(result["bundle_path"])


def _source(root, bundle_path, *, slug="study", narrative=None, **updates) -> Path:
    contents = read_bundle(bundle_path)
    manifest = contents.manifest
    path = root / "publications" / slug / "publication.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    metadata = {
        "publication_format_version": 1,
        "slug": slug,
        "title": "Synthetic study",
        "question": "Is there an association?",
        "status": "active",
        "tags": ["synthetic"],
        "geographies": "Synthetic sites",
        "bundle": {
            "recipe_id": manifest.recipe_id,
            "run_id": manifest.run_id,
            "manifest_sha256": contents.manifest_sha256,
            "public_url": f"https://example.org/bundles/{manifest.run_id}",
        },
        "findings": ["effect"],
        "primary_finding": "effect",
        "figures": [],
        **updates,
    }
    path.write_text(yaml.safe_dump(metadata), encoding="utf-8")
    path.with_name("narrative.md").write_text(
        narrative or "The estimate was {{ estimate : fit.x | round:3 }}.",
        encoding="utf-8",
    )
    return path


def _page(directory):
    return yaml.safe_load((directory / "index.md").read_text().split("---\n", 2)[1])


def _snapshot(directory):
    return {
        path.relative_to(directory).as_posix(): path.read_bytes()
        for path in directory.rglob("*")
        if path.is_file()
    }


def _write_manifest(bundle, manifest):
    manifest_bytes = canonical_bytes(manifest)
    (bundle / "manifest.json").write_bytes(manifest_bytes)
    (bundle / "manifest.sha256").write_text(
        f"{hashlib.sha256(manifest_bytes).hexdigest()}  manifest.json\n"
    )


@pytest.mark.parametrize("verdict", SYNTHETIC_SLOPES)
def test_public_verdicts_run_through_the_same_cli_and_package(tmp_path, verdict):
    bundle = _run_bundle(tmp_path, verdict)
    source = _source(tmp_path, bundle)
    dest = tmp_path / "published"
    cli = CliRunner()
    command_args = [
        "--publication", str(source), "--bundle", str(bundle), "--dest", str(dest), "--json"
    ]
    checked = cli.invoke(app, ["results", "publish-check", *command_args])
    dry = cli.invoke(app, ["results", "publish", *command_args, "--dry-run"])
    assert checked.exit_code == dry.exit_code == 0, (checked.output, dry.output)
    assert checked.output == dry.output
    assert not dest.exists()
    published = cli.invoke(app, ["results", "publish", *command_args])
    assert published.exit_code == 0, published.output
    metadata = _page(dest / "study")
    assert metadata["headline_verdict"] == verdict
    assert metadata["reproducibility"] == "pass"
    assert metadata["lock_status"] == {"status": "unlocked"}
    assert metadata["findings"][0]["base_verdict"] == verdict
    if verdict == "inconclusive":
        assert metadata["findings"][0]["compatible_with"] == [SYNTHETIC_SLOPES[verdict]]
        assert "declared reference values" in (dest / "study/index.md").read_text()
    contents = read_bundle(bundle)
    provenance = json.loads((dest / "study/provenance.json").read_bytes())
    estimate = next(
        row["estimate"] for row in contents.result_tables["regression_estimates"]
        if row["base_term"] == "x"
    )
    assert provenance["bundle"]["manifest_sha256"] == contents.manifest_sha256
    assert provenance["placeholders"][0]["raw_value"] == estimate
    assert set(json.loads(published.output)["paths"]) == set(_snapshot(dest))


def test_unknown_aggregate_override_does_not_grant_publication(tmp_path, monkeypatch):
    def unknown(source_ids):
        return {
            source: PermissionSource(
                UNKNOWN_TERMS.row_level, UNKNOWN_TERMS.aggregate,
                PermissionTermsOrigin.DEFAULT_UNKNOWN,
            )
            for source in source_ids
        }

    monkeypatch.setattr("hhplab.results.runner.resolve_permission_sources", unknown)
    bundle = _run_bundle(tmp_path, "supports_claim", allow_unknown_aggregates=True)
    source = _source(tmp_path, bundle)
    dest = tmp_path / "published"
    with pytest.raises(publisher.PublicationError, match="strict permission"):
        publisher.publish(source, dest, bundle=bundle, skip_reproduce=True)
    assert not dest.exists()


@pytest.mark.parametrize(
    ("case", "code"),
    [
        ("changed_permissions", "integrity_failed"),
        ("wrong_digest", "bundle_identity_mismatch"),
        ("private", "private_bundle"),
        ("missing_band_verdict", "missing_public_verdict"),
        ("unknown_figure", "invalid_source"),
    ],
)
def test_invalid_publication_preconditions_leave_destination_untouched(tmp_path, case, code):
    configuration = None
    if case == "missing_band_verdict":
        finding = RECIPE_CONFIGURATION["findings"][0]
        bands = [dict(band) for band in finding["bands"]]
        bands[0].pop("public_verdict")
        configuration = {**RECIPE_CONFIGURATION, "findings": [{**finding, "bands": bands}]}
    bundle = _run_bundle(
        tmp_path, "supports_claim", configuration=configuration, private=case == "private"
    )
    source = _source(tmp_path, bundle)
    if case == "changed_permissions":
        path = bundle / "permissions.json"
        permission_record = json.loads(path.read_bytes())
        permission_record["sources"]["hud/pit"]["aggregate"] = "unknown"
        path.write_bytes(canonical_bytes(permission_record))
    elif case in {"wrong_digest", "unknown_figure"}:
        publication = yaml.safe_load(source.read_text())
        if case == "wrong_digest":
            publication["bundle"]["manifest_sha256"] = "a" * 64
        else:
            publication["figures"] = [{"kind": "binned_scatter", "finding": "effect"}]
        source.write_text(yaml.safe_dump(publication))

    dest = tmp_path / "published"
    with pytest.raises(publisher.PublicationError) as caught:
        publisher.publish(source, dest, bundle=bundle, skip_reproduce=True)
    assert caught.value.code == code
    assert not dest.exists()


@pytest.mark.parametrize("terms_origin", [PermissionTermsOrigin.CATALOG, "default_unknown"])
def test_allowed_terms_require_reviewed_evidence(tmp_path, monkeypatch, terms_origin):
    def no_evidence(source_ids):
        return {
            source: PermissionSource("allowed", "allowed", terms_origin)
            for source in source_ids
        }

    monkeypatch.setattr("hhplab.results.runner.resolve_permission_sources", no_evidence)
    bundle = _run_bundle(tmp_path, "supports_claim")
    source, _, _ = publisher._read_source(_source(tmp_path, bundle))
    with pytest.raises(publisher.PublicationError, match="reviewed public permission"):
        publisher.prepare_publication_bundle(source, bundle, skip_reproduce=True)


def _variant_configuration(override):
    return {
        **RECIPE_CONFIGURATION,
        "variants": [{"id": "alternative", "applies_to": ["fit"], "override": override}],
        "findings": [
            {**RECIPE_CONFIGURATION["findings"][0], "robustness": {"variants": ["alternative"]}}
        ],
    }


@pytest.mark.parametrize(
    ("case", "code"),
    [
        ("failed_run", "run_failed"),
        ("required_analysis_failed", "analysis_incomplete"),
        ("ambiguous_variant", "variant_ambiguous"),
        ("missing_description", "variant_description_missing"),
    ],
)
def test_failed_or_ambiguous_release_has_no_public_side_effects(tmp_path, case, code):
    configuration = (
        _variant_configuration({"inference": {"method": "classical"}})
        if case in {"ambiguous_variant", "missing_description"} else None
    )
    bundle = _run_bundle(tmp_path, "supports_claim", configuration=configuration)
    manifest = json.loads((bundle / "manifest.json").read_bytes())
    if case == "failed_run":
        manifest["execution_status"] = "failed"
    elif case == "required_analysis_failed":
        manifest["execution_status"] = "partial"
        manifest["analysis_statuses"]["fit@base"] = {
            "status": "failed",
            "errors": [
                ResultError(
                    code="test_failure", message="private details", scope="analysis"
                ).to_dict()
            ],
        }
    elif case == "ambiguous_variant":
        path = bundle / "findings.json"
        findings = json.loads(path.read_bytes())
        findings["findings"][0]["variants"]["alternative"].update(
            status="ambiguous_rule", label=None, reason="private variant evaluation detail"
        )
        contents = canonical_bytes(findings)
        path.write_bytes(contents)
        record = next(item for item in manifest["files"] if item["path"] == "findings.json")
        record.update(bytes=len(contents), sha256=hashlib.sha256(contents).hexdigest())
    _write_manifest(bundle, manifest)
    descriptions = {} if case == "missing_description" else {"alternative": "Alternate fit."}
    source = _source(tmp_path, bundle, variant_descriptions=descriptions)
    dest = tmp_path / "published"
    with pytest.raises(publisher.PublicationError) as caught:
        publisher.publish(source, dest, bundle=bundle, skip_reproduce=True)
    assert caught.value.code == code
    assert "private" not in str(caught.value)
    assert not dest.exists()


@pytest.mark.parametrize(
    ("override", "alternate_year", "execution", "robustness", "headline", "variant_verdict"),
    [
        pytest.param(
            {"sample.geographies": {"include": ["missing"]}},
            False, "partial", "incomplete", "supports_claim", None,
            id="unavailable",
        ),
        pytest.param(
            {"sample.years": {"from": SOURCE_YEARS[-1], "to": SOURCE_YEARS[-1]}},
            True, "complete", "fragile", "mixed", "no_meaningful_effect",
            id="fragile",
        ),
    ],
)
def test_publication_preserves_variant_meaning(
    tmp_path, override, alternate_year, execution, robustness, headline, variant_verdict
):
    bundle = _run_bundle(
        tmp_path,
        "supports_claim",
        configuration=_variant_configuration(override),
        alternate_year=alternate_year,
        expected_execution=execution,
    )
    source, _, _ = publisher._read_source(_source(
        tmp_path,
        bundle,
        variant_descriptions={"alternative": "Uses the declared alternative sample."},
    ))
    data = publisher.prepare_publication_bundle(source, bundle, skip_reproduce=True)
    finding = data.findings["effect"]
    assert finding.robustness_status == robustness
    assert finding.headline_verdict == headline
    assert finding.variants[0].evaluation.verdict == variant_verdict
    assert finding.variants[0].agrees is (None if variant_verdict is None else False)


def test_legacy_bundle_remains_readable_and_requests_rerun_for_publication(tmp_path):
    recipe = load_recipe(RECIPE_CONFIGURATION)
    original = yaml.safe_dump(RECIPE_CONFIGURATION).encode()
    plan = plan_results(recipe, original_recipe_bytes=original)
    RunLedger(tmp_path).create_lock(
        recipe_id=recipe.id,
        recipe_sha256=hashlib.sha256(original).hexdigest(),
        components=plan.component_hashes,
        spec_hash=plan.plan_sha256,
    )
    bundle = _run_bundle(tmp_path, "supports_claim")
    contents = read_bundle(bundle)
    files = tuple(item for item in contents.manifest.files if item["path"] != "permissions.json")
    legacy = tmp_path / "legacy"
    for record in files:
        path = legacy / record["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((bundle / record["path"]).read_bytes())
    manifest = replace(
        contents.manifest, files=files, bundle_format_version=LEGACY_BUNDLE_FORMAT_VERSION
    )
    _write_manifest(legacy, manifest.to_dict())
    source, _, _ = publisher._read_source(_source(tmp_path, legacy))

    assert read_bundle(legacy).permissions is None
    with pytest.raises(publisher.PublicationError, match="Re-run the recipe"):
        publisher.prepare_publication_bundle(source, legacy, skip_reproduce=True)
    rerun = _run_bundle(tmp_path, "supports_claim")
    source = _source(tmp_path, rerun)
    dest = tmp_path / "published"
    publisher.publish(source, dest, bundle=rerun, skip_reproduce=True)
    legacy_lock = read_bundle(legacy).manifest.lock_status.to_dict()
    assert _page(dest / "study")["lock_status"] == legacy_lock
    assert contents.manifest.lock_status.to_dict() == {"status": "matched"}


@pytest.mark.parametrize(
    ("reproducibility", "allow_failure", "expected"),
    [
        ("pass", False, "pass"),
        ("partial", False, "partial"),
        ("not_run", False, "not_run"),
        ("fail", False, "replay_failure"),
        ("fail", True, "fail"),
    ],
)
def test_public_reproducibility_status_and_reasons_are_safe(
    tmp_path, monkeypatch, reproducibility, allow_failure, expected
):
    bundle = _run_bundle(tmp_path, "supports_claim")
    source, _, _ = publisher._read_source(_source(tmp_path, bundle))
    sensitive = "/private/inputs/source-result.parquet"
    report = VerificationReport(
        "pass",
        reproducibility,
        unavailable=({"path": sensitive},) if reproducibility == "partial" else (),
        errors=(sensitive,),
    )
    monkeypatch.setattr(publisher, "verify_bundle", lambda _: report)
    if expected == "replay_failure":
        with pytest.raises(publisher.PublicationError) as caught:
            publisher.prepare_publication_bundle(source, bundle)
        assert caught.value.code == expected
        assert sensitive not in str(caught.value)
    else:
        parsed = publisher.prepare_publication_bundle(
            source, bundle, allow_reproducibility_failure=allow_failure
        )
        assert parsed.reproducibility == expected
        assert all(sensitive not in reason for reason in parsed.reproducibility_reasons)


def test_strict_numeric_lint_reports_but_does_not_publish(tmp_path):
    bundle = _run_bundle(tmp_path, "supports_claim")
    source = _source(tmp_path, bundle, narrative="The proportion was 0.43.")
    dest = tmp_path / "published"
    preview = publisher.publish(source, dest, bundle=bundle, dry_run=True, skip_reproduce=True)
    assert preview["warnings"]
    with pytest.raises(publisher.PublicationError):
        publisher.publish(source, dest, bundle=bundle, strict=True, skip_reproduce=True)
    assert not dest.exists()


def test_estimate_plot_csv_and_svg_match_public_bundle(tmp_path):
    pytest.importorskip("matplotlib")
    base_finding = RECIPE_CONFIGURATION["findings"][0]
    cited_finding = {
        **base_finding,
        "criteria_source": {
            **base_finding["criteria_source"],
            "stats": {"fit_r2": {"analysis": "fit", "stat": "r2"}},
        },
        "bands": [
            {
                **base_finding["bands"][0],
                "stat_gte": {"stat": "fit_r2", "value": {"param": "fit_floor"}},
            },
            base_finding["bands"][1],
        ],
    }
    configuration = {
        **RECIPE_CONFIGURATION,
        "parameters": {**RECIPE_CONFIGURATION["parameters"], "fit_floor": 0.5},
        "analyses": [{**RECIPE_CONFIGURATION["analyses"][0], "report": ["r2"]}],
        "findings": [{**base_finding, "id": "uncited"}, cited_finding],
    }
    bundle = _run_bundle(tmp_path, "supports_claim", configuration=configuration)
    source = _source(tmp_path, bundle, figures=[{"kind": "estimate_plot", "finding": "effect"}])
    dest = tmp_path / "published"
    publisher.publish(source, dest, bundle=bundle, skip_reproduce=True)
    published = dest / "study"
    plot, = (published / "figures").glob("*.svg")
    provenance = json.loads((published / "provenance.json").read_bytes())
    plot_artifact = next(item for item in provenance["artifacts"] if item["path"].endswith(".svg"))
    csv_path = plot_artifact["csv_path"]
    rows = list(csv.DictReader((published / csv_path).read_text().splitlines()))
    contents = read_bundle(bundle)
    estimate = next(
        row for row in contents.result_tables["regression_estimates"]
        if row["base_term"] == "x"
    )
    assert len(rows) == 1
    assert float(rows[0]["estimate"]) == estimate["estimate"]
    parsed = et.parse(plot).getroot()
    description = "".join(parsed.find("{http://www.w3.org/2000/svg}desc").itertext())
    assert f"{estimate['estimate']:.2f}" in description
    assert "CI" in description and "interval" in description
    assert "fit_r2 at least 0.50" in description
    _assert_numeric_provenance_matches(contents, provenance["result_values"])
    assert any(
        item["address"].startswith("recipe/resolved.json#/recipe/findings/1/bands/")
        for item in provenance["result_values"]
    )


def _assert_numeric_provenance_matches(contents, values):
    for record in values:
        address = record["address"]
        raw = record["raw_value"]
        if address.startswith("recipe/resolved.json#/"):
            location = contents.resolved_spec
            for part in address.split("#/", 1)[1].split("/"):
                location = location[int(part)] if isinstance(location, list) else location[part]
            assert raw == location
        elif ".parquet?" in address:
            path, query = address.split("?", 1)
            keys = parse_qsl(query)
            matching = [
                row for row in contents.result_tables[Path(path).stem]
                if all(row[key] == wanted for key, wanted in keys)
            ]
            row, = matching
            numeric = {key: value for key, value in raw.items() if isinstance(value, (int, float))}
            assert numeric == {key: row[key] for key in numeric}


def test_public_assets_exclude_restricted_values_and_annotation_details(tmp_path, monkeypatch):
    pytest.importorskip("matplotlib")
    private_detail = "researcher-only-annotation-detail"
    private_value = 987654321
    public_note = "One source caveat applies to the public estimate."
    unknown_source = "sensitive-data/unreviewed"
    unmatched_aggregate = "results/uncited-aggregate.json"

    def include_unreviewed_aggregate(path, payload):
        artifact = BundleArtifact(
            unmatched_aggregate,
            canonical_bytes({"value": private_value}),
            "aggregate",
            ArtifactLineage(unmatched_aggregate, (unknown_source,), ArtifactScope.AGGREGATE),
        )
        return write_bundle(path, replace(payload, artifacts=(*payload.artifacts, artifact)))

    monkeypatch.setattr("hhplab.results.runner.write_bundle", include_unreviewed_aggregate)
    configuration = {
        **RECIPE_CONFIGURATION,
        "datasets": {
            **RECIPE_CONFIGURATION["datasets"],
            "other_source": {
                "provider": "sensitive-data",
                "product": "unreviewed",
                "version": 1,
                "native_geometry": {"type": "coc", "vintage": 2024},
            },
        },
        "pipelines": [
            *RECIPE_CONFIGURATION["pipelines"],
            {"id": "other_build", "target": "target",
             "steps": [{"kind": "join", "datasets": ["other_source"]}]},
        ],
        "panels": {
            **RECIPE_CONFIGURATION["panels"],
            "other": {
                "ref": {"pipeline": "other_build", "output": "panel"},
                "geo_key": "geo_id",
                "time_key": "year",
            },
        },
        "annotations": [
            {
                "id": "source-caveat",
                "summary": private_detail,
                "text": f"{private_detail} {private_value}",
                "public_text": public_note,
                "scope": {"panels": ["panel"], "columns": ["y"]},
            }
        ],
        "results": {
            **RECIPE_CONFIGURATION["results"],
            "include_panels": ["panel", "other"],
        },
    }
    bundle = _run_bundle(
        tmp_path, "supports_claim", configuration=configuration, allow_unknown_aggregates=True,
        extra_panels={"other": pd.DataFrame(
            {"geo_id": ["s-00"], "year": [SOURCE_YEARS[0]], "value": [private_value]}
        )},
    )
    contents = read_bundle(bundle)
    assert contents.annotations["distribution_status"] == "withheld"
    assert not (bundle / "panels/other.parquet").exists()
    assert (bundle / unmatched_aggregate).is_file()
    recorded, = contents.permissions.parents_for_path(unmatched_aggregate)
    assert recorded.written_disposition.value == "included"
    assert recorded.strict_disposition.value == "withheld"
    with pytest.raises(publisher.PublicationError, match="strict permission"):
        publisher._permission_decision(contents, unmatched_aggregate)
    source = _source(tmp_path, bundle, figures=[{"kind": "estimate_plot", "finding": "effect"}])
    dest = tmp_path / "published"
    publisher.publish(source, dest, bundle=bundle, skip_reproduce=True)
    assets = _snapshot(dest)
    assert {Path(name).suffix for name in assets} == {".csv", ".json", ".md", ".svg"}
    assert public_note.rstrip(".").encode() in assets["study/index.md"]
    for contents_bytes in assets.values():
        assert private_detail.encode() not in contents_bytes
        assert str(private_value).encode() not in contents_bytes
        assert b"sensitive-data/unreviewed" not in contents_bytes


def test_existing_release_rejects_missing_change_note_and_publish_failure(tmp_path, monkeypatch):
    first = _run_bundle(tmp_path, "supports_claim")
    second = _run_bundle(tmp_path, "no_meaningful_effect")
    dest = tmp_path / "published"
    source = _source(tmp_path, first)
    publisher.publish(source, dest, bundle=first, skip_reproduce=True)
    original = _snapshot(dest)
    source = _source(tmp_path, second)

    with pytest.raises(publisher.PublicationError, match="change note"):
        publisher.publish(source, dest, bundle=second, skip_reproduce=True)
    assert _snapshot(dest) == original

    source = _source(tmp_path, second, change_note="Release for the second synthetic run.")
    cli = CliRunner()
    command_args = [
        "--publication", str(source), "--bundle", str(second), "--dest", str(dest),
        "--skip-reproduce", "--json",
    ]
    checked = cli.invoke(app, ["results", "publish-check", *command_args])
    dry = cli.invoke(app, ["results", "publish", *command_args, "--dry-run"])
    assert checked.exit_code == dry.exit_code == 0, (checked.output, dry.output)
    assert checked.output == dry.output
    assert _snapshot(dest) == original

    with pytest.raises(publisher.PublicationError, match="staged release"):
        publisher._atomic_move(tmp_path / "absent-stage", dest / "study", replace_release=True)
    assert _snapshot(dest) == original
    atomic_move = publisher._atomic_move

    def fail_before_commit(*args, **kwargs):
        raise publisher.PublicationError("injected_failure", "Activation interrupted.")

    monkeypatch.setattr(publisher, "_atomic_move", fail_before_commit)
    with pytest.raises(publisher.PublicationError, match="interrupted"):
        publisher.publish(source, dest, bundle=second, skip_reproduce=True)
    assert _snapshot(dest) == original
    monkeypatch.setattr(publisher, "_atomic_move", atomic_move)
    publisher.publish(source, dest, bundle=second, skip_reproduce=True)
    history = _page(dest / "study")["history"]
    assert [entry["headline_verdict"] for entry in history] == [
        "supports_claim", "no_meaningful_effect"
    ]


def test_retraction_and_index_use_committed_state_without_the_old_bundle(tmp_path):
    bundle = _run_bundle(tmp_path, "supports_claim")
    dest = tmp_path / "published"
    source = _source(tmp_path, bundle)
    publisher.publish(source, dest, bundle=bundle, skip_reproduce=True)
    source = _source(
        tmp_path,
        bundle,
        status="retracted",
        bundle=None,
        findings=[],
        primary_finding=None,
        retraction_note="Permission for this release was withdrawn.",
        change_note="Withdrawn release.",
    )
    bundle.rename(tmp_path / "archived-bundle")
    publisher.publish(source, dest)
    artifacts = _snapshot(dest / "study")
    assert set(artifacts) == {"index.md", "provenance.json"}
    assert "example.org" not in artifacts["index.md"].decode()
    assert bundle.name not in artifacts["provenance.json"].decode()
    history = _page(dest / "study")["history"]
    assert history[0].keys() == {"published_at", "status"}
    assert history[-1]["status"] == "retracted"
    result = publisher.publish_index(dest)
    assert result["findings"][0]["headline_verdict"] is None
    assert result["findings"][0]["status"] == "retracted"
    index = (dest / "findings-index.json").read_bytes()
    publisher.publish_index(dest)
    assert (dest / "findings-index.json").read_bytes() == index
    with pytest.raises(publisher.PublicationError, match="retracted"):
        publisher.publish(source, dest)


def test_concurrent_updates_have_one_ordered_history(tmp_path):
    bundle = _run_bundle(tmp_path, "supports_claim")
    source = _source(tmp_path, bundle)
    dest = tmp_path / "published"

    def commit():
        return publisher.publish(source, dest, bundle=bundle, skip_reproduce=True)

    with ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(commit)
        second = workers.submit(commit)
        assert first.result()["status"] == second.result()["status"] == "ok"
    history = _page(dest / "study")["history"]
    assert len(history) == 2
    assert history[0]["published_at"] <= history[1]["published_at"]


def test_supersession_rejects_cycles_and_path_escapes(tmp_path):
    bundle = _run_bundle(tmp_path, "supports_claim")
    dest = tmp_path / "published"
    first = _source(tmp_path, bundle)
    second = _source(tmp_path, bundle, slug="replacement")
    publisher.publish(first, dest, bundle=bundle, skip_reproduce=True)
    publisher.publish(second, dest, bundle=bundle, skip_reproduce=True)
    forward = _source(
        tmp_path, bundle, status="superseded", superseded_by="replacement", change_note="Updated."
    )
    publisher.publish(forward, dest, bundle=bundle, skip_reproduce=True)
    original = _snapshot(dest)
    backward = _source(
        tmp_path, bundle, slug="replacement", status="superseded",
        superseded_by="study", change_note="Updated.",
    )
    with pytest.raises(publisher.PublicationError, match="cycle"):
        publisher.publish(backward, dest, bundle=bundle, dry_run=True, skip_reproduce=True)
    with pytest.raises(publisher.PublicationError, match="contained"):
        publisher.publish(first, dest / ".." / "outside", bundle=bundle, dry_run=True)
    assert _snapshot(dest) == original

    retirement = _source(
        tmp_path, bundle, slug="replacement", status="retracted", bundle=None, findings=[],
        primary_finding=None, retraction_note="Replacement withdrawn.", change_note="Withdrawn.",
    )
    publisher.publish(retirement, dest)
    entries = {entry["slug"]: entry for entry in publisher.publish_index(dest)["findings"]}
    assert entries["study"]["superseded_by"] == "replacement"
    assert entries["study"]["replacement_status"] == "retracted"
    assert entries["replacement"]["supersedes"] == ["study"]
