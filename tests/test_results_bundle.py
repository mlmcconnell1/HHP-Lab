"""Portable results bundle integrity, replay paths, and redistribution tests."""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import replace

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import hhplab.results.bundle as bundle_module
from hhplab.results.bundle import (
    BundleArtifact,
    BundleError,
    BundlePayload,
    BundleWriter,
    read_bundle,
)
from hhplab.results.lineage import ArtifactLineage
from hhplab.schema.redistribution import ArtifactScope, SourceRedistribution
from hhplab.schema.results import (
    AnalysisStatus,
    AnalysisStatusRecord,
    DiagnosticsStatus,
    DistributionStatus,
    ExecutionStatus,
    FindingsStatus,
    InputIdentity,
    LockStatus,
    ResultError,
    StatusFields,
    Tolerance,
)
from hhplab.storage.provenance import PROVENANCE_KEY, ProvenanceBlock

UTC_TIMESTAMP = "2026-10-20T12:00:00Z"
HASH_A = "a" * 64
HASH_B = "b" * 64
PUBLIC = SourceRedistribution("allowed", "allowed")
ROW_RESTRICTED = SourceRedistribution("restricted", "allowed")
AGGREGATE_RESTRICTED = SourceRedistribution("allowed", "restricted")


def test_nullable_integer_result_columns_keep_integer_parquet_type():
    clustered = dict(RESULT_ROWS["regression_fit"][0])
    unclustered = {**clustered, "variant": "hc1", "n_clusters": None}
    contents = bundle_module._result_table_parquet("regression_fit", [clustered, unclustered])
    table = pq.read_table(pa.BufferReader(contents))
    assert table.schema.field("n_clusters").type == pa.int64()
    assert table.to_pylist() == [clustered, unclustered]


def test_empty_result_table_retains_canonical_types():
    contents = bundle_module._result_table_parquet("regression_fit", [])
    table = pq.read_table(pa.BufferReader(contents))
    assert table.schema.field("n_clusters").type == pa.int64()
    assert table.schema.field("absorbed").type == pa.list_(pa.string())
    assert table.to_pylist() == []


RESULT_ROWS = {
    "regression_estimates": [
        {
            "analysis": "fit",
            "variant": "base",
            "term": "x",
            "base_term": "x",
            "role": "predictor",
            "estimate": 12345.0,
            "se": 0.1,
            "df": 9.0,
            "ci_lo": 12344.8,
            "ci_hi": 12345.2,
            "ci_level": 0.95,
            "ci_method": "wald",
            "p": 0.01,
            "p_status": "evaluated",
            "p_reason": None,
            "inference_method": "classical",
        }
    ],
    "regression_fit": [
        {
            "analysis": "fit",
            "variant": "base",
            "sample_source": "main@base",
            "n": 12,
            "n_clusters": 4,
            "design_rank": 3,
            "k_dof": 3,
            "df_resid": 9,
            "r2": 0.4,
            "r2_status": "evaluated",
            "r2_reason": None,
            "r2_within": 0.2,
            "r2_within_status": "evaluated",
            "r2_within_reason": None,
            "absorbed": ["year"],
        }
    ],
    "descriptive_stats": [
        {
            "analysis": "summary",
            "variant": "base",
            "column": "value",
            "group": "{}",
            "stat": "mean",
            "value": 4.5,
            "status": "evaluated",
            "reason": None,
        }
    ],
    "correlations": [
        {
            "analysis": "corr",
            "variant": "base",
            "col_a": "x",
            "col_b": "y",
            "kind": "pearson",
            "r": 0.2,
            "n": 12,
            "status": "evaluated",
            "reason": None,
        }
    ],
}


def _parquet_bytes(frame: pd.DataFrame) -> bytes:
    table = pa.Table.from_pandas(frame, preserve_index=False)
    metadata = dict(table.schema.metadata or {})
    metadata[PROVENANCE_KEY] = (
        ProvenanceBlock(extra={"dataset_type": "test_input", "row_count": len(frame)})
        .to_json()
        .encode("utf-8")
    )
    output = pa.BufferOutputStream()
    pq.write_table(table.replace_schema_metadata(metadata), output)
    return output.getvalue().to_pybytes()


def _payload(
    *,
    source_terms=None,
    private=False,
    allow_unknown_aggregates=False,
) -> BundlePayload:
    source_terms = source_terms or {"panel-source": PUBLIC, "aggregate-source": PUBLIC}
    artifacts = (
        BundleArtifact(
            "panels/cbsa.parquet",
            _parquet_bytes(pd.DataFrame({"geo_id": ["x"], "year": [2020], "value": [7.0]})),
            "panel",
            ArtifactLineage("panels/cbsa.parquet", ("panel-source",), ArtifactScope.ROW_LEVEL),
        ),
        BundleArtifact(
            "derived/cbsa.parquet",
            _parquet_bytes(pd.DataFrame({"geo_id": ["x"], "year": [2020], "change": [1.0]})),
            "derived",
            ArtifactLineage("derived/cbsa.parquet", ("panel-source",), ArtifactScope.ROW_LEVEL),
        ),
        BundleArtifact(
            "samples/main@base.parquet",
            _parquet_bytes(
                pd.DataFrame(
                    {"geo_id": ["x"], "year": [2020], "included": [True], "reason": [None]}
                )
            ),
            "sample",
            ArtifactLineage("samples/main@base.parquet", (), ArtifactScope.SAMPLE),
        ),
    )
    tables = copy.deepcopy(RESULT_ROWS)
    return BundlePayload(
        recipe_id="portable-demo",
        run_id="run-abc",
        created_at=UTC_TIMESTAMP,
        original_recipe=b"version: 2\nid: portable-demo\n# submitted bytes\n",
        recipe={"version": 2, "id": "portable-demo", "panels": {}},
        plan={"analysis_keys": ["fit@base"]},
        statuses=StatusFields(
            ExecutionStatus.COMPLETE,
            DiagnosticsStatus.PASS,
            FindingsStatus.ALL_EVALUATED,
            DistributionStatus.FULL,
        ),
        analysis_statuses={
            "fit@base": AnalysisStatusRecord(AnalysisStatus.COMPLETE),
        },
        spec_hash=HASH_A,
        component_hashes={"analysis": HASH_B},
        lock_status=LockStatus.from_value("unlocked"),
        input_identities=(InputIdentity("cbsa", HASH_A, {"pit": "2024"}, HASH_B),),
        artifacts=artifacts,
        result_tables=tables,
        result_table_lineage={
            name: ArtifactLineage(
                bundle_module.RESULT_TABLE_PATHS[name],
                ("aggregate-source",),
                ArtifactScope.AGGREGATE,
            )
            for name in bundle_module.RESULT_TABLE_NAMES
        },
        source_terms=source_terms,
        report_payload={
            "title": "Portable synthetic results",
            "analyses": {
                "fit@base": {
                    "status": "complete",
                    "regression_estimates": tables["regression_estimates"],
                    "regression_fit": tables["regression_fit"][0],
                }
            },
            "diagnostics": {"records": [{"value": 54321.0}]},
            "findings": {"records": []},
            "exclusions": [],
            "errors": [],
            "limitations": [],
            "withheld": [],
        },
        report_tables=tables,
        report_lineage=ArtifactLineage(
            "report/index.html", ("aggregate-source",), ArtifactScope.AGGREGATE
        ),
        input_measures={"cbsa": {"population": {"unit": "persons"}}},
        measures={"version": 1, "measures": []},
        tolerance=Tolerance(1e-10, 1e-9),
        private=private,
        allow_unknown_aggregates=allow_unknown_aggregates,
    )


def test_bundle_roundtrip_hashes_schemas_and_relative_replay_paths(tmp_path):
    source = _payload()
    result = BundleWriter(tmp_path / "bundle").write(source)

    assert result.manifest.files
    assert "manifest.json" not in {item["path"] for item in result.manifest.files}
    assert "manifest.sha256" not in {item["path"] for item in result.manifest.files}
    for item in result.manifest.files:
        assert not item["path"].startswith("/")
        contents = (tmp_path / "bundle" / item["path"]).read_bytes()
        assert hashlib.sha256(contents).hexdigest() == item["sha256"]
    assert (tmp_path / "bundle" / "recipe" / "original.yaml").read_bytes() == source.original_recipe

    moved = tmp_path / "moved-bundle"
    (tmp_path / "bundle").rename(moved)
    loaded = read_bundle(moved)
    assert loaded.execution["panels"] == {"cbsa": "panels/cbsa.parquet"}
    assert loaded.execution["derived"] == {"cbsa": "derived/cbsa.parquet"}
    assert loaded.execution["samples"] == {"main@base": "samples/main@base.parquet"}
    assert loaded.execution["input_measures"]["cbsa"]["population"]["unit"] == "persons"
    assert loaded.execution["tolerance"] == {"atol": 1e-10, "rtol": 1e-9}
    assert loaded.result_tables == source.result_tables
    resolved, execution, panel_frames = loaded.load_replay_inputs()
    assert resolved["recipe"]["id"] == "portable-demo"
    assert execution["spec_hash"] == HASH_A
    assert panel_frames["cbsa"].to_dict(orient="records") == [
        {"geo_id": "x", "year": 2020, "value": 7.0}
    ]


def test_restricted_row_level_files_are_withheld_without_hiding_allowed_aggregates(tmp_path):
    result = BundleWriter(tmp_path / "bundle").write(
        _payload(source_terms={"panel-source": ROW_RESTRICTED, "aggregate-source": PUBLIC})
    )

    paths = {item["path"] for item in result.manifest.files}
    assert "panels/cbsa.parquet" not in paths
    assert "derived/cbsa.parquet" not in paths
    assert "samples/main@base.parquet" in paths
    assert "results/regression_estimates.parquet" in paths
    assert result.manifest.statuses.distribution_status is DistributionStatus.WITHHELD
    loaded = read_bundle(tmp_path / "bundle")
    assert loaded.execution["panels"] == {}
    assert "regression_estimates" in loaded.result_tables
    assert loaded.documents["withheld.json"]["withheld"]
    assert "12345" in (tmp_path / "bundle" / "report" / "index.html").read_text()


def test_denied_aggregate_data_never_reaches_json_or_html(tmp_path):
    payload = _payload(
        source_terms={"panel-source": ROW_RESTRICTED, "aggregate-source": AGGREGATE_RESTRICTED}
    )
    result = BundleWriter(tmp_path / "bundle").write(payload)
    paths = {item["path"] for item in result.manifest.files}

    assert "results/regression_estimates.parquet" not in paths
    assert "results/results.json" in paths
    assert "diagnostics.json" not in paths
    assert "findings.json" not in paths
    html = (tmp_path / "bundle" / "report" / "index.html").read_text()
    assert "12345" not in html
    assert "54321" not in html
    assert b"12345" not in (tmp_path / "bundle" / "results" / "results.json").read_bytes()
    assert "Withheld under source redistribution terms" in html
    assert result.manifest.statuses.distribution_status is DistributionStatus.WITHHELD
    assert result.withheld_paths


def test_unknown_aggregate_requires_override_and_private_includes_restricted_rows(tmp_path):
    unknown = SourceRedistribution("unknown", "unknown")
    withheld = BundleWriter(tmp_path / "unknown").write(
        _payload(source_terms={"aggregate-source": unknown})
    )
    assert "results/regression_estimates.parquet" not in {
        item["path"] for item in withheld.manifest.files
    }

    allowed = BundleWriter(tmp_path / "override").write(
        _payload(source_terms={"aggregate-source": unknown}, allow_unknown_aggregates=True)
    )
    assert "results/regression_estimates.parquet" in {
        item["path"] for item in allowed.manifest.files
    }

    private = BundleWriter(tmp_path / "private").write(
        _payload(
            source_terms={"panel-source": ROW_RESTRICTED, "aggregate-source": AGGREGATE_RESTRICTED},
            private=True,
        )
    )
    assert private.manifest.private is True
    assert private.manifest.statuses.distribution_status is DistributionStatus.PRIVATE
    assert "panels/cbsa.parquet" in {item["path"] for item in private.manifest.files}


def test_bundle_writer_refuses_to_overwrite_and_marks_interrupted_packaging(tmp_path, monkeypatch):
    existing = tmp_path / "existing"
    existing.mkdir()
    with pytest.raises(BundleError, match="refusing to overwrite"):
        BundleWriter(existing).write(_payload())

    interrupted = tmp_path / "interrupted"

    def fail_serialization(*_args, **_kwargs):
        raise OSError("synthetic parquet interruption")

    monkeypatch.setattr(bundle_module, "_result_table_parquet", fail_serialization)
    with pytest.raises(BundleError, match="packaging interrupted"):
        BundleWriter(interrupted).write(_payload())
    marker = json.loads((interrupted / ".packaging-incomplete.json").read_bytes())
    assert marker["status"] == "interrupted"
    assert marker["written_paths"]


def test_failed_and_partial_bundles_preserve_explicit_status(tmp_path):
    base = _payload()
    empty_tables = {name: [] for name in bundle_module.RESULT_TABLE_NAMES}
    failed = replace(
        base,
        statuses=StatusFields(
            ExecutionStatus.FAILED,
            DiagnosticsStatus.INCOMPLETE,
            FindingsStatus.SOME_NOT_EVALUATED,
            DistributionStatus.FULL,
        ),
        analysis_statuses={
            "fit@base": AnalysisStatusRecord(
                AnalysisStatus.FAILED,
                (ResultError("estimation_failed", "synthetic failure", "analysis"),),
            )
        },
        artifacts=(),
        result_tables=empty_tables,
        report_tables=empty_tables,
        report_payload={
            "analyses": {"fit@base": {"status": "failed", "error": "synthetic failure"}},
            "diagnostics": {},
            "findings": {},
        },
    )
    failed_result = BundleWriter(tmp_path / "failed").write(failed)
    assert failed_result.manifest.statuses.execution_status is ExecutionStatus.FAILED
    assert failed_result.manifest.analysis_statuses["fit@base"].status is AnalysisStatus.FAILED

    partial = replace(
        base,
        statuses=StatusFields(
            ExecutionStatus.PARTIAL,
            DiagnosticsStatus.INCOMPLETE,
            FindingsStatus.SOME_NOT_EVALUATED,
            DistributionStatus.FULL,
        ),
        analysis_statuses={
            "fit@base": AnalysisStatusRecord(AnalysisStatus.SKIPPED),
        },
        result_tables=empty_tables,
        report_tables=empty_tables,
        report_payload={
            "analyses": {"fit@base": {"status": "skipped"}},
            "diagnostics": {},
            "findings": {},
        },
    )
    partial_result = BundleWriter(tmp_path / "partial").write(partial)
    assert partial_result.manifest.statuses.execution_status is ExecutionStatus.PARTIAL
    assert "panels/cbsa.parquet" in {item["path"] for item in partial_result.manifest.files}


def test_corrupt_inventory_and_manifest_sidecar_are_rejected(tmp_path):
    BundleWriter(tmp_path / "bundle").write(_payload())
    target = tmp_path / "bundle" / "results" / "regression_fit.parquet"
    target.write_bytes(target.read_bytes() + b"tamper")
    with pytest.raises(BundleError, match="byte count mismatch|hash mismatch"):
        read_bundle(tmp_path / "bundle")

    second = tmp_path / "second"
    BundleWriter(second).write(_payload())
    (second / "manifest.sha256").write_text("0" * 64 + "  manifest.json\n")
    with pytest.raises(BundleError, match="manifest.sha256"):
        read_bundle(second)


def test_missing_result_lineage_withholds_report_and_every_companion(tmp_path):
    payload = _payload()
    lineage = dict(payload.result_table_lineage)
    del lineage["regression_fit"]
    payload = replace(payload, result_table_lineage=lineage)

    result = BundleWriter(tmp_path / "bundle").write(payload)
    included = {record["path"] for record in result.manifest.files}
    assert "report/index.html" in included  # status-only standalone report
    assert "tables/coefficients.csv" not in included
    assert "tables/fit.csv" not in included
    assert "results/regression_fit.parquet" not in included
    html = (tmp_path / "bundle" / "report" / "index.html").read_text()
    assert "12345" not in html
    assert "54321" not in html
    assert "Withheld under source redistribution terms" in html
    withheld = json.loads((tmp_path / "bundle" / "withheld.json").read_bytes())
    assert "tables/fit.csv" in {record["path"] for record in withheld["withheld"]}


def test_nonportable_metadata_fails_before_creating_bundle(tmp_path):
    payload = replace(_payload(), plan={"invalid": float("nan")})
    target = tmp_path / "bundle"
    with pytest.raises(BundleError, match="not portable JSON"):
        BundleWriter(target).write(payload)
    assert not target.exists()


def test_read_rejects_symlink_even_when_inventory_verification_is_disabled(tmp_path):
    BundleWriter(tmp_path / "bundle").write(_payload())
    external = tmp_path / "outside.json"
    external.write_text("{}")
    link = tmp_path / "bundle" / "unexpected.json"
    link.symlink_to(external)
    with pytest.raises(BundleError, match="cannot contain symlinks"):
        read_bundle(tmp_path / "bundle", verify=False)
