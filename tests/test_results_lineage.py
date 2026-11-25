"""Redistribution truth table: restricted always withheld; unknown aggregate
requires override; unknown rows stay withheld; private includes everything.
Samples contain keys/reasons only and remain allowed.
"""

from datetime import UTC, datetime
from random import Random

import pytest

from hhplab.covariates.catalog import BLS_DATA_EVIDENCE, COVARIATE_SOURCE_SPECS
from hhplab.registry.source_registry import SourceRegistryEntry, register_source
from hhplab.results.lineage import (
    ArtifactLineage,
    declared_source_terms,
    derive_lineage,
    descriptive_scope,
    distribution_plan,
    permission_record,
    resolve_permission_sources,
    union_lineage,
)
from hhplab.schema.redistribution import (
    PUBLIC_DATA_TERMS,
    SMALL_GROUP_THRESHOLD,
    UNKNOWN_TERMS,
    ArtifactScope,
    PermissionArtifact,
    PermissionRecord,
    PermissionSource,
    PermissionTermsOrigin,
    RedistributionEvidence,
    RedistributionStatus,
    SourceRedistribution,
    artifact_disposition,
    combine_source_terms,
)
from hhplab.schema.results import ResultContractError
from hhplab.sources.coverage import CORE_SOURCE_COVERAGE_SPECS

RESTRICTED = SourceRedistribution("restricted", "restricted")
PERMISSION_PROPERTY_SEEDS = range(64)


@pytest.mark.parametrize(
    "terms,scope,override,private,expected",
    [
        (PUBLIC_DATA_TERMS, "row_level", False, False, True),
        (RESTRICTED, "aggregate", True, False, False),
        (UNKNOWN_TERMS, "aggregate", False, False, False),
        (UNKNOWN_TERMS, "aggregate", True, False, True),
        (UNKNOWN_TERMS, "row_level", True, False, False),
        (RESTRICTED, "row_level", False, True, True),
        (UNKNOWN_TERMS, "sample", False, False, True),
    ],
)
def test_distribution_truth_table(terms, scope, override, private, expected):
    plan = distribution_plan(
        [
            ArtifactLineage(
                "table.csv", ("source",), ArtifactScope(scope), ("panel",), ("table.json",)
            )
        ],
        {"source": terms},
        private=private,
        allow_unknown_aggregates=override,
    )
    assert bool(plan["included"]) is expected
    assert len(plan["withheld"]) == (0 if expected else 2)
    if not expected:
        assert plan["withheld"][0]["sources"] == ["source"]
        assert plan["withheld"][0]["replay_dependencies"] == ["panel"]


def test_derived_and_sample_requirements_propagate_restricted_source():
    lineage = derive_lineage(
        {"outcome": ("public",), "control": ("restricted",)},
        {"second": ("first", "outcome"), "first": ("control",)},
    )
    sources = union_lineage(("outcome", "second"), lineage)
    assert sources == ("public", "restricted")
    plan = distribution_plan(
        [
            ArtifactLineage(path, sources, ArtifactScope.AGGREGATE)
            for path in (
                "fit.json",
                "table.csv",
                "finding.json",
                "report.html",
            )
        ],
        {"public": PUBLIC_DATA_TERMS, "restricted": RESTRICTED},
    )
    assert plan["included"] == []
    assert len(plan["withheld"]) == 4


def test_shared_companion_cannot_override_withholding():
    artifacts = [
        ArtifactLineage(
            "restricted.csv", ("r",), ArtifactScope.ROW_LEVEL, companions=("report.html",)
        ),
        ArtifactLineage("public.csv", ("p",), ArtifactScope.AGGREGATE, companions=("report.html",)),
    ]
    plan = distribution_plan(artifacts, {"r": RESTRICTED, "p": PUBLIC_DATA_TERMS})
    assert plan["included"] == ["public.csv"]
    assert plan["decisions"]["report.html"] == "withheld"


@pytest.mark.parametrize(
    "statistic", ["min", "max", "median", "p10", "p25", "p75", "p90", "quantile", "percentile_90"]
)
@pytest.mark.parametrize("rows,expected", [(9, "row_level"), (10, "aggregate")])
def test_small_group_disclosures(statistic, rows, expected):
    assert descriptive_scope(statistic, rows) == expected


@pytest.mark.parametrize("definitions", [{"a": ("b",), "b": ("a",)}, {"input": ("x",)}])
def test_invalid_derivation_graph(definitions):
    with pytest.raises(ResultContractError):
        derive_lineage({"input": ("public",)}, definitions)


def test_missing_lineage_stays_unknown():
    sources = union_lineage(("undeclared",), {})
    assert distribution_plan([ArtifactLineage("fit.json", sources, ArtifactScope.AGGREGATE)], {})[
        "withheld"
    ]


def test_registry_terms_roundtrip_and_unknown_default(tmp_path):
    entry = register_source(
        "other",
        "https://www.bls.gov/data",
        "abc",
        registry_path=tmp_path / "registry.parquet",
        redistribution=PUBLIC_DATA_TERMS,
        redistribution_evidence=(BLS_DATA_EVIDENCE,),
    )
    restored = SourceRegistryEntry.from_dict(entry.to_dict())
    assert restored.redistribution == PUBLIC_DATA_TERMS
    assert restored.redistribution_evidence[0]["url"] == BLS_DATA_EVIDENCE.url
    legacy = SourceRegistryEntry("zori", "url", "hash", datetime.now(UTC))
    assert legacy.redistribution == UNKNOWN_TERMS


def test_reviewed_products_and_unknown_defaults():
    assert CORE_SOURCE_COVERAGE_SPECS["acs5"].redistribution == PUBLIC_DATA_TERMS
    assert CORE_SOURCE_COVERAGE_SPECS["zori"].redistribution == UNKNOWN_TERMS
    assert CORE_SOURCE_COVERAGE_SPECS["pit"].redistribution == PUBLIC_DATA_TERMS
    assert COVARIATE_SOURCE_SPECS["census_bps"].redistribution == PUBLIC_DATA_TERMS
    assert CORE_SOURCE_COVERAGE_SPECS["acs5"].to_dict()["redistribution_evidence"]


def test_exact_product_declarations_and_explicit_ingestion_restrictions():
    restricted = SourceRegistryEntry(
        "other",
        "url",
        "hash",
        datetime.now(UTC),
        metadata={"redistribution": RESTRICTED.to_dict()},
    )
    terms = declared_source_terms(("acs5", "census_unknown", "zori"), {"acs5": restricted})
    assert terms == {"acs5": RESTRICTED, "census_unknown": UNKNOWN_TERMS, "zori": UNKNOWN_TERMS}
    assert declared_source_terms(("census/acs5", "bls/laus", "census/unreviewed")) == {
        "census/acs5": PUBLIC_DATA_TERMS,
        "bls/laus": PUBLIC_DATA_TERMS,
        "census/unreviewed": UNKNOWN_TERMS,
    }


def test_permission_source_resolver_shares_terms_branch_selection():
    registry = SourceRegistryEntry(
        "other",
        "url",
        "hash",
        datetime.now(UTC),
        metadata={"redistribution": RESTRICTED.to_dict()},
    )
    source_ids = ("acs5", "other", "census/acs5", "census/unreviewed")

    terms = declared_source_terms(source_ids, {"other": registry})
    permissions = resolve_permission_sources(source_ids, {"other": registry})

    assert {source_id: item.terms for source_id, item in permissions.items()} == terms
    assert permissions["other"].terms_origin is PermissionTermsOrigin.REGISTRY
    assert permissions["acs5"].terms_origin is PermissionTermsOrigin.CATALOG
    assert permissions["census/unreviewed"].terms_origin is PermissionTermsOrigin.DEFAULT_UNKNOWN
    malformed_registry = SourceRegistryEntry(
        "other",
        "url",
        "hash",
        datetime.now(UTC),
        metadata={"redistribution": RESTRICTED.to_dict(), "redistribution_evidence": ["bad"]},
    )
    # The terms-only compatibility API keeps working when evidence metadata is malformed.
    assert declared_source_terms(("other",), {"other": malformed_registry}) == {
        "other": RESTRICTED
    }
    with pytest.raises(ResultContractError, match="evidence"):
        resolve_permission_sources(("other",), {"other": malformed_registry})


def test_permission_record_roundtrip_captures_sources_and_companion_parents():
    sources = resolve_permission_sources(("acs5", "hud/pit"))
    artifacts = (
        ArtifactLineage(
            "results/estimates.json",
            ("acs5",),
            ArtifactScope.AGGREGATE,
            companions=("tables/coefficients.csv",),
        ),
        ArtifactLineage(
            "tables/coefficients.csv",
            ("hud/pit",),
            ArtifactScope.AGGREGATE,
        ),
    )
    record = permission_record(artifacts, sources)

    restored = PermissionRecord.from_dict(record.to_dict())
    assert restored == record
    assert restored.sources["acs5"].terms_origin is PermissionTermsOrigin.CATALOG
    assert restored.sources["acs5"].evidence
    assert [item.path for item in restored.parents_for_path("tables/coefficients.csv")] == [
        "results/estimates.json",
        "tables/coefficients.csv",
    ]


def test_permission_source_resolution_preserves_direct_terms_without_evidence():
    direct = SourceRedistribution("allowed", "allowed")
    resolved = resolve_permission_sources(
        ("fixture-source",), direct_terms={"fixture-source": direct}
    )

    assert resolved["fixture-source"].terms == direct
    assert resolved["fixture-source"].terms_origin is PermissionTermsOrigin.DEFAULT_UNKNOWN
    assert resolved["fixture-source"].evidence == ()
    record = permission_record(
        [ArtifactLineage("results/value.json", ("fixture-source",), ArtifactScope.AGGREGATE)],
        resolved,
    )
    artifact = record.artifacts[0]
    assert artifact.strict_disposition.value == "included"
    assert record.sources["fixture-source"].evidence == ()


@pytest.mark.parametrize(
    "url,reviewed_on",
    [
        pytest.param("local/policy.pdf", "2026-11-20", id="relative-path"),
        pytest.param("file:///policy.pdf", "2026-11-20", id="file-url"),
        pytest.param("https://user:pass@example.org/policy", "2026-11-20", id="userinfo"),
        pytest.param("https:///policy", "2026-11-20", id="missing-host"),
        pytest.param("https://example.org/policy", "11/20/2026", id="non-iso-date"),
        pytest.param("https://example.org/policy", "2026-2-03", id="noncanonical-date"),
    ],
)
def test_permission_evidence_requires_portable_url_and_iso_date(url, reviewed_on):
    legacy_evidence = RedistributionEvidence(url, reviewed_on, "legacy evidence metadata")
    with pytest.raises(ResultContractError):
        PermissionSource(
            RedistributionStatus.ALLOWED,
            RedistributionStatus.ALLOWED,
            PermissionTermsOrigin.CATALOG,
            (legacy_evidence,),
        )


def test_permission_record_freezes_small_group_threshold():
    record = permission_record(
        [ArtifactLineage("results/value.json", ("hud/pit",), ArtifactScope.AGGREGATE)],
        resolve_permission_sources(("hud/pit",)),
    )
    assert record.small_group_threshold == SMALL_GROUP_THRESHOLD == 10
    with pytest.raises(ResultContractError, match="small_group_threshold"):
        PermissionRecord(
            sources=record.sources,
            artifacts=record.artifacts,
            small_group_threshold=9,
        )


def test_permission_record_fallback_preserves_written_terms_but_marks_default_origin():
    direct = SourceRedistribution("allowed", "allowed")
    artifact = ArtifactLineage("results/value.json", ("fixture-source",), ArtifactScope.AGGREGATE)
    record = permission_record(
        [artifact], {}, fallback_terms={"fixture-source": direct}, allow_unknown_aggregates=True
    )

    assert record.sources["fixture-source"].terms == direct
    assert record.sources["fixture-source"].terms_origin is PermissionTermsOrigin.DEFAULT_UNKNOWN
    assert record.sources["fixture-source"].evidence == ()
    assert record.artifacts[0].written_disposition.value == "included"


def test_permission_record_rejects_inconsistent_terms_or_dispositions():
    record = permission_record(
        [ArtifactLineage("results/value.json", ("hud/pit",), ArtifactScope.AGGREGATE)],
        resolve_permission_sources(("hud/pit",)),
    ).to_dict()
    wrong_disposition = {
        **record,
        "artifacts": [{**record["artifacts"][0], "strict_disposition": "withheld"}],
    }
    with pytest.raises(ResultContractError, match="strict_disposition"):
        PermissionRecord.from_dict(wrong_disposition)

    missing_source = {**record, "sources": {}}
    with pytest.raises(ResultContractError, match="exactly cover"):
        PermissionRecord.from_dict(missing_source)


def test_permission_record_revalidates_written_override_disposition():
    record = permission_record(
        [ArtifactLineage("results/value.json", ("zori",), ArtifactScope.AGGREGATE)],
        resolve_permission_sources(("zori",)),
        allow_unknown_aggregates=True,
    ).to_dict()
    artifact = record["artifacts"][0]
    assert artifact["strict_disposition"] == "withheld"
    assert artifact["written_disposition"] == "included"

    wrong_disposition = {
        **record,
        "artifacts": [{**artifact, "written_disposition": "withheld"}],
    }
    with pytest.raises(ResultContractError, match="written_disposition"):
        PermissionRecord.from_dict(wrong_disposition)


@pytest.mark.parametrize("seed", PERMISSION_PROPERTY_SEEDS)
def test_permission_record_dispositions_equal_the_policy_for_generated_source_terms(seed):
    generator = Random(seed)
    statuses = list(RedistributionStatus)
    terms = {
        name: SourceRedistribution(generator.choice(statuses), generator.choice(statuses))
        for name in ("source-a", "source-b")
    }
    scope = generator.choice(list(ArtifactScope))
    private = generator.choice((False, True))
    override = generator.choice((False, True))
    lineage = ArtifactLineage("results/data.json", tuple(terms), scope)

    record = permission_record(
        [lineage], {}, private=private, allow_unknown_aggregates=override, fallback_terms=terms
    )

    artifact, = PermissionRecord.from_dict(record.to_dict()).artifacts
    combined = combine_source_terms(terms)
    assert artifact.combined_terms == combined
    assert artifact.strict_disposition == artifact_disposition(combined, scope=scope)
    assert artifact.written_disposition == artifact_disposition(
        combined, scope=scope, private=private, allow_unknown_aggregates=override
    )

@pytest.mark.parametrize(
    "path",
    [
        "../escape.csv",
        "/absolute.csv",
        ".",
        "a//b.csv",
        "a/./b.csv",
        "a\\b.csv",
        "C:/drive.csv",
    ],
)
def test_permission_artifact_rejects_noncanonical_paths(path):
    with pytest.raises(ResultContractError, match="path"):
        PermissionArtifact(
            path=path,
            scope=ArtifactScope.AGGREGATE,
            sources=("hud/pit",),
            combined_terms=PUBLIC_DATA_TERMS,
            strict_disposition="included",
            written_disposition="included",
        )
