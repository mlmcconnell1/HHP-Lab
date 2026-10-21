"""Redistribution truth table: restricted always withheld; unknown aggregate
requires override; unknown rows stay withheld; private includes everything.
Samples contain keys/reasons only and remain allowed.
"""

from datetime import UTC, datetime

import pytest

from hhplab.covariates.catalog import BLS_DATA_EVIDENCE, COVARIATE_SOURCE_SPECS
from hhplab.registry.source_registry import SourceRegistryEntry, register_source
from hhplab.results.lineage import (
    ArtifactLineage,
    declared_source_terms,
    derive_lineage,
    descriptive_scope,
    distribution_plan,
    union_lineage,
)
from hhplab.schema.redistribution import (
    PUBLIC_DATA_TERMS,
    UNKNOWN_TERMS,
    ArtifactScope,
    SourceRedistribution,
)
from hhplab.schema.results import ResultContractError
from hhplab.sources.coverage import CORE_SOURCE_COVERAGE_SPECS

RESTRICTED = SourceRedistribution("restricted", "restricted")


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
    assert declared_source_terms(("census/acs5", "census/unreviewed")) == {
        "census/acs5": PUBLIC_DATA_TERMS,
        "census/unreviewed": UNKNOWN_TERMS,
    }
