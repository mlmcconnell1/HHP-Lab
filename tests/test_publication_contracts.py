"""Strict source/page/provenance contracts and publication verdict truth table.

Verdict fixture truth table:

* base ``claim`` + variant ``claim`` -> same band, agrees true, robust;
* base ``claim`` + variant ``near`` -> different bands, agrees false even when
  both bands map to ``supports_claim``, fragile/mixed;
* an unavailable variant has no verdict or agreement and only fixed safe text;
* inconclusive compatibility prose preserves exactly the declared numeric
  references and never invents their substantive meaning.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError

from hhplab.results.verdicts import (
    HeadlineVerdict,
    PublicVerdict,
    SafeUnavailableReason,
    VerdictResolutionCode,
    VerdictResolutionError,
    resolve_public_finding,
)
from hhplab.schema.publication import (
    BandVerdict,
    PublicationHistoryEntry,
    PublicationIndexEntry,
    PublicationPage,
    PublicationProvenance,
    PublicationSource,
)
from hhplab.schema.results import FindingRecord, RobustnessStatus

SHA256 = "a" * 64
PUBLISHED_AT = "2026-11-24T20:00:00Z"
BUNDLE_REFERENCE = {
    "recipe_id": "sample-recipe",
    "run_id": "run-001",
    "manifest_sha256": SHA256,
    "public_url": "https://example.test/bundles/run-001.zip",
    "doi": None,
}
ACTIVE_SOURCE = {
    "publication_format_version": 1,
    "slug": "sample-finding",
    "title": "Does the program change the outcome?",
    "question": "What happened after implementation?",
    "status": "active",
    "superseded_by": None,
    "retraction_note": None,
    "tags": ["housing", "evaluation"],
    "geographies": "Participating metro areas",
    "bundle": BUNDLE_REFERENCE,
    "findings": ["finding-a"],
    "primary_finding": "finding-a",
    "variant_descriptions": {"alt-model": "Uses a different model specification."},
    "figures": [{"kind": "estimate_plot", "finding": "finding-a"}],
    "change_note": None,
}
ACTIVE_PAGE = {
    "publication_format_version": 1,
    "slug": "sample-finding",
    "title": "Does the program change the outcome?",
    "question": "What happened after implementation?",
    "headline_verdict": "supports_claim",
    "base_verdict": "supports_claim",
    "robustness_status": "robust",
    "headline_text": "The evidence supports this.",
    "status": "active",
    "superseded_by": None,
    "retraction_note": None,
    "change_note": None,
    "tags": ["housing"],
    "geographies": "Participating metro areas",
    "published_at": PUBLISHED_AT,
    "lock_status": {"status": "matched"},
    "provenance_tier": "recipe_bundle",
    "reproducibility": "pass",
    "execution_status": "complete",
    "diagnostics_status": "pass",
    "findings_status": "all_evaluated",
    "distribution_status": "full",
    "findings": [
        {
            "id": "finding-a",
            "claim": "The program improved the outcome.",
            "base_verdict": "supports_claim",
            "compatible_with": None,
            "variants": [],
        }
    ],
    "limitations": [],
    "history": [],
}
BANDS = [
    {"label": "claim", "public_verdict": "supports_claim", "ci_excludes": 0},
    {"label": "near", "public_verdict": "supports_claim", "ci_excludes": 1},
    {
        "label": "equivalent",
        "public_verdict": "no_meaningful_effect",
        "equivalent_to": {"value": 0, "margin": {"param": "delta"}},
    },
    {"label": "opposite", "public_verdict": "contradicts_claim", "ci_excludes": 0},
]
DECLARATION = {
    "id": "finding-a",
    "claim": "The program improved the outcome.",
    "reference_values": [-1, 0],
    "bands": BANDS,
    "otherwise": "inconclusive",
}


def _evaluation(
    status: str,
    *,
    label: str | None = None,
    compatible_with: list[int | float] | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "label": (
            status
            if label is None and status not in {"not_evaluated", "ambiguous_rule"}
            else label
        ),
        "compatible_with": [] if compatible_with is None else compatible_with,
        "reason": reason,
        "criteria": {},
    }


def _record(
    base: dict[str, Any] | None = None,
    *,
    variants: dict[str, dict[str, Any]] | None = None,
    robustness: RobustnessStatus = RobustnessStatus.UNINFORMATIVE,
) -> FindingRecord:
    return FindingRecord(
        id="finding-a",
        base=_evaluation("claim") if base is None else base,
        variants={} if variants is None else variants,
        robustness_status=robustness,
    )


def _resolve(
    declaration: dict[str, Any] | None = None,
    record: FindingRecord | None = None,
    *,
    descriptions: dict[str, str] | None = None,
    parameters: dict[str, int | float] | None = None,
):
    return resolve_public_finding(
        DECLARATION if declaration is None else declaration,
        _record() if record is None else record,
        variant_descriptions={"alt-model": "Uses an alternative model."}
        if descriptions is None else descriptions,
        resolved_parameters={"delta": 0.2} if parameters is None else parameters,
    )


@pytest.mark.parametrize(
    "value",
    [
        "supports_claim",
        "partly_supports_claim",
        "no_meaningful_effect",
        "contradicts_claim",
    ],
    ids=["supports", "partly-supports", "no-meaningful-effect", "contradicts"],
)
def test_band_verdict_alias_accepts_only_recipe_declarable_outcomes(value: str) -> None:
    assert TypeAdapter(BandVerdict).validate_python(value) == value


@pytest.mark.parametrize(
    "value",
    ["inconclusive", "mixed", "unknown"],
    ids=["inconclusive", "mixed", "unknown"],
)
def test_band_verdict_alias_rejects_evaluation_or_display_only_values(value: str) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(BandVerdict).validate_python(value)


def test_publication_source_accepts_active_and_tombstone_only_retraction() -> None:
    active = PublicationSource.model_validate(ACTIVE_SOURCE)
    assert active.findings == ["finding-a"]
    assert active.bundle is not None and active.bundle.manifest_sha256 == SHA256

    tombstone = PublicationSource.model_validate(
        {
            "publication_format_version": 1,
            "slug": "sample-finding",
            "title": "Does the program change the outcome?",
            "question": "What happened after implementation?",
            "status": "retracted",
            "retraction_note": "The underlying permission was withdrawn.",
        }
    )
    assert tombstone.bundle is None
    assert tombstone.findings == []


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("slug", "Upper-case", "slug"),
        ("slug", "../escape", "slug"),
        ("publication_format_version", 2, "publication_format_version"),
        ("bundle", {**BUNDLE_REFERENCE, "manifest_sha256": "abc"}, "manifest_sha256"),
        (
            "bundle",
            {**BUNDLE_REFERENCE, "public_url": "https://user:secret@example.test/x"},
            "credentials",
        ),
        ("bundle", {**BUNDLE_REFERENCE, "public_url": "file:///tmp/bundle.zip"}, r"HTTP\(S\)"),
        (
            "bundle",
            {**BUNDLE_REFERENCE, "public_url": "https://example.test/a b"},
            "whitespace",
        ),
        (
            "bundle",
            {**BUNDLE_REFERENCE, "public_url": "https://example.test/<bundle>"},
            "angle brackets",
        ),
        ("findings", ["finding-a", "finding-a"], "unique"),
        ("primary_finding", "uncited", "included in findings"),
        ("tags", ["housing", "housing"], "unique"),
        ("variant_descriptions", {"alt-model": "two\nlines"}, "one line"),
        ("figures", [{"kind": "binned_scatter", "finding": "finding-a"}], "kind"),
        ("status", "superseded", "superseded_by"),
    ],
    ids=[
        "slug-uppercase", "slug-traversal", "unsupported-version", "bad-manifest-digest",
        "url-credentials", "url-scheme", "url-whitespace", "url-angle-brackets",
        "duplicate-findings", "primary-not-cited",
        "duplicate-tags", "multiline-variant-description", "phase-b-figure", "missing-replacement",
    ],
)
def test_publication_source_rejects_invalid_schema_and_lifecycle(
    field: str, value: Any, match: str
) -> None:
    source = deepcopy(ACTIVE_SOURCE)
    source[field] = value
    with pytest.raises(ValidationError, match=match):
        PublicationSource.model_validate(source)


@pytest.mark.parametrize(
    ("source", "match"),
    [
        (
            {**ACTIVE_SOURCE, "status": "superseded", "superseded_by": "sample-finding"},
            "different publication slug",
        ),
        (
            {
                **ACTIVE_SOURCE,
                "status": "superseded",
                "superseded_by": "replacement",
                "retraction_note": "x",
            },
            "cannot set retraction_note",
        ),
        (
            {
                **ACTIVE_SOURCE,
                "status": "retracted",
                "superseded_by": "replacement",
                "retraction_note": "x",
            },
            "cannot set superseded_by",
        ),
        (
            {**ACTIVE_SOURCE, "status": "retracted", "retraction_note": None},
            "requires retraction_note",
        ),
        ({**ACTIVE_SOURCE, "bundle": None}, "requires a bundle reference"),
    ],
    ids=[
        "self-supersession",
        "superseded-with-retraction",
        "retracted-with-link",
        "empty-retraction",
        "active-without-bundle",
    ],
)
def test_publication_source_enforces_lifecycle_transitions(
    source: dict[str, Any], match: str
) -> None:
    with pytest.raises(ValidationError, match=match):
        PublicationSource.model_validate(source)


def test_publication_page_accepts_yaml_strings_and_canonical_page_links() -> None:
    page = PublicationPage.model_validate(ACTIVE_PAGE)
    assert page.headline_verdict is HeadlineVerdict.SUPPORTS_CLAIM
    assert page.base_verdict is PublicVerdict.SUPPORTS_CLAIM
    assert page.lock_status is not None and page.lock_status.status == "matched"

    superseded = {**ACTIVE_PAGE, "status": "superseded", "superseded_by": "replacement-finding"}
    assert PublicationPage.model_validate(superseded).superseded_by == "replacement-finding"


def test_publication_page_accepts_minimal_retraction_tombstone() -> None:
    tombstone = {
        "publication_format_version": 1,
        "slug": "sample-finding",
        "title": "Does the program change the outcome?",
        "question": "What happened after implementation?",
        "published_at": PUBLISHED_AT,
        "status": "retracted",
        "retraction_note": "Permission to disclose these results was withdrawn.",
        "history": [
            {
                "published_at": PUBLISHED_AT,
                "run_id": None,
                "manifest_sha256": None,
                "headline_verdict": None,
                "lock_status": None,
                "status": "retracted",
                "change_note": None,
            }
        ],
    }
    page = PublicationPage.model_validate(tombstone)
    assert page.headline_verdict is None
    assert page.history[0].run_id is None


def test_publication_page_rejects_result_detail_on_tombstone() -> None:
    tombstone = {
        "publication_format_version": 1,
        "slug": "sample-finding",
        "title": "A result",
        "question": "What happened?",
        "published_at": PUBLISHED_AT,
        "status": "retracted",
        "retraction_note": "Withdrawn.",
        "headline_verdict": "supports_claim",
        "history": [],
    }
    with pytest.raises(ValidationError, match="cannot expose result details"):
        PublicationPage.model_validate(tombstone)


@pytest.mark.parametrize(
    ("model", "payload"),
    [
        (PublicationSource, {**ACTIVE_SOURCE, "publication_format_version": True}),
        (PublicationPage, {**ACTIVE_PAGE, "publication_format_version": True}),
        (
            PublicationProvenance,
            {"publication_format_version": True, "asset_sha256": {"index.md": SHA256}},
        ),
    ],
    ids=["source-version-bool", "page-version-bool", "provenance-version-bool"],
)
def test_publication_format_version_rejects_boolean(model, payload) -> None:
    with pytest.raises(ValidationError, match="integer 1"):
        model.model_validate(payload)


def test_publication_history_and_index_support_safe_retraction_and_links() -> None:
    history = PublicationHistoryEntry.model_validate(
        {
            "published_at": PUBLISHED_AT,
            "run_id": None,
            "manifest_sha256": None,
            "headline_verdict": None,
            "lock_status": None,
            "status": "retracted",
            "change_note": None,
        }
    )
    assert history.manifest_sha256 is None
    entry = PublicationIndexEntry.model_validate(
        {
            "slug": "replacement-finding",
            "title": "Corrected result",
            "question": "What happened?",
            "headline_verdict": "supports_claim",
            "status": "active",
            "tags": [],
            "published_at": PUBLISHED_AT,
            "superseded_by": None,
            "supersedes": ["sample-finding"],
        }
    )
    assert entry.supersedes == ["sample-finding"]
    assert entry.headline_verdict is HeadlineVerdict.SUPPORTS_CLAIM
    superseded_entry = PublicationIndexEntry.model_validate(
        {
            "slug": "sample-finding",
            "title": "Original result",
            "question": "What happened?",
            "headline_verdict": "supports_claim",
            "status": "superseded",
            "tags": [],
            "published_at": PUBLISHED_AT,
            "superseded_by": "replacement-finding",
            "supersedes": [],
            "replacement_status": "retracted",
        }
    )
    assert superseded_entry.replacement_status == "retracted"


def test_publication_provenance_accepts_active_and_minimal_tombstone_forms() -> None:
    tombstone = PublicationProvenance.model_validate(
        {"publication_format_version": 1, "asset_sha256": {"index.md": SHA256}}
    )
    assert tombstone.bundle is None

    active = PublicationProvenance.model_validate(
        {
            "publication_format_version": 1,
            "bundle": {
                **BUNDLE_REFERENCE,
                "bundle_format_version": "1.1",
                "created_at": PUBLISHED_AT,
                "spec_hash": SHA256,
                "component_hashes": {"findings": SHA256},
                "lock_status": {"status": "matched"},
                "execution_status": "complete",
                "diagnostics_status": "pass",
                "findings_status": "all_evaluated",
                "distribution_status": "full",
            },
            "publish_tool_version": "0.1.0",
            "published_at": PUBLISHED_AT,
            "source_files": {"publications/sample-finding/publication.yaml": SHA256},
            "placeholders": [],
            "result_values": [],
            "artifacts": [],
            "permission_decisions": [],
            "reproducibility": {"status": "pass", "reasons": []},
            "asset_sha256": {"index.md": SHA256},
        }
    )
    assert active.bundle is not None and active.bundle.bundle_format_version == "1.1"


def test_publication_provenance_tombstone_rejects_old_result_metadata() -> None:
    with pytest.raises(ValidationError, match="only format version and asset_sha256"):
        PublicationProvenance.model_validate(
            {
                "publication_format_version": 1,
                "bundle": None,
                "asset_sha256": {"index.md": SHA256},
            }
        )


def test_resolver_maps_declared_band_and_preserves_lock_independent_base_text() -> None:
    result = _resolve()
    assert result.base_verdict is PublicVerdict.SUPPORTS_CLAIM
    assert result.headline_verdict is HeadlineVerdict.SUPPORTS_CLAIM
    assert result.base.verdict_text == "The evidence supports this."
    assert result.robustness_text == "No robustness checks were specified."


@pytest.mark.parametrize(
    ("band_edit", "code"),
    [
        (
            lambda bands: bands[0].pop("public_verdict"),
            VerdictResolutionCode.MISSING_PUBLIC_VERDICT,
        ),
        (
            lambda bands: bands[0].update(public_verdict="mixed"),
            VerdictResolutionCode.INVALID_PUBLIC_VERDICT,
        ),
        (
            lambda bands: bands[2].update(equivalent_to={"value": 1, "margin": 0.2}),
            VerdictResolutionCode.INVALID_EQUIVALENCE_RULE,
        ),
        (
            lambda bands: bands[2].update(equivalent_to={"value": 0, "margin": 0}),
            VerdictResolutionCode.INVALID_EQUIVALENCE_RULE,
        ),
    ],
    ids=[
        "missing-verdict",
        "closed-verdict-vocabulary",
        "equivalence-not-zero",
        "equivalence-margin-not-positive",
    ],
)
def test_resolver_rejects_invalid_band_public_verdicts(band_edit, code) -> None:
    declaration = deepcopy(DECLARATION)
    declaration["bands"] = deepcopy(BANDS)
    band_edit(declaration["bands"])
    with pytest.raises(VerdictResolutionError) as error:
        _resolve(declaration)
    assert error.value.code is code


def test_no_meaningful_effect_accepts_resolved_positive_parameter_margin() -> None:
    declaration = deepcopy(DECLARATION)
    declaration["bands"] = deepcopy(BANDS)
    declaration["bands"][2]["equivalent_to"] = {
        "value": {"param": "zero"},
        "margin": {"param": "delta"},
    }
    resolved = _resolve(declaration, parameters={"zero": 0, "delta": 0.2})
    assert resolved.base_verdict is PublicVerdict.SUPPORTS_CLAIM


def test_equivalence_margin_preserves_large_resolved_integer() -> None:
    declaration = deepcopy(DECLARATION)
    declaration["bands"] = deepcopy(BANDS)
    declaration["bands"][2]["equivalent_to"] = {
        "value": 0,
        "margin": {"param": "delta"},
    }
    resolved = _resolve(declaration, parameters={"delta": 10**1000})
    assert resolved.base_verdict is PublicVerdict.SUPPORTS_CLAIM


@pytest.mark.parametrize(
    ("status", "label", "code"),
    [
        ("ambiguous_rule", None, VerdictResolutionCode.BASE_AMBIGUOUS),
        ("not_evaluated", None, VerdictResolutionCode.BASE_NOT_EVALUATED),
    ],
    ids=["ambiguous-base", "not-evaluated-base"],
)
def test_resolver_refuses_ambiguous_or_unevaluated_base(
    status: str, label: str | None, code
) -> None:
    reason = "internal/path/and/raw-error" if status == "not_evaluated" else "multiple_bands_match"
    with pytest.raises(VerdictResolutionError) as error:
        _resolve(record=_record(_evaluation(status, label=label, reason=reason)))
    assert error.value.code is code
    assert "internal/path" not in str(error.value)


@pytest.mark.parametrize(
    ("references", "compatible", "expected"),
    [
        ([-1, 0], [0], "The data are consistent with the declared reference values (0)."),
        ([-1, 0], [], "The interval contains none of the declared reference values."),
        ([], [], "No reference values were declared."),
    ],
    ids=["some-compatible", "none-compatible", "no-declared-references"],
)
def test_inconclusive_text_never_infers_reference_meaning(references, compatible, expected) -> None:
    declaration = {**DECLARATION, "reference_values": references}
    base = _evaluation("inconclusive", compatible_with=compatible)
    result = _resolve(declaration, _record(base=base))
    assert result.base.verdict is PublicVerdict.INCONCLUSIVE
    assert result.base.compatible_with == tuple(compatible)
    assert result.base.compatible_with_text == expected


def test_inconclusive_reference_values_are_not_deduplicated_by_resolver() -> None:
    declaration = {**DECLARATION, "reference_values": [-1, -1, 0]}
    result = _resolve(
        declaration,
        _record(base=_evaluation("inconclusive", compatible_with=[0])),
    )
    assert result.base.compatible_with == (0,)


def test_fragile_compares_exact_band_labels_even_when_public_verdict_is_same() -> None:
    declaration = {**DECLARATION, "robustness": {"variants": ["alt-model"]}}
    record = _record(
        variants={"alt-model": _evaluation("near")},
        robustness=RobustnessStatus.FRAGILE,
    )
    result = _resolve(declaration, record)
    variant = result.variants[0]
    assert result.headline_verdict is HeadlineVerdict.MIXED
    assert result.base_verdict is PublicVerdict.SUPPORTS_CLAIM
    assert variant.evaluation.verdict is PublicVerdict.SUPPORTS_CLAIM
    assert variant.agrees is False
    assert result.disagreeing_variant_ids == ("alt-model",)


def test_robust_count_uses_declared_variants_in_declaration_order() -> None:
    declaration = {**DECLARATION, "robustness": {"variants": ["alt-b", "alt-a"]}}
    record = _record(
        variants={"alt-a": _evaluation("claim"), "alt-b": _evaluation("claim")},
        robustness=RobustnessStatus.ROBUST,
    )
    result = _resolve(
        declaration,
        record,
        descriptions={"alt-a": "Alternative A.", "alt-b": "Alternative B."},
    )
    assert [item.id for item in result.variants] == ["alt-b", "alt-a"]
    assert result.robustness_text == "Holds up across 2 alternative analyses."


def test_unavailable_variant_has_only_fixed_safe_reason_and_no_agreement() -> None:
    declaration = {**DECLARATION, "robustness": {"variants": ["alt-model"]}}
    raw_reason = "missing_result:/private/run/path:secret-error"
    record = _record(
        variants={"alt-model": _evaluation("not_evaluated", reason=raw_reason)},
        robustness=RobustnessStatus.INCOMPLETE,
    )
    result = _resolve(declaration, record)
    variant = result.variants[0]
    assert variant.evaluation.verdict is None
    assert variant.agrees is None
    assert variant.unavailable_reason is SafeUnavailableReason.ANALYSIS_NOT_AVAILABLE
    assert raw_reason not in repr(result)
    assert "/private/run/path" not in result.robustness_text


def test_ambiguous_variant_and_variant_set_mismatch_refuse_publication() -> None:
    declaration = {**DECLARATION, "robustness": {"variants": ["alt-model"]}}
    with pytest.raises(VerdictResolutionError) as error:
        _resolve(
            declaration,
            _record(
                variants={
                    "alt-model": _evaluation(
                        "ambiguous_rule", label=None, reason="ambiguous"
                    )
                },
                robustness=RobustnessStatus.INCOMPLETE,
            ),
        )
    assert error.value.code is VerdictResolutionCode.VARIANT_AMBIGUOUS

    with pytest.raises(VerdictResolutionError) as error:
        _resolve(declaration, _record())
    assert error.value.code is VerdictResolutionCode.VARIANT_SET_MISMATCH


def test_no_declared_variants_uses_no_check_text_even_if_uninformative() -> None:
    result = _resolve(record=_record(robustness=RobustnessStatus.UNINFORMATIVE))
    assert result.robustness_text == "No robustness checks were specified."
