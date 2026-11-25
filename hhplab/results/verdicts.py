"""Resolve recipe finding records into publication-safe public verdicts."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from hhplab.schema.publication import (
    HeadlineVerdict,
    PublicVerdict,
    SafeUnavailableReason,
)
from hhplab.schema.results import FindingRecord, FindingStatus, RobustnessStatus


class VerdictResolutionCode(StrEnum):
    """Stable machine-readable failures for publication preconditions."""

    DECLARATION_MISMATCH = "declaration_mismatch"
    INVALID_BANDS = "invalid_bands"
    MISSING_PUBLIC_VERDICT = "missing_public_verdict"
    INVALID_PUBLIC_VERDICT = "invalid_public_verdict"
    INVALID_EQUIVALENCE_RULE = "invalid_no_meaningful_effect_rule"
    BASE_AMBIGUOUS = "base_ambiguous"
    BASE_NOT_EVALUATED = "base_not_evaluated"
    VARIANT_SET_MISMATCH = "variant_set_mismatch"
    VARIANT_DESCRIPTION_MISSING = "variant_description_missing"
    VARIANT_AMBIGUOUS = "variant_ambiguous"
    ROBUSTNESS_MISMATCH = "robustness_status_mismatch"


class VerdictResolutionError(ValueError):
    """A safe, structured reason the cited finding cannot be published."""

    def __init__(
        self,
        code: VerdictResolutionCode,
        *,
        finding_id: str,
        variant_id: str | None = None,
    ) -> None:
        self.code = code
        self.finding_id = finding_id
        self.variant_id = variant_id
        subject = f"variant {variant_id!r}" if variant_id is not None else "finding"
        super().__init__(f"{subject} {finding_id!r} cannot be published: {code.value}")


@dataclass(frozen=True)
class PublishedEvaluation:
    """Public representation of one stored base or variant evaluation."""

    evaluation_status: str
    band_label: str | None
    verdict: PublicVerdict | None
    verdict_text: str | None
    compatible_with: tuple[int | float, ...]
    compatible_with_text: str | None


@dataclass(frozen=True)
class PublishedVariant:
    """One declared robustness check, including safe unavailable state."""

    id: str
    description: str
    evaluation: PublishedEvaluation
    agrees: bool | None
    unavailable_reason: SafeUnavailableReason | None


@dataclass(frozen=True)
class PublishedFinding:
    """Resolved headline, base evaluation, and ordered robustness checks."""

    id: str
    claim: str
    base: PublishedEvaluation
    headline_verdict: HeadlineVerdict
    headline_text: str
    robustness_status: RobustnessStatus
    robustness_text: str
    variants: tuple[PublishedVariant, ...]
    disagreeing_variant_ids: tuple[str, ...]

    @property
    def base_verdict(self) -> PublicVerdict:
        """The unadjusted verdict that is shown second for mixed headlines."""
        assert self.base.verdict is not None
        return self.base.verdict


_BAND_VERDICT_VALUES = frozenset(
    {
        "supports_claim",
        "partly_supports_claim",
        "no_meaningful_effect",
        "contradicts_claim",
    }
)
_DEFAULT_VERDICT_TEXT: Mapping[PublicVerdict, str] = {
    PublicVerdict.SUPPORTS_CLAIM: "The evidence supports this.",
    PublicVerdict.PARTLY_SUPPORTS_CLAIM: "The evidence partly supports this.",
    PublicVerdict.NO_MEANINGFUL_EFFECT: (
        "The range of effects supported by the data lies within the study's equivalence margin."
    ),
    PublicVerdict.CONTRADICTS_CLAIM: "The evidence points the other way.",
    PublicVerdict.INCONCLUSIVE: "The data can't settle this.",
}


def resolve_public_finding(
    declaration: Mapping[str, Any],
    record: FindingRecord,
    *,
    variant_descriptions: Mapping[str, str],
    resolved_parameters: Mapping[str, int | float],
) -> PublishedFinding:
    """Map one declared finding and its exact bundle record to public text.

    Stored evaluation reasons are never copied to the return value. Only closed,
    generic ``SafeUnavailableReason`` values can describe an unevaluated variant.
    """
    if not isinstance(declaration, Mapping) or not isinstance(record, FindingRecord):
        raise TypeError("declaration must be a mapping and record must be FindingRecord")
    finding_id = declaration.get("id")
    if not isinstance(finding_id, str) or not finding_id.strip() or finding_id != record.id:
        raise VerdictResolutionError(
            VerdictResolutionCode.DECLARATION_MISMATCH,
            finding_id=str(finding_id or record.id),
        )
    claim = declaration.get("claim")
    if not isinstance(claim, str) or not claim.strip():
        raise VerdictResolutionError(
            VerdictResolutionCode.DECLARATION_MISMATCH, finding_id=finding_id
        )

    bands = _validated_bands(
        declaration,
        finding_id=finding_id,
        resolved_parameters=resolved_parameters,
    )
    references = _validated_references(declaration, finding_id=finding_id)
    base = _resolve_evaluation(
        record.base,
        bands,
        references,
        finding_id=finding_id,
        variant_id=None,
    )

    declared_variants = _declared_variants(declaration, finding_id=finding_id)
    record_variant_ids = set(record.variants)
    if record_variant_ids != set(declared_variants):
        raise VerdictResolutionError(
            VerdictResolutionCode.VARIANT_SET_MISMATCH, finding_id=finding_id
        )

    resolved_variants: list[PublishedVariant] = []
    for variant_id in declared_variants:
        description = variant_descriptions.get(variant_id)
        if (
            not isinstance(description, str)
            or not description.strip()
            or "\n" in description
            or "\r" in description
        ):
            raise VerdictResolutionError(
                VerdictResolutionCode.VARIANT_DESCRIPTION_MISSING,
                finding_id=finding_id,
                variant_id=variant_id,
            )
        raw_evaluation = record.variants[variant_id]
        status = raw_evaluation.get("status")
        if status == FindingStatus.AMBIGUOUS_RULE.value:
            raise VerdictResolutionError(
                VerdictResolutionCode.VARIANT_AMBIGUOUS,
                finding_id=finding_id,
                variant_id=variant_id,
            )
        evaluation = _resolve_evaluation(
            raw_evaluation,
            bands,
            references,
            finding_id=finding_id,
            variant_id=variant_id,
        )
        if status == FindingStatus.NOT_EVALUATED.value:
            resolved_variants.append(
                PublishedVariant(
                    id=variant_id,
                    description=description,
                    evaluation=evaluation,
                    agrees=None,
                    unavailable_reason=_safe_unavailable_reason(raw_evaluation),
                )
            )
        else:
            resolved_variants.append(
                PublishedVariant(
                    id=variant_id,
                    description=description,
                    evaluation=evaluation,
                    agrees=evaluation.evaluation_status == base.evaluation_status,
                    unavailable_reason=None,
                )
            )

    robustness_status = record.robustness_status
    _validate_robustness_status(
        robustness_status,
        base=base,
        variants=resolved_variants,
        finding_id=finding_id,
    )
    disagreement_ids = tuple(item.id for item in resolved_variants if item.agrees is False)
    headline_verdict = (
        HeadlineVerdict.MIXED
        if robustness_status is RobustnessStatus.FRAGILE
        else HeadlineVerdict(base.verdict.value)  # type: ignore[union-attr]
    )
    headline_text = (
        "The alternative analyses give mixed results."
        if headline_verdict is HeadlineVerdict.MIXED
        else base.verdict_text
    )
    return PublishedFinding(
        id=finding_id,
        claim=claim,
        base=base,
        headline_verdict=headline_verdict,
        headline_text=headline_text,
        robustness_status=robustness_status,
        robustness_text=_robustness_text(robustness_status, resolved_variants),
        variants=tuple(resolved_variants),
        disagreeing_variant_ids=disagreement_ids,
    )


def _validated_bands(
    declaration: Mapping[str, Any],
    *,
    finding_id: str,
    resolved_parameters: Mapping[str, int | float],
) -> dict[str, PublicVerdict]:
    raw_bands = declaration.get("bands")
    if not isinstance(raw_bands, Sequence) or isinstance(raw_bands, (str, bytes)) or not raw_bands:
        raise VerdictResolutionError(VerdictResolutionCode.INVALID_BANDS, finding_id=finding_id)
    verdicts: dict[str, PublicVerdict] = {}
    for raw_band in raw_bands:
        if not isinstance(raw_band, Mapping):
            raise VerdictResolutionError(VerdictResolutionCode.INVALID_BANDS, finding_id=finding_id)
        label = raw_band.get("label")
        if not isinstance(label, str) or not label.strip() or label in verdicts:
            raise VerdictResolutionError(VerdictResolutionCode.INVALID_BANDS, finding_id=finding_id)
        if "public_verdict" not in raw_band or raw_band["public_verdict"] is None:
            raise VerdictResolutionError(
                VerdictResolutionCode.MISSING_PUBLIC_VERDICT, finding_id=finding_id
            )
        raw_verdict = raw_band["public_verdict"]
        if not isinstance(raw_verdict, str) or raw_verdict not in _BAND_VERDICT_VALUES:
            raise VerdictResolutionError(
                VerdictResolutionCode.INVALID_PUBLIC_VERDICT, finding_id=finding_id
            )
        try:
            verdict = PublicVerdict(raw_verdict)
        except ValueError as exc:
            raise VerdictResolutionError(
                VerdictResolutionCode.INVALID_PUBLIC_VERDICT, finding_id=finding_id
            ) from exc
        if verdict is PublicVerdict.NO_MEANINGFUL_EFFECT and not _is_zero_equivalence(
            raw_band, resolved_parameters
        ):
            raise VerdictResolutionError(
                VerdictResolutionCode.INVALID_EQUIVALENCE_RULE, finding_id=finding_id
            )
        verdicts[label] = verdict
    return verdicts


def _is_zero_equivalence(
    band: Mapping[str, Any], resolved_parameters: Mapping[str, int | float]
) -> bool:
    condition = band.get("equivalent_to")
    if not isinstance(condition, Mapping) or set(condition) - {"value", "margin"}:
        return False
    if "value" not in condition or "margin" not in condition:
        return False
    value = _resolve_number(condition["value"], resolved_parameters)
    margin = _resolve_number(condition["margin"], resolved_parameters)
    return value == 0 and margin is not None and margin > 0


def _resolve_number(
    value: Any, parameters: Mapping[str, int | float]
) -> int | float | None:
    if isinstance(value, Mapping):
        if set(value) != {"param"} or not isinstance(value.get("param"), str):
            return None
        value = parameters.get(value["param"])
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not _is_finite(value):
        return None
    return value


def _is_finite(value: int | float) -> bool:
    return isinstance(value, int) or math.isfinite(value)


def _resolution_error(
    code: VerdictResolutionCode,
    finding_id: str,
    variant_id: str | None = None,
) -> VerdictResolutionError:
    return VerdictResolutionError(code, finding_id=finding_id, variant_id=variant_id)


def _validated_references(
    declaration: Mapping[str, Any], *, finding_id: str
) -> tuple[int | float, ...]:
    values = declaration.get("reference_values", ())
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        raise _resolution_error(VerdictResolutionCode.DECLARATION_MISMATCH, finding_id)
    references: list[int | float] = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not _is_finite(value):
            raise _resolution_error(VerdictResolutionCode.DECLARATION_MISMATCH, finding_id)
        references.append(value)
    return tuple(references)


def _declared_variants(
    declaration: Mapping[str, Any], *, finding_id: str
) -> tuple[str, ...]:
    robustness = declaration.get("robustness")
    if robustness is None:
        return ()
    if not isinstance(robustness, Mapping):
        raise _resolution_error(VerdictResolutionCode.DECLARATION_MISMATCH, finding_id)
    variants = robustness.get("variants")
    if not isinstance(variants, Sequence) or isinstance(variants, (str, bytes)):
        raise _resolution_error(VerdictResolutionCode.DECLARATION_MISMATCH, finding_id)
    if any(not isinstance(value, str) or not value.strip() for value in variants):
        raise _resolution_error(VerdictResolutionCode.DECLARATION_MISMATCH, finding_id)
    if len(variants) != len(set(variants)):
        raise _resolution_error(VerdictResolutionCode.DECLARATION_MISMATCH, finding_id)
    return tuple(variants)


def _resolve_evaluation(
    evaluation: Mapping[str, Any],
    band_verdicts: Mapping[str, PublicVerdict],
    references: tuple[int | float, ...],
    *,
    finding_id: str,
    variant_id: str | None,
) -> PublishedEvaluation:
    status = evaluation.get("status")
    label = evaluation.get("label")
    compatible = evaluation.get("compatible_with")
    context_variant = variant_id is not None
    if (
        not isinstance(status, str)
        or not isinstance(compatible, Sequence)
        or isinstance(compatible, (str, bytes))
    ):
        raise _resolution_error(
            VerdictResolutionCode.DECLARATION_MISMATCH, finding_id, variant_id
        )
    compatible_values: list[int | float] = []
    for value in compatible:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not _is_finite(value):
            raise _resolution_error(
                VerdictResolutionCode.DECLARATION_MISMATCH, finding_id, variant_id
            )
        if value not in references:
            raise _resolution_error(
                VerdictResolutionCode.DECLARATION_MISMATCH, finding_id, variant_id
            )
        compatible_values.append(value)

    if status == FindingStatus.AMBIGUOUS_RULE.value:
        code = (
            VerdictResolutionCode.VARIANT_AMBIGUOUS
            if context_variant
            else VerdictResolutionCode.BASE_AMBIGUOUS
        )
        raise _resolution_error(code, finding_id, variant_id)
    if status == FindingStatus.NOT_EVALUATED.value:
        if not context_variant:
            raise _resolution_error(VerdictResolutionCode.BASE_NOT_EVALUATED, finding_id)
        if label is not None:
            raise _resolution_error(
                VerdictResolutionCode.DECLARATION_MISMATCH, finding_id, variant_id
            )
        return PublishedEvaluation(
            evaluation_status=status,
            band_label=None,
            verdict=None,
            verdict_text=None,
            compatible_with=tuple(compatible_values),
            compatible_with_text=None,
        )

    if status == FindingStatus.INCONCLUSIVE.value:
        if label != FindingStatus.INCONCLUSIVE.value:
            raise _resolution_error(
                VerdictResolutionCode.DECLARATION_MISMATCH, finding_id, variant_id
            )
        compatible_text = _compatible_with_text(compatible_values, references)
        return PublishedEvaluation(
            evaluation_status=status,
            band_label=None,
            verdict=PublicVerdict.INCONCLUSIVE,
            verdict_text=_DEFAULT_VERDICT_TEXT[PublicVerdict.INCONCLUSIVE],
            compatible_with=tuple(compatible_values),
            compatible_with_text=compatible_text,
        )

    if not isinstance(label, str) or status != label or label not in band_verdicts:
        raise _resolution_error(
            VerdictResolutionCode.DECLARATION_MISMATCH, finding_id, variant_id
        )
    verdict = band_verdicts[label]
    return PublishedEvaluation(
        evaluation_status=status,
        band_label=label,
        verdict=verdict,
        verdict_text=_DEFAULT_VERDICT_TEXT[verdict],
        compatible_with=tuple(compatible_values),
        compatible_with_text=None,
    )


def _compatible_with_text(
    compatible_values: Sequence[int | float], references: Sequence[int | float]
) -> str:
    if not references:
        return "No reference values were declared."
    if not compatible_values:
        return "The interval contains none of the declared reference values."
    rendered = ", ".join(_format_number(value) for value in compatible_values)
    return f"The data are consistent with the declared reference values ({rendered})."


def _format_number(value: int | float) -> str:
    if isinstance(value, int):
        return str(value)
    return format(value, ".15g")


def _safe_unavailable_reason(evaluation: Mapping[str, Any]) -> SafeUnavailableReason:
    reason = evaluation.get("reason")
    if isinstance(reason, str):
        if reason.startswith("analysis_failed:"):
            return SafeUnavailableReason.ANALYSIS_DID_NOT_COMPLETE
        if reason.startswith("missing_result:"):
            return SafeUnavailableReason.ANALYSIS_NOT_AVAILABLE
        if reason.startswith(("missing_criterion:", "missing_statistic:", "undefined_")):
            return SafeUnavailableReason.REQUIRED_VALUE_NOT_AVAILABLE
    return SafeUnavailableReason.EVALUATION_NOT_AVAILABLE


def _validate_robustness_status(
    status: RobustnessStatus,
    *,
    base: PublishedEvaluation,
    variants: Sequence[PublishedVariant],
    finding_id: str,
) -> None:
    if status is RobustnessStatus.NOT_EVALUATED:
        raise _resolution_error(VerdictResolutionCode.BASE_NOT_EVALUATED, finding_id)
    if not variants:
        if status is not RobustnessStatus.UNINFORMATIVE:
            raise _resolution_error(VerdictResolutionCode.ROBUSTNESS_MISMATCH, finding_id)
        return
    if base.evaluation_status == FindingStatus.INCONCLUSIVE.value:
        if status is not RobustnessStatus.UNINFORMATIVE:
            raise _resolution_error(VerdictResolutionCode.ROBUSTNESS_MISMATCH, finding_id)
        return
    has_unavailable = any(item.agrees is None for item in variants)
    has_disagreement = any(item.agrees is False for item in variants)
    expected = (
        RobustnessStatus.INCOMPLETE
        if has_unavailable
        else RobustnessStatus.FRAGILE
        if has_disagreement
        else RobustnessStatus.ROBUST
    )
    if status is not expected:
        raise _resolution_error(VerdictResolutionCode.ROBUSTNESS_MISMATCH, finding_id)


def _robustness_text(
    status: RobustnessStatus, variants: Sequence[PublishedVariant]
) -> str:
    if not variants:
        return "No robustness checks were specified."
    if status is RobustnessStatus.ROBUST:
        return f"Holds up across {len(variants)} alternative analyses."
    if status is RobustnessStatus.FRAGILE:
        return "The alternative checks disagree with the base finding."
    if status is RobustnessStatus.UNINFORMATIVE:
        return "The checks were too noisy to confirm or contradict this."
    if status is RobustnessStatus.INCOMPLETE:
        reasons = tuple(dict.fromkeys(
            item.unavailable_reason.value
            for item in variants
            if item.unavailable_reason is not None
        ))
        if not reasons:
            reasons = (SafeUnavailableReason.EVALUATION_NOT_AVAILABLE.value,)
        return "Some checks could not be run: " + " ".join(reasons)
    return "Robustness status was not available."


__all__ = [
    "HeadlineVerdict",
    "PublicVerdict",
    "PublishedEvaluation",
    "PublishedFinding",
    "PublishedVariant",
    "SafeUnavailableReason",
    "VerdictResolutionCode",
    "VerdictResolutionError",
    "resolve_public_finding",
]
