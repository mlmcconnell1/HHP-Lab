"""Truth table for exact finding-band predicates and evaluation."""

from __future__ import annotations

from decimal import Decimal
from fractions import Fraction

import pytest

from hhplab.results.bands import (
    BandValidationError,
    evaluate_bands,
    validate_band_exclusivity,
)
from hhplab.schema.results import FindingRecord

INCONCLUSIVE_BANDS = [
    {"label": "negative", "ci_below": {"value": -1}},
    {"label": "positive", "ci_above": {"value": 0}},
]
INCONCLUSIVE_CRITERIA = {"ci_lo": -2, "ci_hi": 1}
EQUIVALENCE_BAND = [{"label": "equivalent", "equivalent_to": {"value": 0, "margin": 0.5}}]
NARROW_LEFT = [
    {
        "label": "left",
        "stat_gte": {"stat": "fit", "value": 1.0},
        "stat_lte": {"stat": "fit", "value": 1.0000000005},
    }
]
NARROW_RIGHT = [
    {
        "label": "right",
        "stat_gte": {"stat": "fit", "value": 1.00000000025},
        "stat_lte": {"stat": "fit", "value": 1.00000000075},
    }
]


@pytest.mark.parametrize(
    ("bands", "criteria", "status", "compatible"),
    [
        pytest.param(
            INCONCLUSIVE_BANDS,
            INCONCLUSIVE_CRITERIA,
            "inconclusive",
            [-1, 0],
            id="minus-two-to-one-remains-inconclusive",
        ),
        pytest.param(
            EQUIVALENCE_BAND,
            {"ci_lo": -0.25, "ci_hi": 0.25},
            "equivalent",
            [0],
            id="equivalence-margin-contains-reference-value",
        ),
        pytest.param(
            EQUIVALENCE_BAND,
            {"ci_lo": -0.5, "ci_hi": 0.5},
            "equivalent",
            [0],
            id="both-equivalence-endpoints-are-closed",
        ),
        pytest.param(
            EQUIVALENCE_BAND,
            {"ci_lo": 0.5, "ci_hi": 1},
            "inconclusive",
            [],
            id="equivalence-margin-excludes-open-outside",
        ),
    ],
)
def test_evaluate_bands_truth_table(
    bands: list[dict[str, object]],
    criteria: dict[str, float | int],
    status: str,
    compatible: list[int],
) -> None:
    result = evaluate_bands(bands, criteria, reference_values=[-1, 0])

    assert result["status"] == status
    assert result["compatible_with"] == compatible
    assert result["reason"] is None
    assert all(record["reason"] is None for record in result["criteria"].values())


def test_decimal_strict_boundary_is_exact() -> None:
    bands = [{"label": "above", "ci_above": {"value": Decimal("0.1")}}]

    at_boundary = evaluate_bands(
        bands,
        {"ci_lo": Decimal("0.1"), "ci_hi": Decimal("0.2")},
    )
    above_boundary = evaluate_bands(
        bands,
        {"ci_lo": Decimal("0.1000000000000000001"), "ci_hi": Decimal("0.2")},
    )

    assert at_boundary["status"] == "inconclusive"
    assert above_boundary["status"] == "above"


@pytest.mark.parametrize(
    ("bands", "expected_overlap"),
    [
        pytest.param(
            [
                {"label": "below-one", "ci_below": {"value": 1}},
                {"label": "point-one", "equivalent_to": {"value": 1, "margin": 0}},
            ],
            False,
            id="touching-open-boundary-does-not-overlap",
        ),
        pytest.param(
            [
                {"label": "fit-at-least-one", "stat_gte": {"stat": "fit", "value": 1}},
                {"label": "fit-at-most-one", "stat_lte": {"stat": "fit", "value": 1}},
            ],
            True,
            id="closed-endpoint-overlap",
        ),
        pytest.param(
            [
                {"label": "low", "ci_below": {"value": 0}},
                {"label": "high", "ci_above": {"value": 0}},
            ],
            False,
            id="incompatible-ordering-ci-lo-le-ci-hi",
        ),
    ],
)
def test_exact_exclusivity_boundaries(
    bands: list[dict[str, object]], expected_overlap: bool
) -> None:
    overlaps = validate_band_exclusivity(bands)

    assert bool(overlaps) is expected_overlap
    if overlaps:
        _assert_witness_matches_both(overlaps[0], bands)


def test_overlap_narrower_than_one_billionth_is_found_and_witnessed() -> None:
    bands = [NARROW_LEFT[0], NARROW_RIGHT[0]]

    overlaps = validate_band_exclusivity(bands)

    assert len(overlaps) == 1
    witness = overlaps[0].witness
    assert Fraction(witness["stat:fit"]) == Fraction(8_000_000_003, 8_000_000_000)
    _assert_witness_matches_both(overlaps[0], bands)


@pytest.mark.parametrize(
    "bands",
    [
        pytest.param(
            [{"label": "impossible", "ci_above": {"value": 1}, "ci_below": {"value": 1}}],
            id="contradictory-band",
        ),
        pytest.param(
            [{"label": "negative-margin", "equivalent_to": {"value": 0, "margin": -1}}],
            id="negative-margin",
        ),
        pytest.param(
            [{"label": "inconclusive", "ci_above": {"value": 0}}],
            id="reserved-inconclusive-label",
        ),
        pytest.param(
            [{"label": "ambiguous_rule", "ci_above": {"value": 0}}],
            id="reserved-ambiguous-label",
        ),
        pytest.param(
            [{"label": "not_evaluated", "ci_above": {"value": 0}}],
            id="reserved-not-evaluated-label",
        ),
    ],
)
def test_invalid_band_specs_are_rejected(bands: list[dict[str, object]]) -> None:
    with pytest.raises(BandValidationError):
        validate_band_exclusivity(bands)


@pytest.mark.parametrize(
    ("criteria", "value_reason"),
    [
        pytest.param({"ci_hi": 2}, "missing_criterion:ci_lo", id="missing-endpoint"),
        pytest.param(
            {"ci_lo": float("nan"), "ci_hi": 2},
            "nonfinite_criterion:ci_lo",
            id="nan-endpoint",
        ),
        pytest.param(
            {"ci_lo": Decimal("NaN"), "ci_hi": 2},
            "nonfinite_criterion:ci_lo",
            id="decimal-nan-endpoint",
        ),
        pytest.param(
            {"ci_lo": False, "ci_hi": 2},
            "invalid_criterion:ci_lo",
            id="boolean-endpoint",
        ),
    ],
)
def test_undefined_criteria_keep_null_value_and_reason(
    criteria: dict[str, object], value_reason: str
) -> None:
    result = evaluate_bands(
        [{"label": "positive", "ci_above": {"value": 0}}],
        criteria,
    )

    assert result["status"] == "not_evaluated"
    assert result["reason"] == value_reason
    assert result["criteria"]["ci_lo"] == {"value": None, "reason": value_reason}


def test_otherwise_cannot_create_an_undeclared_decisive_label() -> None:
    with pytest.raises(BandValidationError, match="otherwise must be 'inconclusive'"):
        evaluate_bands(
            [{"label": "positive", "ci_above": {"value": 0}}],
            {"ci_lo": -1, "ci_hi": 1},
            otherwise="custom-decisive",
        )


def test_invalid_ci_order_is_not_evaluated_with_serialized_criteria() -> None:
    result = evaluate_bands(
        [{"label": "positive", "ci_above": {"value": 0}}],
        {"ci_lo": 2, "ci_hi": 1},
    )

    assert result["status"] == "not_evaluated"
    assert result["reason"] == "invalid_interval:ci_lo_gt_ci_hi"
    assert result["criteria"] == {
        "ci_hi": {"value": 1, "reason": None},
        "ci_lo": {"value": 2, "reason": None},
    }


def test_evaluation_record_matches_closed_finding_contract() -> None:
    evaluation = evaluate_bands(
        [{"label": "positive", "ci_above": {"value": 0}}],
        {"ci_lo": float("inf"), "ci_hi": 2},
    )
    record = FindingRecord.from_dict(
        {
            "id": "positive-effect",
            "base": evaluation,
            "variants": {},
            "robustness_status": "not_evaluated",
        }
    )

    assert record.base["criteria"]["ci_lo"] == {
        "value": None,
        "reason": "nonfinite_criterion:ci_lo",
    }


def _assert_witness_matches_both(overlap: object, bands: list[dict[str, object]]) -> None:
    witness = overlap.witness
    ci_lo = Fraction(witness["ci_lo"])
    ci_hi = Fraction(witness["ci_hi"])
    assert ci_lo <= ci_hi
    criteria = {"ci_lo": ci_lo, "ci_hi": ci_hi}
    criteria.update(
        {
            name.removeprefix("stat:"): Fraction(value)
            for name, value in witness.items()
            if name.startswith("stat:")
        }
    )
    for band in bands:
        result = evaluate_bands([band], criteria)
        assert result["status"] == band["label"]
