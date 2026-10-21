"""Truth table for site-universe CoC boundary evidence validation.

The fixture has two frozen CoCs in one MSA over two years. PIT and HIC use
separate source boundary vintages. A missing row is explicitly unknown; a
stable identifier is never accepted as compatibility evidence by itself.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from hhplab.geographies.coc.coc_validate import (
    BoundaryCompatibilityError,
    validate_coc_boundary_compatibility,
)

ARTIFACT_ID = "overlap_samples"
THRESHOLD = 0.95
MEMBERSHIP_IDENTITY = "sha256:membership-fixture"
FROZEN_BOUNDARY_VINTAGE = "2024"
FROZEN_MEMBERSHIPS = (("MSA-A", "CO-1"), ("MSA-A", "CO-2"))
REQUESTED_YEARS = (2020, 2021)
SOURCE_PANEL_VINTAGES = {
    ("pit_panel", 2020): "2020",
    ("pit_panel", 2021): "2021",
    ("hic_panel", 2020): "2020",
    ("hic_panel", 2021): "2021",
}

COMPATIBLE_PIT_CO1_2020 = {
    "source_panel": "pit_panel",
    "frozen_coc_id": "CO-1",
    "year": 2020,
    "frozen_boundary_vintage": "2024",
    "source_boundary_vintage": "2020",
    "status": "compatible",
    "reason": "Archived boundary comparison confirms equivalent coverage.",
    "evidence_ref": "audit://pit/CO-1/2020",
}
MAPPED_PIT_CO2_2020 = {
    "source_panel": "pit_panel",
    "frozen_coc_id": "CO-2",
    "year": 2020,
    "frozen_boundary_vintage": "2024",
    "source_boundary_vintage": "2020",
    "status": "mapped",
    "reason": "Documented one-to-one CoC rename.",
    "evidence_ref": "audit://pit/CO-2/2020",
    "source_coc_id": "CO-2-OLD",
    "approval_ref": "approval://boundary-map/2020-CO-2",
}
INCOMPATIBLE_PIT_CO2_2021 = {
    "source_panel": "pit_panel",
    "frozen_coc_id": "CO-2",
    "year": 2021,
    "frozen_boundary_vintage": "2024",
    "source_boundary_vintage": "2021",
    "status": "incompatible",
    "reason": "The historical CoC boundary has a documented material change.",
    "evidence_ref": "audit://pit/CO-2/2021",
}
COMPATIBLE_HIC_CO1_2020 = {
    **COMPATIBLE_PIT_CO1_2020,
    "source_panel": "hic_panel",
    "source_boundary_vintage": "2020",
    "evidence_ref": "audit://hic/CO-1/2020",
}
MAPPED_HIC_CO2_2020 = {
    **MAPPED_PIT_CO2_2020,
    "source_panel": "hic_panel",
    "evidence_ref": "audit://hic/CO-2/2020",
    "source_coc_id": "HIC-CO-2-OLD",
    "approval_ref": "approval://hic-boundary-map/2020-CO-2",
}
COMPATIBLE_HIC_CO1_2021 = {
    **COMPATIBLE_PIT_CO1_2020,
    "source_panel": "hic_panel",
    "year": 2021,
    "source_boundary_vintage": "2021",
    "evidence_ref": "audit://hic/CO-1/2021",
}
VALID_RECORDS = (
    COMPATIBLE_PIT_CO1_2020,
    MAPPED_PIT_CO2_2020,
    {
        **COMPATIBLE_PIT_CO1_2020,
        "year": 2021,
        "source_boundary_vintage": "2021",
        "evidence_ref": "audit://pit/CO-1/2021",
    },
    INCOMPATIBLE_PIT_CO2_2021,
    COMPATIBLE_HIC_CO1_2020,
    MAPPED_HIC_CO2_2020,
    COMPATIBLE_HIC_CO1_2021,
)


def _evidence_payload(records=VALID_RECORDS):
    return {
        "schema_version": "coc_boundary_compatibility/1",
        "evidence_id": "boundary-audit-2026-01",
        "artifact_id": ARTIFACT_ID,
        "threshold": THRESHOLD,
        "membership_identity": MEMBERSHIP_IDENTITY,
        "frozen_boundary_vintage": FROZEN_BOUNDARY_VINTAGE,
        "records": deepcopy(list(records)),
    }


def _validate(payload=None, **overrides):
    arguments = {
        "artifact_id": ARTIFACT_ID,
        "threshold": THRESHOLD,
        "membership_identity": MEMBERSHIP_IDENTITY,
        "frozen_boundary_vintage": FROZEN_BOUNDARY_VINTAGE,
        "frozen_memberships": FROZEN_MEMBERSHIPS,
        "requested_years": REQUESTED_YEARS,
        "source_panel_vintages": SOURCE_PANEL_VINTAGES,
    }
    arguments.update(overrides)
    return validate_coc_boundary_compatibility(
        _evidence_payload() if payload is None else payload,
        **arguments,
    )


def test_valid_evidence_resolves_per_panel_and_emits_unknown_for_missing_rows():
    result = _validate()

    assert result.evidence_id == "boundary-audit-2026-01"
    assert result.artifact_id == ARTIFACT_ID
    assert result.threshold == THRESHOLD
    assert result.membership_identity == MEMBERSHIP_IDENTITY
    assert result.frozen_boundary_vintage == FROZEN_BOUNDARY_VINTAGE
    assert result.evidence_fingerprint.startswith("sha256:")
    assert len(result.records) == 8

    pit_rename = next(
        row for row in result.records
        if (row.source_panel, row.frozen_coc_id, row.year) == ("pit_panel", "CO-2", 2020)
    )
    assert (pit_rename.msa_id, pit_rename.status, pit_rename.source_coc_id) == (
        "MSA-A", "mapped", "CO-2-OLD"
    )
    hic_rename = next(
        row for row in result.records
        if (row.source_panel, row.frozen_coc_id, row.year) == ("hic_panel", "CO-2", 2020)
    )
    assert hic_rename.source_coc_id == "HIC-CO-2-OLD"

    missing = next(
        row for row in result.records
        if (row.source_panel, row.frozen_coc_id, row.year) == ("hic_panel", "CO-2", 2021)
    )
    assert missing.status == "unknown"
    assert missing.source_coc_id is None
    assert "No boundary compatibility evidence" in missing.reason


def test_integer_frozen_vintage_and_optional_evidence_fields_are_supported():
    result = _validate(frozen_boundary_vintage=2024)

    assert result.frozen_boundary_vintage == "2024"
    assert next(row for row in result.records if row.status == "compatible").source_coc_id == "CO-1"


def test_explicit_unknown_record_can_omit_optional_evidence_fields():
    payload = _evidence_payload()
    unknown_record = payload["records"].pop(0)
    unknown_record["status"] = "unknown"
    unknown_record["reason"] = "Historical boundary evidence is unavailable."
    unknown_record.pop("evidence_ref")
    payload["records"].append(unknown_record)

    result = _validate(payload)

    unknown = next(
        row for row in result.records
        if (row.source_panel, row.frozen_coc_id, row.year) == ("pit_panel", "CO-1", 2020)
    )
    assert unknown.status == "unknown"
    assert unknown.evidence_ref is None


def test_evidence_fingerprint_changes_when_evidence_changes():
    original = _validate()
    payload = _evidence_payload()
    payload["records"][0]["reason"] = "A different documented boundary comparison."

    changed = _validate(payload)

    assert changed.evidence_fingerprint != original.evidence_fingerprint


@pytest.mark.parametrize(
    ("field", "value", "error_code"),
    [
        pytest.param(
            "schema_version", "coc_boundary_compatibility/99", "unknown_schema", id="unknown-schema"
        ),
        pytest.param(
            "artifact_id", "another-artifact", "artifact_mismatch", id="producer-artifact-mismatch"
        ),
        pytest.param("threshold", 0.90, "threshold_mismatch", id="threshold-mismatch"),
        pytest.param(
            "membership_identity",
            "sha256:other-membership",
            "membership_mismatch",
            id="membership-changed",
        ),
        pytest.param(
            "frozen_boundary_vintage",
            "2020",
            "frozen_vintage_mismatch",
            id="producer-vintage-mismatch",
        ),
    ],
)
def test_header_identity_mismatches_are_rejected(field, value, error_code):
    payload = _evidence_payload()
    payload[field] = value

    with pytest.raises(BoundaryCompatibilityError) as error:
        _validate(payload)

    assert error.value.code == error_code


@pytest.mark.parametrize(
    ("change", "error_code"),
    [
        pytest.param("duplicate", "duplicate_record", id="duplicate-panel-member-year"),
        pytest.param(
            "wrong-source-vintage", "source_vintage_mismatch", id="source-vintage-mismatch"
        ),
        pytest.param(
            "wrong-frozen-vintage", "frozen_vintage_mismatch", id="record-frozen-vintage-mismatch"
        ),
        pytest.param("wrong-panel", "record_outside_universe", id="unknown-source-panel"),
        pytest.param("mapped-many-to-one", "non_bijective_mapping", id="mapped-many-to-one"),
        pytest.param("mapped-missing-approval", "incomplete_mapping", id="mapped-missing-approval"),
        pytest.param(
            "compatible-no-evidence",
            "missing_evidence_reference",
            id="stable-id-without-evidence",
        ),
    ],
)
def test_record_evidence_failures_are_actionable(change, error_code):
    payload = _evidence_payload()
    if change == "duplicate":
        payload["records"].append(deepcopy(COMPATIBLE_PIT_CO1_2020))
    elif change == "wrong-source-vintage":
        payload["records"][0]["source_boundary_vintage"] = "2019"
    elif change == "wrong-frozen-vintage":
        payload["records"][0]["frozen_boundary_vintage"] = "2020"
    elif change == "wrong-panel":
        payload["records"][0]["source_panel"] = "other_panel"
    elif change == "mapped-many-to-one":
        payload["records"][1]["source_coc_id"] = "CO-1"
    elif change == "mapped-missing-approval":
        payload["records"][1]["approval_ref"] = None
    elif change == "compatible-no-evidence":
        payload["records"][0]["evidence_ref"] = None

    with pytest.raises(BoundaryCompatibilityError) as error:
        _validate(payload)

    assert error.value.code == error_code


@pytest.mark.parametrize(
    ("memberships", "years", "panel_vintages", "error_code"),
    [
        pytest.param(
            (("MSA-A", "CO-1"), ("MSA-B", "CO-1")),
            REQUESTED_YEARS,
            SOURCE_PANEL_VINTAGES,
            "shared_coc_membership",
            id="shared-coc-rejected",
        ),
        pytest.param(
            FROZEN_MEMBERSHIPS,
            (),
            SOURCE_PANEL_VINTAGES,
            "empty_years",
            id="empty-requested-years",
        ),
        pytest.param(
            FROZEN_MEMBERSHIPS,
            REQUESTED_YEARS,
            {("pit_panel", 2020): "2020"},
            "missing_source_vintage",
            id="source-vintage-required-for-each-year",
        ),
    ],
)
def test_membership_and_source_vintage_contracts_are_enforced(
    memberships, years, panel_vintages, error_code
):
    with pytest.raises(BoundaryCompatibilityError) as error:
        _validate(
            frozen_memberships=memberships,
            requested_years=years,
            source_panel_vintages=panel_vintages,
        )

    assert error.value.code == error_code
